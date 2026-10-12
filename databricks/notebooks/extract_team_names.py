# Databricks notebook: extract_team_names (ONE CELL)
# PURPOSE: Discover identities from BRONZE before Silver fails on unknown clubs.
# NEXT STEPS: If none found, run Silver. Otherwise review CSV in the export folder,
# prepare the TWO additions CSVs described below, upload, run loader, rerun Silver.
from functools import reduce
from pyspark.sql import functions as F

CATALOG = 'football_data_platform'
BRONZE = f'{CATALOG}.bronze'
SILVER = f'{CATALOG}.silver'
EXPORT = '/Volumes/football_data_platform/raw/source_files/team_standardization_review'
MAPPINGS = f'{SILVER}.team_mappings'
API_SOURCE = 'football-data.org'
FBREF_SOURCE = 'fbref'
CSV_SOURCE = 'football-data.co.uk'

print('=== TEAM DISCOVERY (Bronze -> review only; no reference-table writes) ===')
print('Run AFTER Bronze ingestion and BEFORE the Silver transformation notebooks.')
if not spark.catalog.tableExists(MAPPINGS):
    raise ValueError(f'Missing {MAPPINGS}. Initial reference-data setup is required.')

available = {t.name for t in spark.catalog.listTables(BRONZE) if not t.isTemporary}
api_names = ['football_data_org_teams', 'football_data_org_standings', 'football_data_org_matches']
missing = [n for n in api_names if n not in available]
if missing:
    raise ValueError(f'Required API Bronze tables missing: {missing}')
fbref_datasets = ('standard', 'shooting', 'goalkeeping', 'playing_time', 'miscellaneous', 'standings')
fbref_names = sorted(n for n in available if any(n.startswith(f'fbref_{dataset}_') for dataset in fbref_datasets))
csv_names = sorted(n for n in available if n.startswith('football_data_') and not n.startswith('football_data_org_'))
if not fbref_names or not csv_names:
    raise ValueError(f'Expected FBref and football-data.co.uk Bronze tables; found {len(fbref_names)} FBref and {len(csv_names)} football-data.co.uk tables.')

observations = []
def add_observation(table, system, name_path, id_path=None):
    df = spark.table(f'{BRONZE}.{table}')
    def has_path(path):
        if not path:
            return False
        dtype = df.schema
        from pyspark.sql.types import StructType
        for part in path.split('.'):
            if not isinstance(dtype, StructType) or part not in dtype.fieldNames():
                return False
            dtype = dtype[part].dataType
        return True
    if not has_path(name_path):
        raise ValueError(f'{table} is missing required team field: {name_path}')
    if id_path and not has_path(id_path):
        raise ValueError(f'{table} is missing required team ID: {id_path}')
    league = F.col('_league').cast('string') if has_path('_league') else F.lit(None).cast('string')
    identity = F.trim(F.col(id_path).cast('string')) if id_path else F.lit(None).cast('string')
    observations.append(
        df.select(
            F.lit(system).alias('source_system'),
            identity.alias('source_team_id'),
            F.trim(F.col(name_path).cast('string')).alias('source_team_name'),
            league.alias('league_code'),
            F.lit(table).alias('source_table'),
        ).filter(F.col('source_team_name').isNotNull() & (F.col('source_team_name') != ''))
    )

add_observation('football_data_org_teams', API_SOURCE, 'name', 'id')
add_observation('football_data_org_standings', API_SOURCE, 'team.name', 'team.id')
add_observation('football_data_org_matches', API_SOURCE, 'homeTeam.name', 'homeTeam.id')
add_observation('football_data_org_matches', API_SOURCE, 'awayTeam.name', 'awayTeam.id')
for table in fbref_names:
    add_observation(table, FBREF_SOURCE, 'Squad')
for table in csv_names:
    add_observation(table, CSV_SOURCE, 'HomeTeam')
    add_observation(table, CSV_SOURCE, 'AwayTeam')

all_df = reduce(lambda a, b: a.unionByName(b), observations)
# An API team ID may have multiple observed display names. Preserve them as context,
# but group its identity by ID, not by name.
all_df = all_df.withColumn(
    'identity_key',
    F.when(F.col('source_system') == API_SOURCE, F.col('source_team_id'))
     .otherwise(F.col('source_team_name')),
)
invalid = all_df.filter(F.col('identity_key').isNull() | (F.col('identity_key') == ''))
if invalid.limit(1).count():
    display(invalid.limit(20))
    raise ValueError('Bronze contains empty team identities; fix source parsing first.')

observed = (
    all_df.groupBy('source_system', 'identity_key')
    .agg(
        F.min('source_team_id').alias('source_team_id'),
        F.min('source_team_name').alias('source_team_name'),
        F.concat_ws('|', F.sort_array(F.collect_set('source_team_name'))).alias('observed_names'),
        F.concat_ws('|', F.sort_array(F.collect_set('league_code'))).alias('league_codes'),
        F.concat_ws('|', F.sort_array(F.collect_set('source_table'))).alias('source_tables'),
    )
)
existing = spark.table(MAPPINGS).select(
    'source_system',
    F.trim(F.col('source_team_id').cast('string')).alias('source_team_id'),
    F.trim(F.col('source_team_name')).alias('source_team_name'),
)
existing = existing.withColumn(
    'identity_key',
    F.when(F.col('source_system') == API_SOURCE, F.col('source_team_id'))
     .otherwise(F.col('source_team_name')),
)
existing_ids = existing.select('source_system', 'identity_key').distinct()
new_df = (observed.join(existing_ids, ['source_system', 'identity_key'], 'left_anti')
          .drop('identity_key')
          .orderBy('source_system', 'source_team_name'))

n = new_df.count()
print(f'Observed unique source identities: {observed.count()}')
print(f'New/unmapped identities: {n}')
if n == 0:
    print('NEXT: No onboarding needed. Run the Silver transformations normally.')
else:
    print('NEXT 1: Review the unknown identities displayed below.')
    print('NEXT 2: For a known club, use its EXACT canonical team_name from silver.canonical_teams.')
    print('NEXT 3: For a truly new club, add its canonical name to canonical_teams_additions.csv.')
    print('NEXT 4: For EACH unknown identity, add a reviewed row to team_mappings_additions.csv.')
    print('NEXT 5: Upload both additions CSV files to the review Volume; run load_team_mappings_to_silver.')
    print('NEXT 6: Once loader passes, rerun the Silver notebooks that failed.')
    display(new_df)

# Export contains ONLY unknown identities. Never edit this generated file in place.
# It is diagnostic input for human review, NOT the approved mapping file.
(new_df.coalesce(1).write.mode('overwrite').option('header', True)
 .csv(f'{EXPORT}/unmapped_source_teams'))
print(f'Unknown-identity export: {EXPORT}/unmapped_source_teams (part-*.csv)')
print('REVIEWED CSV CONTRACTS (UTF-8 with headers):')
print('  canonical_teams_additions.csv: team_name  [one row per genuinely new canonical club; optional/empty if none]')
print('  team_mappings_additions.csv: source_system,source_team_id,source_team_name,canonical_team_name')
print('    - source_team_id must be filled for football-data.org; blank for other providers')
print('    - canonical_team_name must exactly equal an existing canonical name or a newly approved name')
print('    - do not upload incomplete/guessed assignments; all supplied rows are treated as approved')
