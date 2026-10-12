# Databricks notebook: load_team_mappings_to_silver (ONE CELL)
# PURPOSE: Incrementally load HUMAN-APPROVED team assignments. Never overwrite stable team keys.
# INPUT: Two UTF-8 CSVs in the review Volume; see extract_team_names notebook.
# RERUN: Safe after a partial failure; inserts missing rows only, rejects reassignment.
import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

CATALOG = 'football_data_platform'
SILVER = f'{CATALOG}.silver'
REVIEW = '/Volumes/football_data_platform/raw/source_files/team_standardization_review'
CANONICAL_TABLE = f'{SILVER}.canonical_teams'
MAPPINGS_TABLE = f'{SILVER}.team_mappings'
CANONICAL_FILE = f'{REVIEW}/canonical_teams_additions.csv'
MAPPINGS_FILE = f'{REVIEW}/team_mappings_additions.csv'
API = 'football-data.org'
SOURCES = [API, 'fbref', 'football-data.co.uk']

print('=== APPROVED TEAM REFERENCE DATA: INCREMENTAL LOAD ===')
print('This cell writes to TWO existing Delta tables; run only after reviewing both input CSVs.')
for table in [CANONICAL_TABLE, MAPPINGS_TABLE]:
    if not spark.catalog.tableExists(table):
        raise ValueError(f'{table} does not exist. Run the initial setup once before incremental loading.')
if not os.path.isfile(MAPPINGS_FILE):
    print('NO APPROVED MAPPINGS FILE: nothing written.')
    print('NEXT: Run extract_team_names, review unmapped_source_teams, then upload the reviewed CSVs.')
    raise ValueError(f'Upload {MAPPINGS_FILE} before running this loader.')
if not os.path.isfile(CANONICAL_FILE):
    print('MISSING CANONICAL INPUT FILE: nothing written.')
    print('NEXT: Upload canonical_teams_additions.csv. For aliases of existing clubs, use a header-only file: team_name')
    raise ValueError(f'Missing {CANONICAL_FILE}')

new_canonical = spark.read.option('header', True).option('encoding', 'UTF-8').csv(CANONICAL_FILE)
incoming = spark.read.option('header', True).option('encoding', 'UTF-8').csv(MAPPINGS_FILE)
required_mapping = ['source_system', 'source_team_id', 'source_team_name', 'canonical_team_name']
if 'team_name' not in new_canonical.columns or any(c not in incoming.columns for c in required_mapping):
    raise ValueError('Bad CSV headers. canonical: team_name; mappings: ' + ','.join(required_mapping))

new_canonical = (new_canonical.select(F.trim('team_name').alias('team_name'))
                 .filter(F.col('team_name').isNotNull() & (F.col('team_name') != '')))
incoming = incoming.select(
    F.trim('source_system').alias('source_system'),
    F.trim(F.col('source_team_id').cast('string')).alias('source_team_id'),
    F.trim('source_team_name').alias('source_team_name'),
    F.trim('canonical_team_name').alias('canonical_team_name'),
)
if incoming.limit(1).count() == 0:
    print('No approved mapping rows. No data changed.')
    print('NEXT: If discovery has no unmapped identities, continue with Silver. Otherwise review and upload mappings.')
    raise ValueError('Empty team_mappings_additions.csv; no updates needed.')

# === PREWRITE VALIDATION; no Delta writes above this line ===
for c in ['source_system', 'source_team_name', 'canonical_team_name']:
    if incoming.filter(F.col(c).isNull() | (F.col(c) == '')).limit(1).count():
        raise ValueError(f'Empty values in required mapping field {c}. No data written.')
if incoming.filter(~F.col('source_system').isin(SOURCES)).limit(1).count():
    raise ValueError(f'Unknown source_system (expected {SOURCES}). No data written.')
if incoming.filter((F.col('source_system') == API) &
                   (F.col('source_team_id').isNull() | (F.col('source_team_id') == ''))).limit(1).count():
    raise ValueError('football-data.org requires source_team_id. No data written.')
if incoming.filter((F.col('source_system') != API) & F.col('source_team_id').isNotNull()).limit(1).count():
    raise ValueError('FBref and football-data.co.uk must have blank source_team_id. No data written.')
if incoming.filter((F.col('source_system') == API) &
                   ~F.col('source_team_id').rlike('^[0-9]+$')).limit(1).count():
    raise ValueError('football-data.org source_team_id must be a decimal integer. No data written.')
if new_canonical.groupBy('team_name').count().filter('count > 1').limit(1).count():
    raise ValueError('Duplicate proposed canonical team_name. No data written.')

identity = lambda df: df.withColumn(
    '_identity', F.when(F.col('source_system') == API, F.col('source_team_id'))
    .otherwise(F.col('source_team_name'))
)
incoming = identity(incoming)
if incoming.groupBy('source_system', '_identity').count().filter('count > 1').limit(1).count():
    raise ValueError('Duplicate source identity in approved additions CSV. No data written.')

canonical_current = spark.table(CANONICAL_TABLE).select(
    F.col('team_key').cast('long').alias('team_key'),
    F.col('team_name').alias('team_name')
)
if canonical_current.filter(F.col('team_key').isNull() | F.col('team_name').isNull()).limit(1).count():
    raise ValueError('Existing canonical table has null keys/names; no data written.')
for field in ['team_key', 'team_name']:
    if canonical_current.groupBy(field).count().filter('count > 1').limit(1).count():
        raise ValueError(f'Existing canonical table contains duplicate {field}; no data written.')

# All approved canonical names must exist already or be explicitly proposed as new.
needed_names = incoming.select(F.col('canonical_team_name').alias('team_name')).distinct()
known_names = canonical_current.select('team_name').unionByName(new_canonical).distinct()
unknown_names = needed_names.join(known_names, 'team_name', 'left_anti')
if unknown_names.limit(1).count():
    display(unknown_names)
    raise ValueError('Mappings refer to unknown canonical names. Add approved new clubs to canonical CSV.')

existing = identity(spark.table(MAPPINGS_TABLE).select(
    'source_system', F.col('source_team_id').cast('string').alias('source_team_id'),
    'source_team_name', F.col('team_key').cast('long').alias('team_key')))
if existing.groupBy('source_system', '_identity').count().filter('count > 1').limit(1).count():
    raise ValueError('Existing team_mappings contains ambiguous source identities.')
# Check conflicts BEFORE writing either Delta table. Replays of the same mapping are no-ops.
existing_named = existing.join(canonical_current, 'team_key', 'left')
conflicts = (incoming.alias('i').join(existing_named.alias('e'), ['source_system', '_identity'], 'inner')
             .filter(F.col('i.canonical_team_name') != F.col('e.team_name')))
if conflicts.limit(1).count():
    display(conflicts.select('source_system', '_identity', 'canonical_team_name', 'team_name'))
    raise ValueError('Attempt to reassign a previously mapped source identity. No data written.')

# Allocate new keys only for names that do not already exist. Deterministic per batch.
missing_canonical = new_canonical.join(canonical_current.select('team_name'), 'team_name', 'left_anti')
max_key = canonical_current.agg(F.coalesce(F.max('team_key'), F.lit(0)).alias('max_key')).first()['max_key']
add_canonical = (missing_canonical
                 .withColumn('team_key', F.row_number().over(Window.orderBy('team_name')) + F.lit(max_key))
                 .select('team_key', 'team_name'))
all_canonical = canonical_current.unionByName(add_canonical)
resolved = incoming.join(all_canonical.select(
    F.col('team_name').alias('canonical_team_name'), 'team_key'), 'canonical_team_name', 'inner')
new_mappings = resolved.join(existing.select('source_system', '_identity'),
                             ['source_system', '_identity'], 'left_anti')

# Validate the target contract before writing EITHER Delta table.
target_cols = spark.table(MAPPINGS_TABLE).columns
supported = {
    'source_system', 'source_team_id', 'source_team_name',
    'team_key', 'canonical_team_name', 'league_codes', 'source_tables',
    'match_method', 'review_status',
}
extra = set(target_cols) - supported
if extra:
    raise ValueError(f'Unsupported target metadata columns {extra}; add explicit defaults before insertion.')

print(f'Approved canonical names to insert: {add_canonical.count()}')
print(f'Approved source mappings to insert: {new_mappings.count()}')
print('Prewrite checks passed. Applying insert-only Delta MERGEs.')

# === WRITES; two Delta tables are not one transaction. Reruns recover partial success. ===
if add_canonical.limit(1).count():
    (DeltaTable.forName(spark, CANONICAL_TABLE).alias('t')
     .merge(add_canonical.alias('s'), 't.team_key = s.team_key OR t.team_name = s.team_name')
     .whenNotMatchedInsert(values={'team_key': 's.team_key', 'team_name': 's.team_name'})
     .execute())

# Existing mappings table has extra metadata columns from the original CSV.
# Set these for new approved rows; untouched existing rows retain their original metadata.
new_mappings = (new_mappings
    .withColumn('league_codes', F.lit(None).cast('string'))
    .withColumn('source_tables', F.lit(None).cast('string'))
    .withColumn('match_method', F.lit('manual_review'))
    .withColumn('review_status', F.lit('approved')))

# Only write fields that actually exist in the established target schema.
values = {c: f's.{c}' for c in target_cols}
merge_condition = '''t.source_system = s.source_system AND (
    (t.source_system = 'football-data.org' AND t.source_team_id = s.source_team_id)
    OR (t.source_system <> 'football-data.org' AND t.source_team_name = s.source_team_name)
)'''
if new_mappings.limit(1).count():
    (DeltaTable.forName(spark, MAPPINGS_TABLE).alias('t')
     .merge(new_mappings.select(*target_cols).alias('s'), merge_condition)
     .whenNotMatchedInsert(values=values)
     .execute())

print('SUCCESS: Approved changes loaded; existing team keys and mappings preserved.')
print('NEXT 1: Rerun extract_team_names. Expect zero unmapped identities for the current Bronze data.')
print('NEXT 2: If any remain, review those identities and load further approved additions.')
print('NEXT 3: Once discovery is clean, rerun the affected Silver transformations.')
print('NEXT 4: Archive/remove processed additions CSVs from the review folder to avoid future ambiguity.')
print('CAUTION: Do not run parallel reference-data onboarding jobs; key allocation assumes one writer.')
