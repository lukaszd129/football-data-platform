# Databricks notebook source
from functools import reduce
from typing import Any, Mapping, Sequence

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType


# ---------------------------------------------------------------------------
# Nested and optional column handling
# ---------------------------------------------------------------------------

def field_exists(
    df: DataFrame,
    column_path: str,
) -> bool:
    """
    Check whether a top-level or nested field exists in a DataFrame.

    Examples:
        field_exists(df, "id")
        field_exists(df, "team.name")
        field_exists(df, "score.fullTime.home")

    Returns False when any part of the path is missing
    or an intermediate field is not a struct.
    """

    if not column_path:
        raise ValueError("column_path cannot be empty.")

    current_type = df.schema

    for field_name in column_path.split("."):

        if not isinstance(current_type, StructType):
            return False

        field = next(
            (
                field
                for field in current_type.fields
                if field.name == field_name
            ),
            None,
        )

        if field is None:
            return False

        current_type = field.dataType

    return True


def optional_col(
    df: DataFrame,
    column_path: str,
    data_type: str = "string",
) -> Column:
    """
    Return a Spark column cast to the requested type.

    If the field does not exist, return a typed NULL.

    Examples:
        optional_col(df, "founded", "int")
        optional_col(df, "area.id", "int")
        optional_col(df, "lastUpdated", "timestamp")

    Intended for optional source attributes only.
    Required fields should be validated separately.
    """

    if field_exists(df, column_path):
        return F.col(column_path).cast(data_type)

    return F.lit(None).cast(data_type)


def optional_string(
    df: DataFrame,
    column_path: str,
) -> Column:
    """
    Return a cleaned string column.

    Trims whitespace and converts empty strings to NULL.
    Missing fields also return NULL.

    Examples:
        optional_string(df, "venue")
        optional_string(df, "team.shortName")
        optional_string(df, "area.name")
    """

    if field_exists(df, column_path):
        return clean_string(column_path)

    return F.lit(None).cast("string")

# ---------------------------------------------------------------------------
# Column validation
# ---------------------------------------------------------------------------

def validate_columns_exist(
    df: DataFrame,
    required_columns: Sequence[str],
) -> None:
    """Fail if any required columns are missing from the DataFrame."""

    if not required_columns:
        raise ValueError("required_columns cannot be empty.")

    missing_columns = [
        column_name
        for column_name in required_columns
        if column_name not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Required columns are missing: {missing_columns}"
        )


# ---------------------------------------------------------------------------
# String cleaning
# ---------------------------------------------------------------------------

def clean_top_level_strings(df: DataFrame) -> DataFrame:
    """
    Trim all top-level string columns and convert empty strings to NULL.

    Nested string fields inside structs are intentionally not modified.
    """

    expressions: list[Column] = []

    for column_name, data_type in df.dtypes:
        column = F.col(f"`{column_name}`")

        if data_type == "string":
            trimmed = F.trim(column)

            column = F.when(
                trimmed == "",
                F.lit(None),
            ).otherwise(trimmed)

        expressions.append(column.alias(column_name))

    return df.select(*expressions)


def clean_string(column_path: str) -> Column:
    """
    Return a trimmed string expression with empty strings converted to NULL.

    Useful for nested fields such as:
        clean_string("player.firstname")
        clean_string("team.name")
    """

    trimmed = F.trim(F.col(column_path))

    return F.when(
        trimmed == "",
        F.lit(None),
    ).otherwise(trimmed)


# ---------------------------------------------------------------------------
# Table reading / union
# ---------------------------------------------------------------------------

def read_and_union_tables(
    spark: SparkSession,
    table_names: Sequence[str],
    *,
    allow_missing_tables: bool = False,
    allow_missing_columns: bool = False,
) -> DataFrame:
    """
    Read Delta tables and union them by column name.

    By default:
      - all expected tables must exist
      - schemas must contain the same columns

    Missing tables or missing columns must therefore be explicitly allowed.
    """

    if not table_names:
        raise ValueError("table_names cannot be empty.")

    dataframes: list[DataFrame] = []
    missing_tables: list[str] = []

    for table_name in table_names:
        if not spark.catalog.tableExists(table_name):
            missing_tables.append(table_name)
            continue

        dataframes.append(spark.table(table_name))

    if missing_tables:
        if not allow_missing_tables:
            raise ValueError(
                f"Expected tables are missing: {missing_tables}"
            )

        for table_name in missing_tables:
            print(
                f"WARNING: Table does not exist and will be skipped: "
                f"{table_name}"
            )

    if not dataframes:
        raise ValueError("None of the expected tables were found.")

    return reduce(
        lambda left, right: left.unionByName(
            right,
            allowMissingColumns=allow_missing_columns,
        ),
        dataframes,
    )


# ---------------------------------------------------------------------------
# Duplicate handling
# ---------------------------------------------------------------------------

def duplicate_metrics(
    df: DataFrame,
    key_columns: Sequence[str],
) -> tuple[int, int]:
    """
    Return duplicate metrics for a business key.

    Returns:
      - number of duplicate key groups
      - number of extra rows that would be removed
    """

    if not key_columns:
        raise ValueError("key_columns cannot be empty.")

    validate_columns_exist(df, key_columns)

    duplicate_groups = (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
    )

    metrics = duplicate_groups.agg(
        F.count(F.lit(1)).alias("duplicate_groups"),
        F.coalesce(
            F.sum(F.col("count") - F.lit(1)),
            F.lit(0),
        ).alias("duplicate_rows"),
    ).first()

    return (
        int(metrics["duplicate_groups"] or 0),
        int(metrics["duplicate_rows"] or 0),
    )


def deduplicate_latest(
    df: DataFrame,
    key_columns: Sequence[str],
    order_column: str = "_bronze_ingested_at",
    *,
    tie_breaker_columns: Sequence[str] = (),
) -> DataFrame:
    """
    Remove duplicate business keys and retain the latest record.

    Rows are ordered by:
      1. order_column descending
      2. optional tie-breaker columns descending

    Business-key NULL validation should normally be performed before calling
    this function.
    """

    if not key_columns:
        raise ValueError("key_columns cannot be empty.")

    required_columns = [
        *key_columns,
        order_column,
        *tie_breaker_columns,
    ]

    validate_columns_exist(df, required_columns)

    duplicate_groups, duplicate_rows = duplicate_metrics(
        df,
        key_columns,
    )

    print(f"Duplicate groups found: {duplicate_groups}")
    print(f"Duplicate rows to remove: {duplicate_rows}")

    if duplicate_rows == 0:
        return df

    ordering = [
        F.col(order_column).desc_nulls_last(),
        *[
            F.col(column_name).desc_nulls_last()
            for column_name in tie_breaker_columns
        ],
    ]

    window = (
        Window
        .partitionBy(*key_columns)
        .orderBy(*ordering)
    )

    return (
        df
        .withColumn(
            "_dedup_rank",
            F.row_number().over(window),
        )
        .filter(F.col("_dedup_rank") == 1)
        .drop("_dedup_rank")
    )


# ---------------------------------------------------------------------------
# Data-quality validation
# ---------------------------------------------------------------------------

def validate_required_not_null(
    df: DataFrame,
    required_columns: Sequence[str],
    *,
    fail_on_null: bool = False,
) -> dict[str, int]:
    """
    Calculate row count and NULL counts using a single Spark action.

    Parameters
    ----------
    fail_on_null:
        False:
            print warnings but allow processing to continue.

        True:
            raise ValueError when required columns contain NULLs.
    """

    if not required_columns:
        raise ValueError("required_columns cannot be empty.")

    validate_columns_exist(df, required_columns)

    aggregations = [
        F.count(F.lit(1)).alias("_row_count")
    ]

    for column_name in required_columns:
        aggregations.append(
            F.sum(
                F.when(
                    F.col(column_name).isNull(),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias(column_name)
        )

    result = df.agg(*aggregations).first().asDict()

    row_count = int(result["_row_count"] or 0)

    null_counts = {
        column_name: int(result[column_name] or 0)
        for column_name in required_columns
    }

    print(f"Rows: {row_count}")

    invalid_columns = {
        column_name: null_count
        for column_name, null_count in null_counts.items()
        if null_count > 0
    }

    for column_name, null_count in invalid_columns.items():
        print(
            f"WARNING: NULL {column_name}: {null_count}"
        )

    if fail_on_null and invalid_columns:
        raise ValueError(
            "Required-column validation failed. "
            f"NULL counts: {invalid_columns}"
        )

    return {
        "row_count": row_count,
        **null_counts,
    }


# ---------------------------------------------------------------------------
# Entity resolution: canonical team identities
# ---------------------------------------------------------------------------

def load_team_mappings_by_id(
    spark: SparkSession,
    silver_schema: str,
    source_system: str,
) -> DataFrame:
    """
    Load validated mappings from source team IDs to canonical team keys.

    Intended for providers that expose stable team IDs,
    such as football-data.org.

    Validates:
      - reference tables exist
      - source IDs and team keys are non-null
      - source IDs are unique within the selected provider
      - canonical keys are unique
      - all mappings reference existing canonical teams

    Returns:
      source_team_id: string
      team_key: long
    """

    if not silver_schema or not source_system:
        raise ValueError(
            "silver_schema and source_system cannot be empty."
        )

    mappings_table = f"{silver_schema}.team_mappings"
    canonical_table = f"{silver_schema}.canonical_teams"

    for table_name in [mappings_table, canonical_table]:
        if not spark.catalog.tableExists(table_name):
            raise ValueError(
                f"Required reference table does not exist: {table_name}"
            )

    raw_mappings_df = spark.table(mappings_table)
    raw_canonical_df = spark.table(canonical_table)

    validate_columns_exist(
        raw_mappings_df,
        ["source_system", "source_team_id", "team_key"],
    )
    validate_columns_exist(
        raw_canonical_df,
        ["team_key"],
    )

    mappings_df = (
        raw_mappings_df
        .filter(F.col("source_system") == source_system)
        .select(
            F.trim(
                F.col("source_team_id").cast("string")
            ).alias("source_team_id"),
            F.col("team_key").cast("long").alias("team_key"),
        )
    )

    canonical_df = raw_canonical_df.select(
        F.col("team_key").cast("long").alias("team_key")
    )

    # Required reference values
    validate_required_not_null(
        mappings_df,
        ["source_team_id", "team_key"],
        fail_on_null=True,
    )

    validate_required_not_null(
        canonical_df,
        ["team_key"],
        fail_on_null=True,
    )

    if not mappings_df.limit(1).count():
        raise ValueError(
            f"No team mappings found for: {source_system}"
        )

    if mappings_df.filter(
        F.col("source_team_id") == ""
    ).limit(1).count():
        raise ValueError(
            "Empty source_team_id values found in team mappings."
        )

    # Source IDs must uniquely identify a canonical team.
    source_duplicates = (
        mappings_df
        .groupBy("source_team_id")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if source_duplicates:
        raise ValueError(
            f"Duplicate team mappings for source: {source_system}"
        )

    # Canonical team keys must be unique.
    canonical_duplicates = (
        canonical_df
        .groupBy("team_key")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if canonical_duplicates:
        raise ValueError(
            "Duplicate team_key values in canonical_teams."
        )

    # All mappings must reference existing canonical teams.
    invalid_keys_df = (
        mappings_df
        .select("team_key")
        .distinct()
        .join(
            canonical_df,
            on="team_key",
            how="left_anti",
        )
    )

    if invalid_keys_df.limit(1).count():
        raise ValueError(
            "Team mappings reference nonexistent canonical teams."
        )

    print(
        f"Validated team mappings for source: {source_system}"
    )

    return mappings_df


def resolve_team_keys_by_id(
    df: DataFrame,
    mappings_df: DataFrame,
    *,
    source_id_column: str,
    output_key_column: str,
    fail_on_unmapped: bool = True,
) -> DataFrame:
    """
    Resolve canonical team keys using source-specific team IDs.

    Parameters
    ----------
    df:
        Source DataFrame containing team IDs.

    mappings_df:
        Validated DataFrame returned by load_team_mappings_by_id().

    source_id_column:
        Name of the source team ID column, e.g. "home_team_id".

    output_key_column:
        Name of the resulting canonical key column,
        e.g. "home_team_key".

    fail_on_unmapped:
        When True, fail if any source team cannot be resolved.

    Returns
    -------
    DataFrame with an additional canonical team key column.

    Original source ID and name columns are preserved.
    """

    validate_columns_exist(df, [source_id_column])
    validate_columns_exist(
        mappings_df,
        ["source_team_id", "team_key"],
    )

    if output_key_column in df.columns:
        raise ValueError(
            f"Output column already exists: {output_key_column}"
        )

    # Use internal names to avoid collisions with source columns.
    lookup_id = "__team_mapping_source_id"
    lookup_key = "__team_mapping_canonical_key"

    if any(
        col in df.columns
        for col in [lookup_id, lookup_key]
    ):
        raise ValueError(
            "Source DataFrame contains reserved mapping columns."
        )

    lookup_df = (
        mappings_df
        .select(
            F.col("source_team_id").alias(lookup_id),
            F.col("team_key").alias(lookup_key),
        )
    )

    # Detect ambiguous mappings before joining.
    duplicate_ids = (
        lookup_df
        .groupBy(lookup_id)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicate_ids:
        raise ValueError(
            "Mapping lookup contains duplicate source team IDs."
        )

    resolved_df = (
        df
        .join(
            F.broadcast(lookup_df),
            F.trim(
                F.col(source_id_column).cast("string")
            ) == F.col(lookup_id),
            how="left",
        )
        .withColumn(
            output_key_column,
            F.col(lookup_key),
        )
        .drop(lookup_id, lookup_key)
    )

    # Do not silently publish unresolved team identities.
    if fail_on_unmapped:
        unmapped_df = (
            resolved_df
            .filter(F.col(output_key_column).isNull())
            .select(source_id_column)
            .distinct()
        )

        if unmapped_df.limit(1).count():
            examples = [
                row[source_id_column]
                for row in unmapped_df.limit(20).collect()
            ]

            raise ValueError(
                f"Unmapped teams for {source_id_column}. "
                f"Example source IDs: {examples}"
            )

    return resolved_df


# ---------------------------------------------------------------------------
# Entity resolution: team names
# ---------------------------------------------------------------------------

def load_team_mappings_by_name(
    spark: SparkSession,
    silver_schema: str,
    source_system: str,
) -> DataFrame:
    """
    Load validated source-name-to-canonical-team mappings.

    Intended for providers without stable team IDs in our datasets,
    such as FBref and football-data.co.uk.

    Matching uses exact source names after trimming whitespace.
    """

    if not silver_schema or not source_system:
        raise ValueError(
            "silver_schema and source_system cannot be empty."
        )

    mappings_table = f"{silver_schema}.team_mappings"
    canonical_table = f"{silver_schema}.canonical_teams"

    for table_name in [mappings_table, canonical_table]:
        if not spark.catalog.tableExists(table_name):
            raise ValueError(
                f"Required reference table does not exist: {table_name}"
            )

    raw_mappings_df = spark.table(mappings_table)
    raw_canonical_df = spark.table(canonical_table)

    validate_columns_exist(
        raw_mappings_df,
        ["source_system", "source_team_name", "team_key"],
    )
    validate_columns_exist(
        raw_canonical_df,
        ["team_key"],
    )

    mappings_df = (
        raw_mappings_df
        .filter(F.col("source_system") == source_system)
        .select(
            F.trim(F.col("source_team_name"))
            .alias("source_team_name"),
            F.col("team_key").cast("long")
            .alias("team_key"),
        )
    )

    canonical_df = raw_canonical_df.select(
        F.col("team_key").cast("long").alias("team_key")
    )

    # Required values must be present.
    validate_required_not_null(
        mappings_df,
        ["source_team_name", "team_key"],
        fail_on_null=True,
    )

    validate_required_not_null(
        canonical_df,
        ["team_key"],
        fail_on_null=True,
    )

    if not mappings_df.limit(1).count():
        raise ValueError(
            f"No team mappings found for source: {source_system}"
        )

    if mappings_df.filter(
        F.col("source_team_name") == ""
    ).limit(1).count():
        raise ValueError(
            f"Empty source team names found for: {source_system}"
        )

    # Trimmed names must be unique within the provider.
    duplicate_names = (
        mappings_df
        .groupBy("source_team_name")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicate_names:
        raise ValueError(
            f"Duplicate source team names found for: {source_system}"
        )

    # Canonical keys must be unique.
    duplicate_canonical_keys = (
        canonical_df
        .groupBy("team_key")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicate_canonical_keys:
        raise ValueError(
            "Duplicate team_key values in canonical_teams."
        )

    # Every mapping must point to an existing canonical team.
    invalid_keys = (
        mappings_df
        .select("team_key")
        .distinct()
        .join(canonical_df, on="team_key", how="left_anti")
        .limit(1)
        .count()
    )

    if invalid_keys:
        raise ValueError(
            "Team mappings reference nonexistent canonical teams."
        )

    print(
        f"Validated name-based team mappings for: {source_system}"
    )

    return mappings_df


def resolve_team_keys_by_name(
    df: DataFrame,
    mappings_df: DataFrame,
    *,
    source_name_column: str,
    output_key_column: str,
    fail_on_unmapped: bool = True,
) -> DataFrame:
    """
    Resolve a source team name to a canonical team_key.

    Preserves the original source name and all existing columns.

    Raises ValueError on missing mappings by default.
    """

    validate_columns_exist(df, [source_name_column])
    validate_columns_exist(
        mappings_df,
        ["source_team_name", "team_key"],
    )

    if output_key_column in df.columns:
        raise ValueError(
            f"Output column already exists: {output_key_column}"
        )

    lookup_name = "__team_mapping_source_name"
    lookup_key = "__team_mapping_canonical_key"

    if any(
        col in df.columns
        for col in [lookup_name, lookup_key]
    ):
        raise ValueError(
            "Source DataFrame contains reserved mapping columns."
        )

    lookup_df = mappings_df.select(
        F.col("source_team_name").alias(lookup_name),
        F.col("team_key").alias(lookup_key),
    )

    # Protect against one-to-many joins.
    duplicate_names = (
        lookup_df
        .groupBy(lookup_name)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicate_names:
        raise ValueError(
            "Mapping lookup contains duplicate source team names."
        )

    resolved_df = (
        df
        .join(
            F.broadcast(lookup_df),
            F.trim(F.col(source_name_column)) == F.col(lookup_name),
            how="left",
        )
        .withColumn(output_key_column, F.col(lookup_key))
        .drop(lookup_name, lookup_key)
    )

    if fail_on_unmapped:
        unmapped_df = (
            resolved_df
            .filter(F.col(output_key_column).isNull())
            .select(source_name_column)
            .distinct()
        )

        if unmapped_df.limit(1).count():
            examples = [
                row[source_name_column]
                for row in unmapped_df.limit(20).collect()
            ]

            raise ValueError(
                f"Unmapped teams for {source_name_column}. "
                f"Example names: {examples}"
            )

    return resolved_df


# ---------------------------------------------------------------------------
# Delta writing
# ---------------------------------------------------------------------------

def write_full_refresh(
    df: DataFrame,
    table_name: str,
) -> None:
    """
    Overwrite a Delta table.

    Intended for the current full-refresh Silver/Gold implementation.
    This should generally not be used for append-oriented Bronze ingestion.
    """

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

    print(f"Overwritten table: {table_name}")


# ---------------------------------------------------------------------------
# Domain transformations
# ---------------------------------------------------------------------------

def season_code_to_start_year(
    column_name: str,
) -> Column:
    """
    Normalize season representation to its starting year.

    Examples:
        '2122' -> 2021
        '2223' -> 2022
        '2425' -> 2024
        '2021' -> 2021

    Invalid values return NULL.
    """

    season = F.trim(
        F.col(column_name).cast("string")
    )

    return (
        F.when(
            season.rlike(r"^(19|20)\d{2}$"),
            season.cast("int"),
        )
        .when(
            season.rlike(r"^\d{4}$"),
            F.concat(
                F.lit("20"),
                F.substring(season, 1, 2),
            ).cast("int"),
        )
        .otherwise(F.lit(None).cast("int"))
    )


def map_values(
    column_name: str,
    mapping: Mapping[Any, Any],
    *,
    default_value: Any | None = None,
) -> Column:
    """
    Map values through a small Python dictionary using a Spark map expression.

    Values missing from the mapping return default_value.
    The default is NULL so unexpected source values remain visible to
    downstream data-quality checks.

    Example:
        map_values(
            "_league",
            {
                "pl": "Premier League",
                "ll": "La Liga",
                "bl": "Bundesliga",
            },
        )
    """

    if not mapping:
        raise ValueError("mapping cannot be empty.")

    mapping_items: list[Column] = []

    for key, value in mapping.items():
        mapping_items.extend(
            [
                F.lit(key),
                F.lit(value),
            ]
        )

    mapping_expression = F.create_map(*mapping_items)

    mapped_value = F.element_at(
        mapping_expression,
        F.col(column_name),
    )

    if default_value is None:
        return mapped_value

    return F.coalesce(
        mapped_value,
        F.lit(default_value),
    )


# ---------------------------------------------------------------------------
# Bronze source-file incremental loading
# ---------------------------------------------------------------------------
# Each source file is treated as a complete snapshot for one logical
# Bronze slice (for example: league + season).
#
# An unchanged source hash is skipped.
# A changed source replaces its slice atomically in Delta.
#
# Silver remains responsible for business transformations.
# ---------------------------------------------------------------------------

import hashlib
from pathlib import Path


def calculate_file_sha256(file_path: str) -> str:
    """Calculate a reproducible hash of the raw source file."""

    digest = hashlib.sha256()

    with open(file_path, "rb") as file:
        for chunk in iter(
            lambda: file.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()



def bronze_file_needs_processing(
    spark: SparkSession,
    table_name: str,
    replace_predicate: str,
    source_hash: str,
) -> bool:
    """
    Return True when a Bronze snapshot needs processing.

    Handles:
    - Bronze table does not exist
    - Legacy table missing incremental metadata columns
    - Existing slice contains no records
    - Existing slice contains a different source hash
    - Existing slice is unchanged
    """

    # First execution: target table doesn't exist.
    if not spark.catalog.tableExists(table_name):
        return True

    existing_df = spark.table(table_name)

    # Check that the columns used by the incremental-loading
    # strategy exist before evaluating the replacement predicate.
    #
    # These metadata columns are introduced by the new Bronze
    # implementation. Legacy tables may not contain them.
    if (
        "_raw_sha256" not in existing_df.columns
        or (
            "_season_id" in replace_predicate
            and "_season_id" not in existing_df.columns
        )
    ):
        return True

    # Check whether this particular source snapshot
    # has already been processed.
    existing_df = existing_df.where(
        F.expr(replace_predicate)
    )

    hashes = (
        existing_df
        .select("_raw_sha256")
        .distinct()
        .limit(2)
        .collect()
    )

    return not (
        len(hashes) == 1
        and hashes[0]["_raw_sha256"] == source_hash
    )



def write_bronze_file_snapshot(
    df: DataFrame,
    table_name: str,
    replace_predicate: str,
    source_hash: str,
) -> None:
    """
    Atomically load one complete source snapshot into Bronze.

    Existing rows satisfying replace_predicate are replaced.
    All other records are preserved.

    The DataFrame must contain the columns referenced in
    replace_predicate and must represent a complete source slice.
    """

    # Never replace existing Bronze records with an empty input.
    if df.limit(1).count() == 0:
        raise ValueError(
            f"Refusing empty Bronze snapshot: {table_name}"
        )

    bronze_df = df.withColumn(
        "_raw_sha256",
        F.lit(source_hash),
    )

    if not df.sparkSession.catalog.tableExists(table_name):

        (
            bronze_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(table_name)
        )

    else:

        (
            bronze_df.write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere", replace_predicate)
            .option("mergeSchema", "true")
            .saveAsTable(table_name)
        )
