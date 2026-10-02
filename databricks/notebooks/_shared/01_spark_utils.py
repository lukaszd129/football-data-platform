# Databricks notebook source
from functools import reduce
from typing import Any, Mapping, Sequence

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


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