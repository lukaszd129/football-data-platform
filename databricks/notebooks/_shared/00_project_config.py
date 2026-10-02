# Databricks notebook source
# ---------------------------------------------------------------------------
# Football Data Platform - Shared Project Configuration
#
# Contains project-wide identifiers, schemas, source mappings, paths,
# seasons, API configuration, and secret references.
#
# Transformation logic should not be placed in this notebook.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Catalog and schemas
# ---------------------------------------------------------------------------

CATALOG = "football_data_platform"

BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA = f"{CATALOG}.gold"


# ---------------------------------------------------------------------------
# Raw storage
# ---------------------------------------------------------------------------

RAW_ROOT = f"/Volumes/{CATALOG}/raw/source_files"

FOOTBALL_DATA_RAW_ROOT = f"{RAW_ROOT}/football_data"
FBREF_RAW_ROOT = f"{RAW_ROOT}/fbref"
API_FOOTBALL_RAW_ROOT = f"{RAW_ROOT}/api_football"


# ---------------------------------------------------------------------------
# Canonical league identifiers
# ---------------------------------------------------------------------------

LEAGUE_PREMIER_LEAGUE = "premier_league"
LEAGUE_BUNDESLIGA = "bundesliga"
LEAGUE_SERIE_A = "serie_a"
LEAGUE_LA_LIGA = "la_liga"
LEAGUE_LIGUE_ONE = "ligue_one"

SUPPORTED_LEAGUES = [
    LEAGUE_PREMIER_LEAGUE,
    LEAGUE_BUNDESLIGA,
    LEAGUE_SERIE_A,
    LEAGUE_LA_LIGA,
    LEAGUE_LIGUE_ONE,
]


# ---------------------------------------------------------------------------
# Seasons
#
# Canonical project representation uses the starting year.
#
# Examples:
#   2021 -> 2021/22
#   2025 -> 2025/26
# ---------------------------------------------------------------------------

SEASON_START_YEARS = [
    2021,
    2022,
    2023,
    2024,
    2025,
]


def _season_start_year_to_code(year: int) -> str:
    """
    Convert a season starting year into the compact source representation.

    Examples:
        2021 -> "2122"
        2025 -> "2526"
    """
    return f"{year % 100:02d}{(year + 1) % 100:02d}"


SEASON_CODES = [
    _season_start_year_to_code(year)
    for year in SEASON_START_YEARS
]


# ---------------------------------------------------------------------------
# football-data.co.uk
#
# Keys are football-data.co.uk league identifiers.
# Values are canonical platform league identifiers.
# ---------------------------------------------------------------------------

FOOTBALL_DATA_LEAGUES = {
    "E0": LEAGUE_PREMIER_LEAGUE,
    "D1": LEAGUE_BUNDESLIGA,
    "I1": LEAGUE_SERIE_A,
    "SP1": LEAGUE_LA_LIGA,
    "F1": LEAGUE_LIGUE_ONE,
}

FOOTBALL_DATA_SEASONS = SEASON_CODES


# ---------------------------------------------------------------------------
# FBref
#
# Keys are project/source FBref league identifiers.
# Values are canonical platform league identifiers.
# ---------------------------------------------------------------------------

FBREF_LEAGUES = {
    "bl": LEAGUE_BUNDESLIGA,
    "ll": LEAGUE_LA_LIGA,
    "lo": LEAGUE_LIGUE_ONE,
    "pl": LEAGUE_PREMIER_LEAGUE,
    "sa": LEAGUE_SERIE_A,
}

FBREF_SEASONS = SEASON_CODES

FBREF_DATASETS = [
    "goalkeeping_stats",
    "miscellaneous",
    "playing_time",
    "shooting_stats",
    "standard_stats",
    "standings",
]


# ---------------------------------------------------------------------------
# API-Football
#
# Keys are API-Football league IDs.
# Values are canonical platform league identifiers.
# ---------------------------------------------------------------------------

API_FOOTBALL_LEAGUES = {
    39: LEAGUE_PREMIER_LEAGUE,
    61: LEAGUE_LIGUE_ONE,
    78: LEAGUE_BUNDESLIGA,
    135: LEAGUE_SERIE_A,
    140: LEAGUE_LA_LIGA,
}

API_FOOTBALL_SEASONS = SEASON_START_YEARS

API_FOOTBALL_BASE_URL = "https://v3.football.api-sports.io"

# -----------------------------------------------------------------------------
# API-Football venue countries
# -----------------------------------------------------------------------------
# Countries used to retrieve venue reference data for the five supported
# leagues.

API_FOOTBALL_VENUE_COUNTRIES = [
    "England",
    "France",
    "Germany",
    "Italy",
    "Spain",
]

# ---------------------------------------------------------------------------
# API-Football secrets
#
# Store credentials in Databricks Secrets.
# Never hard-code API keys in notebooks or source control.
# ---------------------------------------------------------------------------

API_FOOTBALL_SECRET_SCOPE = "football-data-platform"
API_FOOTBALL_SECRET_KEY = "api-football-key"


# ---------------------------------------------------------------------------
# API-Football request limits
#
# The current free API-Football plan allows 100 requests per day.
#
# Keep the daily limit here so ingestion notebooks can estimate request cost
# before starting larger extraction jobs.
# ---------------------------------------------------------------------------

API_FOOTBALL_DAILY_REQUEST_LIMIT = 100

# Minimum delay between client-level requests.
# Keep aligned with the rate limits of the active API plan.
API_FOOTBALL_MIN_REQUEST_INTERVAL_SECONDS = 6.5

# Keep a small safety margin so ingestion does not intentionally consume
# every request available on the account.
API_FOOTBALL_DAILY_REQUEST_RESERVE = 5