# Databricks notebook source
import json
import os
import time
from pathlib import Path
from typing import Any, Mapping

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class APIFootballError(RuntimeError):
    """Base exception for API-Football client errors."""


class APIFootballRateLimitError(APIFootballError):
    """Raised when API-Football rejects a request because of rate limits."""


class APIFootballClient:
    """
    API-Football HTTP client.

    Features:
      - persistent HTTP session
      - retry/backoff for temporary server failures
      - configurable request pacing
      - HTTP and API-level error handling
      - rate-limit tracking
      - pagination support
      - context-manager support
    """

    def __init__(
        self,
        api_key: str,
        base_url: str,
        min_request_interval_seconds: float = 6.5,
        timeout_seconds: int = 30,
    ) -> None:

        if not api_key:
            raise ValueError("api_key cannot be empty.")

        if not base_url:
            raise ValueError("base_url cannot be empty.")

        if min_request_interval_seconds < 0:
            raise ValueError(
                "min_request_interval_seconds cannot be negative."
            )

        if timeout_seconds <= 0:
            raise ValueError(
                "timeout_seconds must be greater than zero."
            )

        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.min_request_interval_seconds = (
            min_request_interval_seconds
        )

        self._headers = {
            "x-apisports-key": api_key,
        }

        self._last_request_started_at: float | None = None

        self.daily_remaining: int | None = None
        self.minute_remaining: int | None = None

        retry = Retry(
            total=3,
            connect=3,
            read=3,
            status=3,
            backoff_factor=1.5,
            status_forcelist=(
                500,
                502,
                503,
                504,
            ),
            allowed_methods=frozenset({"GET"}),
            respect_retry_after_header=True,
        )

        adapter = HTTPAdapter(
            max_retries=retry,
        )

        self.session = requests.Session()
        self.session.mount(
            "https://",
            adapter,
        )

    def __enter__(self) -> "APIFootballClient":
        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ) -> None:
        self.close()

    def _wait_for_request_slot(self) -> None:
        """Respect the configured delay between client-level requests."""

        if self._last_request_started_at is None:
            return

        elapsed = (
            time.monotonic()
            - self._last_request_started_at
        )

        remaining_wait = (
            self.min_request_interval_seconds
            - elapsed
        )

        if remaining_wait > 0:
            time.sleep(remaining_wait)

    @staticmethod
    def _to_int(
        value: str | None,
    ) -> int | None:
        """Safely convert a response header value to integer."""

        if value is None:
            return None

        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _update_rate_limit_state(
        self,
        response: requests.Response,
    ) -> None:
        """Update known API quota state from response headers."""

        self.daily_remaining = self._to_int(
            response.headers.get(
                "x-ratelimit-requests-remaining"
            )
        )

        self.minute_remaining = self._to_int(
            response.headers.get(
                "X-RateLimit-Remaining"
            )
        )

        print(
            "API quota - "
            f"daily remaining: {self.daily_remaining}, "
            f"minute remaining: {self.minute_remaining}"
        )

    def _validate_known_quota(self) -> None:
        """Fail early when previously observed quota is exhausted."""

        if (
            self.daily_remaining is not None
            and self.daily_remaining <= 0
        ):
            raise APIFootballRateLimitError(
                "Daily API quota is exhausted. "
                "Stop this run and continue after the quota resets."
            )

        if (
            self.minute_remaining is not None
            and self.minute_remaining <= 0
        ):
            raise APIFootballRateLimitError(
                "Per-minute API quota is exhausted."
            )

    def get(
        self,
        endpoint: str,
        params: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Execute one API request and return the validated JSON payload."""

        if not endpoint:
            raise ValueError("endpoint cannot be empty.")

        self._validate_known_quota()
        self._wait_for_request_slot()

        url = (
            f"{self.base_url}/"
            f"{endpoint.lstrip('/')}"
        )

        self._last_request_started_at = (
            time.monotonic()
        )

        try:
            response = self.session.get(
                url=url,
                params=dict(params),
                headers=self._headers,
                timeout=self.timeout_seconds,
            )
        except requests.RequestException as exc:
            raise APIFootballError(
                f"API-Football request failed for "
                f"endpoint '{endpoint}'."
            ) from exc

        self._update_rate_limit_state(response)

        if response.status_code == 429:
            retry_after = response.headers.get(
                "Retry-After"
            )

            message = (
                "API-Football returned HTTP 429 "
                "(rate limit exceeded)."
            )

            if retry_after:
                message += (
                    f" Retry-After: {retry_after} seconds."
                )

            raise APIFootballRateLimitError(
                message
            )

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise APIFootballError(
                f"API-Football returned HTTP "
                f"{response.status_code} "
                f"for endpoint '{endpoint}'."
            ) from exc

        try:
            payload = response.json()
        except ValueError as exc:
            raise APIFootballError(
                "API-Football returned a non-JSON "
                f"response for endpoint '{endpoint}'."
            ) from exc

        if not isinstance(payload, dict):
            raise APIFootballError(
                "API-Football returned an unexpected "
                f"JSON structure for endpoint '{endpoint}'."
            )

        api_errors = payload.get("errors")

        if api_errors:
            raise APIFootballError(
                "API-Football returned an API-level "
                f"error for endpoint '{endpoint}': "
                f"{api_errors}"
            )

        return payload

    def get_all_pages(
        self,
        endpoint: str,
        params: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        """Fetch all business records from a paginated endpoint."""

        page = 1
        records: list[dict[str, Any]] = []

        while True:
            page_params = {
                **params,
                "page": page,
            }

            payload = self.get(
                endpoint,
                page_params,
            )

            page_records = payload.get(
                "response",
                [],
            )

            if not isinstance(page_records, list):
                raise APIFootballError(
                    "Expected API-Football 'response' "
                    f"to be a list for endpoint '{endpoint}'."
                )

            records.extend(page_records)

            paging = payload.get(
                "paging",
                {},
            )

            if not isinstance(paging, dict):
                raise APIFootballError(
                    "Expected API-Football 'paging' "
                    f"to be an object for endpoint '{endpoint}'."
                )

            current_page = int(
                paging.get("current", page)
                or page
            )

            total_pages = int(
                paging.get("total", 1)
                or 1
            )

            print(
                f"Fetched page "
                f"{current_page}/{total_pages}: "
                f"{len(page_records)} records"
            )

            if current_page >= total_pages:
                break

            next_page = current_page + 1

            if next_page <= page:
                raise APIFootballError(
                    "API-Football pagination did not "
                    f"advance for endpoint '{endpoint}'."
                )

            page = next_page

        return records

    def close(self) -> None:
        """Close the underlying HTTP session."""

        self.session.close()


def write_json_atomic(
    data: Any,
    file_path: str,
) -> None:
    """
    Write JSON through a temporary file.

    The final path is replaced only after serialization succeeds,
    preventing partially written final output.
    """

    path = Path(file_path)
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_path = path.with_suffix(
        path.suffix + ".tmp"
    )

    try:
        with temporary_path.open(
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(
                data,
                file,
                indent=2,
                ensure_ascii=False,
            )

        os.replace(
            temporary_path,
            path,
        )

    except Exception:
        if temporary_path.exists():
            temporary_path.unlink()

        raise