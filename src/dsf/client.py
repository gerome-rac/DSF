"""client for fetching data from daten.sg.ch"""

import logging
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests
from requests.exceptions import RequestException


class StGallenAPI:
    """Client for accessing any dataset from the St. Gallen Open Data Portal."""

    BASE_URL = "https://daten.sg.ch/api/records/1.0/"

    def __init__(self) -> None:
        """Initialize the API client."""
        self.session = requests.Session()
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    def _make_request(self, endpoint: str, params: dict | None = None, **kwargs) -> dict[str, Any]:
        """Make HTTP request to the API.

        Args:
            endpoint: API endpoint to call
            params: Query parameters to include
            **kwargs: Additional arguments to pass to requests

        Returns:
            JSON response from the API

        Raises:
            RequestException: If the request fails
        """
        url = urljoin(self.BASE_URL, endpoint)

        try:
            response = self.session.get(url, params=params, **kwargs)
            response.raise_for_status()
            return response.json()

        except RequestException as e:
            self.logger.error(f"API request failed: {e!s}")
            raise

    def get_dataset(
        self,
        dataset_id: str,
        select: str | None = None,
        where: str | None = None,
        limit: int | None = 100,
        offset: int = 0,
        order_by: str | None = None,
        as_dataframe: bool = True,
        get_all: bool = False,
    ) -> pd.DataFrame | dict[str, Any]:
        """Get records from any dataset.

        Args:
            dataset_id: ID of the dataset to query
            select: Fields to include in results (comma-separated)
            where: Filter condition
            limit: Maximum number of results to return
            offset: Number of results to skip
            order_by: Field to sort by
            as_dataframe: Return results as pandas DataFrame if True, else dict
            get_all: If True, fetches all records regardless of limit

        Returns:
            Dataset records matching the query as DataFrame or dict
        """
        params = {"dataset": dataset_id, "rows": 1, "start": 0}

        initial_response = self._make_request("search/", params=params)
        total_records = initial_response.get("nhits", 0)
        self.logger.info(f"Total records available: {total_records}")

        if get_all:
            limit = total_records

        params.update({"rows": limit, "start": offset})

        if select:
            params["select"] = select
        if where:
            params["where"] = where
        if order_by:
            params["order_by"] = order_by

        response = self._make_request("search/", params=params)

        if as_dataframe:
            records = response.get("records", [])
            if not records:
                return pd.DataFrame()

            if "fields" in records[0]:
                return pd.DataFrame([record["fields"] for record in records])
            return pd.DataFrame(records)

        return response

    def get_dataset_metadata(self, dataset_id: str) -> dict[str, Any]:
        """Get metadata about a dataset.

        Args:
            dataset_id: ID of the dataset

        Returns:
            Dataset metadata
        """
        params = {"dataset": dataset_id, "rows": 0}
        return self._make_request("search/", params=params)
