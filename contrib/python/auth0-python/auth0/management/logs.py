from __future__ import annotations

from typing import Any

from ..rest import RestClient, RestClientOptions
from ..types import TimeoutType


class Logs:
    """Auth0 logs endpoints

    Args:
        domain (str): Your Auth0 domain, e.g: 'username.auth0.com'

        token (str): Management API v2 Token

        telemetry (bool, optional): Enable or disable Telemetry
            (defaults to True)

        timeout (float or tuple, optional): Change the requests
            connect and read timeout. Pass a tuple to specify
            both values separately or a float to set both to it.
            (defaults to 5.0 for both)

        protocol (str, optional): Protocol to use when making requests.
            (defaults to "https")

        rest_options (RestClientOptions): Pass an instance of
            RestClientOptions to configure additional RestClient
            options, such as rate-limit retries.
            (defaults to None)
    """

    def __init__(
        self,
        domain: str,
        token: str,
        telemetry: bool = True,
        timeout: TimeoutType = 5.0,
        protocol: str = "https",
        rest_options: RestClientOptions | None = None,
    ) -> None:
        self.domain = domain
        self.protocol = protocol
        self.client = RestClient(
            jwt=token, telemetry=telemetry, timeout=timeout, options=rest_options
        )

    def _url(self, id: str | None = None) -> str:
        url = f"{self.protocol}://{self.domain}/api/v2/logs"
        if id is not None:
            return f"{url}/{id}"
        return url

    def search(
        self,
        page: int = 0,
        per_page: int = 50,
        sort: str | None = None,
        q: str | None = None,
        include_totals: bool = True,
        fields: list[str] | None = None,
        from_param: str | None = None,
        take: int | None = None,
        include_fields: bool = True,
    ):
        """Search log events.

        Args:
            page (int, optional): The result's page number (zero based). By default,
               retrieves the first page of results.

            per_page (int, optional): The amount of entries per page. By default,
               retrieves 50 results per page.

            sort (str, optional): The field to use for sorting.
                1 == ascending and -1 == descending. (e.g: date:1)
                When not set, the default value is up to the server.

            q (str, optional): Query in Lucene query string syntax.

            fields (list of str, optional): A list of fields to include or
                exclude from the result (depending on include_fields). Leave empty to
                retrieve all fields.

            include_fields (bool, optional): True if the fields specified are
                to be included in the result, False otherwise. Defaults to True.

            include_totals (bool, optional): True if the query summary is
                to be included in the result, False otherwise. Defaults to True.

            from_param (str, optional): Log Event Id to start retrieving logs. You can
                limit the amount of logs using the take parameter.

            take (int, optional): The total amount of entries to retrieve when
                using the from parameter. When not set, the default value is up to the server.

        See: https://auth0.com/docs/api/management/v2#!/Logs/get_logs
        """
        params = {
            "per_page": per_page,
            "page": page,
            "include_totals": str(include_totals).lower(),
            "sort": sort,
            "fields": fields and ",".join(fields) or None,
            "include_fields": str(include_fields).lower(),
            "q": q,
            "from": from_param,
            "take": take,
        }
        return self.client.get(self._url(), params=params)

    def get(self, id: str) -> dict[str, Any]:
        """Retrieves the data related to the log entry identified by id.

        Args:
            id (str): The log_id of the log to retrieve.

        See: https://auth0.com/docs/api/management/v2#!/Logs/get_logs_by_id
        """

        return self.client.get(self._url(id))
