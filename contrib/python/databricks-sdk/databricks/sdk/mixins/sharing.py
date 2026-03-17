from typing import Iterator, Optional

from databricks.sdk.service import sharing
from databricks.sdk.service.sharing import ShareInfo


class SharesExt(sharing.SharesAPI):
    def list(self, *, max_results: Optional[int] = None, page_token: Optional[str] = None) -> Iterator[ShareInfo]:
        """Gets an array of data object shares from the metastore. The caller must be a metastore admin or the
        owner of the share. There is no guarantee of a specific ordering of the elements in the array.

        :param max_results: int (optional)
          Maximum number of shares to return. - when set to 0, the page length is set to a server configured
          value (recommended); - when set to a value greater than 0, the page length is the minimum of this
          value and a server configured value; - when set to a value less than 0, an invalid parameter error
          is returned; - If not set, all valid shares are returned (not recommended). - Note: The number of
          returned shares might be less than the specified max_results size, even zero. The only definitive
          indication that no further shares can be fetched is when the next_page_token is unset from the
          response.
        :param page_token: str (optional)
          Opaque pagination token to go to next page based on previous query.

        :returns: Iterator over :class:`ShareInfo`
        """

        query = {}
        if max_results is not None:
            query["max_results"] = max_results
        if page_token is not None:
            query["page_token"] = page_token
        headers = {
            "Accept": "application/json",
        }

        if "max_results" not in query:
            query["max_results"] = 0
        while True:
            json = self._api.do("GET", "/api/2.1/unity-catalog/shares", query=query, headers=headers)
            if "shares" in json:
                for v in json["shares"]:
                    yield ShareInfo.from_dict(v)
            if "next_page_token" not in json or not json["next_page_token"]:
                return
            query["page_token"] = json["next_page_token"]
