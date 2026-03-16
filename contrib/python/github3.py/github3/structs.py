import collections.abc
import functools
import typing as t

from requests.compat import urlencode
from requests.compat import urlparse

from . import exceptions
from . import models

if t.TYPE_CHECKING:
    import requests.models

    from . import session


T = t.TypeVar("T")


class GitHubIterator(models.GitHubCore, collections.abc.Iterator):
    """The :class:`GitHubIterator` class powers all of the iter_* methods."""

    def __init__(
        self,
        count: int,
        url: str,
        cls: t.Type[T],
        session: "session.GitHubSession",
        params: t.Optional[t.Mapping[str, t.Optional[str]]] = None,
        etag: t.Optional[str] = None,
        headers: t.Optional[t.Mapping[str, str]] = None,
        list_key: t.Optional[str] = None,
    ) -> None:
        models.GitHubCore.__init__(self, {}, session)
        #: Original number of items requested
        self.original: t.Final[int] = count
        #: Number of items left in the iterator
        self.count: int = count
        #: URL the class used to make it's first GET
        self.url: str = url
        #: Last URL that was requested
        self.last_url: t.Optional[str] = None
        self._api: str = self.url
        #: Class for constructing an item to return
        self.cls: t.Type[T] = cls
        #: Parameters of the query string
        self.params: t.Mapping[str, t.Optional[str]] = params or {}
        self._remove_none(self.params)
        # We do not set this from the parameter sent. We want this to
        # represent the ETag header returned by GitHub no matter what.
        # If this is not None, then it won't be set from the response and
        # that's not what we want.
        #: The ETag Header value returned by GitHub
        self.etag: t.Optional[str] = None
        #: Headers generated for the GET request
        self.headers: t.Dict[str, str] = dict(headers or {})
        #: The last response seen
        self.last_response: "requests.models.Response" = None
        #: Last status code received
        self.last_status: int = 0
        #: Key to get the list of items in case a dict is returned
        self.list_key: t.Final[t.Optional[str]] = list_key

        if etag:
            self.headers.update({"If-None-Match": etag})

        self.path: str = urlparse(self.url).path

    def _repr(self) -> str:
        return f"<GitHubIterator [{self.count}, {self.path}]>"

    def __iter__(self) -> t.Generator[T, None, None]:
        self.last_url, params = self.url, self.params
        headers = self.headers

        if 0 < self.count <= 100 and self.count != -1:
            params["per_page"] = self.count

        if "per_page" not in params and self.count == -1:
            params["per_page"] = 100

        cls = self.cls
        if issubclass(self.cls, models.GitHubCore):
            cls = functools.partial(self.cls, session=self)

        while (self.count == -1 or self.count > 0) and self.last_url:
            response = self._get(
                self.last_url, params=params, headers=headers
            )
            self.last_response = response
            self.last_status = response.status_code
            if params:
                params = None  # rel_next already has the params

            if not self.etag and response.headers.get("ETag"):
                self.etag = response.headers.get("ETag")

            json = self._get_json(response)

            if json is None:
                break

            # Some APIs return the list of items inside a dict
            if isinstance(json, dict) and self.list_key is not None:
                try:
                    json = json[self.list_key]
                except KeyError:
                    raise exceptions.UnprocessableResponseBody(
                        "GitHub's API returned a body that could not be"
                        " handled",
                        json,
                    )

            # languages returns a single dict. We want the items.
            if isinstance(json, dict):
                if issubclass(self.cls, models.GitHubCore):
                    raise exceptions.UnprocessableResponseBody(
                        "GitHub's API returned a body that could not be"
                        " handled",
                        json,
                    )
                if json.get("ETag"):
                    del json["ETag"]
                if json.get("Last-Modified"):
                    del json["Last-Modified"]
                json = json.items()

            for i in json:
                if i is None:
                    continue
                yield cls(i)
                self.count -= 1 if self.count > 0 else 0
                if self.count == 0:
                    break

            rel_next = response.links.get("next", {})
            self.last_url = rel_next.get("url", "")

    def __next__(self) -> T:
        if not hasattr(self, "__i__"):
            self.__i__ = self.__iter__()
        return next(self.__i__)

    def _get_json(self, response: "requests.models.Response"):
        return self._json(response, 200)

    def refresh(self, conditional: bool = False) -> "GitHubIterator":
        self.count = self.original
        if conditional and self.etag:
            self.headers["If-None-Match"] = self.etag
        self.etag = None
        self.__i__ = self.__iter__()
        return self

    def next(self) -> T:
        return self.__next__()


class SearchIterator(GitHubIterator):

    """This is a special-cased class for returning iterable search results.

    It inherits from :class:`GitHubIterator <github3.structs.GitHubIterator>`.
    All members and methods documented here are unique to instances of this
    class. For other members and methods, check its parent class.

    """

    _ratelimit_resource = "search"

    def __init__(
        self,
        count: int,
        url: str,
        cls: t.Type[T],
        session: "session.GitHubSession",
        params: t.Optional[t.Mapping[str, t.Optional[str]]] = None,
        etag: t.Optional[str] = None,
        headers: t.Optional[t.Mapping[str, str]] = None,
    ):
        super().__init__(count, url, cls, session, params, etag, headers)
        #: Total count returned by GitHub
        self.total_count: int = 0
        #: Items array returned in the last request
        self.items: t.List[t.Mapping[str, t.Any]] = []

    def _repr(self):
        return "<SearchIterator [{}, {}?{}]>".format(
            self.count, self.path, urlencode(self.params)
        )

    def _get_json(self, response):
        json = self._json(response, 200)
        # I'm not sure if another page will retain the total_count attribute,
        # so if it's not in the response, just set it back to what it used to
        # be
        self.total_count = json.get("total_count", self.total_count)
        self.items = json.get("items", [])
        # If we return None then it will short-circuit the while loop.
        return json.get("items")
