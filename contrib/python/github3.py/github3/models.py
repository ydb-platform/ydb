"""This module provides the basic models used in github3.py."""
import json as jsonlib
import logging
import typing as t

import dateutil.parser
import requests.compat

from . import exceptions
from . import session


if t.TYPE_CHECKING:
    from . import structs

LOG = logging.getLogger(__package__)


T = t.TypeVar("T")


class GitHubCore:
    """The base object for all objects that require a session.

    The :class:`GitHubCore <GitHubCore>` object provides some
    basic attributes and methods to other sub-classes that are very useful to
    have.
    """

    _ratelimit_resource = "core"
    _refresh_to: t.Optional["GitHubCore"] = None

    def __init__(self, json, session: session.GitHubSession):
        """Initialize our basic object.

        Pretty much every object will pass in decoded JSON and a Session.
        """
        # Either or 'session' is an instance of a GitHubCore sub-class or it
        # is a session. In the former case it will have a 'session' attribute.
        # If it doesn't, we can just default to using the passed in session.
        self.session = getattr(session, "session", session)

        # set a sane default
        self._github_url = "https://api.github.com"

        if json is not None:
            self.etag = json.pop("ETag", None)
            self.last_modified = json.pop("Last-Modified", None)
            self._uniq = json.get("url", None)
        self._json_data = json
        try:
            self._update_attributes(json)
        except KeyError as kerr:
            raise exceptions.IncompleteResponse(json, kerr)

    def _update_attributes(self, json):
        pass

    def __getattr__(self, attribute):
        """Proxy access to stored JSON."""
        _json_data = object.__getattribute__(self, "_json_data")
        if attribute not in _json_data:
            raise AttributeError(attribute)
        value = _json_data[attribute]
        setattr(self, attribute, value)
        return value

    def as_dict(self):
        """Return the attributes for this object as a dictionary.

        This is equivalent to calling::

            json.loads(obj.as_json())

        :returns: this object's attributes serialized to a dictionary
        :rtype: dict
        """
        return self._json_data

    def as_json(self):
        """Return the json data for this object.

        This is equivalent to calling::

            json.dumps(obj.as_dict())

        :returns: this object's attributes as a JSON string
        :rtype: str
        """
        return jsonlib.dumps(self._json_data)

    @classmethod
    def _strptime(cls, time_str):
        """Convert an ISO 8601 formatted string to a datetime object.

        We assume that the ISO 8601 formatted string is in UTC and we create
        the datetime object so that it is timezone-aware.

        :param str time_str: ISO 8601 formatted string
        :returns: timezone-aware datetime object
        :rtype: datetime or None
        """
        if time_str:
            return dateutil.parser.parse(time_str)
        return None

    def __repr__(self):
        repr_string = self._repr()
        if requests.compat.is_py2:
            return repr_string.encode("utf-8")
        return repr_string

    @classmethod
    def from_dict(cls, json_dict, session):
        """Return an instance of this class formed from ``json_dict``."""
        return cls(json_dict, session)

    @classmethod
    def from_json(cls, json, session):
        """Return an instance of this class formed from ``json``."""
        return cls(jsonlib.loads(json), session)

    def __eq__(self, other):
        return self._uniq == other._uniq

    def __ne__(self, other):
        return self._uniq != other._uniq

    def __hash__(self):
        return hash(self._uniq)

    def _repr(self):
        return f"<github3-core at 0x{id(self):x}>"

    @staticmethod
    def _remove_none(data):
        if not data:
            return
        for k, v in list(data.items()):
            if v is None:
                del data[k]

    def _instance_or_null(self, instance_class, json):
        if json is not None and not isinstance(json, dict):
            raise exceptions.UnprocessableResponseBody(
                "GitHub's API returned a body that could not be handled", json
            )
        if not json:
            return None

        return instance_class(json, self)

    def _json(self, response, expected_status_code, include_cache_info=True):
        ret = None
        if response is None:
            return None

        actual_status_code = response.status_code
        if actual_status_code != expected_status_code:
            if actual_status_code >= 400:
                raise exceptions.error_for(response)

            if actual_status_code == 304:
                # Received a response from someone passing in `etag=`
                return None

            LOG.warning(
                "Expected status_code %d but got %d",
                expected_status_code,
                actual_status_code,
            )

        try:
            ret = response.json()
        except ValueError:
            raise exceptions.UnexpectedResponse(response)

        headers = response.headers
        if (
            include_cache_info
            and (headers.get("Last-Modified") or headers.get("ETag"))
            and isinstance(ret, dict)
        ):
            ret["Last-Modified"] = response.headers.get("Last-Modified", "")
            ret["ETag"] = response.headers.get("ETag", "")
        LOG.info("JSON was %sreturned", "not " if ret is None else "")
        return ret

    def _boolean(self, response, true_code, false_code):
        if response is not None:
            status_code = response.status_code
            if status_code == true_code:
                return True
            if status_code != false_code and status_code >= 400:
                raise exceptions.error_for(response)
        return False

    def _request(self, method, *args, **kwargs):
        try:
            request_method = getattr(self.session, method)
            return request_method(*args, **kwargs)
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as exc:
            raise exceptions.ConnectionError(exc)
        except requests.exceptions.RequestException as exc:
            raise exceptions.TransportError(exc)

    def _delete(self, url, **kwargs):
        LOG.debug("DELETE %s with %s", url, kwargs)
        return self._request("delete", url, **kwargs)

    def _get(self, url, **kwargs):
        LOG.debug("GET %s with %s", url, kwargs)
        return self._request("get", url, **kwargs)

    def _patch(self, url, **kwargs):
        LOG.debug("PATCH %s with %s", url, kwargs)
        return self._request("patch", url, **kwargs)

    def _post(self, url, data=None, json=True, **kwargs):
        if json:
            data = jsonlib.dumps(data) if data is not None else data
        LOG.debug("POST %s with %s, %s", url, data, kwargs)
        return self._request("post", url, data, **kwargs)

    def _put(self, url, **kwargs):
        LOG.debug("PUT %s with %s", url, kwargs)
        return self._request("put", url, **kwargs)

    def _build_url(self, *args, **kwargs):
        """Build a new API url from scratch."""
        return self.session.build_url(*args, **kwargs)

    @property
    def _api(self):
        value = "{0.scheme}://{0.netloc}{0.path}".format(self._uri)
        if self._uri.query:
            value += f"?{self._uri.query}"
        return value

    @staticmethod
    def _uri_parse(uri):
        return requests.compat.urlparse(uri)

    @_api.setter
    def _api(self, uri):
        if uri:
            self._uri = self._uri_parse(uri)
        self.url = uri

    def _iter(
        self,
        count: int,
        url: str,
        cls: t.Type[T],
        params: t.Optional[t.Mapping[str, t.Optional[str]]] = None,
        etag: t.Optional[str] = None,
        headers: t.Optional[t.Mapping[str, str]] = None,
        list_key: t.Optional[str] = None,
    ) -> "structs.GitHubIterator[T]":
        """Generic iterator for this project.

        :param int count: How many items to return.
        :param int url: First URL to start with
        :param class cls: cls to return an object of
        :param params dict: (optional) Parameters for the request
        :param str etag: (optional), ETag from the last call
        :param dict headers: (optional) HTTP Headers for the request
        :param str list_key: (optional) Key for extracting the list of items
            from a dict response
        :returns: A lazy iterator over the pagianted resource
        :rtype: :class:`GitHubIterator <github3.structs.GitHubIterator>`
        """
        from .structs import GitHubIterator

        return GitHubIterator(
            count, url, cls, self, params, etag, headers, list_key
        )

    @property
    def ratelimit_remaining(self):
        """Number of requests before GitHub imposes a ratelimit.

        :returns: int
        """
        json = self._json(self._get(self._build_url("rate_limit")), 200)
        core = json.get("resources", {}).get(self._ratelimit_resource, {})
        self._remaining = core.get("remaining", 0)
        return self._remaining

    def refresh(self, conditional: bool = False) -> "GitHubCore":
        """Re-retrieve the information for this object.

        The reasoning for the return value is the following example: ::

            repos = [r.refresh() for r in g.repositories_by('kennethreitz')]

        Without the return value, that would be an array of ``None``'s and you
        would otherwise have to do: ::

            repos = [r for i in g.repositories_by('kennethreitz')]
            [r.refresh() for r in repos]

        Which is really an anti-pattern.

        .. versionchanged:: 0.5

        .. _Conditional Requests:
            http://developer.github.com/v3/#conditional-requests

        :param bool conditional: If True, then we will search for a stored
            header ('Last-Modified', or 'ETag') on the object and send that
            as described in the `Conditional Requests`_ section of the docs
        :returns: self
        """
        headers = getattr(self, "CUSTOM_HEADERS", {})
        if conditional:
            if self.last_modified:
                headers["If-Modified-Since"] = self.last_modified
            elif self.etag:
                headers["If-None-Match"] = self.etag

        headers = headers or None
        json = self._json(self._get(self._api, headers=headers), 200)
        if json is not None:
            if self._refresh_to is None:
                self._json_data = json
                self._update_attributes(json)
            else:
                return self._refresh_to(json, self)
        return self

    def new_session(self):
        """Generate a new session.

        :returns:
            A brand new session
        :rtype:
            :class:`~github3.session.GitHubSession`
        """
        return session.GitHubSession()
