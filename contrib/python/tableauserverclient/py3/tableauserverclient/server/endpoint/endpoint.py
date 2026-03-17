from typing_extensions import Concatenate, ParamSpec
from tableauserverclient import datetime_helpers as datetime

import abc
from packaging.version import Version
from functools import wraps
from xml.etree.ElementTree import ParseError
from typing import (
    Any,
    Callable,
    Generic,
    Optional,
    TYPE_CHECKING,
    TypeVar,
    Union,
)
from typing_extensions import Self

from tableauserverclient.models.pagination_item import PaginationItem
from tableauserverclient.server.request_options import RequestOptions

from tableauserverclient.server.endpoint.exceptions import (
    FailedSignInError,
    ServerResponseError,
    InternalServerError,
    NonXMLResponseError,
    NotSignedInError,
)
from tableauserverclient.server.exceptions import EndpointUnavailableError

from tableauserverclient.server.query import QuerySet
from tableauserverclient import helpers, get_versions

from tableauserverclient.helpers.logging import logger

if TYPE_CHECKING:
    from tableauserverclient.server.server import Server
    from requests import Response


Success_codes = [200, 201, 202, 204]

XML_CONTENT_TYPE = "text/xml"
JSON_CONTENT_TYPE = "application/json"

CONTENT_TYPE_HEADER = "content-type"
TABLEAU_AUTH_HEADER = "x-tableau-auth"
USER_AGENT_HEADER = "User-Agent"


class Endpoint:
    def __init__(self, parent_srv: "Server"):
        self.parent_srv = parent_srv

    async_response = None

    @staticmethod
    def set_parameters(http_options, auth_token, content, content_type, parameters) -> dict[str, Any]:
        parameters = parameters or {}
        parameters.update(http_options)
        if "headers" not in parameters:
            parameters["headers"] = {}

        if auth_token is not None:
            parameters["headers"][TABLEAU_AUTH_HEADER] = auth_token
        if content_type is not None:
            parameters["headers"][CONTENT_TYPE_HEADER] = content_type

        Endpoint.set_user_agent(parameters)
        if content is not None:
            parameters["data"] = content
        return parameters or {}

    @staticmethod
    def set_user_agent(parameters):
        if "headers" not in parameters:
            parameters["headers"] = {}
        if USER_AGENT_HEADER not in parameters["headers"]:
            if USER_AGENT_HEADER in parameters:
                parameters["headers"][USER_AGENT_HEADER] = parameters[USER_AGENT_HEADER]
            else:
                # only set the TSC user agent if not already populated
                _client_version: Optional[str] = get_versions()["version"]
                parameters["headers"][USER_AGENT_HEADER] = f"Tableau Server Client/{_client_version}"

        # result: parameters["headers"]["User-Agent"] is set
        # return explicitly for testing only
        return parameters

    def _blocking_request(self, method, url, parameters={}) -> Optional[Union["Response", Exception]]:
        response = None
        logger.debug(f"[{datetime.timestamp()}] Begin blocking request to {url}")
        try:
            response = method(url, **parameters)
            logger.debug(f"[{datetime.timestamp()}] Call finished")
        except Exception as e:
            logger.debug(f"Error making request to server: {e}")
            raise e
        return response

    def send_request_while_show_progress_threaded(
        self, method, url, parameters={}, request_timeout=None
    ) -> Optional[Union["Response", Exception]]:
        return self._blocking_request(method, url, parameters)

    def _make_request(
        self,
        method: Callable[..., "Response"],
        url: str,
        content: Optional[bytes] = None,
        auth_token: Optional[str] = None,
        content_type: Optional[str] = None,
        parameters: Optional[dict[str, Any]] = None,
    ) -> "Response":
        parameters = Endpoint.set_parameters(
            self.parent_srv.http_options, auth_token, content, content_type, parameters
        )

        logger.debug(f"request method {method.__name__}, url: {url}")
        if content:
            redacted = helpers.strings.redact_xml(content[:200])
            # this needs to be under a trace or something, it's a LOT
            # logger.debug("request content: {}".format(redacted))

        # a request can, for stuff like publishing, spin for ages waiting for a response.
        # we need some user-facing activity so they know it's not dead.
        request_timeout = self.parent_srv.http_options.get("timeout") or 0
        server_response: Optional[Union["Response", Exception]] = self.send_request_while_show_progress_threaded(
            method, url, parameters, request_timeout
        )
        logger.debug(f"[{datetime.timestamp()}] Async request returned: received {server_response}")
        # is this blocking retry really necessary? I guess if it was just the threading messing it up?
        if server_response is None:
            logger.debug(server_response)
            logger.debug(f"[{datetime.timestamp()}] Async request failed: retrying")
            server_response = self._blocking_request(method, url, parameters)
        if server_response is None:
            logger.debug(f"[{datetime.timestamp()}] Request failed")
            raise RuntimeError
        if isinstance(server_response, Exception):
            raise server_response
        self._check_status(server_response, url)

        loggable_response = self.log_response_safely(server_response)
        logger.debug(f"Server response from {url}")
        # uncomment the following to log full responses in debug mode
        # BE CAREFUL WHEN SHARING THESE RESULTS - MAY CONTAIN YOUR SENSITIVE DATA
        # logger.debug(loggable_response)

        if content_type == "application/xml":
            self.parent_srv._namespace.detect(server_response.content)

        return server_response

    def _check_status(self, server_response: "Response", url: Optional[str] = None):
        logger.debug(f"Response status: {server_response}")
        if not hasattr(server_response, "status_code"):
            raise OSError("Response is not a http response?")
        if server_response.status_code >= 500:
            raise InternalServerError(server_response, url)
        elif server_response.status_code not in Success_codes:
            try:
                if server_response.status_code == 401:
                    # TODO: catch this in server.py and attempt to sign in again, in case it's a session expiry
                    raise FailedSignInError.from_response(server_response.content, self.parent_srv.namespace, url)

                raise ServerResponseError.from_response(server_response.content, self.parent_srv.namespace, url)
            except ParseError:
                # This will happen if we get a non-success HTTP code that doesn't return an xml error object
                # e.g. metadata endpoints, 503 pages, totally different servers
                # we convert this to a better exception and pass through the raw response body
                raise NonXMLResponseError(server_response.content)
            except Exception:
                # anything else re-raise here
                raise

    def log_response_safely(self, server_response: "Response") -> str:
        # Checking the content type header prevents eager evaluation of streaming requests.
        content_type = server_response.headers.get("Content-Type")

        # Response.content is a property. Calling it will load the entire response into memory. Checking if the
        # content-type is an octet-stream accomplishes the same goal without eagerly loading content.
        # This check is to determine if the response is a text response (xml or otherwise)
        # so that we do not attempt to log bytes and other binary data.
        loggable_response = f"Content type `{content_type}`"
        if content_type == "application/octet-stream":
            loggable_response = f"A stream of type {content_type} [Truncated File Contents]"
        elif server_response.encoding and len(server_response.content) > 0:
            loggable_response = helpers.strings.redact_xml(server_response.content.decode(server_response.encoding))
        return loggable_response

    def get_unauthenticated_request(self, url):
        return self._make_request(self.parent_srv.session.get, url)

    def get_request(self, url, request_object=None, parameters=None):
        if request_object is not None:
            try:
                # Query param delimiters don't need to be encoded for versions before 3.7 (2020.1)
                self.parent_srv.assert_at_least_version("3.7", "Query param encoding")
                parameters = parameters or {}
                parameters["params"] = request_object.get_query_params()
            except EndpointUnavailableError:
                url = request_object.apply_query_params(url)

        return self._make_request(
            self.parent_srv.session.get,
            url,
            auth_token=self.parent_srv.auth_token,
            parameters=parameters,
        )

    def delete_request(self, url):
        # We don't return anything for a delete request
        self._make_request(self.parent_srv.session.delete, url, auth_token=self.parent_srv.auth_token)

    def put_request(self, url, xml_request=None, content_type=XML_CONTENT_TYPE, parameters=None):
        return self._make_request(
            self.parent_srv.session.put,
            url,
            content=xml_request,
            auth_token=self.parent_srv.auth_token,
            content_type=content_type,
            parameters=parameters,
        )

    def post_request(self, url, xml_request, content_type=XML_CONTENT_TYPE, parameters=None):
        return self._make_request(
            self.parent_srv.session.post,
            url,
            content=xml_request,
            auth_token=self.parent_srv.auth_token,
            content_type=content_type,
            parameters=parameters,
        )

    def patch_request(self, url, xml_request, content_type=XML_CONTENT_TYPE, parameters=None):
        return self._make_request(
            self.parent_srv.session.patch,
            url,
            content=xml_request,
            auth_token=self.parent_srv.auth_token,
            content_type=content_type,
            parameters=parameters,
        )


E = TypeVar("E", bound="Endpoint")
P = ParamSpec("P")
R = TypeVar("R")


def api(version: str) -> Callable[[Callable[Concatenate[E, P], R]], Callable[Concatenate[E, P], R]]:
    """Annotate the minimum supported version for an endpoint.

    Checks the version on the server object and compares normalized versions.
    It will raise an exception if the server version is > the version specified.

    Args:
        `version` minimum version that supports the endpoint. String.
    Raises:
        EndpointUnavailableError
    Returns:
        None

    Example:
    >>> @api(version="2.3")
    >>> def get(self, req_options=None):
    >>>     ...
    """

    def _decorator(func: Callable[Concatenate[E, P], R]) -> Callable[Concatenate[E, P], R]:
        @wraps(func)
        def wrapper(self: E, *args: P.args, **kwargs: P.kwargs) -> R:
            self.parent_srv.assert_at_least_version(version, self.__class__.__name__)
            return func(self, *args, **kwargs)

        return wrapper

    return _decorator


def parameter_added_in(**params: str) -> Callable[[Callable[Concatenate[E, P], R]], Callable[Concatenate[E, P], R]]:
    """Annotate minimum versions for new parameters or request options on an endpoint.

    The api decorator documents when an endpoint was added, this decorator annotates
    keyword arguments on endpoints that may control functionality added after an endpoint was introduced.

    The REST API will ignore invalid parameters in most cases, so this raises a warning instead of throwing
    an exception.

    Args:
        Key/value pairs of the form `parameter`=`version`. Kwargs.
    Raises:
        UserWarning
    Returns:
        None

    Example:
    >>> @api(version="2.0")
    >>> @parameter_added_in(no_extract='2.5')
    >>> def download(self, workbook_id, filepath=None, extract_only=False):
    >>>     ...
    """

    def _decorator(func: Callable[Concatenate[E, P], R]) -> Callable[Concatenate[E, P], R]:
        @wraps(func)
        def wrapper(self: E, *args: P.args, **kwargs: P.kwargs) -> R:
            import warnings

            server_ver = Version(self.parent_srv.version or "0.0")
            params_to_check = set(params) & set(kwargs)
            for p in params_to_check:
                min_ver = Version(str(params[p]))
                if server_ver < min_ver:
                    error = f"{p!r} not available in {server_ver}, it will be ignored. Added in {min_ver}"
                    warnings.warn(error)
            return func(self, *args, **kwargs)

        return wrapper

    return _decorator


T = TypeVar("T")


class QuerysetEndpoint(Endpoint, Generic[T]):
    @api(version="2.0")
    def all(self, *args, page_size: Optional[int] = None, **kwargs) -> QuerySet[T]:
        if args or kwargs:
            raise ValueError(".all method takes no arguments.")
        queryset = QuerySet(self, page_size=page_size)
        return queryset

    @api(version="2.0")
    def filter(self, *_, page_size: Optional[int] = None, **kwargs) -> QuerySet[T]:
        if _:
            raise RuntimeError("Only keyword arguments accepted.")
        queryset = QuerySet(self, page_size=page_size).filter(**kwargs)
        return queryset

    @api(version="2.0")
    def order_by(self, *args, **kwargs) -> QuerySet[T]:
        if kwargs:
            raise ValueError(".order_by does not accept keyword arguments.")
        queryset = QuerySet(self).order_by(*args)
        return queryset

    @api(version="2.0")
    def paginate(self, **kwargs) -> QuerySet[T]:
        queryset = QuerySet(self).paginate(**kwargs)
        return queryset

    @abc.abstractmethod
    def get(self, request_options: Optional[RequestOptions] = None) -> tuple[list[T], PaginationItem]:
        raise NotImplementedError(f".get has not been implemented for {self.__class__.__qualname__}")

    def fields(self: Self, *fields: str) -> QuerySet:
        """
        Add fields to the request options. If no fields are provided, the
        default fields will be used. If fields are provided, the default fields
        will be used in addition to the provided fields.

        Parameters
        ----------
        fields : str
            The fields to include in the request options.

        Returns
        -------
        QuerySet
        """
        queryset = QuerySet(self)
        queryset.request_options.fields |= set(fields) | set(("_default_",))
        return queryset

    def only_fields(self: Self, *fields: str) -> QuerySet:
        """
        Add fields to the request options. If no fields are provided, the
        default fields will be used. If fields are provided, the default fields
        will be replaced by the provided fields.

        Parameters
        ----------
        fields : str
            The fields to include in the request options.

        Returns
        -------
        QuerySet
        """
        queryset = QuerySet(self)
        queryset.request_options.fields |= set(fields)
        return queryset
