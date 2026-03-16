from .exceptions import ServerResponseError, InternalServerError, NonXMLResponseError
from functools import wraps
from xml.etree.ElementTree import ParseError

import logging

try:
    from distutils2.version import NormalizedVersion as Version
except ImportError:
    from distutils.version import LooseVersion as Version

logger = logging.getLogger('tableau.endpoint')

Success_codes = [200, 201, 202, 204]


class Endpoint(object):
    def __init__(self, parent_srv):
        self.parent_srv = parent_srv

    @staticmethod
    def _make_common_headers(auth_token, content_type):
        headers = {}
        if auth_token is not None:
            headers['x-tableau-auth'] = auth_token
        if content_type is not None:
            headers['content-type'] = content_type

        return headers

    @staticmethod
    def _safe_to_log(server_response):
        """Checks if the server_response content is not xml (eg binary image or zip)
        and replaces it with a constant
        """
        ALLOWED_CONTENT_TYPES = ('application/xml', 'application/xml;charset=utf-8')
        if server_response.headers.get('Content-Type', None) not in ALLOWED_CONTENT_TYPES:
            return '[Truncated File Contents]'
        else:
            return server_response.content

    def _make_request(self, method, url, content=None, request_object=None,
                      auth_token=None, content_type=None, parameters=None):
        if request_object is not None:
            url = request_object.apply_query_params(url)
        parameters = parameters or {}
        parameters.update(self.parent_srv.http_options)
        parameters['headers'] = Endpoint._make_common_headers(auth_token, content_type)

        if content is not None:
            parameters['data'] = content

        server_response = method(url, **parameters)
        self.parent_srv._namespace.detect(server_response.content)
        self._check_status(server_response)

        # This check is to determine if the response is a text response (xml or otherwise)
        # so that we do not attempt to log bytes and other binary data.
        if server_response.encoding:
            logger.debug(u'Server response from {0}:\n\t{1}'.format(
                url, server_response.content.decode(server_response.encoding)))
        return server_response

    def _check_status(self, server_response):
        logger.debug(self._safe_to_log(server_response))
        if server_response.status_code >= 500:
            raise InternalServerError(server_response)
        elif server_response.status_code not in Success_codes:
            try:
                raise ServerResponseError.from_response(server_response.content, self.parent_srv.namespace)
            except ParseError:
                # This will happen if we get a non-success HTTP code that
                # doesn't return an xml error object (like metadata endpoints)
                # we convert this to a better exception and pass through the raw
                # response body
                raise NonXMLResponseError(server_response.content)
            except Exception:
                # anything else re-raise here
                raise

    def get_unauthenticated_request(self, url, request_object=None):
        return self._make_request(self.parent_srv.session.get, url, request_object=request_object)

    def get_request(self, url, request_object=None, parameters=None):
        return self._make_request(self.parent_srv.session.get, url, auth_token=self.parent_srv.auth_token,
                                  request_object=request_object, parameters=parameters)

    def delete_request(self, url):
        # We don't return anything for a delete
        self._make_request(self.parent_srv.session.delete, url, auth_token=self.parent_srv.auth_token)

    def put_request(self, url, xml_request=None, content_type='text/xml'):
        return self._make_request(self.parent_srv.session.put, url,
                                  content=xml_request,
                                  auth_token=self.parent_srv.auth_token,
                                  content_type=content_type)

    def post_request(self, url, xml_request, content_type='text/xml'):
        return self._make_request(self.parent_srv.session.post, url,
                                  content=xml_request,
                                  auth_token=self.parent_srv.auth_token,
                                  content_type=content_type)


def api(version):
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
    def _decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            self.parent_srv.assert_at_least_version(version)
            return func(self, *args, **kwargs)
        return wrapper
    return _decorator


def parameter_added_in(**params):
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
    def _decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            import warnings
            server_ver = Version(self.parent_srv.version or "0.0")
            params_to_check = set(params) & set(kwargs)
            for p in params_to_check:
                min_ver = Version(str(params[p]))
                if server_ver < min_ver:
                    error = "{!r} not available in {}, it will be ignored. Added in {}".format(p, server_ver, min_ver)
                    warnings.warn(error)
            return func(self, *args, **kwargs)
        return wrapper
    return _decorator
