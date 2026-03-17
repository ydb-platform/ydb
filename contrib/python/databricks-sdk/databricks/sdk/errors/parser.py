import logging
from typing import List, Optional

import requests

from ..logger import RoundTrip
from .base import DatabricksError
from .customizer import _ErrorCustomizer, _RetryAfterCustomizer
from .deserializer import (_EmptyDeserializer, _ErrorDeserializer,
                           _HtmlErrorDeserializer, _StandardErrorDeserializer,
                           _StringErrorDeserializer)
from .mapper import _error_mapper
from .private_link import (_get_private_link_validation_error,
                           _is_private_link_redirect)

# A list of _ErrorDeserializers that are tried in order to parse an API error from a response body. Most errors should
# be parsable by the _StandardErrorDeserializer, but additional parsers can be added here for specific error formats.
# The order of the parsers is not important, as the set of errors that can be parsed by each parser should be disjoint.
_error_deserializers = [
    _EmptyDeserializer(),
    _StandardErrorDeserializer(),
    _StringErrorDeserializer(),
    _HtmlErrorDeserializer(),
]

# A list of _ErrorCustomizers that are applied to the error arguments after they are parsed. Customizers can modify the
# error arguments in any way, including adding or removing fields. Customizers are applied in order, so later
# customizers can override the changes made by earlier customizers.
_error_customizers = [
    _RetryAfterCustomizer(),
]


def _unknown_error(response: requests.Response, debug_headers: bool = False) -> str:
    """A standard error message that can be shown when an API response cannot be parsed.

    This error message includes a link to the issue tracker for the SDK for users to report the issue to us.

    :param response: The response object from the API request.
    :param debug_headers: Whether to include headers in the request log. Defaults to False to defensively handle cases where request headers might contain sensitive data (e.g. tokens).
    """
    request_log = RoundTrip(response, debug_headers=debug_headers, debug_truncate_bytes=10 * 1024).generate()
    return (
        "This is likely a bug in the Databricks SDK for Python or the underlying "
        "API. Please report this issue with the following debugging information to the SDK issue tracker at "
        f"https://github.com/databricks/databricks-sdk-go/issues. Request log:```{request_log}```"
    )


class _Parser:
    """
    A parser for errors from the Databricks REST API. It attempts to deserialize an error using a sequence of
    deserializers, and then customizes the deserialized error using a sequence of customizers. If the error cannot be
    deserialized, it returns a generic error with debugging information and instructions to report the issue to the SDK
    issue tracker.
    """

    def __init__(
        self,
        extra_error_parsers: List[_ErrorDeserializer] = [],
        extra_error_customizers: List[_ErrorCustomizer] = [],
        debug_headers: bool = False,
    ):
        self._error_parsers = _error_deserializers + (extra_error_parsers if extra_error_parsers is not None else [])
        self._error_customizers = _error_customizers + (
            extra_error_customizers if extra_error_customizers is not None else []
        )
        self._debug_headers = debug_headers

    def get_api_error(self, response: requests.Response) -> Optional[DatabricksError]:
        """
        Handles responses from the REST API and returns a DatabricksError if the response indicates an error.
        :param response: The response from the REST API.
        :return: A DatabricksError if the response indicates an error, otherwise None.
        """
        if not response.ok:
            content = response.content
            for parser in self._error_parsers:
                try:
                    error_args = parser.deserialize_error(response, content)
                    if error_args:
                        for customizer in self._error_customizers:
                            customizer.customize_error(response, error_args)
                        return _error_mapper(response, error_args)
                except Exception as e:
                    logging.debug(
                        f"Error parsing response with {parser}, continuing",
                        exc_info=e,
                    )
            return _error_mapper(
                response,
                {"message": "unable to parse response. " + _unknown_error(response, self._debug_headers)},
            )

        # Private link failures happen via a redirect to the login page. From a requests-perspective, the request
        # is successful, but the response is not what we expect. We need to handle this case separately.
        if _is_private_link_redirect(response):
            return _get_private_link_validation_error(response.url)

        return None
