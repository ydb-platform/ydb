# -*- coding: utf-8 -*-
"""Falcon request argument parsing module.
"""
import falcon
from falcon.util.uri import parse_query_string

from webargs import core
from webargs.core import json

HTTP_422 = "422 Unprocessable Entity"

# Mapping of int status codes to string status
status_map = {422: HTTP_422}


# Collect all exceptions from falcon.status_codes
def _find_exceptions():
    for name in filter(lambda n: n.startswith("HTTP"), dir(falcon.status_codes)):
        status = getattr(falcon.status_codes, name)
        status_code = int(status.split(" ")[0])
        status_map[status_code] = status


_find_exceptions()
del _find_exceptions


def is_json_request(req):
    content_type = req.get_header("Content-Type")
    return content_type and core.is_json(content_type)


def parse_json_body(req):
    if req.content_length in (None, 0):
        # Nothing to do
        return {}
    if is_json_request(req):
        body = req.stream.read()
        if body:
            try:
                return core.parse_json(body)
            except json.JSONDecodeError as e:
                if e.doc == "":
                    return core.missing
                else:
                    raise
    return {}


# NOTE: Adapted from falcon.request.Request._parse_form_urlencoded
def parse_form_body(req):
    if (
        req.content_type is not None
        and "application/x-www-form-urlencoded" in req.content_type
    ):
        body = req.stream.read()
        try:
            body = body.decode("ascii")
        except UnicodeDecodeError:
            body = None
            req.log_error(
                "Non-ASCII characters found in form body "
                "with Content-Type of "
                "application/x-www-form-urlencoded. Body "
                "will be ignored."
            )

        if body:
            return parse_query_string(
                body, keep_blank_qs_values=req.options.keep_blank_qs_values
            )
    return {}


class HTTPError(falcon.HTTPError):
    """HTTPError that stores a dictionary of validation error messages.
    """

    def __init__(self, status, errors, *args, **kwargs):
        self.errors = errors
        super(HTTPError, self).__init__(status, *args, **kwargs)

    def to_dict(self, *args, **kwargs):
        """Override `falcon.HTTPError` to include error messages in responses."""
        ret = super(HTTPError, self).to_dict(*args, **kwargs)
        if self.errors is not None:
            ret["errors"] = self.errors
        return ret


class FalconParser(core.Parser):
    """Falcon request argument parser."""

    def parse_querystring(self, req, name, field):
        """Pull a querystring value from the request."""
        return core.get_value(req.params, name, field)

    def parse_form(self, req, name, field):
        """Pull a form value from the request.

        .. note::

            The request stream will be read and left at EOF.
        """
        form = self._cache.get("form")
        if form is None:
            self._cache["form"] = form = parse_form_body(req)
        return core.get_value(form, name, field)

    def parse_json(self, req, name, field):
        """Pull a JSON body value from the request.

        .. note::

            The request stream will be read and left at EOF.
        """
        json_data = self._cache.get("json_data")
        if json_data is None:
            try:
                self._cache["json_data"] = json_data = parse_json_body(req)
            except json.JSONDecodeError as e:
                return self.handle_invalid_json_error(e, req)
        return core.get_value(json_data, name, field, allow_many_nested=True)

    def parse_headers(self, req, name, field):
        """Pull a header value from the request."""
        # Use req.get_headers rather than req.headers for performance
        return req.get_header(name, required=False) or core.missing

    def parse_cookies(self, req, name, field):
        """Pull a cookie value from the request."""
        cookies = self._cache.get("cookies")
        if cookies is None:
            self._cache["cookies"] = cookies = req.cookies
        return core.get_value(cookies, name, field)

    def get_request_from_view_args(self, view, args, kwargs):
        """Get request from a resource method's arguments. Assumes that
        request is the second argument.
        """
        req = args[1]
        assert isinstance(req, falcon.Request), "Argument is not a falcon.Request"
        return req

    def parse_files(self, req, name, field):
        raise NotImplementedError(
            "Parsing files not yet supported by {0}".format(self.__class__.__name__)
        )

    def handle_error(self, error, req, schema, error_status_code, error_headers):
        """Handles errors during parsing."""
        status = status_map.get(error_status_code or self.DEFAULT_VALIDATION_STATUS)
        if status is None:
            raise LookupError("Status code {0} not supported".format(error_status_code))
        raise HTTPError(status, errors=error.messages, headers=error_headers)

    def handle_invalid_json_error(self, error, req, *args, **kwargs):
        status = status_map[400]
        messages = {"json": ["Invalid JSON body."]}
        raise HTTPError(status, errors=messages)


parser = FalconParser()
use_args = parser.use_args
use_kwargs = parser.use_kwargs
