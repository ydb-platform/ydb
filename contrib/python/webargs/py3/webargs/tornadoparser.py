# -*- coding: utf-8 -*-
"""Tornado request argument parsing module.

Example: ::

    import tornado.web
    from marshmallow import fields
    from webargs.tornadoparser import use_args

    class HelloHandler(tornado.web.RequestHandler):

        @use_args({'name': fields.Str(missing='World')})
        def get(self, args):
            response = {'message': 'Hello {}'.format(args['name'])}
            self.write(response)
"""
import tornado.web
from tornado.escape import _unicode

from webargs import core
from webargs.compat import basestring
from webargs.core import json


class HTTPError(tornado.web.HTTPError):
    """`tornado.web.HTTPError` that stores validation errors."""

    def __init__(self, *args, **kwargs):
        self.messages = kwargs.pop("messages", {})
        self.headers = kwargs.pop("headers", None)
        super(HTTPError, self).__init__(*args, **kwargs)


def parse_json_body(req):
    """Return the decoded JSON body from the request."""
    content_type = req.headers.get("Content-Type")
    if content_type and core.is_json(content_type):
        try:
            return core.parse_json(req.body)
        except TypeError:
            pass
        except json.JSONDecodeError as e:
            if e.doc == "":
                return core.missing
            else:
                raise
    return {}


# From tornado.web.RequestHandler.decode_argument
def decode_argument(value, name=None):
    """Decodes an argument from the request.
    """
    try:
        return _unicode(value)
    except UnicodeDecodeError:
        raise HTTPError(400, "Invalid unicode in %s: %r" % (name or "url", value[:40]))


def get_value(d, name, field):
    """Handle gets from 'multidicts' made of lists

    It handles cases: ``{"key": [value]}`` and ``{"key": value}``
    """
    multiple = core.is_multiple(field)
    value = d.get(name, core.missing)
    if value is core.missing:
        return core.missing
    if multiple and value is not core.missing:
        return [
            decode_argument(v, name) if isinstance(v, basestring) else v for v in value
        ]
    ret = value
    if value and isinstance(value, (list, tuple)):
        ret = value[0]
    if isinstance(ret, basestring):
        return decode_argument(ret, name)
    else:
        return ret


class TornadoParser(core.Parser):
    """Tornado request argument parser."""

    def parse_json(self, req, name, field):
        """Pull a json value from the request."""
        json_data = self._cache.get("json")
        if json_data is None:
            try:
                self._cache["json"] = json_data = parse_json_body(req)
            except json.JSONDecodeError as e:
                return self.handle_invalid_json_error(e, req)
            if json_data is None:
                return core.missing
        return core.get_value(json_data, name, field, allow_many_nested=True)

    def parse_querystring(self, req, name, field):
        """Pull a querystring value from the request."""
        return get_value(req.query_arguments, name, field)

    def parse_form(self, req, name, field):
        """Pull a form value from the request."""
        return get_value(req.body_arguments, name, field)

    def parse_headers(self, req, name, field):
        """Pull a value from the header data."""
        return get_value(req.headers, name, field)

    def parse_cookies(self, req, name, field):
        """Pull a value from the header data."""
        cookie = req.cookies.get(name)

        if cookie is not None:
            return [cookie.value] if core.is_multiple(field) else cookie.value
        else:
            return [] if core.is_multiple(field) else None

    def parse_files(self, req, name, field):
        """Pull a file from the request."""
        return get_value(req.files, name, field)

    def handle_error(self, error, req, schema, error_status_code, error_headers):
        """Handles errors during parsing. Raises a `tornado.web.HTTPError`
        with a 400 error.
        """
        status_code = error_status_code or self.DEFAULT_VALIDATION_STATUS
        if status_code == 422:
            reason = "Unprocessable Entity"
        else:
            reason = None
        raise HTTPError(
            status_code,
            log_message=str(error.messages),
            reason=reason,
            messages=error.messages,
            headers=error_headers,
        )

    def handle_invalid_json_error(self, error, req, *args, **kwargs):
        raise HTTPError(
            400,
            log_message="Invalid JSON body.",
            reason="Bad Request",
            messages={"json": ["Invalid JSON body."]},
        )

    def get_request_from_view_args(self, view, args, kwargs):
        return args[0].request


parser = TornadoParser()
use_args = parser.use_args
use_kwargs = parser.use_kwargs
