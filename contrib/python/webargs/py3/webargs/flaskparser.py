# -*- coding: utf-8 -*-
"""Flask request argument parsing module.

Example: ::

    from flask import Flask

    from webargs import fields
    from webargs.flaskparser import use_args

    app = Flask(__name__)

    hello_args = {
        'name': fields.Str(required=True)
    }

    @app.route('/')
    @use_args(hello_args)
    def index(args):
        return 'Hello ' + args['name']
"""
import flask
from werkzeug.exceptions import HTTPException

from webargs import core
from webargs.core import json


def abort(http_status_code, exc=None, **kwargs):
    """Raise a HTTPException for the given http_status_code. Attach any keyword
    arguments to the exception for later processing.

    From Flask-Restful. See NOTICE file for license information.
    """
    try:
        flask.abort(http_status_code)
    except HTTPException as err:
        err.data = kwargs
        err.exc = exc
        raise err


def is_json_request(req):
    return core.is_json(req.mimetype)


class FlaskParser(core.Parser):
    """Flask request argument parser."""

    __location_map__ = dict(
        view_args="parse_view_args",
        path="parse_view_args",
        **core.Parser.__location_map__
    )

    def parse_view_args(self, req, name, field):
        """Pull a value from the request's ``view_args``."""
        return core.get_value(req.view_args, name, field)

    def parse_json(self, req, name, field):
        """Pull a json value from the request."""
        json_data = self._cache.get("json")
        if json_data is None:
            if not is_json_request(req):
                return core.missing

            # We decode the json manually here instead of
            # using req.get_json() so that we can handle
            # JSONDecodeErrors consistently
            data = req.get_data(cache=True)
            try:
                self._cache["json"] = json_data = core.parse_json(data)
            except json.JSONDecodeError as e:
                if e.doc == "":
                    return core.missing
                else:
                    return self.handle_invalid_json_error(e, req)
        return core.get_value(json_data, name, field, allow_many_nested=True)

    def parse_querystring(self, req, name, field):
        """Pull a querystring value from the request."""
        return core.get_value(req.args, name, field)

    def parse_form(self, req, name, field):
        """Pull a form value from the request."""
        try:
            return core.get_value(req.form, name, field)
        except AttributeError:
            pass
        return core.missing

    def parse_headers(self, req, name, field):
        """Pull a value from the header data."""
        return core.get_value(req.headers, name, field)

    def parse_cookies(self, req, name, field):
        """Pull a value from the cookiejar."""
        return core.get_value(req.cookies, name, field)

    def parse_files(self, req, name, field):
        """Pull a file from the request."""
        return core.get_value(req.files, name, field)

    def handle_error(self, error, req, schema, error_status_code, error_headers):
        """Handles errors during parsing. Aborts the current HTTP request and
        responds with a 422 error.
        """
        status_code = error_status_code or self.DEFAULT_VALIDATION_STATUS
        abort(
            status_code,
            exc=error,
            messages=error.messages,
            schema=schema,
            headers=error_headers,
        )

    def handle_invalid_json_error(self, error, req, *args, **kwargs):
        abort(400, exc=error, messages={"json": ["Invalid JSON body."]})

    def get_default_request(self):
        """Override to use Flask's thread-local request objec by default"""
        return flask.request


parser = FlaskParser()
use_args = parser.use_args
use_kwargs = parser.use_kwargs
