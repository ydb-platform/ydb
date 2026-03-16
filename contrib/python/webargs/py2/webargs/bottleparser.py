# -*- coding: utf-8 -*-
"""Bottle request argument parsing module.

Example: ::

    from bottle import route, run
    from marshmallow import fields
    from webargs.bottleparser import use_args

    hello_args = {
        'name': fields.Str(missing='World')
    }
    @route('/', method='GET', apply=use_args(hello_args))
    def index(args):
        return 'Hello ' + args['name']

    if __name__ == '__main__':
        run(debug=True)
"""
import bottle

from webargs import core
from webargs.core import json


class BottleParser(core.Parser):
    """Bottle.py request argument parser."""

    def parse_querystring(self, req, name, field):
        """Pull a querystring value from the request."""
        return core.get_value(req.query, name, field)

    def parse_form(self, req, name, field):
        """Pull a form value from the request."""
        # For consistency with other parsers' behavior, don't attempt to
        #  parse if content-type is mismatched.
        #  TODO: Make this check more specific
        if core.is_json(req.content_type):
            return core.missing
        return core.get_value(req.forms, name, field)

    def parse_json(self, req, name, field):
        """Pull a json value from the request."""
        json_data = self._cache.get("json")
        if json_data is None:
            try:
                self._cache["json"] = json_data = req.json
            except AttributeError:
                return core.missing
            except json.JSONDecodeError as e:
                if e.doc == "":
                    return core.missing
                else:
                    return self.handle_invalid_json_error(e, req)
            except UnicodeDecodeError as e:
                return self.handle_invalid_json_error(e, req)

            if json_data is None:
                return core.missing
        return core.get_value(json_data, name, field, allow_many_nested=True)

    def parse_headers(self, req, name, field):
        """Pull a value from the header data."""
        return core.get_value(req.headers, name, field)

    def parse_cookies(self, req, name, field):
        """Pull a value from the cookiejar."""
        return req.get_cookie(name)

    def parse_files(self, req, name, field):
        """Pull a file from the request."""
        return core.get_value(req.files, name, field)

    def handle_error(self, error, req, schema, error_status_code, error_headers):
        """Handles errors during parsing. Aborts the current request with a
        400 error.
        """
        status_code = error_status_code or self.DEFAULT_VALIDATION_STATUS
        raise bottle.HTTPError(
            status=status_code,
            body=error.messages,
            headers=error_headers,
            exception=error,
        )

    def handle_invalid_json_error(self, error, req, *args, **kwargs):
        raise bottle.HTTPError(
            status=400, body={"json": ["Invalid JSON body."]}, exception=error
        )

    def get_default_request(self):
        """Override to use bottle's thread-local request object by default."""
        return bottle.request


parser = BottleParser()
use_args = parser.use_args
use_kwargs = parser.use_kwargs
