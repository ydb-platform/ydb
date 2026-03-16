# -*- coding: utf-8 -*-
"""Webapp2 request argument parsing module.

Example: ::

    import webapp2

    from marshmallow import fields
    from webargs.webobparser import use_args

    hello_args = {
        'name': fields.Str(missing='World')
    }

    class MainPage(webapp2.RequestHandler):

        @use_args(hello_args)
        def get_args(self, args):
            self.response.write('Hello, {name}!'.format(name=args['name']))

        @use_kwargs(hello_args)
        def get_kwargs(self, name=None):
            self.response.write('Hello, {name}!'.format(name=name))

    app = webapp2.WSGIApplication([
        webapp2.Route(r'/hello', MainPage, handler_method='get_args'),
        webapp2.Route(r'/hello_dict', MainPage, handler_method='get_kwargs'),
    ], debug=True)
"""
import webapp2
import webob.multidict

from webargs import core
from webargs.core import json


class Webapp2Parser(core.Parser):
    """webapp2 request argument parser."""

    def parse_json(self, req, name, field):
        """Pull a json value from the request."""
        json_data = self._cache.get("json")
        if json_data is None:
            if not core.is_json(req.content_type):
                return core.missing

            try:
                self._cache["json"] = json_data = core.parse_json(req.body)
            except json.JSONDecodeError as e:
                if e.doc == "":
                    return core.missing
                else:
                    raise
        return core.get_value(json_data, name, field, allow_many_nested=True)

    def parse_querystring(self, req, name, field):
        """Pull a querystring value from the request."""
        return core.get_value(req.GET, name, field)

    def parse_form(self, req, name, field):
        """Pull a form value from the request."""
        return core.get_value(req.POST, name, field)

    def parse_cookies(self, req, name, field):
        """Pull the value from the cookiejar."""
        return core.get_value(req.cookies, name, field)

    def parse_headers(self, req, name, field):
        """Pull a value from the header data."""
        return core.get_value(req.headers, name, field)

    def parse_files(self, req, name, field):
        """Pull a file from the request."""
        files = ((k, v) for k, v in req.POST.items() if hasattr(v, "file"))
        return core.get_value(webob.multidict.MultiDict(files), name, field)

    def get_default_request(self):
        return webapp2.get_request()


parser = Webapp2Parser()
use_args = parser.use_args
use_kwargs = parser.use_kwargs
