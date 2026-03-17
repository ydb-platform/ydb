# -*- coding: utf-8 -*-
"""Django request argument parsing.

Example usage: ::

    from django.views.generic import View
    from django.http import HttpResponse
    from marshmallow import fields
    from webargs.djangoparser import use_args

    hello_args = {
        'name': fields.Str(missing='World')
    }

    class MyView(View):

        @use_args(hello_args)
        def get(self, args, request):
            return HttpResponse('Hello ' + args['name'])
"""
from webargs import core
from webargs.core import json


class DjangoParser(core.Parser):
    """Django request argument parser.

    .. warning::

        :class:`DjangoParser` does not override
        :meth:`handle_error <webargs.core.Parser.handle_error>`, so your Django
        views are responsible for catching any :exc:`ValidationErrors` raised by
        the parser and returning the appropriate `HTTPResponse`.
    """

    def parse_querystring(self, req, name, field):
        """Pull the querystring value from the request."""
        return core.get_value(req.GET, name, field)

    def parse_form(self, req, name, field):
        """Pull the form value from the request."""
        return core.get_value(req.POST, name, field)

    def parse_json(self, req, name, field):
        """Pull a json value from the request body."""
        json_data = self._cache.get("json")
        if json_data is None:
            if not core.is_json(req.content_type):
                return core.missing

            try:
                self._cache["json"] = json_data = core.parse_json(req.body)
            except AttributeError:
                return core.missing
            except json.JSONDecodeError as e:
                if e.doc == "":
                    return core.missing
                else:
                    return self.handle_invalid_json_error(e, req)
        return core.get_value(json_data, name, field, allow_many_nested=True)

    def parse_cookies(self, req, name, field):
        """Pull the value from the cookiejar."""
        return core.get_value(req.COOKIES, name, field)

    def parse_headers(self, req, name, field):
        raise NotImplementedError(
            "Header parsing not supported by {0}".format(self.__class__.__name__)
        )

    def parse_files(self, req, name, field):
        """Pull a file from the request."""
        return core.get_value(req.FILES, name, field)

    def get_request_from_view_args(self, view, args, kwargs):
        # The first argument is either `self` or `request`
        try:  # self.request
            return args[0].request
        except AttributeError:  # first arg is request
            return args[0]

    def handle_invalid_json_error(self, error, req, *args, **kwargs):
        raise error


parser = DjangoParser()
use_args = parser.use_args
use_kwargs = parser.use_kwargs
