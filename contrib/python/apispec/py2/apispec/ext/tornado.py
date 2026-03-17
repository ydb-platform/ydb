# -*- coding: utf-8 -*-
"""Tornado plugin. Includes a path helper that allows you to pass an urlspec (path-handler pair)
object to `add_path`.
::

    from pprint import pprint

    from tornado.web import RequestHandler

    class HelloHandler(RequestHandler):
        def get(self):
            '''Get a greeting endpoint.
            ---
            description: Get a greeting
            responses:
                200:
                    description: A greeting to the client
                    schema:
                        $ref: '#/definitions/Greeting'
            '''
            self.write("hello")

    urlspec = (r'/hello', HelloHandler)
    spec.add_path(urlspec=urlspec)
    pprint(spec.to_dict()['paths'])
    # {'/hello': {'get': {'description': 'Get a greeting',
    #                     'responses': {200: {'description': 'A greeting to the '
    #                                                     'client',
    #                                         'schema': {'$ref': '#/definitions/Greeting'}}}}}}

"""
from __future__ import absolute_import
import inspect
import sys
from tornado.web import URLSpec

from apispec import Path, BasePlugin, utils
from apispec.exceptions import APISpecError


class TornadoPlugin(BasePlugin):
    """APISpec plugin for Tornado"""

    @staticmethod
    def _operations_from_methods(handler_class):
        """Generator of operations described in handler's http methods

        :param handler_class:
        :type handler_class: RequestHandler descendant
        """
        for httpmethod in utils.PATH_KEYS:
            method = getattr(handler_class, httpmethod)
            operation_data = utils.load_yaml_from_docstring(method.__doc__)
            if operation_data:
                operation = {httpmethod: operation_data}
                yield operation

    @staticmethod
    def tornadopath2openapi(urlspec, method):
        """Convert Tornado URLSpec to OpenAPI-compliant path.

        :param urlspec:
        :type urlspec: URLSpec
        :param method: Handler http method
        :type method: function
        """
        if sys.version_info >= (3, 3):
            args = list(inspect.signature(method).parameters.keys())[1:]
        else:
            if getattr(method, '__tornado_coroutine__', False):
                method = method.__wrapped__
            args = inspect.getargspec(method).args[1:]
        params = tuple('{{{}}}'.format(arg) for arg in args)
        try:
            path_tpl = urlspec.matcher._path
        except AttributeError:  # tornado<4.5
            path_tpl = urlspec._path
        path = (path_tpl % params)
        if path.count('/') > 1:
            path = path.rstrip('/?*')
        return path

    @staticmethod
    def _extensions_from_handler(handler_class):
        """Returns extensions dict from handler docstring

        :param handler_class:
        :type handler_class: RequestHandler descendant
        """
        extensions = utils.load_yaml_from_docstring(handler_class.__doc__) or {}
        return extensions

    def path_helper(self, urlspec, operations, **kwargs):
        """Path helper that allows passing a Tornado URLSpec or tuple."""
        if not isinstance(urlspec, URLSpec):
            urlspec = URLSpec(*urlspec)
        if not operations:
            operations = {}
            for operation in self._operations_from_methods(urlspec.handler_class):
                operations.update(operation)
        if not operations:
            raise APISpecError(
                'Could not find endpoint for urlspec {0}'.format(urlspec),
            )
        params_method = getattr(urlspec.handler_class, list(operations.keys())[0])
        path = self.tornadopath2openapi(urlspec, params_method)
        extensions = self._extensions_from_handler(urlspec.handler_class)
        operations.update(extensions)
        return Path(path=path, operations=operations)


# Deprecated interface
def setup(spec):
    """Setup for the plugin.

    .. deprecated:: 0.39.0
        Use TornadoPlugin class.
    """
    plugin = TornadoPlugin()
    plugin.init_spec(spec)
    spec.plugins.append(plugin)
