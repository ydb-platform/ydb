# -*- coding: utf-8 -*-
"""Bottle plugin. Includes a path helper that allows you to pass a view function
to `add_path`.
::

    from bottle import route, default_app
    app = default_app()
    @route('/gists/<gist_id>')
    def gist_detail(gist_id):
        '''Gist detail view.
        ---
        get:
            responses:
                200:
                    schema:
                        $ref: '#/definitions/Gist'
        '''
        return 'detail for gist {}'.format(gist_id)

    spec.add_path(view=gist_detail)
    print(spec.to_dict()['paths'])
    # {'/gists/{gist_id}': {'get': {'responses': {200: {'schema': {'$ref': '#/definitions/Gist'}}}}}}
"""
from __future__ import absolute_import
import re

from bottle import default_app

from apispec import Path, BasePlugin, utils
from apispec.exceptions import APISpecError


RE_URL = re.compile(r'<(?:[^:<>]+:)?([^<>]+)>')

_default_app = default_app()


class BottlePlugin(BasePlugin):
    """APISpec plugin for Bottle"""

    @staticmethod
    def bottle_path_to_openapi(path):
        return RE_URL.sub(r'{\1}', path)

    @staticmethod
    def _route_for_view(app, view):
        endpoint = None
        for route in app.routes:
            if route._context['callback'] == view:
                endpoint = route
                break
        if not endpoint:
            raise APISpecError('Could not find endpoint for route {0}'.format(view))
        return endpoint

    def path_helper(self, view, operations, **kwargs):
        """Path helper that allows passing a bottle view function."""
        operations = utils.load_operations_from_docstring(view.__doc__)
        app = kwargs.get('app', _default_app)
        route = self._route_for_view(app, view)
        bottle_path = self.bottle_path_to_openapi(route.rule)
        return Path(path=bottle_path, operations=operations)


# Deprecated interface
def setup(spec):
    """Setup for the plugin.

    .. deprecated:: 0.39.0
        Use BottlePlugin class.
    """
    plugin = BottlePlugin()
    plugin.init_spec(spec)
    spec.plugins.append(plugin)
