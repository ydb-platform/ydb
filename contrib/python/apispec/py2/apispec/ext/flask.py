# -*- coding: utf-8 -*-
"""Flask plugin. Includes a path helper that allows you to pass a view
function to `add_path`. Inspects URL rules and view docstrings.

Passing a view function::

    from flask import Flask

    app = Flask(__name__)

    @app.route('/gists/<gist_id>')
    def gist_detail(gist_id):
        '''Gist detail view.
        ---
        x-extension: metadata
        get:
            responses:
                200:
                    schema:
                        $ref: '#/definitions/Gist'
        '''
        return 'detail for gist {}'.format(gist_id)

    with app.test_request_context():
        spec.add_path(view=gist_detail)
    print(spec.to_dict()['paths'])
    # {'/gists/{gist_id}': {'get': {'responses': {200: {'schema': {'$ref': '#/definitions/Gist'}}}},
    #                  'x-extension': 'metadata'}}

Passing a method view function::

    from flask import Flask
    from flask.views import MethodView

    app = Flask(__name__)

    class GistApi(MethodView):
        '''Gist API.
        ---
        x-extension: metadata
        '''
        def get(self):
           '''Gist view
           ---
           responses:
               200:
                   schema:
                       $ref: '#/definitions/Gist'
           '''
           pass

        def post(self):
           pass

    method_view = GistApi.as_view('gists')
    app.add_url_rule("/gists", view_func=method_view)
    with app.test_request_context():
        spec.add_path(view=method_view)
    print(spec.to_dict()['paths'])
    # {'/gists': {'get': {'responses': {200: {'schema': {'$ref': '#/definitions/Gist'}}}},
    #             'post': {},
    #             'x-extension': 'metadata'}}


"""
from __future__ import absolute_import
import re

try:
    from urllib.parse import urljoin
except ImportError:
    from urlparse import urljoin

from flask import current_app
from flask.views import MethodView

from apispec.compat import iteritems
from apispec import Path, BasePlugin, utils
from apispec.exceptions import APISpecError


# from flask-restplus
RE_URL = re.compile(r'<(?:[^:<>]+:)?([^<>]+)>')


class FlaskPlugin(BasePlugin):
    """APISpec plugin for Flask"""

    @staticmethod
    def flaskpath2openapi(path):
        """Convert a Flask URL rule to an OpenAPI-compliant path.

        :param str path: Flask path template.
        """
        return RE_URL.sub(r'{\1}', path)

    @staticmethod
    def _rule_for_view(view):
        view_funcs = current_app.view_functions
        endpoint = None
        for ept, view_func in iteritems(view_funcs):
            if view_func == view:
                endpoint = ept
        if not endpoint:
            raise APISpecError('Could not find endpoint for view {0}'.format(view))

        # WARNING: Assume 1 rule per view function for now
        rule = current_app.url_map._rules_by_endpoint[endpoint][0]
        return rule

    def path_helper(self, view, **kwargs):
        """Path helper that allows passing a Flask view function."""
        rule = self._rule_for_view(view)
        path = self.flaskpath2openapi(rule.rule)
        app_root = current_app.config['APPLICATION_ROOT'] or '/'
        path = urljoin(app_root.rstrip('/') + '/', path.lstrip('/'))
        operations = utils.load_operations_from_docstring(view.__doc__)
        path = Path(path=path, operations=operations)
        if hasattr(view, 'view_class') and issubclass(view.view_class, MethodView):
            operations = {}
            for method in view.methods:
                if method in rule.methods:
                    method_name = method.lower()
                    method = getattr(view.view_class, method_name)
                    docstring_yaml = utils.load_yaml_from_docstring(method.__doc__)
                    operations[method_name] = docstring_yaml or dict()
            path.operations.update(operations)
        return path


# Deprecated interface
def setup(spec):
    """Setup for the plugin.

    .. deprecated:: 0.39.0
        Use FlaskPlugin class.
    """
    plugin = FlaskPlugin()
    plugin.init_spec(spec)
    spec.plugins.append(plugin)
