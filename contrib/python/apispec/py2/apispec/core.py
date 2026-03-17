# -*- coding: utf-8 -*-
"""Core apispec classes and functions."""
import re
import warnings
from collections import OrderedDict
import copy

import yaml

from apispec.compat import iterkeys, iteritems, PY2, unicode
from apispec.lazy_dict import LazyDict
from .exceptions import PluginError, APISpecError, PluginMethodNotImplementedError
from .utils import OpenAPIVersion

VALID_METHODS = [
    'get',
    'post',
    'put',
    'patch',
    'delete',
    'head',
    'options',
]


def clean_operations(operations, openapi_major_version):
    """Ensure that all parameters with "in" equal to "path" are also required
    as required by the OpenAPI specification, as well as normalizing any
    references to global parameters.

    See https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#parameterObject.

    :param dict operations: Dict mapping status codes to operations
    :param int openapi_major_version: The major version of the OpenAPI standard
        to use. Supported values are 2 and 3.
    """
    def get_ref(param, openapi_major_version):
        if isinstance(param, dict):
            return param

        ref_paths = {
            2: 'parameters',
            3: 'components/parameters',
        }
        ref_path = ref_paths[openapi_major_version]
        return {'$ref': '#/{0}/{1}'.format(ref_path, param)}

    for operation in (operations or {}).values():
        if 'parameters' in operation:
            parameters = operation.get('parameters')
            for parameter in parameters:
                if (
                    isinstance(parameter, dict) and
                    'in' in parameter and parameter['in'] == 'path'
                ):
                    parameter['required'] = True
            operation['parameters'] = [
                get_ref(p, openapi_major_version)
                for p in parameters
            ]


class Path(object):
    """Represents an OpenAPI Path object.

    https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#pathsObject

    :param str path: The path template, e.g. ``"/pet/{petId}"``
    :param str method: The HTTP method.
    :param dict operation: The operation object, as a `dict`. See
        https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#operationObject
    :param str|OpenAPIVersion openapi_version: The OpenAPI version to use.
        Should be in the form '2.x' or '3.x.x' to comply with the OpenAPI standard.
    """
    def __init__(self, path=None, operations=None, openapi_version='2.0'):
        self.path = path
        operations = operations or OrderedDict()
        openapi_version = OpenAPIVersion(openapi_version)
        clean_operations(operations, openapi_version.major)
        invalid = {key for key in
                   set(iterkeys(operations)) - set(VALID_METHODS)
                   if not key.startswith('x-')}
        if invalid:
            raise APISpecError(
                'One or more HTTP methods are invalid: {0}'.format(', '.join(invalid)),
            )
        self.operations = operations

    def to_dict(self):
        if not self.path:
            raise APISpecError('Path template is not specified')
        return {
            self.path: self.operations,
        }

    def update(self, path):
        if path.path:
            self.path = path.path
        self.operations.update(path.operations)


class APISpec(object):
    """Stores metadata that describes a RESTful API using the OpenAPI specification.

    :param str title: API title
    :param str version: API version
    :param tuple plugins: Import paths to plugins.
    :param dict info: Optional dict to add to `info`
        See https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#infoObject
    :param callable schema_name_resolver: Callable to generate the schema definition name.
        This parameter is deprecated. It is now a parameter of MarshmallowPlugin.
    :param str|OpenAPIVersion openapi_version: The OpenAPI version to use.
        Should be in the form '2.x' or '3.x.x' to comply with the OpenAPI standard.
    :param dict options: Optional top-level keys
        See https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#swagger-object
    """
    def __init__(
        self, title, version, plugins=(), info=None,
        schema_name_resolver=None, openapi_version='2.0', **options
    ):
        self.info = {
            'title': title,
            'version': version,
        }
        self.info.update(info or {})

        self.openapi_version = OpenAPIVersion(openapi_version)

        self.options = options
        if schema_name_resolver is not None:
            warnings.warn(
                'schema_name_resolver parameter is deprecated. '
                'It is now a parameter of MarshmallowPlugin.',
                DeprecationWarning,
            )
        self.schema_name_resolver = schema_name_resolver
        # Metadata
        self._definitions = {}
        self._parameters = {}
        self._tags = []
        self._paths = OrderedDict()

        # Plugins
        plugins = list(plugins)  # Cast to list in case a generator is passed
        # Backward compatibility: plugins can be passed as objects or strings
        self.plugins = list(p for p in plugins if not isinstance(p, str))
        for plugin in self.plugins:
            plugin.init_spec(self)

        # Deprecated interface
        # Plugins and helpers
        self.old_plugins = {}
        self._definition_helpers = []
        self._path_helpers = []
        self._operation_helpers = []
        # {'get': {200: [my_helper]}}
        self._response_helpers = {}
        old_plugins = list(p for p in plugins if isinstance(p, str))
        for plugin_path in old_plugins:
            self.setup_plugin(plugin_path)

    def to_dict(self):
        ret = {
            'info': self.info,
            'paths': self._paths,
            'tags': self._tags,
        }

        if self.openapi_version.major == 2:
            ret['swagger'] = self.openapi_version.vstring
            ret['definitions'] = self._definitions
            ret['parameters'] = self._parameters
            ret.update(self.options)

        elif self.openapi_version.major == 3:
            ret['openapi'] = self.openapi_version.vstring
            options = copy.deepcopy(self.options)
            components = options.pop('components', {})

            # deep update components object
            definitions = components.pop('schemas', {})
            definitions.update(self._definitions)
            parameters = components.pop('parameters', {})
            parameters.update(self._parameters)

            ret['components'] = dict(
                schemas=definitions,
                parameters=parameters,
                **components
            )
            ret.update(options)

        return ret

    def to_yaml(self):
        return yaml.dump(self.to_dict(), Dumper=YAMLDumper)

    def add_parameter(self, param_id, location, **kwargs):
        """ Add a parameter which can be referenced.

        :param str param_id: identifier by which parameter may be referenced.
        :param str location: location of the parameter.
        :param dict kwargs: parameter fields.
        """
        if 'name' not in kwargs:
            kwargs['name'] = param_id
        kwargs['in'] = location
        self._parameters[param_id] = kwargs

    def add_tag(self, tag):
        """ Store information about a tag.

        :param dict tag: the dictionary storing information about the tag.
        """
        self._tags.append(tag)

    def add_path(self, path=None, operations=None, **kwargs):
        """Add a new path object to the spec.

        https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#pathsObject

        :param str|Path|None path: URL Path component or Path instance
        :param dict|None operations: describes the http methods and options for `path`
        :param dict kwargs: parameters used by any path helpers see :meth:`register_path_helper`
        """
        def normalize_path(path):
            if path and 'basePath' in self.options:
                pattern = '^{0}'.format(re.escape(self.options['basePath']))
                path = re.sub(pattern, '', path)
            return path

        if isinstance(path, Path):
            path.path = normalize_path(path.path)
            if operations:
                path.operations.update(operations)
        else:
            path = Path(
                path=normalize_path(path),
                operations=operations,
                openapi_version=self.openapi_version,
            )

        # Execute path helpers
        for plugin in self.plugins:
            try:
                ret = plugin.path_helper(path=path, operations=path.operations, **kwargs)
            except PluginMethodNotImplementedError:
                continue
            if isinstance(ret, Path):
                ret.path = normalize_path(ret.path)
                path.update(ret)
        # Deprecated interface
        for func in self._path_helpers:
            try:
                ret = func(
                    self, path=path, operations=path.operations, **kwargs
                )
            except TypeError:
                continue
            if isinstance(ret, Path):
                ret.path = normalize_path(ret.path)
                path.update(ret)
        if not path.path:
            raise APISpecError('Path template is not specified')

        # Execute operation helpers
        for plugin in self.plugins:
            try:
                plugin.operation_helper(path=path, operations=path.operations, **kwargs)
            except PluginMethodNotImplementedError:
                continue
        # Deprecated interface
        for func in self._operation_helpers:
            func(self, path=path, operations=path.operations, **kwargs)

        # Execute response helpers
        # TODO: cache response helpers output for each (method, status_code) couple
        for method, operation in iteritems(path.operations):
            if method in VALID_METHODS and 'responses' in operation:
                for status_code, response in iteritems(operation['responses']):
                    for plugin in self.plugins:
                        try:
                            response.update(plugin.response_helper(method, status_code, **kwargs) or {})
                        except PluginMethodNotImplementedError:
                            continue
        # Deprecated interface
        # Rule is that method + http status exist in both operations and helpers
        methods = set(iterkeys(path.operations)) & set(iterkeys(self._response_helpers))
        for method in methods:
            responses = path.operations[method]['responses']
            statuses = set(iterkeys(responses)) & set(iterkeys(self._response_helpers[method]))
            for status_code in statuses:
                for func in self._response_helpers[method][status_code]:
                    responses[status_code].update(
                        func(self, **kwargs),
                    )

        self._paths.setdefault(path.path, path.operations).update(path.operations)

    def definition(
        self, name, properties=None, enum=None, description=None, extra_fields=None,
        **kwargs
    ):
        """Add a new definition to the spec.

        .. note::

            If you are using `apispec.ext.marshmallow`, you can pass fields' metadata as
            additional keyword arguments.

            For example, to add ``enum`` to your field: ::

                status = fields.String(required=True, enum=['open', 'closed'])

        https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#definitionsObject
        """
        ret = {}
        # Execute all helpers from plugins
        for plugin in self.plugins:
            try:
                ret.update(plugin.definition_helper(name, definition=ret, **kwargs))
            except PluginMethodNotImplementedError:
                continue
        # Deprecated interface
        for func in self._definition_helpers:
            try:
                ret.update(func(self, name, definition=ret, **kwargs))
            except TypeError:
                continue
        if properties:
            ret['properties'] = properties
        if enum:
            ret['enum'] = enum
        if description:
            ret['description'] = description
        if extra_fields:
            ret.update(extra_fields)
        self._definitions[name] = ret

    # Deprecated PLUGIN INTERFACE

    # adapted from Sphinx
    def setup_plugin(self, path):
        """Import and setup a plugin. No-op if called twice
        for the same plugin.

        :param str path: Import path to the plugin.
        :raise: PluginError if the given plugin is invalid.
        """
        warnings.warn(
            'Old style plugins are deprecated. Use classes instead. '
            'See https://apispec.readthedocs.io/en/latest/writing_plugins.html.',
            DeprecationWarning,
        )
        if path in self.old_plugins:
            return
        try:
            mod = __import__(
                path, globals=None, locals=None, fromlist=('setup', ),
            )
        except ImportError as err:
            raise PluginError(
                'Could not import plugin "{0}"\n\n{1}'.format(path, err),
            )
        if not hasattr(mod, 'setup'):
            raise PluginError('Plugin "{0}" has no setup(spec) function'.format(path))
        else:
            # Each plugin gets a dict to store arbitrary data
            self.old_plugins[path] = {}
            mod.setup(self)

    # Deprecated helpers interface

    def register_definition_helper(self, func):
        """Register a new definition helper. The helper **must** meet the following conditions:

        - Receive the `APISpec` instance as the first argument.
        - Receive the definition `name` as the second argument.
        - Include ``**kwargs`` in its signature.
        - Return a `dict` representation of the definition's Schema object.

        The helper may define any named arguments after the `name` argument.
        ``kwargs`` will include (among other things):
        - definition (dict): current state of the definition

        https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md#definitionsObject

        :param callable func: The definition helper function.
        """
        warnings.warn(
            'Helper functions are deprecated. Use plugin classes. '
            'See https://apispec.readthedocs.io/en/latest/writing_plugins.html.',
            DeprecationWarning,
        )
        self._definition_helpers.append(func)

    def register_path_helper(self, func):
        """Register a new path helper. The helper **must** meet the following conditions:

        - Receive the `APISpec` instance as the first argument.
        - Include ``**kwargs`` in signature.
        - Return a `apispec.core.Path` object.

        The helper may define any named arguments in its signature.
        """
        warnings.warn(
            'Helper functions are deprecated. Use plugin classes. '
            'See https://apispec.readthedocs.io/en/latest/writing_plugins.html.',
            DeprecationWarning,
        )
        self._path_helpers.append(func)

    def register_operation_helper(self, func):
        """Register a new operation helper. The helper **must** meet the following conditions:

        - Receive the `APISpec` instance as the first argument.
        - Receive ``operations`` as a keyword argument.
        - Include ``**kwargs`` in signature.

        The helper may define any named arguments in its signature.
        """
        warnings.warn(
            'Helper functions are deprecated. Use plugin classes. '
            'See https://apispec.readthedocs.io/en/latest/writing_plugins.html.',
            DeprecationWarning,
        )
        self._operation_helpers.append(func)

    def register_response_helper(self, func, method, status_code):
        """Register a new response helper. The helper **must** meet the following conditions:

        - Receive the `APISpec` instance as the first argument.
        - Include ``**kwargs`` in signature.
        - Return a `dict` response object.

        The helper may define any named arguments in its signature.
        """
        warnings.warn(
            'Helper functions are deprecated. Use plugin classes. '
            'See https://apispec.readthedocs.io/en/latest/writing_plugins.html.',
            DeprecationWarning,
        )
        method = method.lower()
        if method not in self._response_helpers:
            self._response_helpers[method] = {}
        self._response_helpers[method].setdefault(status_code, []).append(func)


class YAMLDumper(yaml.Dumper):

    @staticmethod
    def _represent_dict(dumper, instance):
        return dumper.represent_mapping('tag:yaml.org,2002:map', instance.items())

    if PY2:
        @staticmethod
        def _represent_unicode(_, uni):
            return yaml.ScalarNode(tag=u'tag:yaml.org,2002:str', value=uni)


if PY2:
    yaml.add_representer(unicode, YAMLDumper._represent_unicode, Dumper=YAMLDumper)
yaml.add_representer(OrderedDict, YAMLDumper._represent_dict, Dumper=YAMLDumper)
yaml.add_representer(LazyDict, YAMLDumper._represent_dict, Dumper=YAMLDumper)
yaml.add_representer(Path, YAMLDumper._represent_dict, Dumper=YAMLDumper)
