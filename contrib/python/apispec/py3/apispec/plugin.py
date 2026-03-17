"""Base class for Plugin classes."""


from .exceptions import PluginMethodNotImplementedError


class BasePlugin:
    """Base class for APISpec plugin classes."""

    def init_spec(self, spec):
        """Initialize plugin with APISpec object

        :param APISpec spec: APISpec object this plugin instance is attached to
        """

    def schema_helper(self, name, definition, **kwargs):
        """May return definition as a dict.

        :param str name: Identifier by which schema may be referenced
        :param dict definition: Schema definition
        :param dict kwargs: All additional keywords arguments sent to `APISpec.schema()`
        """
        raise PluginMethodNotImplementedError

    def response_helper(self, response, **kwargs):
        """May return response component description as a dict.

        :param dict response: Response fields
        :param dict kwargs: All additional keywords arguments sent to `APISpec.response()`
        """
        raise PluginMethodNotImplementedError

    def parameter_helper(self, parameter, **kwargs):
        """May return parameter component description as a dict.

        :param dict parameter: Parameter fields
        :param dict kwargs: All additional keywords arguments sent to `APISpec.parameter()`
        """
        raise PluginMethodNotImplementedError

    def path_helper(self, path=None, operations=None, parameters=None, **kwargs):
        """May return a path as string and mutate operations dict and parameters list.

        :param str path: Path to the resource
        :param dict operations: A `dict` mapping HTTP methods to operation object. See
            https://github.com/OAI/OpenAPI-Specification/blob/master/versions/3.0.2.md#operationObject
        :param list parameters: A `list` of parameters objects or references for the path. See
            https://github.com/OAI/OpenAPI-Specification/blob/master/versions/3.0.0.md#parameterObject
            and https://github.com/OAI/OpenAPI-Specification/blob/master/versions/3.0.0.md#referenceObject
        :param dict kwargs: All additional keywords arguments sent to `APISpec.path()`

        Return value should be a string or None. If a string is returned, it
        is set as the path.

        The last path helper returning a string sets the path value. Therefore,
        the order of plugin registration matters. However, generally, registering
        several plugins that return a path does not make sense.
        """
        raise PluginMethodNotImplementedError

    def operation_helper(self, path=None, operations=None, **kwargs):
        """May mutate operations.

        :param str path: Path to the resource
        :param dict operations: A `dict` mapping HTTP methods to operation object.
            See https://github.com/OAI/OpenAPI-Specification/blob/master/versions/3.0.0.md#operationObject
        :param dict kwargs: All additional keywords arguments sent to `APISpec.path()`
        """
        raise PluginMethodNotImplementedError
