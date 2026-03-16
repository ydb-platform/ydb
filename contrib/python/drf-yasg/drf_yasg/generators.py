import copy
import logging
import re
import urllib.parse as urlparse
from collections import defaultdict

import uritemplate
from django.urls import URLPattern, URLResolver
from rest_framework import versioning
from rest_framework.schemas.generators import EndpointEnumerator as _EndpointEnumerator
from rest_framework.schemas.generators import endpoint_ordering, get_pk_name
from rest_framework.schemas.openapi import SchemaGenerator
from rest_framework.schemas.utils import get_pk_description
from rest_framework.settings import api_settings

from . import openapi
from .app_settings import swagger_settings
from .errors import SwaggerGenerationError
from .inspectors.field import (
    get_basic_type_info,
    get_queryset_field,
    get_queryset_from_view,
)
from .openapi import ReferenceResolver, SwaggerDict
from .utils import force_real_str, get_consumes, get_produces, is_list_view

logger = logging.getLogger(__name__)

PATH_PARAMETER_RE = re.compile(r"{(?P<parameter>\w+)}")


def common_path(paths):
    split_paths = [path.strip("/").split("/") for path in paths]
    s1 = min(split_paths)
    s2 = max(split_paths)
    common = s1
    for i, c in enumerate(s1):
        if c != s2[i]:
            common = s1[:i]
            break
    return "/" + "/".join(common)


def is_custom_action(action):
    return action not in {
        "retrieve",
        "list",
        "create",
        "update",
        "partial_update",
        "destroy",
    }


class EndpointEnumerator(_EndpointEnumerator):
    def __init__(self, patterns=None, urlconf=None, request=None):
        super(EndpointEnumerator, self).__init__(patterns, urlconf)
        self.request = request

    def get_path_from_regex(self, path_regex):
        if path_regex.endswith(")"):
            logger.warning(
                "url pattern does not end in $ ('%s') - unexpected things might happen",
                path_regex,
            )
        return self.unescape_path(
            super(EndpointEnumerator, self).get_path_from_regex(path_regex)
        )

    def should_include_endpoint(
        self, path, callback, app_name="", namespace="", url_name=None
    ):
        if not super(EndpointEnumerator, self).should_include_endpoint(path, callback):
            return False

        version = getattr(self.request, "version", None)
        versioning_class = getattr(callback.cls, "versioning_class", None)
        if versioning_class is not None and issubclass(
            versioning_class, versioning.NamespaceVersioning
        ):
            if version and version not in namespace.split(":"):
                return False

        if getattr(callback.cls, "swagger_schema", object()) is None:
            return False

        return True

    def replace_version(self, path, callback):
        """If ``request.version`` is not ``None`` and `callback` uses
        ``URLPathVersioning``, this function replaces the ``version`` parameter in
        `path` with the actual version.

        :param str path: the templated path
        :param callback: the view callback
        :rtype: str
        """
        versioning_class = getattr(callback.cls, "versioning_class", None)
        if versioning_class is not None and issubclass(
            versioning_class, versioning.URLPathVersioning
        ):
            version = getattr(self.request, "version", None)
            if version:
                version_param = getattr(versioning_class, "version_param", "version")
                version_param = "{%s}" % version_param
                if version_param not in path:
                    logger.info(
                        "view %s uses URLPathVersioning but URL %s has no param %s"
                        % (callback.cls, path, version_param)
                    )
                path = path.replace(version_param, version)

        return path

    def get_api_endpoints(
        self,
        patterns=None,
        prefix="",
        app_name=None,
        namespace=None,
        ignored_endpoints=None,
    ):
        """
        Return a list of all available API endpoints by inspecting the URL conf.

        Copied entirely from super.
        """
        if patterns is None:
            patterns = self.patterns

        api_endpoints = []
        if ignored_endpoints is None:
            ignored_endpoints = set()

        for pattern in patterns:
            path_regex = prefix + str(pattern.pattern)
            if isinstance(pattern, URLPattern):
                try:
                    path = self.get_path_from_regex(path_regex)
                    callback = pattern.callback
                    url_name = pattern.name
                    if self.should_include_endpoint(
                        path, callback, app_name or "", namespace or "", url_name
                    ):
                        path = self.replace_version(path, callback)

                        # avoid adding endpoints that have already been seen,
                        # as Django resolves urls in top-down order
                        if path in ignored_endpoints:
                            continue
                        ignored_endpoints.add(path)

                        for method in self.get_allowed_methods(callback):
                            endpoint = (path, method, callback)
                            api_endpoints.append(endpoint)
                except Exception:  # pragma: no cover
                    logger.warning("failed to enumerate view", exc_info=True)

            elif isinstance(pattern, URLResolver):
                nested_endpoints = self.get_api_endpoints(
                    patterns=pattern.url_patterns,
                    prefix=path_regex,
                    app_name="%s:%s" % (app_name, pattern.app_name)
                    if app_name
                    else pattern.app_name,
                    namespace="%s:%s" % (namespace, pattern.namespace)
                    if namespace
                    else pattern.namespace,
                    ignored_endpoints=ignored_endpoints,
                )
                api_endpoints.extend(nested_endpoints)
            else:
                logger.warning("unknown pattern type {}".format(type(pattern)))

        api_endpoints = sorted(api_endpoints, key=endpoint_ordering)

        return api_endpoints

    def unescape(self, s):
        """Unescape all backslash escapes from `s`.

        :param str s: string with backslash escapes
        :rtype: str
        """
        # unlike .replace('\\', ''), this correctly transforms a double backslash into a
        # single backslash
        return re.sub(r"\\(.)", r"\1", s)

    def unescape_path(self, path):
        """Remove backslashes escapes from all path components outside {parameters}.
        This is needed because ``simplify_regex`` does not handle this correctly.

        **NOTE:** this might destructively affect some url regex patterns that contain
        metacharacters (e.g. \\w, \\d) outside path parameter groups; if you are in this
        category, God help you

        :param str path: path possibly containing
        :return: the unescaped path
        :rtype: str
        """
        clean_path = ""
        while path:
            match = PATH_PARAMETER_RE.search(path)
            if not match:
                clean_path += self.unescape(path)
                break
            clean_path += self.unescape(path[: match.start()])
            clean_path += match.group()
            path = path[match.end() :]

        return clean_path


class OpenAPISchemaGenerator:
    """
    This class iterates over all registered API endpoints and returns an appropriate
    OpenAPI 2.0 compliant schema. Method implementations shamelessly stolen and adapted
    from rest-framework ``SchemaGenerator``.
    """

    endpoint_enumerator_class = EndpointEnumerator
    reference_resolver_class = ReferenceResolver

    # Map HTTP methods onto actions.
    default_mapping = {
        "get": "retrieve",
        "post": "create",
        "put": "update",
        "patch": "partial_update",
        "delete": "destroy",
    }

    def __init__(self, info, version="", url=None, patterns=None, urlconf=None):
        """

        :param openapi.Info info: information about the API
        :param str version: API version string; if omitted, `info.default_version` will
            be used
        :param str url: API scheme, host and port; if ``None`` is passed and
            ``DEFAULT_API_URL`` is not set, the url will be inferred from the request
            made against the schema view, so you should generally not need to set this
            parameter explicitly; if the empty string is passed, no host and scheme will
            be emitted

            If `url` is not ``None`` or the empty string, it must be a scheme-absolute
            uri (i.e. starting with http:// or https://), and any path component is
            ignored;

            See also:
            :ref:`documentation on base URL construction <custom-spec-base-url>`
        :param patterns: if given, only these patterns will be enumerated for inclusion
            in the API spec
        :param urlconf: if patterns is not given, use this urlconf to enumerate
            patterns; if not given, the default urlconf is used
        """
        self._gen = SchemaGenerator(
            info.title, url, info.get("description", ""), patterns, urlconf
        )
        self.info = info
        self.version = version
        self.consumes = []
        self.produces = []
        self.coerce_method_names = api_settings.SCHEMA_COERCE_METHOD_NAMES

        if url is None and swagger_settings.DEFAULT_API_URL is not None:
            url = swagger_settings.DEFAULT_API_URL

        if url:
            parsed_url = urlparse.urlparse(url)
            if parsed_url.scheme not in ("http", "https") or not parsed_url.netloc:
                raise SwaggerGenerationError("`url` must be an absolute HTTP(S) url")
            if parsed_url.path:
                logger.warning(
                    "path component of api base URL %s is ignored; use "
                    "FORCE_SCRIPT_NAME instead" % url
                )
            else:
                self._gen.url = url

    @property
    def url(self):
        return self._gen.url

    def get_security_definitions(self):
        """Get the security schemes for this API. This determines what is usable in
        security requirements, and helps clients configure their authorization
        credentials.

        :return: the security schemes usable with this API
        :rtype: dict[str,dict] or None
        """
        security_definitions = swagger_settings.SECURITY_DEFINITIONS
        if security_definitions is not None:
            security_definitions = SwaggerDict._as_dict(security_definitions, {})

        return security_definitions

    def get_security_requirements(self, security_definitions):
        """Get the base (global) security requirements of the API. This is never called
        if :meth:`.get_security_definitions` returns `None`.

        :param security_definitions: security definitions as returned by
            :meth:`.get_security_definitions`
        :return: the security schemes accepted by default
        :rtype: list[dict[str,list[str]]] or None
        """
        security_requirements = swagger_settings.SECURITY_REQUIREMENTS
        if security_requirements is None:
            security_requirements = [
                {security_scheme: []} for security_scheme in security_definitions
            ]

        security_requirements = [
            SwaggerDict._as_dict(sr, {}) for sr in security_requirements
        ]
        security_requirements = sorted(security_requirements, key=list)
        return security_requirements

    def get_schema(self, request=None, public=False):
        """Generate a :class:`.Swagger` object representing the API schema.

        :param request: the request used for filtering accessible endpoints and finding
            the spec URI
        :type request: rest_framework.request.Request or None
        :param bool public: if True, all endpoints are included regardless of access
            through `request`

        :return: the generated Swagger specification
        :rtype: openapi.Swagger
        """
        endpoints = self.get_endpoints(request)
        components = self.reference_resolver_class(
            openapi.SCHEMA_DEFINITIONS, force_init=True
        )
        self.consumes = get_consumes(api_settings.DEFAULT_PARSER_CLASSES)
        self.produces = get_produces(api_settings.DEFAULT_RENDERER_CLASSES)
        paths, prefix = self.get_paths(endpoints, components, request, public)

        security_definitions = self.get_security_definitions()
        if security_definitions:
            security_requirements = self.get_security_requirements(security_definitions)
        else:
            security_requirements = None

        url = self.url
        if url is None and request is not None:
            url = request.build_absolute_uri()

        return openapi.Swagger(
            info=self.info,
            paths=paths,
            consumes=self.consumes or None,
            produces=self.produces or None,
            security_definitions=security_definitions,
            security=security_requirements,
            _url=url,
            _prefix=prefix,
            _version=self.version,
            **dict(components),
        )

    def create_view(self, callback, method, request=None):
        """Create a view instance from a view callback as registered in urlpatterns.

        :param callback: view callback registered in urlpatterns
        :param str method: HTTP method
        :param request: request to bind to the view
        :type request: rest_framework.request.Request or None
        :return: the view instance
        """
        view = self._gen.create_view(callback, method, request)
        overrides = getattr(callback, "_swagger_auto_schema", None)
        if overrides is not None:
            # decorated function based view must have its decorator information passed
            # on to the re-instantiated view
            for method, _ in overrides.items():
                view_method = getattr(view, method, None)
                if view_method is not None:  # pragma: no cover
                    setattr(view_method.__func__, "_swagger_auto_schema", overrides)

        setattr(view, "swagger_fake_view", True)
        return view

    def coerce_path(self, path, view):
        """Coerce {pk} path arguments into the name of the model field, where possible.
        This is cleaner for an external representation (i.e. "this is an identifier",
        not "this is a database primary key").

        :param str path: the path
        :param rest_framework.views.APIView view: associated view
        :rtype: str
        """
        if "{pk}" not in path:
            return path

        model = getattr(get_queryset_from_view(view), "model", None)
        if model:
            field_name = get_pk_name(model)
        else:
            field_name = "id"
        return path.replace("{pk}", "{%s}" % field_name)

    def get_endpoints(self, request):
        """Iterate over all the registered endpoints in the API and return a fake view
        with the right parameters.

        :param request: request to bind to the endpoint views
        :type request: rest_framework.request.Request or None
        :return: {path: (view_class, list[(http_method, view_instance)])
        :rtype: dict[str,(type,list[(str,rest_framework.views.APIView)])]
        """
        enumerator = self.endpoint_enumerator_class(
            self._gen.patterns, self._gen.urlconf, request=request
        )
        endpoints = enumerator.get_api_endpoints()

        view_paths = defaultdict(list)
        view_cls = {}
        for path, method, callback in endpoints:
            view = self.create_view(callback, method, request)
            path = self.coerce_path(path, view)
            view_paths[path].append((method, view))
            view_cls[path] = callback.cls
        return {path: (view_cls[path], methods) for path, methods in view_paths.items()}

    def get_operation_keys(self, subpath, method, view):
        """Return a list of keys that should be used to group an operation within the specification. ::

          /users/                   ("users", "list"), ("users", "create")
          /users/{pk}/              ("users", "read"), ("users", "update"), ("users", "delete")
          /users/enabled/           ("users", "enabled")  # custom viewset list action
          /users/{pk}/star/         ("users", "star")     # custom viewset detail action
          /users/{pk}/groups/       ("users", "groups", "list"), ("users", "groups", "create")
          /users/{pk}/groups/{pk}/  ("users", "groups", "read"), ("users", "groups", "update")

        :param str subpath: path to the operation with any common prefix/base path removed
        :param str method: HTTP method
        :param view: the view associated with the operation
        :rtype: list[str]
        """  # noqa: E501
        if hasattr(view, "action"):
            # Viewsets have explicitly named actions.
            action = view.action
        else:
            # Views have no associated action, so we determine one from the method.
            if is_list_view(subpath, method, view):
                action = "list"
            else:
                action = self.default_mapping[method.lower()]

        named_path_components = [
            component
            for component in subpath.strip("/").split("/")
            if "{" not in component
        ]

        if is_custom_action(action):
            # Custom action, eg "/users/{pk}/activate/", "/users/active/"
            mapped_methods = {
                # Don't count head mapping, e.g. not part of the schema
                method
                for method in view.action_map
                if method != "head"
            }
            if len(mapped_methods) > 1:
                action = self.default_mapping[method.lower()]
                if action in self.coerce_method_names:
                    action = self.coerce_method_names[action]
                return named_path_components + [action]
            else:
                return named_path_components[:-1] + [action]

        if action in self.coerce_method_names:
            action = self.coerce_method_names[action]

        # Default action, eg "/users/", "/users/{pk}/"
        return named_path_components + [action]

    def determine_path_prefix(self, paths):
        """
        Given a list of all paths, return the common prefix which should be
        discounted when generating a schema structure.

        This will be the longest common string that does not include that last
        component of the URL, or the last component before a path parameter.

        For example: ::

            /api/v1/users/
            /api/v1/users/{pk}/

        The path prefix is ``/api/v1/``.

        :param list[str] paths: list of paths
        :rtype: str
        """
        prefixes = []
        for path in paths:
            components = path.strip("/").split("/")
            initial_components = []
            for component in components:
                if "{" in component:
                    break
                initial_components.append(component)
            prefix = "/".join(initial_components[:-1])
            if not prefix:
                # We can just break early in the case that there's at least
                # one URL that doesn't have a path prefix.
                return "/"
            prefixes.append("/" + prefix + "/")
        return common_path(prefixes)

    def should_include_endpoint(self, path, method, view, public):
        """Check if a given endpoint should be included in the resulting schema.

        :param str path: request path
        :param str method: http request method
        :param view: instantiated view callback
        :param bool public: if True, all endpoints are included regardless of access
            through `request`
        :returns: true if the view should be excluded
        :rtype: bool
        """
        return public or self._gen.has_view_permissions(path, method, view)

    def get_paths_object(self, paths):
        """Construct the Swagger Paths object.

        :param dict[str,openapi.PathItem] paths: mapping of paths to
            :class:`.PathItem` objects
        :returns: the :class:`.Paths` object
        :rtype: openapi.Paths
        """
        return openapi.Paths(paths=paths)

    def get_paths(self, endpoints, components, request, public):
        """Generate the Swagger Paths for the API from the given endpoints.

        :param dict endpoints: endpoints as returned by get_endpoints
        :param ReferenceResolver components: resolver/container for Swagger References
        :param Request request: the request made against the schema view; can be None
        :param bool public: if True, all endpoints are included regardless of access
            through `request`
        :returns: the :class:`.Paths` object and the longest common path prefix, as a
            2-tuple
        :rtype: tuple[openapi.Paths,str]
        """
        if not endpoints:
            return openapi.Paths(paths={}), ""

        prefix = self.determine_path_prefix(list(endpoints.keys())) or ""
        assert "{" not in prefix, "base path cannot be templated in swagger 2.0"

        paths = {}
        for path, (view_cls, methods) in sorted(endpoints.items()):
            operations = {}
            for method, view in methods:
                if not self.should_include_endpoint(path, method, view, public):
                    continue

                operation = self.get_operation(
                    view, path, prefix, method, components, request
                )
                if operation is not None:
                    operations[method.lower()] = operation

            if operations:
                # since the common prefix is used as the API basePath, it must be
                # stripped from individual paths when writing them into the swagger
                # document
                path_suffix = path[len(prefix) :]
                if not path_suffix.startswith("/"):
                    path_suffix = "/" + path_suffix
                paths[path_suffix] = self.get_path_item(path, view_cls, operations)

        return self.get_paths_object(paths), prefix

    def get_operation(self, view, path, prefix, method, components, request):
        """Get an :class:`.Operation` for the given API endpoint (path, method). This
        method delegates to :meth:`~.inspectors.ViewInspector.get_operation` of a
        :class:`~.inspectors.ViewInspector` determined according to settings and
        :func:`@swagger_auto_schema <.swagger_auto_schema>` overrides.

        :param view: the view associated with this endpoint
        :param str path: the path component of the operation URL
        :param str prefix: common path prefix among all endpoints
        :param str method: the http method of the operation
        :param openapi.ReferenceResolver components: referenceable components
        :param Request request: the request made against the schema view; can be None
        :rtype: openapi.Operation
        """
        operation_keys = self.get_operation_keys(path[len(prefix) :], method, view)
        overrides = self.get_overrides(view, method)

        # the inspector class can be specified, in decreasing order of priority,
        #   1. globally via DEFAULT_AUTO_SCHEMA_CLASS
        view_inspector_cls = swagger_settings.DEFAULT_AUTO_SCHEMA_CLASS
        #   2. on the view/viewset class
        view_inspector_cls = getattr(view, "swagger_schema", view_inspector_cls)
        #   3. on the swagger_auto_schema decorator
        view_inspector_cls = overrides.get("auto_schema", view_inspector_cls)

        if view_inspector_cls is None:
            return None

        view_inspector = view_inspector_cls(
            view, path, method, components, request, overrides, operation_keys
        )
        operation = view_inspector.get_operation(operation_keys)
        if operation is None:
            return None

        if "consumes" in operation and set(operation.consumes) == set(self.consumes):
            del operation.consumes
        if "produces" in operation and set(operation.produces) == set(self.produces):
            del operation.produces
        return operation

    def get_path_item(self, path, view_cls, operations):
        """Get a :class:`.PathItem` object that describes the parameters and operations
        related to a single path in the API.

        :param str path: the path
        :param type view_cls: the view that was bound to this path in urlpatterns
        :param dict[str,openapi.Operation] operations: operations defined on this path,
            keyed by lowercase HTTP method
        :rtype: openapi.PathItem
        """
        path_parameters = self.get_path_parameters(path, view_cls)
        return openapi.PathItem(parameters=path_parameters, **operations)

    def get_overrides(self, view, method):
        """Get overrides specified for a given operation.

        :param view: the view associated with the operation
        :param str method: HTTP method
        :return: a dictionary containing any overrides set by
            :func:`@swagger_auto_schema <.swagger_auto_schema>`
        :rtype: dict
        """
        method = method.lower()
        action = getattr(view, "action", method)
        action_method = getattr(view, action, None)
        overrides = getattr(action_method, "_swagger_auto_schema", {})
        if method in overrides:
            overrides = overrides[method]

        return copy.deepcopy(overrides)

    def get_path_parameters(self, path, view_cls):
        """Return a list of Parameter instances corresponding to any templated path
        variables.

        :param str path: templated request path
        :param type view_cls: the view class associated with the path
        :return: path parameters
        :rtype: list[openapi.Parameter]
        """
        parameters = []
        queryset = get_queryset_from_view(view_cls)

        for variable in uritemplate.variables(path):
            model, model_field = get_queryset_field(queryset, variable)
            attrs = get_basic_type_info(model_field) or {"type": openapi.TYPE_STRING}
            if (
                getattr(view_cls, "lookup_field", None) == variable
                and attrs["type"] == openapi.TYPE_STRING
            ):
                attrs["pattern"] = getattr(
                    view_cls, "lookup_value_regex", attrs.get("pattern", None)
                )

            if model_field and getattr(model_field, "help_text", False):
                description = model_field.help_text
            elif model_field and getattr(model_field, "primary_key", False):
                description = get_pk_description(model, model_field)
            else:
                description = None

            field = openapi.Parameter(
                name=variable,
                description=force_real_str(description),
                required=True,
                in_=openapi.IN_PATH,
                **attrs,
            )
            parameters.append(field)

        return parameters
