import warnings
from django.core.exceptions import ImproperlyConfigured
from django.http import HttpResponse, HttpResponseBadRequest
from tastypie.compat import reverse
from tastypie.exceptions import NotRegistered, BadRequest
from tastypie.serializers import Serializer
from tastypie.utils import is_valid_jsonp_callback_value, string_to_python, trailing_slash
from tastypie.utils.mime import determine_format, build_content_type
from tastypie.resources import Resource
from django.urls.conf import re_path, include


class Api(object):
    """
    Implements a registry to tie together the various resources that make up
    an API.

    Especially useful for navigation, HATEOAS and for providing multiple
    versions of your API.

    Optionally supplying ``api_name`` allows you to name the API. Generally,
    this is done with version numbers (i.e. ``v1``, ``v2``, etc.) but can
    be named any string.
    """
    def __init__(self, api_name="v1", serializer_class=Serializer):
        self.api_name = api_name
        self._registry = {}
        self._canonicals = {}
        self.serializer = serializer_class()

    def register(self, resource, canonical=True):
        """
        Registers an instance of a ``Resource`` subclass with the API.

        Optionally accept a ``canonical`` argument, which indicates that the
        resource being registered is the canonical variant. Defaults to
        ``True``.
        """
        resource_name = getattr(resource._meta, 'resource_name', None)

        if not isinstance(resource, Resource):
            raise ValueError("An instance of ``Resource`` subclass should be passed in for %s" % resource_name)

        if resource_name is None:
            raise ImproperlyConfigured("Resource %r must define a 'resource_name'." % resource)

        self._registry[resource_name] = resource

        if canonical is True:
            if resource_name in self._canonicals:
                warnings.warn("A new resource '%r' is replacing the existing canonical URL for '%s'." % (resource, resource_name), Warning, stacklevel=2)

            self._canonicals[resource_name] = resource
            # TODO: This is messy, but makes URI resolution on FK/M2M fields
            #       work consistently.
            resource._meta.api_name = self.api_name
            resource.__class__.Meta.api_name = self.api_name

    def unregister(self, resource_name):
        """
        If present, unregisters a resource from the API.
        """
        if resource_name in self._registry:
            del self._registry[resource_name]

        if resource_name in self._canonicals:
            del self._canonicals[resource_name]

    def canonical_resource_for(self, resource_name):
        """
        Returns the canonical resource for a given ``resource_name``.
        """
        if resource_name in self._canonicals:
            return self._canonicals[resource_name]

        raise NotRegistered("No resource was registered as canonical for '%s'." % resource_name)

    def wrap_view(self, view):
        def wrapper(request, *args, **kwargs):
            try:
                return getattr(self, view)(request, *args, **kwargs)
            except BadRequest:
                return HttpResponseBadRequest()
        return wrapper

    def override_urls(self):
        """
        Deprecated. Will be removed by v1.0.0. Please use ``prepend_urls`` instead.
        """
        return []

    def prepend_urls(self):
        """
        A hook for adding your own URLs or matching before the default URLs.
        """
        return []

    @property
    def urls(self):
        """
        Provides URLconf details for the ``Api`` and all registered
        ``Resources`` beneath it.
        """
        pattern_list = [
            re_path(r"^(?P<api_name>%s)%s$" % (self.api_name, trailing_slash), self.wrap_view('top_level'), name="api_%s_top_level" % self.api_name),
        ]

        for name in sorted(self._registry.keys()):
            self._registry[name].api_name = self.api_name
            pattern_list.append(re_path(r"^(?P<api_name>%s)/" % self.api_name, include(self._registry[name].urls)))

        urlpatterns = self.prepend_urls()

        overridden_urls = self.override_urls()
        if overridden_urls:
            warnings.warn("'override_urls' is a deprecated method & will be removed by v1.0.0. Please rename your method to ``prepend_urls``.")
            urlpatterns += overridden_urls

        urlpatterns += pattern_list
        return urlpatterns

    def top_level(self, request, api_name=None):
        """
        A view that returns a serialized list of all resources registers
        to the ``Api``. Useful for discovery.
        """
        fullschema = request.GET.get('fullschema', False)
        fullschema = string_to_python(fullschema)

        available_resources = {}

        if api_name is None:
            api_name = self.api_name

        for name, resource in self._registry.items():
            if not fullschema:
                schema = self._build_reverse_url("api_get_schema", kwargs={
                    'api_name': api_name,
                    'resource_name': name,
                })
            else:
                schema = resource.build_schema()

            available_resources[name] = {
                'list_endpoint': self._build_reverse_url("api_dispatch_list", kwargs={
                    'api_name': api_name,
                    'resource_name': name,
                }),
                'schema': schema,
            }

        desired_format = determine_format(request, self.serializer)

        options = {}

        if 'text/javascript' in desired_format:
            callback = request.GET.get('callback', 'callback')

            if not is_valid_jsonp_callback_value(callback):
                raise BadRequest('JSONP callback name is invalid.')

            options['callback'] = callback

        serialized = self.serializer.serialize(available_resources, desired_format, options)
        return HttpResponse(content=serialized, content_type=build_content_type(desired_format))

    def _build_reverse_url(self, name, args=None, kwargs=None):
        """
        A convenience hook for overriding how URLs are built.

        See ``NamespacedApi._build_reverse_url`` for an example.
        """
        return reverse(name, args=args, kwargs=kwargs)


class NamespacedApi(Api):
    """
    An API subclass that respects Django namespaces.
    """
    def __init__(self, api_name="v1", urlconf_namespace=None, **kwargs):
        super(NamespacedApi, self).__init__(api_name=api_name, **kwargs)
        self.urlconf_namespace = urlconf_namespace

    def register(self, resource, canonical=True):
        super(NamespacedApi, self).register(resource, canonical=canonical)

        if canonical is True:
            # Plop in the namespace here as well.
            resource._meta.urlconf_namespace = self.urlconf_namespace

    def _build_reverse_url(self, name, args=None, kwargs=None):
        namespaced = "%s:%s" % (self.urlconf_namespace, name)
        return reverse(namespaced, args=args, kwargs=kwargs)
