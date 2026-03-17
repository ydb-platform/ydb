from copy import copy, deepcopy
from datetime import datetime
import logging
import sys
from time import mktime
import traceback
import warnings
from wsgiref.handlers import format_date_time

from django.conf import settings
from django.core.exceptions import (
    ObjectDoesNotExist, MultipleObjectsReturned, ValidationError, FieldDoesNotExist
)
from django.core.signals import got_request_exception
from django.core.exceptions import ImproperlyConfigured
from django.db.models.fields.related import ForeignKey
from django.urls.conf import re_path
from tastypie.utils.timezone import make_naive_utc
try:
    from django.contrib.gis.db.models.fields import GeometryField
except (ImproperlyConfigured, ImportError):
    GeometryField = None
from django.db.models.constants import LOOKUP_SEP
try:
    from django.db.models.fields.related import\
        SingleRelatedObjectDescriptor as ReverseOneToOneDescriptor
except ImportError:
    from django.db.models.fields.related_descriptors import\
        ReverseOneToOneDescriptor

from django.http import HttpResponse, HttpResponseNotFound, Http404
from django.utils.cache import patch_cache_control, patch_vary_headers
from django.utils.html import escape
from django.views.decorators.csrf import csrf_exempt

from tastypie.authentication import Authentication
from tastypie.authorization import ReadOnlyAuthorization
from tastypie.bundle import Bundle
from tastypie.cache import NoCache
from tastypie.compat import NoReverseMatch, reverse, Resolver404, get_script_prefix, is_ajax
from tastypie.constants import ALL, ALL_WITH_RELATIONS
from tastypie.exceptions import (
    NotFound, BadRequest, InvalidFilterError, HydrationError, InvalidSortError,
    ImmediateHttpResponse, Unauthorized, UnsupportedFormat,
    UnsupportedSerializationFormat, UnsupportedDeserializationFormat,
)
from tastypie import fields
from tastypie import http
from tastypie.paginator import Paginator
from tastypie.serializers import Serializer
from tastypie.throttle import BaseThrottle
from tastypie.utils import (
    is_valid_jsonp_callback_value, string_to_python,
    trailing_slash,
)
from tastypie.utils.mime import determine_format, build_content_type
from tastypie.validation import Validation
from tastypie.compat import get_module_name, atomic_decorator


def sanitize(text):
    # We put the single quotes back, due to their frequent usage in exception
    # messages.
    return escape(text).replace('&#39;', "'").replace('&quot;', '"').replace('&#x27;', "'")


class ResourceOptions(object):
    """
    A configuration class for ``Resource``.

    Provides sane defaults and the logic needed to augment these settings with
    the internal ``class Meta`` used on ``Resource`` subclasses.
    """
    serializer = Serializer()
    authentication = Authentication()
    authorization = ReadOnlyAuthorization()
    cache = NoCache()
    throttle = BaseThrottle()
    validation = Validation()
    paginator_class = Paginator
    allowed_methods = ['get', 'post', 'put', 'delete', 'patch']
    list_allowed_methods = None
    detail_allowed_methods = None
    limit = getattr(settings, 'API_LIMIT_PER_PAGE', 20)
    max_limit = 1000
    api_name = None
    resource_name = None
    urlconf_namespace = None
    default_format = 'application/json'
    filtering = {}
    ordering = []
    object_class = None
    queryset = None
    fields = None
    excludes = []
    include_resource_uri = True
    include_absolute_url = False
    always_return_data = False
    collection_name = 'objects'
    detail_uri_name = 'pk'

    def __new__(cls, meta=None):
        overrides = {}

        # Handle overrides.
        if meta:
            for override_name in dir(meta):
                # No internals please.
                if not override_name.startswith('_'):
                    overrides[override_name] = getattr(meta, override_name)

        allowed_methods = overrides.get('allowed_methods', ['get', 'post', 'put', 'delete', 'patch'])

        if overrides.get('list_allowed_methods', None) is None:
            overrides['list_allowed_methods'] = allowed_methods

        if overrides.get('detail_allowed_methods', None) is None:
            overrides['detail_allowed_methods'] = allowed_methods

        return object.__new__(type('ResourceOptions', (cls,), overrides))


class DeclarativeMetaclass(type):
    def __new__(cls, name, bases, attrs):
        attrs['base_fields'] = {}
        declared_fields = {}

        # Inherit any fields from parent(s).
        parents = [b for b in bases if issubclass(b, Resource)]
        # Simulate the MRO.
        parents.reverse()

        for p in parents:
            parent_fields = getattr(p, 'base_fields', {})

            for field_name, field_object in parent_fields.items():
                attrs['base_fields'][field_name] = deepcopy(field_object)

        for field_name, obj in attrs.copy().items():
            # Look for ``dehydrated_type`` instead of doing ``isinstance``,
            # which can break down if Tastypie is re-namespaced as something
            # else.
            if hasattr(obj, 'dehydrated_type'):
                field = attrs.pop(field_name)
                declared_fields[field_name] = field

        attrs['base_fields'].update(declared_fields)
        attrs['declared_fields'] = declared_fields
        new_class = super(DeclarativeMetaclass, cls).__new__(cls, name, bases, attrs)
        opts = getattr(new_class, 'Meta', None)
        new_class._meta = ResourceOptions(opts)
        abstract = getattr(new_class._meta, 'abstract', False)

        if not getattr(new_class._meta, 'resource_name', None):
            # No ``resource_name`` provided. Attempt to auto-name the resource.
            class_name = new_class.__name__
            name_bits = [bit for bit in class_name.split('Resource') if bit]
            resource_name = ''.join(name_bits).lower()
            new_class._meta.resource_name = resource_name

        if getattr(new_class._meta, 'include_resource_uri', True):
            if 'resource_uri' not in new_class.base_fields:
                new_class.base_fields['resource_uri'] = fields.CharField(readonly=True, verbose_name="resource uri")
        elif 'resource_uri' in new_class.base_fields and 'resource_uri' not in attrs:
            del new_class.base_fields['resource_uri']

        if abstract and 'resource_uri' not in attrs:
            # abstract classes don't have resource_uris unless explicitly provided
            if 'resource_uri' in new_class.base_fields:
                del new_class.base_fields['resource_uri']

        for field_name, field_object in new_class.base_fields.items():
            if hasattr(field_object, 'contribute_to_class'):
                field_object.contribute_to_class(new_class, field_name)

        return new_class


class Resource(metaclass=DeclarativeMetaclass):
    """
    Handles the data, request dispatch and responding to requests.

    Serialization/deserialization is handled "at the edges" (i.e. at the
    beginning/end of the request/response cycle) so that everything internally
    is Python data structures.

    This class tries to be non-model specific, so it can be hooked up to other
    data sources, such as search results, files, other data, etc.
    """
    def __init__(self, api_name=None):
        # this can cause:
        # TypeError: object.__new__(method-wrapper) is not safe, use method-wrapper.__new__()
        # when trying to copy a generator used as a default. Wrap call to
        # generator in lambda to get around this error.
        self.fields = {k: copy(v) for k, v in self.base_fields.items()}

        if api_name is not None:
            self._meta.api_name = api_name

    def __getattr__(self, name):
        if name == '__setstate__':
            raise AttributeError(name)
        try:
            return self.fields[name]
        except KeyError:
            raise AttributeError(name)

    def wrap_view(self, view):
        """
        Wraps methods so they can be called in a more functional way as well
        as handling exceptions better.

        Note that if ``BadRequest`` or an exception with a ``response`` attr
        are seen, there is special handling to either present a message back
        to the user or return the response traveling with the exception.
        """
        @csrf_exempt
        def wrapper(request, *args, **kwargs):
            try:
                callback = getattr(self, view)
                response = callback(request, *args, **kwargs)

                # Our response can vary based on a number of factors, use
                # the cache class to determine what we should ``Vary`` on so
                # caches won't return the wrong (cached) version.
                varies = getattr(self._meta.cache, "varies", [])

                if varies:
                    patch_vary_headers(response, varies)

                if self._meta.cache.cacheable(request, response):
                    if self._meta.cache.cache_control():
                        # If the request is cacheable and we have a
                        # ``Cache-Control`` available then patch the header.
                        patch_cache_control(response, **self._meta.cache.cache_control())

                if is_ajax(request) and not response.has_header("Cache-Control"):
                    # IE excessively caches XMLHttpRequests, so we're disabling
                    # the browser cache here.
                    # See http://www.enhanceie.com/ie/bugs.asp for details.
                    patch_cache_control(response, no_cache=True)

                return response
            except (BadRequest, fields.ApiFieldError) as e:
                data = {"error": sanitize(e.args[0]) if getattr(e, 'args') else ''}
                return self.error_response(request, data, response_class=http.HttpBadRequest)
            except ValidationError as e:
                data = {"error": sanitize(e.messages)}
                return self.error_response(request, data, response_class=http.HttpBadRequest)
            except Exception as e:
                # Prevent muting non-django's exceptions
                # i.e. RequestException from 'requests' library
                if hasattr(e, 'response') and isinstance(e.response, HttpResponse):
                    return e.response

                # A real, non-expected exception.
                # Handle the case where the full traceback is more helpful
                # than the serialized error.
                if settings.DEBUG and getattr(settings, 'TASTYPIE_FULL_DEBUG', False):
                    raise

                # Re-raise the error to get a proper traceback when the error
                # happend during a test case
                if request.META.get('SERVER_NAME') == 'testserver':
                    raise

                # Rather than re-raising, we're going to things similar to
                # what Django does. The difference is returning a serialized
                # error message.
                return self._handle_500(request, e)

        return wrapper

    def get_response_class_for_exception(self, request, exception):
        """
        Can be overridden to customize response classes used for uncaught
        exceptions. Should always return a subclass of
        ``django.http.HttpResponse``.
        """
        if isinstance(exception, (NotFound, ObjectDoesNotExist, Http404)):
            return HttpResponseNotFound
        elif isinstance(exception, UnsupportedSerializationFormat):
            return http.HttpNotAcceptable
        elif isinstance(exception, UnsupportedDeserializationFormat):
            return http.HttpUnsupportedMediaType
        elif isinstance(exception, UnsupportedFormat):
            return http.HttpBadRequest

        return http.HttpApplicationError

    def _handle_500(self, request, exception):
        the_trace = traceback.format_exception(*sys.exc_info())
        the_trace = u'\n'.join(the_trace)

        response_class = self.get_response_class_for_exception(request, exception)

        if settings.DEBUG:
            data = {
                "error_message": sanitize(str(exception)),
                "traceback": the_trace,
            }
        else:
            data = {
                "error_message": getattr(settings, 'TASTYPIE_CANNED_ERROR', "Sorry, this request could not be processed. Please try again later."),
            }

        if response_class.status_code >= 500:
            log = logging.getLogger('django.request.tastypie')
            log.error('Internal Server Error: %s' % request.path, exc_info=True,
                      extra={'status_code': response_class.status_code, 'request': request})

        # Send the signal so other apps are aware of the exception.
        got_request_exception.send(self.__class__, request=request)

        return self.error_response(request, data, response_class=response_class)

    def _build_reverse_url(self, name, args=None, kwargs=None):
        """
        A convenience hook for overriding how URLs are built.

        See ``NamespacedModelResource._build_reverse_url`` for an example.
        """
        return reverse(name, args=args, kwargs=kwargs)

    def base_urls(self):
        """
        The standard URLs this ``Resource`` should respond to.
        """
        return [
            re_path(r"^(?P<resource_name>%s)%s$" % (self._meta.resource_name, trailing_slash), self.wrap_view('dispatch_list'), name="api_dispatch_list"),
            re_path(r"^(?P<resource_name>%s)/schema%s$" % (self._meta.resource_name, trailing_slash), self.wrap_view('get_schema'), name="api_get_schema"),
            re_path(r"^(?P<resource_name>%s)/set/(?P<%s_list>.*?)%s$" % (self._meta.resource_name, self._meta.detail_uri_name, trailing_slash), self.wrap_view('get_multiple'), name="api_get_multiple"),
            re_path(r"^(?P<resource_name>%s)/(?P<%s>.*?)%s$" % (self._meta.resource_name, self._meta.detail_uri_name, trailing_slash), self.wrap_view('dispatch_detail'), name="api_dispatch_detail"),
        ]

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
        The endpoints this ``Resource`` responds to.

        Mostly a standard URLconf, this is suitable for either automatic use
        when registered with an ``Api`` class or for including directly in
        a URLconf should you choose to.
        """
        urls = self.prepend_urls()

        overridden_urls = self.override_urls()
        if overridden_urls:
            warnings.warn("'override_urls' is a deprecated method & will be removed by v1.0.0. Please rename your method to ``prepend_urls``.")
            urls += overridden_urls

        urls += self.base_urls()
        return urls

    def determine_format(self, request):
        """
        Used to determine the desired format.

        Largely relies on ``tastypie.utils.mime.determine_format`` but here
        as a point of extension.
        """
        return determine_format(request, self._meta.serializer, default_format=self._meta.default_format)

    def serialize(self, request, data, format, options=None):
        """
        Given a request, data and a desired format, produces a serialized
        version suitable for transfer over the wire.

        Mostly a hook, this uses the ``Serializer`` from ``Resource._meta``.
        """
        options = options or {}

        if 'text/javascript' in format:
            # get JSONP callback name. default to "callback"
            callback = request.GET.get('callback', 'callback')

            if not is_valid_jsonp_callback_value(callback):
                raise BadRequest('JSONP callback name is invalid.')

            options['callback'] = callback

        return self._meta.serializer.serialize(data, format, options)

    def deserialize(self, request, data, format='application/json'):
        """
        Given a request, data and a format, deserializes the given data.

        It relies on the request properly sending a ``CONTENT_TYPE`` header,
        falling back to ``application/json`` if not provided.

        Mostly a hook, this uses the ``Serializer`` from ``Resource._meta``.
        """
        deserialized = self._meta.serializer.deserialize(data, format=request.META.get('CONTENT_TYPE', format))
        return deserialized

    def alter_list_data_to_serialize(self, request, data):
        """
        A hook to alter list data just before it gets serialized & sent to the user.

        Useful for restructuring/renaming aspects of the what's going to be
        sent.

        Should accommodate for a list of objects, generally also including
        meta data.
        """
        return data

    def alter_detail_data_to_serialize(self, request, data):
        """
        A hook to alter detail data just before it gets serialized & sent to the user.

        Useful for restructuring/renaming aspects of the what's going to be
        sent.

        Should accommodate for receiving a single bundle of data.
        """
        return data

    def alter_deserialized_list_data(self, request, data):
        """
        A hook to alter list data just after it has been received from the user &
        gets deserialized.

        Useful for altering the user data before any hydration is applied.
        """
        return data

    def alter_deserialized_detail_data(self, request, data):
        """
        A hook to alter detail data just after it has been received from the user &
        gets deserialized.

        Useful for altering the user data before any hydration is applied.
        """
        return data

    def dispatch_list(self, request, **kwargs):
        """
        A view for handling the various HTTP methods (GET/POST/PUT/DELETE) over
        the entire list of resources.

        Relies on ``Resource.dispatch`` for the heavy-lifting.
        """
        return self.dispatch('list', request, **kwargs)

    def dispatch_detail(self, request, **kwargs):
        """
        A view for handling the various HTTP methods (GET/POST/PUT/DELETE) on
        a single resource.

        Relies on ``Resource.dispatch`` for the heavy-lifting.
        """
        return self.dispatch('detail', request, **kwargs)

    def dispatch(self, request_type, request, **kwargs):
        """
        Handles the common operations (allowed HTTP method, authentication,
        throttling, method lookup) surrounding most CRUD interactions.
        """
        allowed_methods = getattr(self._meta, "%s_allowed_methods" % request_type, None)

        if 'HTTP_X_HTTP_METHOD_OVERRIDE' in request.META:
            request.method = request.META['HTTP_X_HTTP_METHOD_OVERRIDE']

        request_method = self.method_check(request, allowed=allowed_methods)
        method = getattr(self, "%s_%s" % (request_method, request_type), None)

        if method is None:
            raise ImmediateHttpResponse(response=http.HttpNotImplemented())

        self.is_authenticated(request)
        self.throttle_check(request)

        # All clear. Process the request.
        request = convert_post_to_put(request)
        response = method(request, **kwargs)

        # Add the throttled request.
        self.log_throttled_access(request)

        # If what comes back isn't a ``HttpResponse``, assume that the
        # request was accepted and that some action occurred. This also
        # prevents Django from freaking out.
        if not isinstance(response, HttpResponse):
            return http.HttpNoContent()

        return response

    def remove_api_resource_names(self, url_dict):
        """
        Given a dictionary of regex matches from a URLconf, removes
        ``api_name`` and/or ``resource_name`` if found.

        This is useful for converting URLconf matches into something suitable
        for data lookup. For example::

            Model.objects.filter(**self.remove_api_resource_names(matches))
        """
        kwargs_subset = url_dict.copy()

        for key in ['api_name', 'resource_name']:
            try:
                del kwargs_subset[key]
            except KeyError:
                pass

        return kwargs_subset

    def method_check(self, request, allowed=None):
        """
        Ensures that the HTTP method used on the request is allowed to be
        handled by the resource.

        Takes an ``allowed`` parameter, which should be a list of lowercase
        HTTP methods to check against. Usually, this looks like::

            # The most generic lookup.
            self.method_check(request, self._meta.allowed_methods)

            # A lookup against what's allowed for list-type methods.
            self.method_check(request, self._meta.list_allowed_methods)

            # A useful check when creating a new endpoint that only handles
            # GET.
            self.method_check(request, ['get'])
        """
        if allowed is None:
            allowed = []

        request_method = request.method.lower()
        allows = ','.join([meth.upper() for meth in allowed])

        if request_method == "options":
            response = HttpResponse(allows)
            response['Allow'] = allows
            raise ImmediateHttpResponse(response=response)

        if request_method not in allowed:
            response = http.HttpMethodNotAllowed(allows)
            response['Allow'] = allows
            raise ImmediateHttpResponse(response=response)

        return request_method

    def is_authenticated(self, request):
        """
        Handles checking if the user is authenticated and dealing with
        unauthenticated users.

        Mostly a hook, this uses class assigned to ``authentication`` from
        ``Resource._meta``.
        """
        # Authenticate the request as needed.
        auth_result = self._meta.authentication.is_authenticated(request)

        if isinstance(auth_result, HttpResponse):
            raise ImmediateHttpResponse(response=auth_result)

        if auth_result is not True:
            raise ImmediateHttpResponse(response=http.HttpUnauthorized())

    def throttle_check(self, request):
        """
        Handles checking if the user should be throttled.

        Mostly a hook, this uses class assigned to ``throttle`` from
        ``Resource._meta``.
        """
        identifier = self._meta.authentication.get_identifier(request)

        # Check to see if they should be throttled.
        throttle = self._meta.throttle.should_be_throttled(identifier)

        if throttle:
            # Throttle limit exceeded.

            response = http.HttpTooManyRequests()

            if isinstance(throttle, int) and not isinstance(throttle, bool):
                response['Retry-After'] = throttle
            elif isinstance(throttle, datetime):
                # change to UTC (GMT) and make naive, to avoid wsgiref also doing an implicit TZ conversion
                throttle_utc = make_naive_utc(throttle)
                response['Retry-After'] = format_date_time(mktime(throttle_utc.timetuple()))

            raise ImmediateHttpResponse(response=response)

    def log_throttled_access(self, request):
        """
        Handles the recording of the user's access for throttling purposes.

        Mostly a hook, this uses class assigned to ``throttle`` from
        ``Resource._meta``.
        """
        request_method = request.method.lower()
        self._meta.throttle.accessed(self._meta.authentication.get_identifier(request), url=request.get_full_path(), request_method=request_method)

    def unauthorized_result(self, exception):
        raise ImmediateHttpResponse(response=http.HttpUnauthorized())

    def authorized_read_list(self, object_list, bundle):
        """
        Handles checking of permissions to see if the user has authorization
        to GET this resource.
        """
        try:
            auth_result = self._meta.authorization.read_list(object_list, bundle)
        except Unauthorized as e:
            self.unauthorized_result(e)

        return auth_result

    def authorized_read_detail(self, object_list, bundle):
        """
        Handles checking of permissions to see if the user has authorization
        to GET this resource.
        """
        try:
            auth_result = self._meta.authorization.read_detail(object_list, bundle)
            if auth_result is not True:
                raise Unauthorized()
        except Unauthorized as e:
            self.unauthorized_result(e)

        return auth_result

    def authorized_create_list(self, object_list, bundle):
        """
        Handles checking of permissions to see if the user has authorization
        to POST this resource.
        """
        try:
            auth_result = self._meta.authorization.create_list(object_list, bundle)
        except Unauthorized as e:
            self.unauthorized_result(e)

        return auth_result

    def authorized_create_detail(self, object_list, bundle):
        """
        Handles checking of permissions to see if the user has authorization
        to POST this resource.
        """
        try:
            auth_result = self._meta.authorization.create_detail(object_list, bundle)
            if auth_result is not True:
                raise Unauthorized()
        except Unauthorized as e:
            self.unauthorized_result(e)

        return auth_result

    def authorized_update_list(self, object_list, bundle):
        """
        Handles checking of permissions to see if the user has authorization
        to PUT this resource.
        """
        try:
            auth_result = self._meta.authorization.update_list(object_list, bundle)
        except Unauthorized as e:
            self.unauthorized_result(e)

        return auth_result

    def authorized_update_detail(self, object_list, bundle):
        """
        Handles checking of permissions to see if the user has authorization
        to PUT this resource.
        """
        try:
            auth_result = self._meta.authorization.update_detail(object_list, bundle)
            if auth_result is not True:
                raise Unauthorized()
        except Unauthorized as e:
            self.unauthorized_result(e)

        return auth_result

    def authorized_delete_list(self, object_list, bundle):
        """
        Handles checking of permissions to see if the user has authorization
        to DELETE this resource.
        """
        try:
            auth_result = self._meta.authorization.delete_list(object_list, bundle)
        except Unauthorized as e:
            self.unauthorized_result(e)

        return auth_result

    def authorized_delete_detail(self, object_list, bundle):
        """
        Handles checking of permissions to see if the user has authorization
        to DELETE this resource.
        """
        try:
            auth_result = self._meta.authorization.delete_detail(object_list, bundle)
            if not auth_result:
                raise Unauthorized()
        except Unauthorized as e:
            self.unauthorized_result(e)

        return auth_result

    def build_bundle(self, obj=None, data=None, request=None, objects_saved=None, via_uri=None):
        """
        Given either an object, a data dictionary or both, builds a ``Bundle``
        for use throughout the ``dehydrate/hydrate`` cycle.

        If no object is provided, an empty object from
        ``Resource._meta.object_class`` is created so that attempts to access
        ``bundle.obj`` do not fail.
        """
        if obj is None and self._meta.object_class:
            obj = self._meta.object_class()

        return Bundle(
            obj=obj,
            data=data,
            request=request,
            objects_saved=objects_saved,
            via_uri=via_uri
        )

    def build_filters(self, filters=None, ignore_bad_filters=False):
        """
        Allows for the filtering of applicable objects.

        This needs to be implemented at the user level.'

        ``ModelResource`` includes a full working version specific to Django's
        ``Models``.
        """
        return filters

    def apply_sorting(self, obj_list, options=None):
        """
        Allows for the sorting of objects being returned.

        This needs to be implemented at the user level.

        ``ModelResource`` includes a full working version specific to Django's
        ``Models``.
        """
        return obj_list

    def get_bundle_detail_data(self, bundle):
        """
        Convenience method to return the ``detail_uri_name`` attribute off
        ``bundle.obj``.

        Usually just accesses ``bundle.obj.pk`` by default.
        """
        return getattr(bundle.obj, self._meta.detail_uri_name, None)

    # URL-related methods.

    def detail_uri_kwargs(self, bundle_or_obj):
        """
        Given a ``Bundle`` or an object (typically a ``Model`` instance),
        it returns the extra kwargs needed to generate a detail URI.

        By default, it uses this resource's ``detail_uri_name`` in order to
        create the URI.
        """
        kwargs = {}

        if isinstance(bundle_or_obj, Bundle):
            bundle_or_obj = bundle_or_obj.obj

        kwargs[self._meta.detail_uri_name] = getattr(bundle_or_obj, self._meta.detail_uri_name)

        return kwargs

    def resource_uri_kwargs(self, bundle_or_obj=None):
        """
        Builds a dictionary of kwargs to help generate URIs.

        Automatically provides the ``Resource.Meta.resource_name`` (and
        optionally the ``Resource.Meta.api_name`` if populated by an ``Api``
        object).

        If the ``bundle_or_obj`` argument is provided, it calls
        ``Resource.detail_uri_kwargs`` for additional bits to create
        """
        kwargs = {
            'resource_name': self._meta.resource_name,
        }

        if self._meta.api_name is not None:
            kwargs['api_name'] = self._meta.api_name

        if bundle_or_obj is not None:
            kwargs.update(self.detail_uri_kwargs(bundle_or_obj))

        return kwargs

    def get_resource_uri(self, bundle_or_obj=None, url_name='api_dispatch_list'):
        """
        Handles generating a resource URI.

        If the ``bundle_or_obj`` argument is not provided, it builds the URI
        for the list endpoint.

        If the ``bundle_or_obj`` argument is provided, it builds the URI for
        the detail endpoint.

        Return the generated URI. If that URI can not be reversed (not found
        in the URLconf), it will return an empty string.
        """
        if bundle_or_obj is not None:
            url_name = 'api_dispatch_detail'

        try:
            return self._build_reverse_url(url_name, kwargs=self.resource_uri_kwargs(bundle_or_obj))
        except NoReverseMatch:
            return ''

    def get_via_uri(self, uri, request=None):
        """
        This pulls apart the salient bits of the URI and populates the
        resource via a ``obj_get``.

        Optionally accepts a ``request``.

        If you need custom behavior based on other portions of the URI,
        simply override this method.
        """
        prefix = get_script_prefix()
        chomped_uri = uri

        if prefix and chomped_uri.startswith(prefix):
            chomped_uri = chomped_uri[len(prefix) - 1:]

        # We know that we are dealing with a "detail" URI
        # Look for the beginning of object key (last meaningful part of the URI)
        end_of_resource_name = chomped_uri.rstrip('/').rfind('/')
        if end_of_resource_name == -1:
            raise NotFound("An incorrect URL was provided '%s' for the '%s' resource." % (uri, self.__class__.__name__))
        # We mangle the path a bit further & run URL resolution against *only*
        # the current class (but up to detail key). This ought to prevent bad
        # URLs from resolving to incorrect data.
        split_url = chomped_uri.rstrip('/').rsplit('/', 1)[0]
        if not split_url.endswith('/' + self._meta.resource_name):
            raise NotFound("An incorrect URL was provided '%s' for the '%s' resource." % (uri, self.__class__.__name__))
        found_at = chomped_uri.rfind(self._meta.resource_name, 0, end_of_resource_name)
        chomped_uri = chomped_uri[found_at:]
        try:
            for url_resolver in getattr(self, 'urls', []):
                result = url_resolver.resolve(chomped_uri)

                if result is not None:
                    view, args, kwargs = result
                    break
            else:
                raise Resolver404("URI not found in 'self.urls'.")
        except Resolver404:
            raise NotFound("The URL provided '%s' was not a link to a valid resource." % uri)

        bundle = self.build_bundle(request=request)
        return self.obj_get(bundle=bundle, **self.remove_api_resource_names(kwargs))

    # Data preparation.

    def full_dehydrate(self, bundle, for_list=False):
        """
        Given a bundle with an object instance, extract the information from it
        to populate the resource.
        """
        data = bundle.data

        api_name = self._meta.api_name
        resource_name = self._meta.resource_name

        # Dehydrate each field.
        for field_name, field_object in self.fields.items():
            # If it's not for use in this mode, skip
            field_use_in = field_object.use_in
            if callable(field_use_in):
                if not field_use_in(bundle):
                    continue
            else:
                if field_use_in not in ['all', 'list' if for_list else 'detail']:
                    continue

            # A touch leaky but it makes URI resolution work.
            if field_object.dehydrated_type == 'related':
                field_object.api_name = api_name
                field_object.resource_name = resource_name

            data[field_name] = field_object.dehydrate(bundle, for_list=for_list)

            # Check for an optional method to do further dehydration.
            method = getattr(self, "dehydrate_%s" % field_name, None)

            if method:
                data[field_name] = method(bundle)

        bundle = self.dehydrate(bundle)
        return bundle

    def dehydrate(self, bundle):
        """
        A hook to allow a final manipulation of data once all fields/methods
        have built out the dehydrated data.

        Useful if you need to access more than one dehydrated field or want
        to annotate on additional data.

        Must return the modified bundle.
        """
        return bundle

    def full_hydrate(self, bundle):
        """
        Given a populated bundle, distill it and turn it back into
        a full-fledged object instance.
        """
        if bundle.obj is None:
            bundle.obj = self._meta.object_class()

        bundle = self.hydrate(bundle)

        for field_name, field_object in self.fields.items():
            if field_object.readonly is True:
                continue

            # Check for an optional method to do further hydration.
            method = getattr(self, "hydrate_%s" % field_name, None)

            if method:
                bundle = method(bundle)

            if field_object.attribute:
                value = field_object.hydrate(bundle)

                # NOTE: We only get back a bundle when it is related field.
                if isinstance(value, Bundle) and value.errors.get(field_name):
                    bundle.errors[field_name] = value.errors[field_name]

                if value is not None or field_object.null:
                    # We need to avoid populating M2M data here as that will
                    # cause things to blow up.
                    if not field_object.is_related:
                        setattr(bundle.obj, field_object.attribute, value)
                    elif not field_object.is_m2m:
                        if value is not None:
                            # NOTE: A bug fix in Django (ticket #18153) fixes incorrect behavior
                            # which Tastypie was relying on.  To fix this, we store value.obj to
                            # be saved later in save_related.
                            try:
                                setattr(bundle.obj, field_object.attribute, value.obj)
                            except (ValueError, ObjectDoesNotExist):
                                bundle.related_objects_to_save[field_object.attribute] = value.obj
                        # not required, not sent
                        elif field_object.blank and field_name not in bundle.data:
                            continue
                        elif field_object.null:
                            if not isinstance(getattr(bundle.obj.__class__, field_object.attribute, None), ReverseOneToOneDescriptor):
                                # only update if not a reverse one to one field
                                setattr(bundle.obj, field_object.attribute, value)

        return bundle

    def hydrate(self, bundle):
        """
        A hook to allow an initial manipulation of data before all methods/fields
        have built out the hydrated data.

        Useful if you need to access more than one hydrated field or want
        to annotate on additional data.

        Must return the modified bundle.
        """
        return bundle

    def hydrate_m2m(self, bundle):
        """
        Populate the ManyToMany data on the instance.
        """
        if bundle.obj is None:
            raise HydrationError("You must call 'full_hydrate' before attempting to run 'hydrate_m2m' on %r." % self)

        for field_name, field_object in self.fields.items():
            if not field_object.is_m2m:
                continue

            if field_object.attribute:
                # Note that we only hydrate the data, leaving the instance
                # unmodified. It's up to the user's code to handle this.
                # The ``ModelResource`` provides a working baseline
                # in this regard.
                bundle.data[field_name] = field_object.hydrate_m2m(bundle)

        for field_name, field_object in self.fields.items():
            if not field_object.is_m2m:
                continue

            method = getattr(self, "hydrate_%s" % field_name, None)

            if method:
                method(bundle)

        return bundle

    def build_schema(self):
        """
        Returns a dictionary of all the fields on the resource and some
        properties about those fields.

        Used by the ``schema/`` endpoint to describe what will be available.
        """
        data = {
            'fields': {},
            'default_format': self._meta.default_format,
            'allowed_list_http_methods': self._meta.list_allowed_methods,
            'allowed_detail_http_methods': self._meta.detail_allowed_methods,
            'default_limit': self._meta.limit,
        }

        if self._meta.ordering:
            data['ordering'] = self._meta.ordering

        if self._meta.filtering:
            data['filtering'] = self._meta.filtering

        # Skip assigning pk_field_name for non-model resources
        try:
            pk_field_name = self._meta.queryset.model._meta.pk.name
        except AttributeError:
            pk_field_name = None

        for field_name, field_object in self.fields.items():
            data['fields'][field_name] = {
                'default': field_object.default,
                'type': field_object.dehydrated_type,
                'nullable': field_object.null,
                'blank': field_object.blank,
                'readonly': field_object.readonly,
                'help_text': field_object.help_text,
                'unique': field_object.unique,
                'primary_key': True if field_name == pk_field_name else False,
                'verbose_name': field_object.verbose_name or field_name.replace("_", " "),
            }

            if field_object.dehydrated_type == 'related':
                if field_object.is_m2m:
                    related_type = 'to_many'
                else:
                    related_type = 'to_one'
                data['fields'][field_name]['related_type'] = related_type
                try:
                    uri = self._build_reverse_url('api_get_schema', kwargs={
                        'api_name': self._meta.api_name,
                        'resource_name': field_object.to_class()._meta.resource_name
                    })
                except NoReverseMatch:
                    uri = ''
                data['fields'][field_name]['related_schema'] = uri

        return data

    def dehydrate_resource_uri(self, bundle):
        """
        For the automatically included ``resource_uri`` field, dehydrate
        the URI for the given bundle.

        Returns empty string if no URI can be generated.
        """
        try:
            return self.get_resource_uri(bundle)
        except NotImplementedError:
            return ''
        except NoReverseMatch:
            return ''

    def generate_cache_key(self, *args, **kwargs):
        """
        Creates a unique-enough cache key.

        This is based off the current api_name/resource_name/args/kwargs.
        """
        smooshed = ["%s=%s" % (key, value) for key, value in kwargs.items()]

        # Use a list plus a ``.join()`` because it's faster than concatenation.
        return "%s:%s:%s:%s" % (self._meta.api_name, self._meta.resource_name, ':'.join(args), ':'.join(sorted(smooshed)))

    # Data access methods.

    def get_object_list(self, request):
        """
        A hook to allow making returning the list of available objects.

        This needs to be implemented at the user level.

        ``ModelResource`` includes a full working version specific to Django's
        ``Models``.
        """
        raise NotImplementedError()

    def can_create(self):
        """
        Checks to ensure ``post`` is within ``allowed_methods``.
        """
        allowed = set(self._meta.list_allowed_methods + self._meta.detail_allowed_methods)
        return 'post' in allowed

    def can_update(self):
        """
        Checks to ensure ``put`` is within ``allowed_methods``.

        Used when hydrating related data.
        """
        allowed = set(self._meta.list_allowed_methods + self._meta.detail_allowed_methods)
        return 'put' in allowed

    def can_delete(self):
        """
        Checks to ensure ``delete`` is within ``allowed_methods``.
        """
        allowed = set(self._meta.list_allowed_methods + self._meta.detail_allowed_methods)
        return 'delete' in allowed

    def apply_filters(self, request, applicable_filters):
        """
        A hook to alter how the filters are applied to the object list.

        This needs to be implemented at the user level.

        ``ModelResource`` includes a full working version specific to Django's
        ``Models``.
        """
        raise NotImplementedError()

    def obj_get_list(self, bundle, **kwargs):
        """
        Fetches the list of objects available on the resource.

        This needs to be implemented at the user level.

        ``ModelResource`` includes a full working version specific to Django's
        ``Models``.
        """
        raise NotImplementedError()

    def cached_obj_get_list(self, bundle, **kwargs):
        """
        A version of ``obj_get_list`` that uses the cache as a means to get
        commonly-accessed data faster.
        """
        cache_key = self.generate_cache_key('list', **kwargs)
        obj_list = self._meta.cache.get(cache_key)

        if obj_list is None:
            obj_list = self.obj_get_list(bundle=bundle, **kwargs)
            self._meta.cache.set(cache_key, obj_list)

        return obj_list

    def obj_get(self, bundle, **kwargs):
        """
        Fetches an individual object on the resource.

        This needs to be implemented at the user level. If the object can not
        be found, this should raise a ``NotFound`` exception.

        ``ModelResource`` includes a full working version specific to Django's
        ``Models``.
        """
        raise NotImplementedError()

    def cached_obj_get(self, bundle, **kwargs):
        """
        A version of ``obj_get`` that uses the cache as a means to get
        commonly-accessed data faster.
        """
        cache_key = self.generate_cache_key('detail', **kwargs)
        cached_bundle = self._meta.cache.get(cache_key)

        if cached_bundle is None:
            cached_bundle = self.obj_get(bundle=bundle, **kwargs)
            self._meta.cache.set(cache_key, cached_bundle)

        return cached_bundle

    def obj_create(self, bundle, **kwargs):
        """
        Creates a new object based on the provided data.

        This needs to be implemented at the user level.

        ``ModelResource`` includes a full working version specific to Django's
        ``Models``.
        """
        raise NotImplementedError()

    def obj_update(self, bundle, **kwargs):
        """
        Updates an existing object (or creates a new object) based on the
        provided data.

        This needs to be implemented at the user level.

        ``ModelResource`` includes a full working version specific to Django's
        ``Models``.
        """
        raise NotImplementedError()

    def obj_delete_list(self, bundle, **kwargs):
        """
        Deletes an entire list of objects.

        This needs to be implemented at the user level.

        ``ModelResource`` includes a full working version specific to Django's
        ``Models``.
        """
        raise NotImplementedError()

    def obj_delete_list_for_update(self, bundle, **kwargs):
        """
        Deletes an entire list of objects, specific to PUT list.

        This needs to be implemented at the user level.

        ``ModelResource`` includes a full working version specific to Django's
        ``Models``.
        """
        raise NotImplementedError()

    def obj_delete(self, bundle, **kwargs):
        """
        Deletes a single object.

        This needs to be implemented at the user level.

        ``ModelResource`` includes a full working version specific to Django's
        ``Models``.
        """
        raise NotImplementedError()

    def create_response(self, request, data, response_class=HttpResponse, **response_kwargs):
        """
        Extracts the common "which-format/serialize/return-response" cycle.

        Mostly a useful shortcut/hook.
        """
        desired_format = self.determine_format(request)
        serialized = self.serialize(request, data, desired_format)
        return response_class(content=serialized, content_type=build_content_type(desired_format), **response_kwargs)

    def error_response(self, request, errors, response_class=None):
        """
        Extracts the common "which-format/serialize/return-error-response"
        cycle.

        Should be used as much as possible to return errors.
        """
        if response_class is None:
            response_class = http.HttpBadRequest

        desired_format = None

        if request:
            if request.GET.get('callback', None) is None:
                try:
                    desired_format = self.determine_format(request)
                except BadRequest:
                    pass  # Fall through to default handler below
            else:
                # JSONP can cause extra breakage.
                desired_format = 'application/json'

        if not desired_format:
            desired_format = self._meta.default_format

        try:
            serialized = self.serialize(request, errors, desired_format)
        except BadRequest as e:
            error = "Additional errors occurred, but serialization of those errors failed."

            if settings.DEBUG:
                error += " %s" % e

            return response_class(content=error, content_type='text/plain')

        return response_class(content=serialized, content_type=build_content_type(desired_format))

    def is_valid(self, bundle):
        """
        Handles checking if the data provided by the user is valid.

        Mostly a hook, this uses class assigned to ``validation`` from
        ``Resource._meta``.

        If validation fails, an error is raised with the error messages
        serialized inside it.
        """
        errors = self._meta.validation.is_valid(bundle, bundle.request)

        if errors:
            bundle.errors[self._meta.resource_name] = errors
            return False

        return True

    def rollback(self, bundles):
        """
        Given the list of bundles, delete all objects pertaining to those
        bundles.

        This needs to be implemented at the user level. No exceptions should
        be raised if possible.

        ``ModelResource`` includes a full working version specific to Django's
        ``Models``.
        """
        raise NotImplementedError()

    # Views.

    def get_list(self, request, **kwargs):
        """
        Returns a serialized list of resources.

        Calls ``obj_get_list`` to provide the data, then handles that result
        set and serializes it.

        Should return a HttpResponse (200 OK).
        """
        # TODO: Uncached for now. Invalidation that works for everyone may be
        #       impossible.
        base_bundle = self.build_bundle(request=request)
        objects = self.obj_get_list(bundle=base_bundle, **self.remove_api_resource_names(kwargs))
        sorted_objects = self.apply_sorting(objects, options=request.GET)

        paginator = self._meta.paginator_class(request.GET, sorted_objects, resource_uri=self.get_resource_uri(), limit=self._meta.limit, max_limit=self._meta.max_limit, collection_name=self._meta.collection_name)
        to_be_serialized = paginator.page()

        # Dehydrate the bundles in preparation for serialization.
        bundles = [
            self.full_dehydrate(self.build_bundle(obj=obj, request=request), for_list=True)
            for obj in to_be_serialized[self._meta.collection_name]
        ]

        to_be_serialized[self._meta.collection_name] = bundles
        to_be_serialized = self.alter_list_data_to_serialize(request, to_be_serialized)
        return self.create_response(request, to_be_serialized)

    def get_detail(self, request, **kwargs):
        """
        Returns a single serialized resource.

        Calls ``cached_obj_get/obj_get`` to provide the data, then handles that result
        set and serializes it.

        Should return a HttpResponse (200 OK).
        """
        basic_bundle = self.build_bundle(request=request)

        try:
            obj = self.cached_obj_get(bundle=basic_bundle, **self.remove_api_resource_names(kwargs))
        except ObjectDoesNotExist:
            return http.HttpNotFound()
        except MultipleObjectsReturned:
            return http.HttpMultipleChoices("More than one resource is found at this URI.")

        bundle = self.build_bundle(obj=obj, request=request)
        bundle = self.full_dehydrate(bundle)
        bundle = self.alter_detail_data_to_serialize(request, bundle)
        return self.create_response(request, bundle)

    def post_list(self, request, **kwargs):
        """
        Creates a new resource/object with the provided data.

        Calls ``obj_create`` with the provided data and returns a response
        with the new resource's location.

        If a new resource is created, return ``HttpCreated`` (201 Created).
        If ``Meta.always_return_data = True``, there will be a populated body
        of serialized data.
        """
        deserialized = self.deserialize(request, request.body, format=request.META.get('CONTENT_TYPE', 'application/json'))
        deserialized = self.alter_deserialized_detail_data(request, deserialized)
        bundle = self.build_bundle(data=deserialized, request=request)
        updated_bundle = self.obj_create(bundle, **self.remove_api_resource_names(kwargs))
        location = self.get_resource_uri(updated_bundle)

        if not self._meta.always_return_data:
            return http.HttpCreated(location=location)
        else:
            updated_bundle = self.full_dehydrate(updated_bundle)
            updated_bundle = self.alter_detail_data_to_serialize(request, updated_bundle)
            return self.create_response(request, updated_bundle, response_class=http.HttpCreated, location=location)

    def post_detail(self, request, **kwargs):
        """
        Creates a new subcollection of the resource under a resource.

        This is not implemented by default because most people's data models
        aren't self-referential.

        If a new resource is created, return ``HttpCreated`` (201 Created).
        """
        return http.HttpNotImplemented()

    def put_list(self, request, **kwargs):
        """
        Replaces a collection of resources with another collection.

        Calls ``delete_list`` to clear out the collection then ``obj_create``
        with the provided the data to create the new collection.

        Return ``HttpNoContent`` (204 No Content) if
        ``Meta.always_return_data = False`` (default).

        Return ``HttpResponse`` (200 OK) with new data if
        ``Meta.always_return_data = True``.
        """
        deserialized = self.deserialize(request, request.body, format=request.META.get('CONTENT_TYPE', 'application/json'))
        deserialized = self.alter_deserialized_list_data(request, deserialized)

        if self._meta.collection_name not in deserialized:
            raise BadRequest("Invalid data sent: missing '%s'" % self._meta.collection_name)

        basic_bundle = self.build_bundle(request=request)
        self.obj_delete_list_for_update(bundle=basic_bundle, **self.remove_api_resource_names(kwargs))
        bundles_seen = []

        for object_data in deserialized[self._meta.collection_name]:
            bundle = self.build_bundle(data=object_data, request=request)

            # Attempt to be transactional, deleting any previously created
            # objects if validation fails.
            try:
                self.obj_create(bundle=bundle, **self.remove_api_resource_names(kwargs))
                bundles_seen.append(bundle)
            except ImmediateHttpResponse:
                self.rollback(bundles_seen)
                raise

        if not self._meta.always_return_data:
            return http.HttpNoContent()
        else:
            to_be_serialized = {
                self._meta.collection_name: [
                    self.full_dehydrate(b, for_list=True)
                    for b in bundles_seen
                ]
            }
            to_be_serialized = self.alter_list_data_to_serialize(request, to_be_serialized)
            return self.create_response(request, to_be_serialized)

    def put_detail(self, request, **kwargs):
        """
        Either updates an existing resource or creates a new one with the
        provided data.

        Calls ``obj_update`` with the provided data first, but falls back to
        ``obj_create`` if the object does not already exist.

        If a new resource is created, return ``HttpCreated`` (201 Created).
        If ``Meta.always_return_data = True``, there will be a populated body
        of serialized data.

        If an existing resource is modified and
        ``Meta.always_return_data = False`` (default), return ``HttpNoContent``
        (204 No Content).
        If an existing resource is modified and
        ``Meta.always_return_data = True``, return ``HttpAccepted`` (200
        OK).
        """
        deserialized = self.deserialize(request, request.body, format=request.META.get('CONTENT_TYPE', 'application/json'))
        deserialized = self.alter_deserialized_detail_data(request, deserialized)
        bundle = self.build_bundle(data=deserialized, request=request)

        try:
            updated_bundle = self.obj_update(bundle=bundle, **self.remove_api_resource_names(kwargs))

            if not self._meta.always_return_data:
                return http.HttpNoContent()
            else:
                # Invalidate prefetched_objects_cache for bundled object
                # because we might have changed a prefetched field
                updated_bundle.obj._prefetched_objects_cache = {}
                updated_bundle = self.full_dehydrate(updated_bundle)
                updated_bundle = self.alter_detail_data_to_serialize(request, updated_bundle)
                return self.create_response(request, updated_bundle)
        except (NotFound, MultipleObjectsReturned):
            updated_bundle = self.obj_create(bundle=bundle, **self.remove_api_resource_names(kwargs))
            location = self.get_resource_uri(updated_bundle)

            if not self._meta.always_return_data:
                return http.HttpCreated(location=location)
            else:
                updated_bundle = self.full_dehydrate(updated_bundle)
                updated_bundle = self.alter_detail_data_to_serialize(request, updated_bundle)
                return self.create_response(request, updated_bundle, response_class=http.HttpCreated, location=location)

    def delete_list(self, request, **kwargs):
        """
        Destroys a collection of resources/objects.

        Calls ``obj_delete_list``.

        If the resources are deleted, return ``HttpNoContent`` (204 No Content).
        """
        bundle = self.build_bundle(request=request)
        self.obj_delete_list(bundle=bundle, request=request, **self.remove_api_resource_names(kwargs))
        return http.HttpNoContent()

    def delete_detail(self, request, **kwargs):
        """
        Destroys a single resource/object.

        Calls ``obj_delete``.

        If the resource is deleted, return ``HttpNoContent`` (204 No Content).
        If the resource did not exist, return ``Http404`` (404 Not Found).
        """
        # Manually construct the bundle here, since we don't want to try to
        # delete an empty instance.
        bundle = Bundle(request=request)

        try:
            self.obj_delete(bundle=bundle, **self.remove_api_resource_names(kwargs))
            return http.HttpNoContent()
        except NotFound:
            return http.HttpNotFound()

    def patch_list(self, request, **kwargs):
        """
        Updates a collection in-place.

        The exact behavior of ``PATCH`` to a list resource is still the matter of
        some debate in REST circles, and the ``PATCH`` RFC isn't standard. So the
        behavior this method implements (described below) is something of a
        stab in the dark. It's mostly cribbed from GData, with a smattering
        of ActiveResource-isms and maybe even an original idea or two.

        The ``PATCH`` format is one that's similar to the response returned from
        a ``GET`` on a list resource::

            {
              "objects": [{object}, {object}, ...],
              "deleted_objects": ["URI", "URI", "URI", ...],
            }

        For each object in ``objects``:

            * If the dict does not have a ``resource_uri`` key then the item is
              considered "new" and is handled like a ``POST`` to the resource list.

            * If the dict has a ``resource_uri`` key and the ``resource_uri`` refers
              to an existing resource then the item is a update; it's treated
              like a ``PATCH`` to the corresponding resource detail.

            * If the dict has a ``resource_uri`` but the resource *doesn't* exist,
              then this is considered to be a create-via-``PUT``.

        Each entry in ``deleted_objects`` referes to a resource URI of an existing
        resource to be deleted; each is handled like a ``DELETE`` to the relevent
        resource.

        In any case:

            * If there's a resource URI it *must* refer to a resource of this
              type. It's an error to include a URI of a different resource.

            * ``PATCH`` is all or nothing. If a single sub-operation fails, the
              entire request will fail and all resources will be rolled back.

          * For ``PATCH`` to work, you **must** have ``put`` in your
            :ref:`detail-allowed-methods` setting.

          * To delete objects via ``deleted_objects`` in a ``PATCH`` request you
            **must** have ``delete`` in your :ref:`detail-allowed-methods`
            setting.

        Substitute appropriate names for ``objects`` and
        ``deleted_objects`` if ``Meta.collection_name`` is set to something
        other than ``objects`` (default).
        """
        request = convert_post_to_patch(request)
        deserialized = self.deserialize(request, request.body, format=request.META.get('CONTENT_TYPE', 'application/json'))

        collection_name = self._meta.collection_name
        deleted_collection_name = 'deleted_%s' % collection_name
        if collection_name not in deserialized:
            raise BadRequest("Invalid data sent: missing '%s'" % collection_name)

        if len(deserialized[collection_name]) and 'put' not in self._meta.detail_allowed_methods:
            raise ImmediateHttpResponse(response=http.HttpMethodNotAllowed())

        bundles_seen = []

        for data in deserialized[collection_name]:
            # If there's a resource_uri then this is either an
            # update-in-place or a create-via-PUT.
            if "resource_uri" in data:
                uri = data.pop('resource_uri')

                try:
                    obj = self.get_via_uri(uri, request=request)

                    # The object does exist, so this is an update-in-place.
                    bundle = self.build_bundle(obj=obj, request=request)
                    bundle = self.full_dehydrate(bundle, for_list=True)
                    bundle = self.alter_detail_data_to_serialize(request, bundle)
                    self.update_in_place(request, bundle, data)
                except (ObjectDoesNotExist, MultipleObjectsReturned):
                    # The object referenced by resource_uri doesn't exist,
                    # so this is a create-by-PUT equivalent.
                    data = self.alter_deserialized_detail_data(request, data)
                    bundle = self.build_bundle(data=data, request=request)
                    self.obj_create(bundle=bundle)
            else:
                # There's no resource URI, so this is a create call just
                # like a POST to the list resource.
                data = self.alter_deserialized_detail_data(request, data)
                bundle = self.build_bundle(data=data, request=request)
                self.obj_create(bundle=bundle)

            bundles_seen.append(bundle)

        deleted_collection = deserialized.get(deleted_collection_name, [])

        if deleted_collection:
            if 'delete' not in self._meta.detail_allowed_methods:
                raise ImmediateHttpResponse(response=http.HttpMethodNotAllowed())

            for uri in deleted_collection:
                obj = self.get_via_uri(uri, request=request)
                bundle = self.build_bundle(obj=obj, request=request)
                self.obj_delete(bundle=bundle)

        if not self._meta.always_return_data:
            return http.HttpAccepted()
        else:
            to_be_serialized = {
                'objects': [
                    self.full_dehydrate(b, for_list=True)
                    for b in bundles_seen
                ]
            }
            to_be_serialized = self.alter_list_data_to_serialize(request, to_be_serialized)
            return self.create_response(request, to_be_serialized, response_class=http.HttpAccepted)

    def patch_detail(self, request, **kwargs):
        """
        Updates a resource in-place.

        Calls ``obj_update``.

        If the resource is updated, return ``HttpAccepted`` (202 Accepted).
        If the resource did not exist, return ``HttpNotFound`` (404 Not Found).
        """
        request = convert_post_to_patch(request)
        basic_bundle = self.build_bundle(request=request)

        # We want to be able to validate the update, but we can't just pass
        # the partial data into the validator since all data needs to be
        # present. Instead, we basically simulate a PUT by pulling out the
        # original data and updating it in-place.
        # So first pull out the original object. This is essentially
        # ``get_detail``.
        try:
            obj = self.cached_obj_get(bundle=basic_bundle, **self.remove_api_resource_names(kwargs))
        except ObjectDoesNotExist:
            return http.HttpNotFound()
        except MultipleObjectsReturned:
            return http.HttpMultipleChoices("More than one resource is found at this URI.")

        bundle = self.build_bundle(obj=obj, request=request)
        bundle = self.full_dehydrate(bundle)
        bundle = self.alter_detail_data_to_serialize(request, bundle)

        # Now update the bundle in-place.
        deserialized = self.deserialize(request, request.body, format=request.META.get('CONTENT_TYPE', 'application/json'))
        # Create a place to store the names of those fields we want to update
        bundle.update_fields = []
        bundle.m2m_update_fields = []
        # When we get to the obj.save() stage, we need to know which fields have changed
        # Otherwise we can't do a proper update.  Thus,
        # For every key in deserialized (e.g. the fields submitted in the PATCH)
        for key in deserialized:
            # If the key is a property of the object, lets add it to the list, except:
            if hasattr(bundle.obj, key):
                # Can't update_fields an m2m field, so instead add it to patch_m2m_fields
                if getattr(self.fields[key], 'is_m2m', False):
                    bundle.m2m_update_fields.append(key)
                    continue
                # Don't add if it is the id/pk field, can't patch that.
                if key == 'id' or key == 'pk':
                    continue
                # No more checks.  Add it.
                bundle.update_fields.append(key)
        self.update_in_place(request, bundle, deserialized)

        if not self._meta.always_return_data:
            return http.HttpAccepted()
        else:
            # Invalidate prefetched_objects_cache for bundled object
            # because we might have changed a prefetched field
            bundle.obj._prefetched_objects_cache = {}
            bundle = self.full_dehydrate(bundle)
            bundle = self.alter_detail_data_to_serialize(request, bundle)
            return self.create_response(request, bundle, response_class=http.HttpAccepted)

    def update_in_place(self, request, original_bundle, new_data):
        """
        Update the object in original_bundle in-place using new_data.
        """
        original_bundle.data.update(**new_data)

        # Now we've got a bundle with the new data sitting in it and we're
        # we're basically in the same spot as a PUT request. SO the rest of this
        # function is cribbed from put_detail.
        self.alter_deserialized_detail_data(request, original_bundle.data)
        kwargs = {
            self._meta.detail_uri_name: self.get_bundle_detail_data(original_bundle),
            'request': request,
        }
        return self.obj_update(bundle=original_bundle, **kwargs)

    def get_schema(self, request, **kwargs):
        """
        Returns a serialized form of the schema of the resource.

        Calls ``build_schema`` to generate the data. This method only responds
        to HTTP GET.

        Should return a HttpResponse (200 OK).
        """
        self.method_check(request, allowed=['get'])
        self.is_authenticated(request)
        self.throttle_check(request)
        self.log_throttled_access(request)
        bundle = self.build_bundle(request=request)
        self.authorized_read_detail(self.get_object_list(bundle.request), bundle)
        return self.create_response(request, self.build_schema())

    def get_multiple(self, request, **kwargs):
        """
        Returns a serialized list of resources based on the identifiers
        from the URL.

        Calls ``obj_get_list`` to fetch only the objects requests in
        a single query. This method only responds to HTTP GET.

        For backward compatibility the method ``obj_get`` is used if
        ``obj_get_list`` is not implemented.

        Should return a HttpResponse (200 OK).
        """
        self.method_check(request, allowed=['get'])
        self.is_authenticated(request)
        self.throttle_check(request)

        # Rip apart the list then iterate.
        kwarg_name = '%s_list' % self._meta.detail_uri_name
        obj_identifiers = kwargs.get(kwarg_name, '').split(';')
        objects = []
        not_found = []
        base_bundle = self.build_bundle(request=request)

        # We will try to get a queryset from obj_get_list.
        queryset = None

        try:
            queryset = self.obj_get_list(bundle=base_bundle).filter(
                **{self._meta.detail_uri_name + '__in': obj_identifiers})
        except NotImplementedError:
            pass

        if queryset is not None:
            # Fetch the objects from the queryset to a dictionary.
            objects_dict = {}
            for obj in queryset:
                objects_dict[str(getattr(obj, self._meta.detail_uri_name))] = obj

            # Walk the list of identifiers in order and get the objects or feed the not_found list.
            for identifier in obj_identifiers:
                if identifier in objects_dict:
                    bundle = self.build_bundle(obj=objects_dict[identifier], request=request)
                    bundle = self.full_dehydrate(bundle, for_list=True)
                    objects.append(bundle)
                else:
                    not_found.append(identifier)
        else:
            # Use the old way.
            for identifier in obj_identifiers:
                try:
                    obj = self.obj_get(bundle=base_bundle, **{self._meta.detail_uri_name: identifier})
                    bundle = self.build_bundle(obj=obj, request=request)
                    bundle = self.full_dehydrate(bundle, for_list=True)
                    objects.append(bundle)
                except (ObjectDoesNotExist, Unauthorized):
                    not_found.append(identifier)

        object_list = {
            self._meta.collection_name: objects,
        }

        if len(not_found):
            object_list['not_found'] = not_found

        self.log_throttled_access(request)
        return self.create_response(request, object_list)


class ModelDeclarativeMetaclass(DeclarativeMetaclass):
    def __new__(cls, name, bases, attrs):
        meta = attrs.get('Meta')
        if getattr(meta, 'abstract', False):
            # abstract base classes do nothing on declaration
            new_class = super(ModelDeclarativeMetaclass, cls).__new__(cls, name, bases, attrs)
            return new_class

        # Sanity check: ModelResource needs either a queryset or object_class:
        if meta and not hasattr(meta, 'queryset') and not hasattr(meta, 'object_class'):
            msg = "ModelResource (%s) requires Meta.object_class or Meta.queryset"
            raise ImproperlyConfigured(msg % name)

        if hasattr(meta, 'queryset') and not hasattr(meta, 'object_class'):
            setattr(meta, 'object_class', meta.queryset.model)

        new_class = super(ModelDeclarativeMetaclass, cls).__new__(cls, name, bases, attrs)
        specified_fields = getattr(new_class._meta, 'fields', None)
        excludes = getattr(new_class._meta, 'excludes', [])
        field_names = list(new_class.base_fields.keys())

        include_fields = specified_fields

        if include_fields is None:
            if meta and meta.object_class:
                include_fields = [f.name for f in meta.object_class._meta.fields]
            else:
                include_fields = []

        for field_name in field_names:
            if field_name == 'resource_uri':
                continue
            if field_name in new_class.declared_fields:
                continue
            if specified_fields is not None and field_name not in include_fields:
                del new_class.base_fields[field_name]
            if field_name in excludes:
                del new_class.base_fields[field_name]

        # Add in the new fields.
        new_class.base_fields.update(new_class.get_fields(include_fields, excludes))

        if getattr(new_class._meta, 'include_absolute_url', True):
            if 'absolute_url' not in new_class.base_fields:
                new_class.base_fields['absolute_url'] = fields.CharField(attribute='get_absolute_url', readonly=True)
        elif 'absolute_url' in new_class.base_fields and 'absolute_url' not in attrs:
            del new_class.base_fields['absolute_url']

        return new_class


class BaseModelResource(Resource):
    """
    A subclass of ``Resource`` designed to work with Django's ``Models``.

    This class will introspect a given ``Model`` and build a field list based
    on the fields found on the model (excluding relational fields).

    Given that it is aware of Django's ORM, it also handles the CRUD data
    operations of the resource.
    """
    @classmethod
    def should_skip_field(cls, field):
        """
        Given a Django model field, return if it should be included in the
        contributed ApiFields.
        """
        if isinstance(field, ForeignKey):
            return True
        # Ignore certain fields (related fields).
        if hasattr(field, 'remote_field'):
            if field.remote_field:
                return True
        elif getattr(field, 'rel'):
            return True

        return False

    @classmethod
    def api_field_from_django_field(cls, f, default=fields.CharField):
        """
        Returns the field type that would likely be associated with each
        Django type.
        """
        result = default
        internal_type = f.get_internal_type()

        if internal_type == 'DateField':
            result = fields.DateField
        elif internal_type == 'DateTimeField':
            result = fields.DateTimeField
        elif internal_type in ('BooleanField', 'NullBooleanField'):
            result = fields.BooleanField
        elif internal_type in ('FloatField',):
            result = fields.FloatField
        elif internal_type in ('DecimalField',):
            result = fields.DecimalField
        elif internal_type in ('IntegerField', 'PositiveIntegerField', 'PositiveSmallIntegerField', 'SmallIntegerField', 'AutoField', 'BigIntegerField', 'BigAutoField'):
            result = fields.IntegerField
        elif internal_type in ('FileField', 'ImageField'):
            result = fields.FileField
        elif internal_type == 'TimeField':
            result = fields.TimeField
        # TODO: Perhaps enable these via introspection. The reason they're not enabled
        #       by default is the very different ``__init__`` they have over
        #       the other fields.
        # elif internal_type == 'ForeignKey':
        #     result = ForeignKey
        # elif internal_type == 'ManyToManyField':
        #     result = ManyToManyField

        return result

    @classmethod
    def get_fields(cls, fields=None, excludes=None):
        """
        Given any explicit fields to include and fields to exclude, add
        additional fields based on the associated model.
        """
        final_fields = {}
        fields = fields or []
        excludes = excludes or []

        if not cls._meta.object_class:
            return final_fields

        for f in cls._meta.object_class._meta.fields:
            # If the field name is already present, skip
            if f.name in cls.base_fields:
                continue

            # If field is not present in explicit field listing, skip
            if f.name not in fields:
                continue

            # If field is in exclude list, skip
            if f.name in excludes:
                continue

            if cls.should_skip_field(f):
                continue

            api_field_class = cls.api_field_from_django_field(f)

            kwargs = {
                'attribute': f.name,
                'help_text': f.help_text,
                'verbose_name': f.verbose_name,
            }

            if f.null is True:
                kwargs['null'] = True

            kwargs['unique'] = f.unique

            if not f.null and f.blank is True:
                kwargs['default'] = ''
                kwargs['blank'] = True

            if f.get_internal_type() == 'TextField':
                kwargs['default'] = ''

            if f.has_default():
                kwargs['default'] = f.default

            if getattr(f, 'auto_now', False):
                kwargs['default'] = f.auto_now

            if getattr(f, 'auto_now_add', False):
                kwargs['default'] = f.auto_now_add

            final_fields[f.name] = api_field_class(**kwargs)
            final_fields[f.name].instance_name = f.name

        return final_fields

    def check_filtering(self, field_name, filter_type='exact', filter_bits=None):
        """
        Given a field name, a optional filter type and an optional list of
        additional relations, determine if a field can be filtered on.

        If a filter does not meet the needed conditions, it should raise an
        ``InvalidFilterError``.

        If the filter meets the conditions, a list of attribute names (not
        field names) will be returned.
        """
        if filter_bits is None:
            filter_bits = []

        if field_name not in self._meta.filtering:
            raise InvalidFilterError("The '%s' field does not allow filtering." % field_name)

        # Check to see if it's an allowed lookup type.
        if self._meta.filtering[field_name] not in (ALL, ALL_WITH_RELATIONS):
            # Must be an explicit whitelist.
            if filter_type not in self._meta.filtering[field_name]:
                raise InvalidFilterError("'%s' is not an allowed filter on the '%s' field." % (filter_type, field_name))

        if self.fields[field_name].attribute is None:
            raise InvalidFilterError("The '%s' field has no 'attribute' for searching with." % field_name)

        # Check to see if it's a relational lookup and if that's allowed.
        if len(filter_bits):
            if not getattr(self.fields[field_name], 'is_related', False):
                raise InvalidFilterError("The '%s' field does not support relations." % field_name)

            if not self._meta.filtering[field_name] == ALL_WITH_RELATIONS:
                raise InvalidFilterError("Lookups are not allowed more than one level deep on the '%s' field." % field_name)

            # Recursively descend through the remaining lookups in the filter,
            # if any. We should ensure that all along the way, we're allowed
            # to filter on that field by the related resource.
            related_resource = self.fields[field_name].get_related_resource(None)
            return [self.fields[field_name].attribute] + related_resource.check_filtering(filter_bits[0], filter_type, filter_bits[1:])

        return [self.fields[field_name].attribute]

    def filter_value_to_python(self, value, field_name, filters, filter_expr,
            filter_type):
        """
        Turn the string ``value`` into a python object.
        """
        # Simple values
        value = string_to_python(value)

        # Split on ',' if not empty string and either an in or range filter.
        if filter_type in ('in', 'range') and len(value):
            if hasattr(filters, 'getlist'):
                value = []

                for part in filters.getlist(filter_expr):
                    value.extend(part.split(','))
            else:
                value = value.split(',')

        return value

    def build_filters(self, filters=None, ignore_bad_filters=False):
        """
        Given a dictionary of filters, create the necessary ORM-level filters.

        Keys should be resource fields, **NOT** model fields.

        Valid values are either a list of Django filter types (i.e.
        ``['startswith', 'exact', 'lte']``), the ``ALL`` constant or the
        ``ALL_WITH_RELATIONS`` constant.
        """
        # At the declarative level:
        #     filtering = {
        #         'resource_field_name': ['exact', 'startswith', 'endswith', 'contains'],
        #         'resource_field_name_2': ['exact', 'gt', 'gte', 'lt', 'lte', 'range'],
        #         'resource_field_name_3': ALL,
        #         'resource_field_name_4': ALL_WITH_RELATIONS,
        #         ...
        #     }
        # Accepts the filters as a dict. None by default, meaning no filters.
        if filters is None:
            filters = {}

        qs_filters = {}

        for filter_expr, value in filters.items():
            filter_bits = filter_expr.split(LOOKUP_SEP)
            field_name = filter_bits.pop(0)
            filter_type = 'exact'

            if field_name not in self.fields:
                # It's not a field we know about. Move along citizen.
                continue

            # Validate filter types other than 'exact' that are supported by the field type
            try:
                django_field_name = self.fields[field_name].attribute
                django_field = self._meta.object_class._meta.get_field(django_field_name)
                if hasattr(django_field, 'field'):
                    django_field = django_field.field  # related field
            except FieldDoesNotExist:
                raise InvalidFilterError("The '%s' field is not a valid field name" % field_name)

            query_terms = django_field.get_lookups().keys()
            if len(filter_bits) and filter_bits[-1] in query_terms:
                filter_type = filter_bits.pop()

            try:
                lookup_bits = self.check_filtering(field_name, filter_type, filter_bits)
            except InvalidFilterError:
                if ignore_bad_filters:
                    continue
                else:
                    raise
            value = self.filter_value_to_python(value, field_name, filters, filter_expr, filter_type)

            db_field_name = LOOKUP_SEP.join(lookup_bits)
            qs_filter = "%s%s%s" % (db_field_name, LOOKUP_SEP, filter_type)
            qs_filters[qs_filter] = value

        return qs_filters

    def apply_sorting(self, obj_list, options=None):
        """
        Given a dictionary of options, apply some ORM-level sorting to the
        provided ``QuerySet``.

        Looks for the ``order_by`` key and handles either ascending (just the
        field name) or descending (the field name with a ``-`` in front).

        The field name should be the resource field, **NOT** model field.
        """
        if options is None:
            options = {}

        parameter_name = 'order_by'

        if 'order_by' not in options:
            if 'sort_by' not in options:
                # Nothing to alter the order. Return what we've got.
                return obj_list
            else:
                warnings.warn("'sort_by' is a deprecated parameter. Please use 'order_by' instead.")
                parameter_name = 'sort_by'

        order_by_args = []

        if hasattr(options, 'getlist'):
            order_bits = options.getlist(parameter_name)
        else:
            order_bits = options.get(parameter_name)

            if not isinstance(order_bits, (list, tuple)):
                order_bits = [order_bits]

        for order_by in order_bits:
            order_by_bits = order_by.split(LOOKUP_SEP)

            field_name = order_by_bits[0]
            order = ''

            if order_by_bits[0].startswith('-'):
                field_name = order_by_bits[0][1:]
                order = '-'

            if field_name not in self.fields:
                # It's not a field we know about. Move along citizen.
                raise InvalidSortError("No matching '%s' field for ordering on." % field_name)

            if field_name not in self._meta.ordering:
                raise InvalidSortError("The '%s' field does not allow ordering." % field_name)

            if self.fields[field_name].attribute is None:
                raise InvalidSortError("The '%s' field has no 'attribute' for ordering with." % field_name)

            order_by_args.append("%s%s" % (order, LOOKUP_SEP.join([self.fields[field_name].attribute] + order_by_bits[1:])))

        return obj_list.order_by(*order_by_args)

    def apply_filters(self, request, applicable_filters):
        """
        An ORM-specific implementation of ``apply_filters``.

        The default simply applies the ``applicable_filters`` as ``**kwargs``,
        but should make it possible to do more advanced things.
        """
        return self.get_object_list(request).filter(**applicable_filters)

    def get_object_list(self, request):
        """
        An ORM-specific implementation of ``get_object_list``.

        Returns a queryset that may have been limited by other overrides.
        """
        return self._meta.queryset._clone()

    def obj_get_list(self, bundle, **kwargs):
        """
        A ORM-specific implementation of ``obj_get_list``.

        ``GET`` dictionary of bundle.request can be used to narrow the query.
        """
        filters = {}

        if hasattr(bundle.request, 'GET'):
            # Grab a mutable copy.
            filters = bundle.request.GET.copy()

        # Update with the provided kwargs.
        filters.update(kwargs)
        applicable_filters = self.build_filters(filters=filters)

        try:
            objects = self.apply_filters(bundle.request, applicable_filters)
            return self.authorized_read_list(objects, bundle)
        except ValueError:
            raise BadRequest("Invalid resource lookup data provided (mismatched type).")

    def obj_get(self, bundle, **kwargs):
        """
        A ORM-specific implementation of ``obj_get``.

        Takes optional ``kwargs``, which are used to narrow the query to find
        the instance.
        """
        # Use ignore_bad_filters=True. `obj_get_list` filters based on
        # request.GET, but `obj_get` usually filters based on `detail_uri_name`
        # or data from a related field, so we don't want to raise errors if
        # something doesn't explicitly match a configured filter.
        applicable_filters = self.build_filters(filters=kwargs, ignore_bad_filters=True)
        if self._meta.detail_uri_name in kwargs:
            applicable_filters[self._meta.detail_uri_name] = kwargs[self._meta.detail_uri_name]

        try:
            object_list = self.apply_filters(bundle.request, applicable_filters)
            stringified_kwargs = ', '.join(["%s=%s" % (k, v) for k, v in applicable_filters.items()])

            if len(object_list) <= 0:
                raise self._meta.object_class.DoesNotExist("Couldn't find an instance of '%s' which matched '%s'." % (self._meta.object_class.__name__, stringified_kwargs))
            elif len(object_list) > 1:
                raise MultipleObjectsReturned("More than one '%s' matched '%s'." % (self._meta.object_class.__name__, stringified_kwargs))

            bundle.obj = object_list[0]
            self.authorized_read_detail(object_list, bundle)
            return bundle.obj
        except ValueError:
            raise NotFound("Invalid resource lookup data provided (mismatched type).")

    def obj_create(self, bundle, **kwargs):
        """
        A ORM-specific implementation of ``obj_create``.
        """
        bundle.obj = self._meta.object_class()

        for key, value in kwargs.items():
            setattr(bundle.obj, key, value)

        bundle = self.full_hydrate(bundle)
        return self.save(bundle)

    def lookup_kwargs_with_identifiers(self, bundle, kwargs):
        """
        Kwargs here represent uri identifiers Ex: /repos/<user_id>/<repo_name>/
        We need to turn those identifiers into Python objects for generating
        lookup parameters that can find them in the DB
        """
        lookup_kwargs = {}

        # Handle detail_uri_name specially
        if self._meta.detail_uri_name in kwargs:
            lookup_kwargs[self._meta.detail_uri_name] = kwargs.pop(self._meta.detail_uri_name)

        bundle.obj = self.get_object_list(bundle.request).model()
        # Override data values, we rely on uri identifiers
        bundle.data.update(kwargs)
        # We're going to manually hydrate, as opposed to calling
        # ``full_hydrate``, to ensure we don't try to flesh out related
        # resources & keep things speedy.
        bundle = self.hydrate(bundle)

        for identifier in kwargs:
            field_object = self.fields[identifier]

            # Skip readonly or related fields.
            if field_object.readonly or field_object.is_related or\
                    not field_object.attribute:
                continue

            # Check for an optional method to do further hydration.
            method = getattr(self, "hydrate_%s" % identifier, None)

            if method:
                bundle = method(bundle)

            lookup_kwargs[identifier] = field_object.hydrate(bundle)

        return lookup_kwargs

    def obj_update(self, bundle, skip_errors=False, **kwargs):
        """
        A ORM-specific implementation of ``obj_update``.
        """
        bundle_detail_data = self.get_bundle_detail_data(bundle)
        arg_detail_data = kwargs.get(self._meta.detail_uri_name)

        if bundle_detail_data is None or (arg_detail_data is not None and str(bundle_detail_data) != str(arg_detail_data)):
            try:
                lookup_kwargs = self.lookup_kwargs_with_identifiers(bundle, kwargs)
            except:  # noqa
                # if there is trouble hydrating the data, fall back to just
                # using kwargs by itself (usually it only contains a "pk" key
                # and this will work fine.
                lookup_kwargs = kwargs

            try:
                bundle.obj = self.obj_get(bundle=bundle, **lookup_kwargs)
            except ObjectDoesNotExist:
                raise NotFound("A model instance matching the provided arguments could not be found.")

        bundle = self.full_hydrate(bundle)
        return self.save(bundle, skip_errors=skip_errors)

    def obj_delete_list(self, bundle, **kwargs):
        """
        A ORM-specific implementation of ``obj_delete_list``.
        """
        objects_to_delete = self.obj_get_list(bundle=bundle, **kwargs)
        deletable_objects = self.authorized_delete_list(objects_to_delete, bundle)

        if hasattr(deletable_objects, 'delete'):
            # It's likely a ``QuerySet``. Call ``.delete()`` for efficiency.
            deletable_objects.delete()
        else:
            for authed_obj in deletable_objects:
                authed_obj.delete()

    def obj_delete_list_for_update(self, bundle, **kwargs):
        """
        A ORM-specific implementation of ``obj_delete_list_for_update``.
        """
        objects_to_delete = self.obj_get_list(bundle=bundle, **kwargs)
        deletable_objects = self.authorized_update_list(objects_to_delete, bundle)

        if hasattr(deletable_objects, 'delete'):
            # It's likely a ``QuerySet``. Call ``.delete()`` for efficiency.
            deletable_objects.delete()
        else:
            for authed_obj in deletable_objects:
                authed_obj.delete()

    def obj_delete(self, bundle, **kwargs):
        """
        A ORM-specific implementation of ``obj_delete``.

        Takes optional ``kwargs``, which are used to narrow the query to find
        the instance.
        """
        if not hasattr(bundle.obj, 'delete'):
            try:
                bundle.obj = self.obj_get(bundle=bundle, **kwargs)
            except ObjectDoesNotExist:
                raise NotFound("A model instance matching the provided arguments could not be found.")

        self.authorized_delete_detail(self.get_object_list(bundle.request), bundle)
        bundle.obj.delete()

    @atomic_decorator()
    def patch_list(self, request, **kwargs):
        """
        An ORM-specific implementation of ``patch_list``.

        Necessary because PATCH should be atomic (all-success or all-fail)
        and the only way to do this neatly is at the database level.
        """
        return super(BaseModelResource, self).patch_list(request, **kwargs)

    def rollback(self, bundles):
        """
        A ORM-specific implementation of ``rollback``.

        Given the list of bundles, delete all models pertaining to those
        bundles.
        """
        for bundle in bundles:
            if bundle.obj and self.get_bundle_detail_data(bundle):
                bundle.obj.delete()

    def create_identifier(self, obj):
        return u"%s.%s.%s" % (obj._meta.app_label, get_module_name(obj._meta), obj.pk)

    def save(self, bundle, skip_errors=False):
        if bundle.via_uri:
            return bundle

        self.is_valid(bundle)

        if bundle.errors and not skip_errors:
            raise ImmediateHttpResponse(response=self.error_response(bundle.request, bundle.errors))

        # Check if they're authorized.
        if bundle.obj.pk:
            self.authorized_update_detail(self.get_object_list(bundle.request), bundle)
        else:
            self.authorized_create_detail(self.get_object_list(bundle.request), bundle)

        # Save FKs just in case.
        self.save_related(bundle)

        # Save the main object.
        obj_id = self.create_identifier(bundle.obj)

        if obj_id not in bundle.objects_saved or bundle.obj._state.adding:
            if hasattr(bundle, 'update_fields'):
                bundle.obj.save(update_fields=bundle.update_fields)
            else:
                bundle.obj.save()
            obj_id = self.create_identifier(bundle.obj)
            bundle.objects_saved.add(obj_id)

        # Now pick up the M2M bits.
        m2m_bundle = self.hydrate_m2m(bundle)
        self.save_m2m(m2m_bundle)
        return bundle

    def save_related(self, bundle):
        """
        Handles the saving of related non-M2M data.

        Calling assigning ``child.parent = parent`` & then calling
        ``Child.save`` isn't good enough to make sure the ``parent``
        is saved.

        To get around this, we go through all our related fields &
        call ``save`` on them if they have related, non-M2M data.
        M2M data is handled by the ``ModelResource.save_m2m`` method.
        """
        for field_name, field_object in self.fields.items():
            if not field_object.is_related:
                continue

            if field_object.is_m2m:
                continue

            if not field_object.attribute:
                continue

            if field_object.readonly:
                continue

            if field_object.blank and field_name not in bundle.data:
                continue

            # Get the object.
            try:
                related_obj = getattr(bundle.obj, field_object.attribute)
            except ObjectDoesNotExist:
                # Django 1.8: unset related objects default to None, no error
                related_obj = None

            # We didn't get it, so maybe we created it but haven't saved it
            if related_obj is None:
                related_obj = bundle.related_objects_to_save.get(field_object.attribute, None)

            if related_obj and field_object.related_name:
                # this might be a reverse relation, so we need to save this
                # model, attach it to the related object, and save the related
                # object.
                if not self.get_bundle_detail_data(bundle):
                    bundle.obj.save()

                setattr(related_obj, field_object.related_name, bundle.obj)

            related_resource = field_object.get_related_resource(related_obj)

            # Before we build the bundle & try saving it, let's make sure we
            # haven't already saved it.
            if related_obj:
                obj_id = self.create_identifier(related_obj)

                if obj_id in bundle.objects_saved:
                    # It's already been saved. We're done here.
                    continue

            if bundle.data.get(field_name):
                if hasattr(bundle.data[field_name], 'keys'):
                    # Only build & save if there's data, not just a URI.
                    related_bundle = related_resource.build_bundle(
                        obj=related_obj,
                        data=bundle.data.get(field_name),
                        request=bundle.request,
                        objects_saved=bundle.objects_saved
                    )
                    related_resource.full_hydrate(related_bundle)
                    related_resource.save(related_bundle)
                    related_obj = related_bundle.obj
                elif field_object.related_name:
                    # This condition probably means a URI for a reverse
                    # relation was provided.
                    related_bundle = related_resource.build_bundle(
                        obj=related_obj,
                        request=bundle.request,
                        objects_saved=bundle.objects_saved
                    )
                    related_resource.save(related_bundle)
                    related_obj = related_bundle.obj

            if related_obj:
                setattr(bundle.obj, field_object.attribute, related_obj)

    def save_m2m(self, bundle):
        """
        Handles the saving of related M2M data.

        Due to the way Django works, the M2M data must be handled after the
        main instance, which is why this isn't a part of the main ``save`` bits.

        Currently slightly inefficient in that it will clear out the whole
        relation and recreate the related data as needed.
        """
        for field_name, field_object in self.fields.items():
            if not field_object.is_m2m:
                continue

            if not field_object.attribute:
                continue

            if field_object.readonly:
                continue

            # If this is a PATCH, make sure that this field name is one of the
            # patched fields (recorded in the update_fields property of the bundle).
            # Otherwise, we do not want to save / recreate this field.
            if hasattr(bundle, 'update_fields'):
                # This bundle is from a PATCH, we should not save an M2M field
                # unless it was present in the PATCH
                if field_name not in bundle.m2m_update_fields:
                    continue  # Skip this field_name
                else:  # this field name WAS in the patch, lets save it.
                    pass
            else:  # Not a PATCH operation, carry on normally.
                pass

            # Get the manager.
            related_mngr = None

            if isinstance(field_object.attribute, str):
                related_mngr = getattr(bundle.obj, field_object.attribute)
            elif callable(field_object.attribute):
                related_mngr = field_object.attribute(bundle)

            if not related_mngr:
                continue

            if hasattr(related_mngr, 'clear'):
                # FIXME: Dupe the original bundle, copy in the new object &
                #        check the perms on that (using the related resource)?

                # Clear it out, just to be safe.
                related_mngr.clear()

            related_objs = []

            for related_bundle in bundle.data[field_name]:
                related_resource = field_object.get_related_resource(bundle.obj)

                # Only build & save if there's data, not just a URI.
                updated_related_bundle = related_resource.build_bundle(
                    obj=related_bundle.obj,
                    data=related_bundle.data,
                    request=bundle.request,
                    objects_saved=bundle.objects_saved,
                    via_uri=related_bundle.via_uri,
                )

                related_resource.save(updated_related_bundle)
                related_objs.append(updated_related_bundle.obj)

            related_mngr.add(*related_objs)


class ModelResource(BaseModelResource, metaclass=ModelDeclarativeMetaclass):
    pass


class NamespacedModelResource(ModelResource):
    """
    A ModelResource subclass that respects Django namespaces.
    """
    def _build_reverse_url(self, name, args=None, kwargs=None):
        namespaced = "%s:%s" % (self._meta.urlconf_namespace, name)
        return reverse(namespaced, args=args, kwargs=kwargs)


# Based off of ``piston.utils.coerce_put_post``. Similarly BSD-licensed.
# And no, the irony is not lost on me.
def convert_post_to_VERB(request, verb):
    """
    Force Django to process the VERB.
    """
    if request.method == verb:
        if not hasattr(request, '_read_started'):
            request._read_started = False

        if hasattr(request, '_post'):
            del request._post
            del request._files

        try:
            request.method = "POST"
            request._load_post_and_files()
            request.method = verb
        except AttributeError:
            request.META['REQUEST_METHOD'] = 'POST'
            request._load_post_and_files()
            request.META['REQUEST_METHOD'] = verb
        setattr(request, verb, request.POST)

    return request


def convert_post_to_put(request):
    return convert_post_to_VERB(request, verb='PUT')


def convert_post_to_patch(request):
    return convert_post_to_VERB(request, verb='PATCH')
