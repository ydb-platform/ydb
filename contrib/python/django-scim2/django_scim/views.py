import json
import logging
from urllib.parse import urljoin

from django import db
from django.core.exceptions import MultipleObjectsReturned, ObjectDoesNotExist
from django.db import transaction
from django.http import HttpResponse
from django.urls import reverse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.views.generic import View
from scim2_filter_parser.parser import SCIMParserError

from . import constants, exceptions
from .settings import scim_settings
from .utils import (
    get_all_schemas_getter,
    get_base_scim_location_getter,
    get_extra_model_exclude_kwargs_getter,
    get_extra_model_filter_kwargs_getter,
    get_group_adapter,
    get_group_filter_parser,
    get_group_model,
    get_object_post_processor_getter,
    get_queryset_post_processor_getter,
    get_service_provider_config_model,
    get_user_adapter,
    get_user_filter_parser,
    get_user_model,
)

logger = logging.getLogger(__name__)


class SCIMView(View):
    lookup_url_kwarg = 'uuid'  # argument in django URL pattern
    implemented = True

    @property
    def lookup_field(self):
        """Database field, possibly redefined in the adapter"""
        return getattr(self.scim_adapter, 'id_field', 'scim_id')

    @property
    def model_cls(self):
        # pull from __class__ to avoid binding model class getter to
        # self instance and passing self to class getter
        return self.__class__.model_cls_getter()

    @property
    def get_extra_filter_kwargs(self):
        return get_extra_model_filter_kwargs_getter(self.model_cls)

    @property
    def get_extra_exclude_kwargs(self):
        return get_extra_model_exclude_kwargs_getter(self.model_cls)

    @property
    def get_object_post_processor(self):
        return get_object_post_processor_getter(self.model_cls)

    @property
    def get_queryset_post_processor(self):
        return get_queryset_post_processor_getter(self.model_cls)

    @property
    def scim_adapter(self):
        # pull from __class__ to avoid binding adapter class getter to
        # self instance and passing self to class getter
        return self.__class__.scim_adapter_getter()

    def get_object(self):
        """Get object by configurable ID."""
        # Perform the lookup filtering.
        lookup_url_kwarg = self.lookup_url_kwarg or self.lookup_field

        if lookup_url_kwarg not in self.kwargs:
            msg = (
                f'Expected view {self.__class__.__name__} to be called with a URL keyword argument '
                f'named "{lookup_url_kwarg}". Fix your URL conf, or set the `.lookup_field` '
                f'attribute on the view correctly.'
            )
            raise exceptions.BadRequestError(msg)

        uuid = self.kwargs[lookup_url_kwarg]

        extra_filter_kwargs = self.get_extra_filter_kwargs(self.request, uuid)
        extra_filter_kwargs[self.lookup_field] = uuid
        # No use of get_extra_exclude_kwargs here since we are
        # searching for a specific single object.

        try:
            obj = self.model_cls.objects.get(**extra_filter_kwargs)
            return self.get_object_post_processor(self.request, obj)
        except ObjectDoesNotExist:
            raise exceptions.NotFoundError(uuid)
        except MultipleObjectsReturned:
            msg = (
                f'Multiple objects returned by lookup of {lookup_url_kwarg} with value {uuid}. '
                f'Make sure {lookup_url_kwarg} identifies a unique instance and try again.'
            )
            raise exceptions.BadRequestError(msg)

    @method_decorator(csrf_exempt)
    @method_decorator(scim_settings.AUTH_CHECK_MIDDLEWARE)
    def dispatch(self, request, *args, **kwargs):
        if not self.implemented:
            return self.status_501(request, *args, **kwargs)

        try:
            return super(SCIMView, self).dispatch(request, *args, **kwargs)
        except Exception as e:
            if not isinstance(e, exceptions.SCIMException):
                logger.exception('Unable to complete SCIM call.')

                # In some circumstances it can be beneficial for the client
                # to know what caused an error. However, this can present an
                # unacceptable security risk for many companies. This flag
                # allows for a generic error message to be returned when such a
                # security risk is unacceptable.
                if scim_settings.EXPOSE_SCIM_EXCEPTIONS:
                    e = exceptions.SCIMException(str(e))
                else:
                    e = exceptions.SCIMException('Exception occurred while processing the SCIM request')

            content = json.dumps(e.to_dict())
            return HttpResponse(content=content,
                                content_type=constants.SCIM_CONTENT_TYPE,
                                status=e.status)

    def status_501(self, request, *args, **kwargs):
        """
        A service provider that does NOT support a feature SHOULD
        respond with HTTP status code 501 (Not Implemented).
        """
        return HttpResponse(content_type=constants.SCIM_CONTENT_TYPE, status=501)

    def load_body(self, body):
        decoded = body.decode(constants.ENCODING)
        stripped = decoded.strip() or '{}'

        try:
            return json.loads(stripped)
        except json.decoder.JSONDecodeError as e:
            msg = 'Could not decode JSON body: ' + e.args[0]
            raise exceptions.BadRequestError(msg)


class FilterMixin(object):

    parser_getter = None
    scim_adapter_getter = None

    def _page(self, request):
        try:
            start = request.GET.get('startIndex', 1)
            if start is not None:
                start = int(start)
                if start < 1:
                    raise exceptions.BadRequestError('Invalid startIndex (must be >= 1)')

            count = request.GET.get('count', 50)
            if count is not None:
                count = int(count)

            return start, count

        except ValueError as e:
            raise exceptions.BadRequestError('Invalid pagination values: ' + str(e))

    def _search(self, request, query, start, count):
        try:
            qs = self.__class__.parser_getter().search(query, request)
        except (ValueError, SCIMParserError) as e:
            raise exceptions.BadRequestError('Invalid filter/search query: ' + str(e))

        extra_filter_kwargs = self.get_extra_filter_kwargs(request)
        qs = self._filter_raw_queryset_with_extra_filter_kwargs(qs, extra_filter_kwargs)
        extra_exclude_kwargs = self.get_extra_exclude_kwargs(request)
        qs = self._filter_raw_queryset_with_extra_exclude_kwargs(qs, extra_exclude_kwargs)

        return self._build_response(request, qs, start, count)

    def _get_nested_field(self, obj, attr_key):
        """Get a nested field for a given object, so 'a__b__c' returns a tuple with (obj.a.b.c, found)"""
        tokens = attr_key.split('__')
        for field_name in tokens:
            if not hasattr(obj, field_name):
                return None, False
            obj = getattr(obj, field_name)
        return obj, True

    def _filter_raw_queryset_with_extra_filter_kwargs(self, qs, extra_filter_kwargs):
        obj_list = []
        for obj in qs:
            add_obj = True
            for attr_key, attr_val in extra_filter_kwargs.items():
                if attr_key.endswith('__in'):
                    attr_key = attr_key.replace('__in', '')
                else:
                    attr_val = [attr_val]

                value, found = self._get_nested_field(obj, attr_key)
                if not found or value not in attr_val:
                    add_obj = False
                    break

            if add_obj:
                obj_list.append(obj)

        return obj_list

    def _filter_raw_queryset_with_extra_exclude_kwargs(self, qs, extra_exclude_kwargs):
        obj_list = []
        for obj in qs:
            add_obj = True
            for attr_key, attr_val in extra_exclude_kwargs.items():
                if attr_key.endswith('__in'):
                    attr_key = attr_key.replace('__in', '')
                else:
                    attr_val = [attr_val]

                value, found = self._get_nested_field(obj, attr_key)
                if found and value in attr_val:
                    add_obj = False
                    break

            if add_obj:
                obj_list.append(obj)

        return obj_list

    def _build_response(self, request, qs, start, count):
        try:
            total_count = sum(1 for _ in qs)
            qs = qs[start - 1:(start - 1) + count]
            resources = [self.scim_adapter(o, request=request).to_dict() for o in qs]
            doc = {
                'schemas': [constants.SchemaURI.LIST_RESPONSE],
                'totalResults': total_count,
                'itemsPerPage': count,
                'startIndex': start,
                'Resources': resources,
            }
        except ValueError as e:
            raise exceptions.BadRequestError(str(e))
        else:
            content = json.dumps(doc)
            return HttpResponse(content=content,
                                content_type=constants.SCIM_CONTENT_TYPE)


class SearchView(FilterMixin, SCIMView):
    http_method_names = ['post']

    # override model class so correct extra_filter/exclude_kwarg getter is fetched
    model_cls = 'search'

    def post(self, request, *args, **kwargs):
        body = self.load_body(request.body)
        if body.get('schemas') != [constants.SchemaURI.SERACH_REQUEST]:
            raise exceptions.BadRequestError('Invalid schema uri. Must be SearchRequest.')

        query = body.get('filter', request.GET.get('filter'))

        if not query:
            raise exceptions.BadRequestError('No filter query specified')

        response = self._search(request, query, *self._page(request))
        path = reverse(self.scim_adapter.url_name)
        url = urljoin(get_base_scim_location_getter()(request=request), path).rstrip('/')
        response['Location'] = url + '/.search'
        return response


class UserSearchView(SearchView):
    scim_adapter_getter = get_user_adapter
    parser_getter = get_user_filter_parser


class GroupSearchView(SearchView):
    scim_adapter_getter = get_group_adapter
    parser_getter = get_group_filter_parser


class GetView(object):
    def get(self, request, *args, **kwargs):
        if kwargs.get(self.lookup_url_kwarg):
            return self.get_single(request)

        return self.get_many(request)

    def get_single(self, request):
        obj = self.get_object()
        scim_obj = self.scim_adapter(obj, request=request)
        content = json.dumps(scim_obj.to_dict())
        response = HttpResponse(content=content,
                                content_type=constants.SCIM_CONTENT_TYPE)
        response['Location'] = scim_obj.location
        return response

    def get_many(self, request):
        query = request.GET.get('filter')
        if query:
            return self._search(request, query, *self._page(request))

        extra_filter_kwargs = self.get_extra_filter_kwargs(request)
        extra_exclude_kwargs = self.get_extra_exclude_kwargs(request)
        qs = self.model_cls.objects.filter(
            **extra_filter_kwargs
        ).exclude(
            **extra_exclude_kwargs
        )
        qs = qs.order_by(self.lookup_field)
        qs = self.get_queryset_post_processor(request, qs)
        return self._build_response(request, qs, *self._page(request))


class DeleteView(object):
    def delete(self, request, *args, **kwargs):
        obj = self.get_object()

        scim_obj = self.scim_adapter(obj, request=request)

        scim_obj.delete()

        return HttpResponse(status=204)


class PostView(object):
    def post(self, request, *args, **kwargs):
        obj = self.model_cls()
        scim_obj = self.scim_adapter(obj, request=request)

        body = self.load_body(request.body)

        if not body:
            raise exceptions.BadRequestError('POST call made with empty body')

        scim_obj.validate_dict(body)
        scim_obj.from_dict(body)

        try:
            scim_obj.save()
        except db.utils.IntegrityError as e:
            # Cast error to a SCIM IntegrityError to use the status
            # attribute on the SCIM IntegrityError.
            raise exceptions.IntegrityError(str(e))

        content = json.dumps(scim_obj.to_dict())
        response = HttpResponse(content=content,
                                content_type=constants.SCIM_CONTENT_TYPE,
                                status=201)
        response['Location'] = scim_obj.location
        return response


class PutView(object):
    def put(self, request, *args, **kwargs):
        obj = self.get_object()

        scim_obj = self.scim_adapter(obj, request=request)

        body = self.load_body(request.body)

        if not body:
            raise exceptions.BadRequestError('PUT call made with empty body')

        scim_obj.validate_dict(body)
        scim_obj.from_dict(body)
        scim_obj.save()

        content = json.dumps(scim_obj.to_dict())
        response = HttpResponse(content=content,
                                content_type=constants.SCIM_CONTENT_TYPE)
        response['Location'] = scim_obj.location
        return response


class PatchView(object):
    def patch(self, request, *args, **kwargs):
        obj = self.get_object()

        scim_obj = self.scim_adapter(obj, request=request)
        body = self.load_body(request.body)

        operations = body.get('Operations')

        if not operations:
            raise exceptions.BadRequestError('PATCH call made without operations array')

        with transaction.atomic():
            scim_obj.handle_operations(operations)

        content = json.dumps(scim_obj.to_dict())
        response = HttpResponse(content=content,
                                content_type=constants.SCIM_CONTENT_TYPE)
        response['Location'] = scim_obj.location
        return response


class UsersView(FilterMixin, GetView, PostView, PutView, PatchView, DeleteView, SCIMView):

    http_method_names = ['get', 'post', 'put', 'patch', 'delete']

    scim_adapter_getter = get_user_adapter
    model_cls_getter = get_user_model
    parser_getter = get_user_filter_parser


class GroupsView(FilterMixin, GetView, PostView, PutView, PatchView, DeleteView, SCIMView):

    http_method_names = ['get', 'post', 'put', 'patch', 'delete']

    scim_adapter_getter = get_group_adapter
    model_cls_getter = get_group_model
    parser_getter = get_group_filter_parser


class ServiceProviderConfigView(SCIMView):
    http_method_names = ['get']

    def get(self, request):
        config = get_service_provider_config_model()(request=request)
        content = json.dumps(config.to_dict())
        return HttpResponse(content=content,
                            content_type=constants.SCIM_CONTENT_TYPE)


class ResourceTypesView(SCIMView):

    http_method_names = ['get']

    def type_dict_by_type_id(self, request):
        type_adapters = get_user_adapter(), get_group_adapter()
        type_dicts = [m.resource_type_dict(request) for m in type_adapters]
        return {d['id']: d for d in type_dicts}

    def get(self, request, uuid=None, *args, **kwargs):
        if uuid:
            doc = self.type_dict_by_type_id(request).get(uuid)
            if not doc:
                return HttpResponse(content_type=constants.SCIM_CONTENT_TYPE, status=404)

        else:
            key_func = lambda o: o.get('id')  # noqa: E731
            type_dicts = self.type_dict_by_type_id(request).values()
            types = list(sorted(type_dicts, key=key_func))
            doc = {
                'schemas': [constants.SchemaURI.LIST_RESPONSE],
                'Resources': types,
            }

        return HttpResponse(content=json.dumps(doc),
                            content_type=constants.SCIM_CONTENT_TYPE)


class SchemasView(SCIMView):

    http_method_names = ['get']

    schemas_by_uri = {s['id']: s for s in get_all_schemas_getter()()}

    def get(self, request, uuid=None, *args, **kwargs):
        if uuid:
            doc = self.schemas_by_uri.get(uuid)
            if not doc:
                return HttpResponse(content_type=constants.SCIM_CONTENT_TYPE, status=404)

        else:
            key_func = lambda o: o.get('id')  # noqa: E731
            schemas = list(sorted(self.schemas_by_uri.values(), key=key_func))
            doc = {
                'schemas': [constants.SchemaURI.LIST_RESPONSE],
                'Resources': schemas,
            }

        content = json.dumps(doc)
        return HttpResponse(content=content,
                            content_type=constants.SCIM_CONTENT_TYPE)
