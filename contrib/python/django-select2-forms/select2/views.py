import copy
import json

from django.db import models
from django.apps import apps
from django.forms.models import ModelChoiceIterator
from django.http import HttpResponse
from django.utils.encoding import force_str
try:
    from django.forms.models import ModelChoiceIteratorValue
except ImportError:
    ModelChoiceIteratorValue = None

from .fields import ManyToManyField, compat_rel


class ViewException(Exception):
    pass


class InvalidParameter(ViewException):
    pass


class JsonResponse(HttpResponse):

    callback = None

    def __init__(self, content='', callback=None, content_type="application/json", *args, **kwargs):
        if not isinstance(content, str):
            content = json.dumps(content)
        if callback is not None:
            self.callback = callback
        if self.callback is not None:
            content = u"%s(\n%s\n)" % (self.callback, content)
            content_type = "text/javascript"
        return super(JsonResponse, self).__init__(content=content,
            content_type=content_type, *args, **kwargs)


class Select2View(object):

    def __init__(self, request, app_label, model_name, field_name):
        self.request = request
        self.app_label = app_label
        self.model_name = model_name
        self.field_name = field_name

    _field = None

    def get_field_and_model(self):
        model_cls = apps.get_model(self.app_label, self.model_name)
        if model_cls is None:
            raise ViewException('Model %s.%s does not exist' % (self.app_label, self.model_name))
        if self._field is None:
            self._field = model_cls._meta.get_field(self.field_name)
        return self._field, model_cls

    def get_response(self, data, **kwargs):
        callback = self.request.GET.get('callback', None)
        if callback is None:
            response_cls = JsonResponse
        else:
            response_cls = type('JsonpResponse', (JsonResponse,), {
                'callback': callback,
            })
        return response_cls(data, **kwargs)

    def get_data(self, queryset, page=None, page_limit=None):
        field, model_cls = self.get_field_and_model()

        # Check for the existences of a callable %s_queryset method on the
        # model class and use it to filter the Select2 queryset.
        #
        # This is useful for model inheritance where the limit_choices_to can
        # not easily be overriden in child classes.
        model_queryset_method = '%s_queryset' % field.name
        if callable(getattr(model_cls, model_queryset_method, None)):
            queryset = getattr(model_cls, model_queryset_method)(queryset)

        formfield = field.formfield()
        total_count = None
        if page is not None and page_limit is not None:
            total_count = queryset.count()
            offset = (page - 1) * page_limit
            end = offset + page_limit
            queryset = queryset[offset:end]
        else:
            offset = None

        formfield.queryset = queryset
        iterator = ModelChoiceIterator(formfield)

        if offset is None:
            total_count = len(iterator)
            more = False
        else:
            paged_count = offset + len(iterator)
            more = bool(paged_count < total_count)

        data = {
            'total': total_count,
            'more': more,
            'results': [],
        }
        for value, label in iterator:
            if value is u'':
                continue
            
            if ModelChoiceIteratorValue and isinstance(value, ModelChoiceIteratorValue):
                # ModelChoiceIteratorValue was added in Django 3.1
                value = value.value

            data['results'].append({
                'id': value,
                'text': label,
            })
        return data

    def init_selection(self):
        try:
            field, model_cls = self.get_field_and_model()
        except ViewException as e:
            return self.get_response({'error': str(e)}, status=500)

        q = self.request.GET.get('q', None)
        try:
            if q is None:
                raise InvalidParameter("q parameter required")
            pks = q.split(u',')
            try:
                pks = [int(pk) for pk in pks]
            except TypeError:
                raise InvalidParameter("q parameter must be comma separated "
                                       "list of integers")
        except InvalidParameter as e:
            return self.get_response({'error': str(e)}, status=500)

        queryset = field.queryset.filter(**{
            (u'%s__in' % compat_rel(field).get_related_field().name): pks,
        }).distinct()
        pk_ordering = dict([(force_str(pk), i) for i, pk in enumerate(pks)])

        data = self.get_data(queryset)

        # Make sure we return in the same order we were passed
        def results_sort_callback(item):
            pk = force_str(item['id'])
            return pk_ordering[pk]
        data['results'] = sorted(data['results'], key=results_sort_callback)

        if len(data['results']) == 1:
            is_multiple = isinstance(field, ManyToManyField)
            try:
                multiple_param = int(self.request.GET.get('multiple'))
            except (TypeError, ValueError):
                pass
            else:
                is_multiple = (multiple_param == 1)
            if not is_multiple:
                data['results'] = data['results'][0]

        return self.get_response(data)

    def fetch_items(self):
        try:
            field, model_cls = self.get_field_and_model()
        except ViewException as e:
            return self.get_response({'error': str(e)}, status=500)

        queryset = copy.deepcopy(field.queryset)

        q = self.request.GET.get('q', None)
        page_limit = self.request.GET.get('page_limit', 10)
        page = self.request.GET.get('page', 1)

        try:
            if q is None:
                raise InvalidParameter("q parameter required")
            try:
                page_limit = int(page_limit)
            except TypeError:
                raise InvalidParameter("Invalid page_limit '%s' passed" % page_limit)
            else:
                if page_limit < 1:
                    raise InvalidParameter("Invalid page_limit '%s' passed" % page_limit)

            try:
                page = int(page)
            except TypeError:
                raise InvalidParameter("Invalid page '%s' passed")
            else:
                if page < 1:
                    raise InvalidParameter("Invalid page '%s' passed")
        except InvalidParameter as e:
            return self.get_response({'error': str(e)}, status=500)

        search_field = field.search_field
        if callable(search_field):
            search_field = search_field(q)
        if isinstance(search_field, models.Q):
            q_obj = search_field
        else:
            qset_contains_filter_key = '%(search_field)s__%(insensitive)scontains' % {
                'search_field': search_field,
                'insensitive': 'i' if not field.case_sensitive else '',
            }
            q_obj = models.Q(**{qset_contains_filter_key: q})

        queryset = queryset.filter(q_obj)

        data = self.get_data(queryset, page, page_limit)
        return self.get_response(data)


def init_selection(request, app_label, model_name, field_name):
    view_cls = Select2View(request, app_label, model_name, field_name)
    return view_cls.init_selection()


def fetch_items(request, app_label, model_name, field_name):
    view_cls = Select2View(request, app_label, model_name, field_name)
    return view_cls.fetch_items()
