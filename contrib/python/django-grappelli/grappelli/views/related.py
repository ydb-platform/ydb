# coding: utf-8

import json
from urllib.parse import unquote

from django.apps import apps
from django.contrib.admin.utils import prepare_lookup_value
from django.core.exceptions import PermissionDenied
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection, models
from django.db.models.constants import LOOKUP_SEP
from django.http import HttpResponse
from django.utils.decorators import method_decorator
from django.utils.encoding import smart_str
from django.utils.safestring import SafeText
from django.utils.translation import gettext as _
from django.utils.translation import ngettext
from django.views.decorators.cache import never_cache
from django.views.generic import View

from grappelli.settings import AUTOCOMPLETE_LIMIT, AUTOCOMPLETE_SEARCH_FIELDS


def get_label(f):
    if getattr(f, "related_label", None):
        return f.related_label()
    return smart_str(f)


def get_label_safe(f):
    if getattr(f, "related_label", None):
        if isinstance(f.related_label(), SafeText):
            return True
    return False


def import_from(module, name):
    module = __import__(module, fromlist=[name])
    return getattr(module, name)


def ajax_response(data):
    return HttpResponse(json.dumps(data, cls=DjangoJSONEncoder), content_type='application/javascript')


def get_autocomplete_search_fields(model):
    """
    Returns the fields to be used for autocomplete of the given model,
    first using the autocomplete_search_fields() static method when defined on
    the model.
    If the staticmethod is not declared, looks for the fields value in the
    GRAPPELLI_AUTOCOMPLETE_SEARCH_FIELDS setting for the given app/model.
    """
    if hasattr(model, 'autocomplete_search_fields'):
        return model.autocomplete_search_fields()

    try:
        return AUTOCOMPLETE_SEARCH_FIELDS[model._meta.app_label][model._meta.model_name]
    except KeyError:
        return


class RelatedLookup(View):
    "Related Lookup"

    def check_user_permission(self):
        if not (self.request.user.is_active and self.request.user.is_staff):
            raise PermissionDenied

    def request_is_valid(self):
        return 'object_id' in self.GET and 'app_label' in self.GET and 'model_name' in self.GET

    def get_model(self):
        try:
            self.model = apps.get_model(self.GET['app_label'], self.GET['model_name'])
        except LookupError:
            self.model = None
        return self.model

    # def get_model_admin(self):
    #     model = self.get_model()
    #     if model is not None:
    #         if model in admin.site._registry:
    #             return admin.site._registry[model]
    #     return None

    def get_filtered_queryset(self, qs):
        filters = {}
        query_string = self.GET.get('query_string', None)

        if query_string:
            for item in query_string.split(":"):
                k, v = item.split("=")
                if k != "_to_field":
                    filters[smart_str(k)] = prepare_lookup_value(smart_str(k), unquote(smart_str(v)))
        return qs.filter(**filters)

    def get_queryset(self):
        qs = self.model._default_manager.get_queryset()
        # model_admin = self.get_model_admin()
        # if model_admin is not None:
        #     qs = model_admin.get_queryset(self.request)
        qs = self.get_filtered_queryset(qs)
        return qs

    def get_return_value(self, obj, obj_id):
        to_field = self.GET.get('to_field', None)
        if to_field is not None:
            return_value = getattr(obj, to_field)
            if not isinstance(return_value, str) and not isinstance(return_value, int):
                return_value = obj.pk
            return return_value
        return obj_id

    def get_data(self):
        obj_id = self.GET['object_id']
        to_field = self.GET.get('to_field', None)
        data = []
        if obj_id:
            try:
                if to_field is not None:
                    obj = self.get_queryset().get(**{to_field: obj_id})
                else:
                    obj = self.get_queryset().get(pk=obj_id)
                data.append({"value": "%s" % self.get_return_value(obj, obj_id), "label": get_label(obj), "safe": get_label_safe(obj)})
            except (self.model.DoesNotExist, ValueError):
                data.append({"value": obj_id, "label": _("?"), "safe": False})
        return data

    @method_decorator(never_cache)
    def get(self, request, *args, **kwargs):
        self.check_user_permission()
        self.GET = self.request.GET

        if self.request_is_valid():
            self.get_model()
            if self.model is not None:
                data = self.get_data()
                if data:
                    return ajax_response(data)

        data = [{"value": None, "label": ""}]
        return ajax_response(data)


class M2MLookup(RelatedLookup):
    "M2M Lookup"

    def get_data(self):
        obj_ids = self.GET['object_id'].split(',')
        data = []
        for obj_id in (i for i in obj_ids if i):
            try:
                obj = self.get_queryset().get(pk=obj_id)
                data.append({"value": obj_id, "label": get_label(obj), "safe": get_label_safe(obj)})
            except (self.model.DoesNotExist, ValueError):
                data.append({"value": obj_id, "label": _("?"), "safe": False})
        return data


class AutocompleteLookup(RelatedLookup):
    "AutocompleteLookup"

    def request_is_valid(self):
        return 'term' in self.GET and 'app_label' in self.GET and 'model_name' in self.GET

    def get_searched_queryset(self, qs):
        model = self.model
        term = self.GET["term"]

        try:
            term = model.autocomplete_term_adjust(term)
        except AttributeError:
            pass

        search_fields = get_autocomplete_search_fields(self.model)
        if search_fields:
            search = models.Q()
            for word in term.split():
                term_query = models.Q()
                for search_field in search_fields:
                    term_query |= models.Q(
                        **{smart_str(search_field): smart_str(word)}
                    )
                search &= term_query
            qs = qs.filter(search)
        else:
            qs = model.objects.none()
        return qs

    def get_final_ordering(self, model, previous_lookup_parts=None):
        """
        This recursive function returns the final lookups
        for the default ordering of a model.

        Considering the models below, `get_final_ordering(Book)` will return
        `['-type__name', 'name']` instead of the simple `['-type', 'name']`
        one would get using `Book._meta.ordering`.

            class BookType(Model):
                name = CharField(max_length=50)

                class Meta:
                    ordering = ['name']

            class Book(Model):
                name = CharField(max_length=50)
                type = ForeignKey(BookType)

                class Meta:
                    ordering = ['-type', 'name']
        """
        ordering = []
        for lookup in model._meta.ordering:
            opts = model._meta
            for part in lookup.lstrip('-').split(LOOKUP_SEP):
                field = opts.get_field(part)
                if field.is_relation:
                    opts = field.related_model._meta
            if previous_lookup_parts is not None:
                lookup = previous_lookup_parts + LOOKUP_SEP + lookup
            if field.is_relation:
                ordering.extend(self.get_final_ordering(opts.model, lookup))
            else:
                ordering.append(lookup)
        return ordering

    def get_queryset(self):
        qs = super(AutocompleteLookup, self).get_queryset()
        qs = self.get_searched_queryset(qs)

        if connection.vendor == 'postgresql':
            ordering = self.get_final_ordering(self.model)
            distinct_columns = [o.lstrip('-') for o in ordering]
            pk_name = self.model._meta.pk.name
            if pk_name not in distinct_columns:
                distinct_columns.append(pk_name)
            return qs.order_by(*ordering).distinct(*distinct_columns)

        return qs.distinct()

    def get_data(self):
        return [{"value": self.get_return_value(f, f.pk), "label": get_label(f)} for f in self.get_queryset()[:AUTOCOMPLETE_LIMIT]]

    @method_decorator(never_cache)
    def get(self, request, *args, **kwargs):
        self.check_user_permission()
        self.GET = self.request.GET

        if self.request_is_valid():
            self.get_model()
            data = self.get_data()
            if data:
                return ajax_response(data)

        # overcomplicated label translation
        label = ngettext('%(counter)s result', '%(counter)s results', 0) % {'counter': 0}
        data = [{"value": None, "label": label}]
        return ajax_response(data)
