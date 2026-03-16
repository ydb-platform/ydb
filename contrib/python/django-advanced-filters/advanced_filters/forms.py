from datetime import datetime as dt
from pprint import pformat
import logging
import operator

from django import forms

from django.apps import apps

from django.conf import settings
from django.contrib import admin
from django.contrib.admin.utils import get_fields_from_path
from django.core.exceptions import FieldDoesNotExist
from django.db.models import Q
from django.db.models.fields import DateField
from django.forms.formsets import formset_factory, BaseFormSet
from functools import reduce
from django.utils.text import capfirst
from django.utils.translation import gettext_lazy as _

from .models import AdvancedFilter
from .form_helpers import CleanWhiteSpacesMixin,  VaryingTypeCharField


logger = logging.getLogger('advanced_filters.forms')

# select2 location can be modified via settings
SELECT2_JS = getattr(settings, 'SELECT2_JS', 'select2/select2.min.js')
SELECT2_CSS = getattr(settings, 'SELECT2_CSS', 'select2/select2.min.css')


def date_to_string(timestamp):
    if timestamp:
        return dt.fromtimestamp(timestamp).strftime('%Y-%m-%d')
    else:
        return ""


class AdvancedFilterQueryForm(CleanWhiteSpacesMixin, forms.Form):
    """ Build the query from field, operator and value """
    OPERATORS = (
        ("iexact", _("Equals")),
        ("icontains", _("Contains")),
        ("iregex", _("One of")),
        ("range", _("DateTime Range")),
        ("isnull", _("Is NULL")),
        ("istrue", _("Is TRUE")),
        ("isfalse", _("Is FALSE")),
        ("lt", _("Less Than")),
        ("gt", _("Greater Than")),
        ("lte", _("Less Than or Equal To")),
        ("gte", _("Greater Than or Equal To")),
    )

    FIELD_CHOICES = (
        ("_OR", _("Or (mark an or between blocks)")),
    )

    field = forms.ChoiceField(required=True, widget=forms.Select(
        attrs={'class': 'query-field'}), label=_('Field'))
    operator = forms.ChoiceField(
        label=_('Operator'),
        required=True, choices=OPERATORS, initial="iexact",
        widget=forms.Select(attrs={'class': 'query-operator'}))
    value = VaryingTypeCharField(required=True, widget=forms.TextInput(
        attrs={'class': 'query-value'}), label=_('Value'))
    value_from = forms.DateTimeField(widget=forms.HiddenInput(
        attrs={'class': 'query-dt-from'}), required=False)
    value_to = forms.DateTimeField(widget=forms.HiddenInput(
        attrs={'class': 'query-dt-to'}), required=False)
    negate = forms.BooleanField(initial=False, required=False, label=_('Negate'))

    def _build_field_choices(self, fields):
        """
        Iterate over passed model fields tuple and update initial choices.
        """
        return tuple(sorted(
            ((fquery, capfirst(fname)) for fquery, fname in fields.items()),
            key=lambda f: f[1].lower())
        ) + self.FIELD_CHOICES

    def _build_query_dict(self, formdata=None):
        """
        Take submitted data from form and create a query dict to be
        used in a Q object (or filter)
        """
        if self.is_valid() and formdata is None:
            formdata = self.cleaned_data
        key = "{field}__{operator}".format(**formdata)
        if formdata['operator'] == "isnull":
            return {key: None}
        elif formdata['operator'] == "istrue":
            return {formdata['field']: True}
        elif formdata['operator'] == "isfalse":
            return {formdata['field']: False}
        return {key: formdata['value']}

    @staticmethod
    def _parse_query_dict(query_data, model):
        """
        Take a list of query field dict and return data for form initialization
        """
        operator = 'iexact'
        if query_data['field'] == '_OR':
            query_data['operator'] = operator
            return query_data

        parts = query_data['field'].split('__')
        if len(parts) < 2:
            field = parts[0]
        else:
            if parts[-1] in dict(AdvancedFilterQueryForm.OPERATORS).keys():
                field = '__'.join(parts[:-1])
                operator = parts[-1]
            else:
                field = query_data['field']

        query_data['field'] = field
        mfield = get_fields_from_path(model, query_data['field'])
        if not mfield:
            raise Exception('Field path "%s" could not be followed to a field'
                            ' in model %s', query_data['field'], model)
        else:
            mfield = mfield[-1]  # get the field object

        if query_data['value'] is None:
            query_data['operator'] = "isnull"
        elif query_data['value'] is True:
            query_data['operator'] = "istrue"
        elif query_data['value'] is False:
            query_data['operator'] = "isfalse"
        else:
            if isinstance(mfield, DateField):
                # this is a date/datetime field
                query_data['operator'] = "range"  # default
            else:
                query_data['operator'] = operator  # default

        if isinstance(query_data.get('value'),
                      list) and query_data['operator'] == 'range':
            date_from = date_to_string(query_data.get('value_from'))
            date_to = date_to_string(query_data.get('value_to'))
            query_data['value'] = ','.join([date_from, date_to])

        return query_data

    def set_range_value(self, data):
        """
        Validates date range by parsing into 2 datetime objects and
        validating them both.
        """
        dtfrom = data.pop('value_from')
        dtto = data.pop('value_to')
        if dtfrom is dtto is None:
            self.errors['value'] = ['Date range requires values']
            raise forms.ValidationError([])
        data['value'] = (dtfrom, dtto)

    def clean(self):
        cleaned_data = super().clean()
        if cleaned_data.get('operator') == "range":
            if ('value_from' in cleaned_data and
                    'value_to' in cleaned_data):
                self.set_range_value(cleaned_data)
        return cleaned_data

    def make_query(self, *args, **kwargs):
        """ Returns a Q object from the submitted form """
        query = Q()  # initial is an empty query
        query_dict = self._build_query_dict(self.cleaned_data)
        if 'negate' in self.cleaned_data and self.cleaned_data['negate']:
            query = query & ~Q(**query_dict)
        else:
            query = query & Q(**query_dict)
        return query

    def __init__(self, model_fields={}, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.FIELD_CHOICES = self._build_field_choices(model_fields)
        self.fields['field'].choices = self.FIELD_CHOICES
        if not self.fields['field'].initial:
            self.fields['field'].initial = self.FIELD_CHOICES[0]


class AdvancedFilterFormSet(BaseFormSet):
    """ """
    fields = ()
    extra_kwargs = {}

    def __init__(self, *args, **kwargs):
        self.model_fields = kwargs.pop('model_fields', {})
        super().__init__(*args, **kwargs)
        if self.forms:
            form = self.forms[0]
            self.fields = form.visible_fields()

    def get_form_kwargs(self, index):
        kwargs = super().get_form_kwargs(index)
        kwargs['model_fields'] = self.model_fields
        return kwargs


AFQFormSet = formset_factory(
    AdvancedFilterQueryForm, formset=AdvancedFilterFormSet,
    extra=1, can_delete=True)

AFQFormSetNoExtra = formset_factory(
    AdvancedFilterQueryForm, formset=AdvancedFilterFormSet,
    extra=0, can_delete=True)


class AdvancedFilterForm(CleanWhiteSpacesMixin, forms.ModelForm):
    """ Form to save/edit advanced filter forms """
    class Meta:
        model = AdvancedFilter
        fields = ('title',)

    class Media:
        required_js = [
            'admin/js/vendor/jquery/jquery.min.js',
            'advanced-filters/jquery_adder.js',
            'orig_inlines%s.js' % ('' if settings.DEBUG else '.min'),
            'magnific-popup/jquery.magnific-popup.js',
            'advanced-filters/advanced-filters.js',
        ]
        js = required_js + [SELECT2_JS]
        css = {'screen': [
            SELECT2_CSS,
            'advanced-filters/advanced-filters.css',
            'magnific-popup/magnific-popup.css'
        ]}

    def get_fields_from_model(self, model, fields):
        """
        Iterate over given <field> names (in "orm query" notation) and find
        the actual field given the initial <model>.

        If <field> is a tuple of the format ('field_name', 'Verbose name'),
        overwrite the field's verbose name with the given name for display
        purposes.
        """
        model_fields = {}
        for field in fields:
            if isinstance(field, tuple) and len(field) == 2:
                field, verbose_name = field[0], field[1]
            else:
                try:
                    model_field = get_fields_from_path(model, field)[-1]
                    verbose_name = model_field.verbose_name
                except (FieldDoesNotExist, IndexError, TypeError) as e:
                    logger.warning(
                        "AdvancedFilterForm: skip invalid field - %s", e
                    )
                    continue
            model_fields[field] = verbose_name
        return model_fields

    def __init__(self, *args, **kwargs):
        model_admin = kwargs.pop('model_admin', None)
        instance = kwargs.get('instance')
        extra_form = kwargs.pop('extra_form', False)
        # TODO: allow all fields to be determined by model
        filter_fields = kwargs.pop('filter_fields', None)
        if model_admin:
            self._model = model_admin.model
        elif instance and instance.model:
            # get existing instance model
            self._model = apps.get_model(*instance.model.split('.'))
            try:
                model_admin = admin.site._registry[self._model]
            except KeyError:
                logger.debug('No ModelAdmin registered for %s', self._model)
        else:
            raise Exception('Adding new AdvancedFilter from admin is '
                            'not supported')

        self._filter_fields = filter_fields or getattr(
            model_admin, 'advanced_filter_fields', ())

        super().__init__(*args, **kwargs)

        # populate existing or empty forms formset
        data = None
        if len(args):
            data = args[0]
        elif kwargs.get('data'):
            data = kwargs.get('data')
        self.initialize_form(instance, self._model, data, extra_form)

    def clean(self):
        cleaned_data = super().clean()
        if not self.fields_formset.is_valid():
            logger.debug(
                "Errors validating advanced query filters: %s",
                pformat([(f.errors, f.non_field_errors())
                         for f in self.fields_formset.forms]))
            raise forms.ValidationError("Error validating filter forms")
        cleaned_data['model'] = "{}.{}".format(self._model._meta.app_label,
                                               self._model._meta.object_name)
        return cleaned_data

    @property
    def _non_deleted_forms(self):
        forms = []
        for form in self.fields_formset.forms:
            if form in self.fields_formset.deleted_forms:
                continue  # skip deleted forms when generating query
            forms.append(form)
        return forms

    def generate_query(self):
        """ Reduces multiple queries into a single usable query """
        query = Q()
        ORed = []
        for form in self._non_deleted_forms:
            if not hasattr(form, 'cleaned_data'):
                continue
            if form.cleaned_data['field'] == "_OR":
                ORed.append(query)
                query = Q()
            else:
                query = query & form.make_query()
        if ORed:
            if query:  # add last query for OR if any
                ORed.append(query)
            query = reduce(operator.or_, ORed)
        return query

    def initialize_form(self, instance, model, data=None, extra=None):
        """ Takes a "finalized" query and generate it's form data """
        model_fields = self.get_fields_from_model(model, self._filter_fields)

        forms = []
        if instance:
            for field_data in instance.list_fields():
                forms.append(
                    AdvancedFilterQueryForm._parse_query_dict(
                        field_data, model))

        formset = AFQFormSetNoExtra if not extra else AFQFormSet
        self.fields_formset = formset(
            data=data,
            initial=forms or None,
            model_fields=model_fields
        )

    def save(self, commit=True):
        self.instance.query = self.generate_query()
        self.instance.model = self.cleaned_data.get('model')
        return super().save(commit)
