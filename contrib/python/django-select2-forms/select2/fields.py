import django
from django import forms
from django.db import models
from django.core.exceptions import ImproperlyConfigured, ValidationError, FieldDoesNotExist
from django.forms.models import ModelChoiceIterator
from django.utils.encoding import force_str
from django.utils.functional import Promise

try:
    from django.db.models.fields.related import lazy_related_operation
except ImportError:
    lazy_related_operation = None
    from django.db.models.fields.related import add_lazy_relation
else:
    add_lazy_relation = None

from sortedm2m.fields import SortedManyToManyField
from sortedm2m.forms import SortedMultipleChoiceField

from .widgets import Select, SelectMultiple


__all__ = (
    'Select2FieldMixin',
    'Select2ModelFieldMixin',
    'ChoiceField',
    'MultipleChoiceField',
    'ModelChoiceField',
    'ModelMultipleChoiceField',
    'ForeignKey',
    'ManyToManyField',
)


def compat_add_lazy_relation(cls, field, relation, operation):
    if add_lazy_relation is not None:
        return add_lazy_relation(cls, field, relation, operation)

    # Rearrange args for new Apps.lazy_model_operation
    def function(local, related, field):
        return operation(field, related, local)

    lazy_related_operation(function, cls, relation, field=field)


dj19 = bool(django.VERSION >= (1, 9))
compat_rel = lambda f: getattr(f, 'remote_field' if dj19 else 'rel')
compat_rel_to = lambda f: getattr(compat_rel(f), 'model' if dj19 else 'to')


class Select2FieldMixin(object):
    def __init__(self, *args, **kwargs):
        widget_kwargs = {}
        # The child field class can pass widget_kwargs as a dict. We use this
        # in MultipleChoiceField to ensure that the field's choices get passed
        # along to the widget. This is unnecessary for model fields since the
        # choices in that case are iterators wrapping the queryset.
        if 'widget_kwargs' in kwargs:
            widget_kwargs.update(kwargs.pop('widget_kwargs'))
        widget_kwarg_keys = ['overlay', 'js_options', 'sortable', 'ajax']
        for k in widget_kwarg_keys:
            if k in kwargs:
                widget_kwargs[k] = kwargs.pop(k)
        widget = kwargs.pop('widget', None)
        if isinstance(widget, type):
            if not issubclass(widget, Select):
                widget = self.widget
        elif not isinstance(widget, Select):
            widget = self.widget
        if isinstance(widget, type):
            kwargs['widget'] = widget(**widget_kwargs)
        else:
            kwargs['widget'] = widget
        super(Select2FieldMixin, self).__init__(*args, **kwargs)

    @property
    def choices(self):
        """
        When it's time to get the choices, if it was a lazy then figure it out
        now and memoize the result.
        """
        if isinstance(self._choices, Promise):
            self._choices = list(self._choices)
        return self._choices

    @choices.setter
    def choices(self, value):
        super(self.__class__, self.__class__).choices.__set__(self, value)


class ChoiceField(Select2FieldMixin, forms.ChoiceField):
    widget = Select


class MultipleChoiceField(Select2FieldMixin, forms.MultipleChoiceField):
    widget = SelectMultiple

    def __init__(self, *args, **kwargs):
        # Explicitly pass the choices kwarg to the widget. "widget_kwargs"
        # is not a standard Django Form Field kwarg, but we pop it off in
        # Select2FieldMixin.__init__
        kwargs['widget_kwargs'] = kwargs.get('widget_kwargs') or {}
        if 'choices' in kwargs:
            kwargs['widget_kwargs']['choices'] = kwargs['choices']
        super(MultipleChoiceField, self).__init__(*args, **kwargs)

    def has_changed(self, initial, data):
        widget = self.widget
        if not isinstance(widget, SelectMultiple) and hasattr(widget, 'widget'):
            widget = widget.widget
        if hasattr(widget, 'format_value'):
            initial = widget.format_value(initial)
        else:
            initial = widget._format_value(initial)
        return super(MultipleChoiceField, self).has_changed(initial, data)


class Select2ModelFieldMixin(Select2FieldMixin):
    search_field = None
    case_sensitive = False

    choice_iterator_cls = ModelChoiceIterator

    def __init__(self, search_field=None, case_sensitive=False, *args, **kwargs):
        if search_field is None and kwargs.get('ajax'):
            raise TypeError(
                ("keyword argument 'search_field' is required for field " "%s <%s>")
                % (self.name, self.__class__.__name__)
            )
        self.search_field = search_field
        self.case_sensitive = case_sensitive
        self.name = kwargs.pop('name')
        self.model = kwargs.pop('model')
        self.choice_iterator_cls = kwargs.pop('choice_iterator_cls', self.choice_iterator_cls)
        super(Select2ModelFieldMixin, self).__init__(*args, **kwargs)

    @property
    def choices(self):
        if not hasattr(self, '_choices'):
            return self.choice_iterator_cls(self)
        return self._choices

    @choices.setter
    def choices(self, value):
        super(self.__class__, self.__class__).choices.__set__(self, value)


class ModelChoiceField(Select2ModelFieldMixin, forms.ModelChoiceField):
    widget = Select

    def __init__(self, *args, **kwargs):
        super(ModelChoiceField, self).__init__(*args, **kwargs)
        self.widget.field = self


class ModelMultipleChoiceField(Select2ModelFieldMixin, SortedMultipleChoiceField):
    widget = SelectMultiple

    #: Instance of the field on the through table used for storing sort position
    sort_field = None

    def __init__(self, *args, **kwargs):
        self.sort_field = kwargs.pop('sort_field', self.sort_field)
        if self.sort_field is not None:
            kwargs['sortable'] = True
        super(ModelMultipleChoiceField, self).__init__(*args, **kwargs)
        self.widget.field = self

    def clean(self, value):
        if self.required and not value:
            raise ValidationError(self.error_messages['required'])
        elif not self.required and not value:
            return []

        if isinstance(value, str):
            value = value.split(',')

        if not isinstance(value, (list, tuple)):
            raise ValidationError(self.error_messages['list'])

        key = self.to_field_name or 'pk'

        for pk in value:
            try:
                self.queryset.filter(**{key: pk})
            except ValueError:
                raise ValidationError(self.error_messages['invalid_pk_value'] % pk)
        qs = self.queryset.filter(
            **{
                ('%s__in' % key): value,
            }
        )
        pks = set([force_str(getattr(o, key)) for o in qs])

        # Create a dictionary for storing the original order of the items
        # passed from the form
        pk_positions = {}

        for i, val in enumerate(value):
            pk = force_str(val)
            if pk not in pks:
                raise ValidationError(self.error_messages['invalid_choice'] % val)
            pk_positions[pk] = i

        if not self.sort_field:
            return qs
        else:
            # Iterate through the objects and set the sort field to its
            # position in the comma-separated request data. Then return
            # a list of objects sorted on the sort field.
            sort_value_field_name = self.sort_field.name
            objs = []
            for i, obj in enumerate(qs):
                pk = force_str(getattr(obj, key))
                setattr(obj, sort_value_field_name, pk_positions[pk])
                objs.append(obj)
            return sorted(objs, key=lambda obj: getattr(obj, sort_value_field_name))


class RelatedFieldMixin(object):
    search_field = None
    js_options = None
    overlay = None
    case_sensitive = False
    ajax = False

    def __init__(self, *args, **kwargs):
        self.search_field = kwargs.pop('search_field', None)
        self.js_options = kwargs.pop('js_options', None)
        self.overlay = kwargs.pop('overlay', self.overlay)
        self.case_sensitive = kwargs.pop('case_sensitive', self.case_sensitive)
        self.ajax = kwargs.pop('ajax', self.ajax)
        super(RelatedFieldMixin, self).__init__(*args, **kwargs)

    def _get_queryset(self, db=None):
        return (
            compat_rel_to(self)
            ._default_manager.using(db)
            .complex_filter(compat_rel(self).limit_choices_to)
        )

    @property
    def queryset(self):
        return self._get_queryset()

    def formfield(self, **kwargs):
        db = kwargs.pop('using', None)
        defaults = {
            'form_class': ModelChoiceField,
            'queryset': self._get_queryset(db),
            'js_options': self.js_options,
            'search_field': self.search_field,
            'ajax': self.ajax,
            'name': self.name,
            'model': self.model,
        }
        defaults.update(kwargs)
        if self.overlay is not None:
            defaults.update({'overlay': self.overlay})

        # If initial is passed in, it's a list of related objects, but the
        # MultipleChoiceField takes a list of IDs.
        if defaults.get('initial') is not None:
            initial = defaults['initial']
            if callable(initial):
                initial = initial()
            defaults['initial'] = [i._get_pk_val() for i in initial]
        return models.Field.formfield(self, **defaults)

    def contribute_to_related_class(self, cls, related):
        if not self.ajax:
            return super(RelatedFieldMixin, self).contribute_to_related_class(cls, related)
        if self.search_field is None:
            raise TypeError(
                (
                    "keyword argument 'search_field' is required for field "
                    "'%(field_name)s' of model %(app_label)s.%(object_name)s"
                )
                % {
                    'field_name': self.name,
                    'app_label': self.model._meta.app_label,
                    'object_name': self.model._meta.object_name,
                }
            )
        if not callable(self.search_field) and not isinstance(self.search_field, str):
            raise TypeError(
                (
                    "keyword argument 'search_field' must be either callable or "
                    "string on field '%(field_name)s' of model "
                    "%(app_label)s.%(object_name)s"
                )
                % {
                    'field_name': self.name,
                    'app_label': self.model._meta.app_label,
                    'object_name': self.model._meta.object_name,
                }
            )
        if isinstance(self.search_field, str):
            try:
                opts = related.parent_model._meta
            except AttributeError:
                # Django 1.8
                opts = related.model._meta
            try:
                opts.get_field(self.search_field)
            except FieldDoesNotExist:
                raise ImproperlyConfigured(
                    (
                        "keyword argument 'search_field' references non-existent "
                        "field '%(search_field)s' in %(field_name)s of model "
                        "<%(app_label)s.%(object_name)s>"
                    )
                    % {
                        'search_field': self.search_field,
                        'field_name': self.name,
                        'app_label': opts.app_label,
                        'object_name': opts.object_name,
                    }
                )
        super(RelatedFieldMixin, self).contribute_to_related_class(cls, related)


class ForeignKey(RelatedFieldMixin, models.ForeignKey):
    def formfield(self, **kwargs):
        defaults = {
            'to_field_name': compat_rel(self).field_name,
        }
        defaults.update(**kwargs)
        return super(ForeignKey, self).formfield(**defaults)


class OneToOneField(RelatedFieldMixin, models.OneToOneField):
    def formfield(self, **kwargs):
        defaults = {
            'to_field_name': compat_rel(self).field_name,
        }
        defaults.update(**kwargs)
        return super(OneToOneField, self).formfield(**defaults)


class ManyToManyField(RelatedFieldMixin, SortedManyToManyField):
    #: Name of the field on the through table used for storing sort position
    sort_value_field_name = None

    #: Instance of the field on the through table used for storing sort position
    sort_field = None

    def __init__(self, *args, **kwargs):
        if 'sort_field' in kwargs:
            kwargs['sort_value_field_name'] = kwargs.pop('sort_field')
        if 'sorted' not in kwargs:
            kwargs['sorted'] = bool(kwargs.get('sort_value_field_name'))
        super(ManyToManyField, self).__init__(*args, **kwargs)

    def formfield(self, **kwargs):
        defaults = {
            'form_class': ModelMultipleChoiceField,
            'sort_field': self.sort_field,
        }
        defaults.update(**kwargs)
        return super(ManyToManyField, self).formfield(**defaults)

    def contribute_to_class(self, cls, name):
        """
        Replace the descriptor with our custom descriptor, so that the
        position field (which is saved in the formfield clean()) gets saved
        """
        super(ManyToManyField, self).contribute_to_class(cls, name)
        if self.sorted:

            def resolve_sort_field(field, model, cls):
                model._sort_field_name = field.sort_value_field_name
                field.sort_field = model._meta.get_field(field.sort_value_field_name)

            if isinstance(compat_rel(self).through, str):
                compat_add_lazy_relation(cls, self, compat_rel(self).through, resolve_sort_field)
            else:
                resolve_sort_field(self, compat_rel(self).through, cls)
