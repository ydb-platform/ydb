"""
Useful form fields for use with the Peewee ORM.
(cribbed from wtforms.ext.django.fields)
"""
import datetime
import operator
import warnings
import json

try:
    from markupsafe import Markup
except ImportError:
    try:
        from wtforms.widgets import HTMLString as Markup
    except ImportError:
        raise ImportError('Could not import markupsafe.Markup. Please install '
                          'markupsafe.')
from wtforms import __version__ as wtforms_version
from wtforms import fields, form, widgets
from wtforms.fields import FormField, _unset_value
from wtforms.validators import ValidationError
from wtfpeewee._compat import text_type, string_types

__all__ = (
    'ModelSelectField', 'ModelSelectMultipleField', 'ModelHiddenField',
    'SelectQueryField', 'SelectMultipleQueryField', 'HiddenQueryField',
    'SelectChoicesField', 'BooleanSelectField', 'WPTimeField', 'WPDateField',
    'WPDateTimeField', 'WPJSONAreaField',
)


class StaticAttributesMixin(object):
    attributes = {}

    def __call__(self, **kwargs):
        for key, value in self.attributes.items():
            if key in kwargs:
                curr = kwargs[key]
                kwargs[key] = '%s %s' % (value, curr)
        return super(StaticAttributesMixin, self).__call__(**kwargs)


if wtforms_version < '3.1.0':
    def wtf_choice(*args):
        return args
else:
    def wtf_choice(*args):
        return args + ({},)


class BooleanSelectField(fields.SelectFieldBase):
    widget = widgets.Select()

    def iter_choices(self):
        yield wtf_choice('1', 'True', self.data)
        yield wtf_choice('', 'False', not self.data)

    def process_data(self, value):
        try:
            self.data = bool(value)
        except (ValueError, TypeError):
            self.data = None

    def process_formdata(self, valuelist):
        if valuelist:
            try:
                self.data = bool(valuelist[0])
            except ValueError:
                raise ValueError(self.gettext('Invalid Choice: could not coerce.'))


class WPJSONAreaField(fields.TextAreaField):
    def _value(self):
        return json.dumps(self.data) if self.data is not None else u''

    def process_formdata(self, valuelist):
        # the empty string is not valid JSON, try setting to NULL
        if not valuelist or not valuelist[0]:
            self.data = None
            return

        try:
            self.data = json.loads(valuelist[0])
        except ValueError as e:
            try:
                # since Python 3.5, json.loads returns a JSONDecodeError which is a
                # subclass of ValueError with additional attributes
                raise ValueError(self.gettext(('Not a valid JSON structure: '
                                               'parser error in line {e.lineno}, '
                                               'column {e.colno}, char {e.pos}.')).format(e=e))
            except AttributeError:
                raise ValueError(self.gettext(('Not a valid JSON structure.')))


class WPTimeField(StaticAttributesMixin, fields.StringField):
    attributes = {'class': 'time-widget'}
    formats = ['%H:%M:%S', '%H:%M']

    def _value(self):
        if self.raw_data:
            return u' '.join(self.raw_data)
        else:
            return self.data and self.data.strftime(self.formats[0]) or u''

    def convert(self, time_str):
        for format in self.formats:
            try:
                return datetime.datetime.strptime(time_str, format).time()
            except ValueError:
                pass

    def process_formdata(self, valuelist):
        if valuelist:
            self.data = self.convert(' '.join(valuelist))
            if self.data is None:
                raise ValueError(self.gettext(u'Not a valid time value.'))


class WPDateField(StaticAttributesMixin, fields.DateField):
    attributes = {'class': 'date-widget'}


def datetime_widget(field, **kwargs):
    kwargs.setdefault('id', field.id)
    kwargs.setdefault('class', '')
    kwargs['class'] += ' datetime-widget'
    html = []
    for subfield in field:
        html.append(subfield(**kwargs))
    return Markup(u''.join(html))


def generate_datetime_form(validators=None):
    class _DateTimeForm(form.Form):
        date = WPDateField(validators=validators)
        time = WPTimeField(validators=validators)
    return _DateTimeForm


class WPDateTimeField(FormField):
    widget = staticmethod(datetime_widget)

    def __init__(self, label='', validators=None, **kwargs):
        DynamicForm = generate_datetime_form(validators)
        super(WPDateTimeField, self).__init__(
            DynamicForm, label, validators=None, **kwargs)

    def process(self, formdata, data=_unset_value, **_):
        prefix = self.name + self.separator
        kwargs = {}
        if data is _unset_value:
            try:
                data = self.default()
            except TypeError:
                data = self.default

        if data and data is not _unset_value:
            kwargs['date'] = data.date()
            kwargs['time'] = data.time()

        self.form = self.form_class(formdata, prefix=prefix, **kwargs)

    def populate_obj(self, obj, name):
        setattr(obj, name, self.data)

    @property
    def data(self):
        date_data = self.date.data
        time_data = self.time.data or datetime.time(0, 0)
        if date_data:
            return datetime.datetime.combine(date_data, time_data)


class ChosenSelectWidget(widgets.Select):
    """
        `Chosen <http://harvesthq.github.com/chosen/>`_ styled select widget.

        You must include chosen.js for styling to work.
    """
    def __call__(self, field, **kwargs):
        if field.allow_blank and not self.multiple:
            kwargs['data-role'] = u'chosenblank'
        else:
            kwargs['data-role'] = u'chosen'

        return super(ChosenSelectWidget, self).__call__(field, **kwargs)


class SelectChoicesField(fields.SelectField):
    widget = ChosenSelectWidget()

    # all of this exists so i can get proper handling of None
    def __init__(self, label=None, validators=None, coerce=text_type, choices=None, allow_blank=False, blank_text=u'', **kwargs):
        super(SelectChoicesField, self).__init__(label, validators, coerce, choices, **kwargs)
        self.allow_blank = allow_blank
        self.blank_text = blank_text or '----------------'

    def iter_choices(self):
        if self.allow_blank:
            yield wtf_choice(u'__None', self.blank_text, self.data is None)

        for value, label in self.choices:
            yield wtf_choice(value, label, self.coerce(value) == self.data)

    def process_data(self, value):
        if value is None:
            self.data = None
        else:
            try:
                self.data = self.coerce(value)
            except (ValueError, TypeError):
                self.data = None

    def process_formdata(self, valuelist):
        if valuelist:
            if valuelist[0] == '__None':
                self.data = None
            else:
                try:
                    self.data = self.coerce(valuelist[0])
                except ValueError:
                    raise ValueError(self.gettext('Invalid Choice: could not coerce.'))

    def pre_validate(self, form):
        if self.allow_blank and self.data is None:
            return
        super(SelectChoicesField, self).pre_validate(form)


class SelectQueryField(fields.SelectFieldBase):
    """
    Given a SelectQuery either at initialization or inside a view, will display a
    select drop-down field of choices. The `data` property actually will
    store/keep an ORM model instance, not the ID. Submitting a choice which is
    not in the queryset will result in a validation error.

    Specify `get_label` to customize the label associated with each option. If
    a string, this is the name of an attribute on the model object to use as
    the label text. If a one-argument callable, this callable will be passed
    model instance and expected to return the label text. Otherwise, the model
    object's `__unicode__` will be used.

    If `allow_blank` is set to `True`, then a blank choice will be added to the
    top of the list. Selecting this choice will result in the `data` property
    being `None`.  The label for the blank choice can be set by specifying the
    `blank_text` parameter.
    """
    widget = ChosenSelectWidget()

    def __init__(self, label=None, validators=None, query=None, get_label=None, allow_blank=False, blank_text=u'', **kwargs):
        super(SelectQueryField, self).__init__(label, validators, **kwargs)
        self.allow_blank = allow_blank
        self.blank_text = blank_text or '----------------'
        self.query = query
        self.model = query.model
        self._set_data(None)

        if get_label is None:
            self.get_label = lambda o: text_type(o)
        elif isinstance(get_label, string_types):
            self.get_label = operator.attrgetter(get_label)
        else:
            self.get_label = get_label

    def get_model(self, pk):
        try:
            return self.query.where(self.model._meta.primary_key==pk).get()
        except self.model.DoesNotExist:
            pass

    def _get_data(self):
        if self._formdata is not None:
            model = self.get_model(self._formdata)
            if model is not None:
                self._set_data(model)
            else:
                self._data = self._formdata
        return self._data

    def _set_data(self, data):
        self._data = data
        self._formdata = None

    data = property(_get_data, _set_data)

    def __call__(self, **kwargs):
        if 'value' in kwargs:
            self._set_data(self.get_model(kwargs['value']))
        return self.widget(self, **kwargs)

    def iter_choices(self):
        if self.allow_blank:
            yield wtf_choice(u'__None', self.blank_text, self.data is None)

        for obj in self.query.clone():
            yield wtf_choice(obj._pk, self.get_label(obj), obj == self.data)

    def process_formdata(self, valuelist):
        if valuelist:
            if valuelist[0] == '__None':
                self.data = None
            else:
                self._data = None
                self._formdata = valuelist[0]

    def pre_validate(self, form):
        if self.data is not None:
            if isinstance(self.data, self.model):
                value = self.data._pk
            else:
                value = self.data

            if not self.query.where(self.model._meta.primary_key == value).exists():
                raise ValidationError(self.gettext('Not a valid choice.'))

        elif not self.allow_blank:
            raise ValidationError(self.gettext('Selection cannot be blank'))


class SelectMultipleQueryField(SelectQueryField):
    widget =  ChosenSelectWidget(multiple=True)

    def __init__(self, *args, **kwargs):
        kwargs.pop('allow_blank', None)
        super(SelectMultipleQueryField, self).__init__(*args, **kwargs)

    def get_model_list(self, pk_list):
        if pk_list:
            return list(self.query.where(self.model._meta.primary_key << pk_list))
        return []

    def _get_data(self):
        if self._formdata is not None:
            self._set_data(self.get_model_list(self._formdata))
        return self._data or []

    def _set_data(self, data):
        self._data = data
        self._formdata = None

    data = property(_get_data, _set_data)

    def __call__(self, **kwargs):
        if 'value' in kwargs:
            self._set_data(self.get_model_list(kwargs['value']))
        return self.widget(self, **kwargs)

    def iter_choices(self):
        for obj in self.query.clone():
            yield wtf_choice(obj._pk, self.get_label(obj), obj in self.data)

    def process_formdata(self, valuelist):
        if valuelist:
            self._data = []
            self._formdata = list(map(int, valuelist))

    def pre_validate(self, form):
        if self.data:
            id_list = [m._pk for m in self.data]
            if id_list and not self.query.where(self.model._meta.primary_key << id_list).count() == len(id_list):
                raise ValidationError(self.gettext('Not a valid choice.'))


class HiddenQueryField(fields.HiddenField):
    def __init__(self, label=None, validators=None, query=None, get_label=None, **kwargs):
        self.allow_blank = kwargs.pop('allow_blank', False)
        super(fields.HiddenField, self).__init__(label, validators, **kwargs)
        self.query = query
        self.model = query.model
        self._set_data(None)

        if get_label is None:
            self.get_label = lambda o: text_type(o)
        elif isinstance(get_label, basestring):
            self.get_label = operator.attrgetter(get_label)
        else:
            self.get_label = get_label

    def get_model(self, pk):
        try:
            return self.query.where(self.model._meta.primary_key==pk).get()
        except self.model.DoesNotExist:
            pass

    def _get_data(self):
        if self._formdata is not None:
            if self.allow_blank and self._formdata == '__None':
                self._set_data(None)
            else:
                self._set_data(self.get_model(self._formdata))
        return self._data

    def _set_data(self, data):
        self._data = data
        self._formdata = None

    data = property(_get_data, _set_data)

    def __call__(self, **kwargs):
        if 'value' in kwargs:
            self._set_data(self.get_model(kwargs['value']))
        return self.widget(self, **kwargs)

    def _value(self):
        return self.data and self.data._pk or ''

    def process_formdata(self, valuelist):
        if valuelist:
            model_id = valuelist[0]
            self._data = None
            self._formdata = model_id or None


class ModelSelectField(SelectQueryField):
    """
    Like a SelectQueryField, except takes a model class instead of a
    queryset and lists everything in it.
    """
    def __init__(self, label=None, validators=None, model=None, **kwargs):
        super(ModelSelectField, self).__init__(label, validators, query=model.select(), **kwargs)


class ModelSelectMultipleField(SelectMultipleQueryField):
    """
    Like a SelectMultipleQueryField, except takes a model class instead of a
    queryset and lists everything in it.
    """
    def __init__(self, label=None, validators=None, model=None, **kwargs):
        super(ModelSelectMultipleField, self).__init__(label, validators, query=model.select(), **kwargs)

class ModelHiddenField(HiddenQueryField):
    """
    Like a HiddenQueryField, except takes a model class instead of a
    queryset and lists everything in it.
    """
    def __init__(self, label=None, validators=None, model=None, **kwargs):
        super(ModelHiddenField, self).__init__(label, validators, query=model.select(), **kwargs)
