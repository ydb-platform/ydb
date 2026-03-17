try:
    from collections.abc import Mapping, MutableMapping
except:
    from collections import Mapping, MutableMapping

import six
from wtforms import Form
try:
    from wtforms_sqlalchemy.fields import (
        QuerySelectField,
        QuerySelectMultipleField
    )
    HAS_SQLALCHEMY_SUPPORT = True
except ImportError:
    try:
        from wtforms.ext.sqlalchemy.fields import (
            QuerySelectField,
            QuerySelectMultipleField
        )
        HAS_SQLALCHEMY_SUPPORT = True
    except ImportError:
        HAS_SQLALCHEMY_SUPPORT = False
from wtforms.fields import (
    _unset_value,
    BooleanField,
    Field,
    FieldList,
    FileField,
    FormField,
    StringField
)
from wtforms.validators import DataRequired, Optional

__version__ = '0.3.5'


class InvalidData(Exception):
    pass


def flatten_json(
    form,
    json,
    parent_key='',
    separator='-',
    skip_unknown_keys=True
):
    """Flattens given JSON dict to cope with WTForms dict structure.

    :form form: WTForms Form object
    :param json: json to be converted into flat WTForms style dict
    :param parent_key: this argument is used internally be recursive calls
    :param separator: default separator
    :param skip_unknown_keys:
        if True unknown keys will be skipped, if False throws InvalidData
        exception whenever unknown key is encountered

    Examples::

        >>> flatten_json(MyForm, {'a': {'b': 'c'}})
        {'a-b': 'c'}
    """
    if not isinstance(json, Mapping):
        raise InvalidData(
            u'This function only accepts dict-like data structures.'
        )

    items = []
    for key, value in json.items():
        try:
            unbound_field = getattr(form, key)
        except AttributeError:
            if skip_unknown_keys:
                continue
            else:
                raise InvalidData(u"Unknown field name '%s'." % key)

        try:
            field_class = unbound_field.field_class
        except AttributeError:
            if skip_unknown_keys:
                continue
            else:
                raise InvalidData(u"Key '%s' is not valid field class." % key)

        new_key = parent_key + separator + key if parent_key else key
        if isinstance(value, MutableMapping):
            if issubclass(field_class, FormField):
                nested_form_class = unbound_field.bind(Form(), '').form_class
                items.extend(
                    flatten_json(nested_form_class, value, new_key)
                    .items()
                )
            else:
                items.append((new_key, value))
        elif isinstance(value, list):
            if issubclass(field_class, FieldList):
                nested_unbound_field = unbound_field.bind(
                    Form(),
                    ''
                ).unbound_field
                items.extend(
                    flatten_json_list(
                        nested_unbound_field,
                        value,
                        new_key,
                        separator
                    )
                )
            else:
                items.append((new_key, value))
        else:
            items.append((new_key, value))
    return dict(items)


def flatten_json_list(field, json, parent_key='', separator='-'):
    items = []
    for i, item in enumerate(json):
        new_key = parent_key + separator + str(i)
        if (
            isinstance(item, dict) and
            issubclass(getattr(field, 'field_class'), FormField)
        ):
            nested_class = field.field_class(
                *field.args,
                **field.kwargs
            ).bind(Form(), '').form_class
            items.extend(
                flatten_json(nested_class, item, new_key)
                .items()
            )
        else:
            items.append((new_key, item))
    return items


@property
def patch_data(self):
    if hasattr(self, '_patch_data'):
        return self._patch_data

    data = {}

    def is_optional(field):
        return Optional in [v.__class__ for v in field.validators]

    def is_required(field):
        return DataRequired in [v.__class__ for v in field.validators]

    for name, f in six.iteritems(self._fields):
        if f.is_missing:
            if is_optional(f):
                continue
            elif not is_required(f) and f.default is None:
                continue
            elif isinstance(f, FieldList) and f.min_entries == 0:
                continue

        if isinstance(f, FormField):
            data[name] = f.patch_data
        elif isinstance(f, FieldList):
            if issubclass(f.unbound_field.field_class, FormField):
                data[name] = [i.patch_data for i in f.entries]
            else:
                data[name] = [i.data for i in f.entries]
        else:
            data[name] = f.data
    return data


def monkey_patch_field_process(func):
    """
    Monkey patches Field.process method to better understand missing values.
    """
    def process(self, formdata, data=_unset_value, **kwargs):
        call_original_func = True
        if not isinstance(self, FormField):

            if formdata and self.name in formdata:
                if (
                    len(formdata.getlist(self.name)) == 1 and
                    formdata.getlist(self.name) == [None]
                ):
                    call_original_func = False
                    self.data = None
                self.is_missing = not bool(formdata.getlist(self.name))
            else:
                self.is_missing = True

        if call_original_func:
            func(self, formdata, data=data, **kwargs)

        if (
            formdata and self.name in formdata and
            formdata.getlist(self.name) == [None] and
            isinstance(self, FormField)
        ):
            self.form._is_missing = False
            self.form._patch_data = None

        if isinstance(self, StringField) and not isinstance(self, FileField):
            if not self.data:
                try:
                    self.data = self.default()
                except TypeError:
                    self.data = self.default
            else:
                self.data = six.text_type(self.data)

    return process


class MultiDict(dict):
    def getlist(self, key):
        val = self[key]
        if not isinstance(val, list):
            val = [val]
        return val

    def getall(self, key):
        return [self[key]]


@classmethod
def from_json(
    cls,
    formdata=None,
    obj=None,
    prefix='',
    data=None,
    meta=None,
    skip_unknown_keys=True,
    **kwargs
):
    form = cls(
        formdata=MultiDict(
            flatten_json(cls, formdata, skip_unknown_keys=skip_unknown_keys)
        ) if formdata else None,
        obj=obj,
        prefix=prefix,
        data=data,
        meta=meta,
        **kwargs
    )
    return form


@property
def is_missing(self):
    if hasattr(self, '_is_missing'):
        return self._is_missing

    for name, field in self._fields.items():
        if not field.is_missing:
            return False
    return True


@property
def field_list_is_missing(self):
    if hasattr(self, '_is_missing'):
        return self._is_missing

    return all([field.is_missing for field in self.entries])


def monkey_patch_process_formdata(func):
    def process_formdata(self, valuelist):
        valuelist = list(map(six.text_type, valuelist))

        return func(self, valuelist)
    return process_formdata


def init():
    Form.is_missing = is_missing
    FieldList.is_missing = field_list_is_missing
    Form.from_json = from_json
    Form.patch_data = patch_data
    FieldList.patch_data = patch_data
    if HAS_SQLALCHEMY_SUPPORT:
        QuerySelectField.process_formdata = monkey_patch_process_formdata(
            QuerySelectField.process_formdata
        )
        QuerySelectMultipleField.process_formdata = \
            monkey_patch_process_formdata(
                QuerySelectMultipleField.process_formdata
            )
    Field.process = monkey_patch_field_process(Field.process)
    FormField.process = monkey_patch_field_process(FormField.process)
    BooleanField.false_values = BooleanField.false_values + (False,)
