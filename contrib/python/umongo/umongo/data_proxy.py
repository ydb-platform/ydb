"""umongo BaseDataProxy"""
import marshmallow as ma

from .abstract import BaseDataObject
from .exceptions import UnknownFieldInDBError
from .i18n import gettext as _


__all__ = ('data_proxy_factory')


class BaseDataProxy:

    __slots__ = ('_data', '_modified_data')
    schema = None
    _fields = None
    _fields_from_mongo_key = None

    def __init__(self, data=None):
        # Inside data proxy, data are stored in mongo world representation
        self._modified_data = set()
        self._data = {}
        self.load(data or {})

    def to_mongo(self, update=False):
        if update:
            return self._to_mongo_update()
        return self._to_mongo()

    def _to_mongo(self):
        mongo_data = {}
        for key, val in self._data.items():
            field = self._fields_from_mongo_key[key]
            val = field.serialize_to_mongo(val)
            if val is not ma.missing:
                mongo_data[key] = val
        return mongo_data

    def _to_mongo_update(self):
        mongo_data = {}
        set_data = {}
        unset_data = []
        for name in self.get_modified_fields():
            field = self._fields[name]
            name = field.attribute or name
            val = field.serialize_to_mongo(self._data[name])
            if val is ma.missing:
                unset_data.append(name)
            else:
                set_data[name] = val
        if set_data:
            mongo_data['$set'] = set_data
        if unset_data:
            mongo_data['$unset'] = {k: "" for k in unset_data}
        return mongo_data or None

    def from_mongo(self, data):
        self._data = {}
        for key, val in data.items():
            try:
                field = self._fields_from_mongo_key[key]
            except KeyError:
                raise UnknownFieldInDBError(_(
                    '{cls}: unknown "{key}" field found in DB.'
                    .format(key=key, cls=self.__class__.__name__)
                ))
            self._data[key] = field.deserialize_from_mongo(val)
        self.clear_modified()
        self._add_missing_fields()

    def dump(self):
        return self.schema.dump(self._data)

    def _mark_as_modified(self, key):
        self._modified_data.add(key)

    def update(self, data):
        # Always use marshmallow partial load to skip required checks
        loaded_data = self.schema.load(data, partial=True)
        self._data.update(loaded_data)
        for key in loaded_data:
            self._mark_as_modified(key)

    def load(self, data):
        # Always use marshmallow partial load to skip required checks
        loaded_data = self.schema.load(data, partial=True)
        # Cast to dict to ignore field order in comparisons
        self._data = dict(loaded_data)
        # Map the modified fields list on the the loaded data
        self.clear_modified()
        for key in loaded_data:
            self._mark_as_modified(key)
        # TODO: mark added missing fields as modified?
        self._add_missing_fields()

    def _get_field(self, name):
        field = self._fields[name]
        name = field.attribute or name
        return name, field

    def get(self, name):
        name, _ = self._get_field(name)
        return self._data[name]

    def set(self, name, value):
        name, field = self._get_field(name)
        if value is None and not getattr(field, 'allow_none', False):
            raise ma.ValidationError(field.error_messages['null'])
        if value is not None:
            value = field._deserialize(value, name, None)
            field._validate(value)
        self._data[name] = value
        self._mark_as_modified(name)

    def delete(self, name):
        name, field = self._get_field(name)
        default = field.default
        self._data[name] = default() if callable(default) else default
        self._mark_as_modified(name)

    def __repr__(self):
        # Display data in oo world format
        return "<%s(%s)>" % (self.__class__.__name__, dict(self.items()))

    def __eq__(self, other):
        if isinstance(other, dict):
            return self._data == other
        if hasattr(other, '_data'):
            return self._data == other._data
        return NotImplemented

    def get_modified_fields(self):
        modified = set()
        for name, field in self._fields.items():
            value_name = field.attribute or name
            value = self._data[value_name]
            if value_name in self._modified_data or (
                    isinstance(value, BaseDataObject) and value.is_modified()):
                modified.add(name)
        return modified

    def clear_modified(self):
        self._modified_data.clear()
        for val in self._data.values():
            if isinstance(val, BaseDataObject):
                val.clear_modified()

    def is_modified(self):
        return (
            bool(self._modified_data) or
            any(isinstance(v, BaseDataObject) and v.is_modified()
                for v in self._data.values())
        )

    def _add_missing_fields(self):
        # TODO: we should be able to do that by configuring marshmallow...
        for name, field in self._fields.items():
            mongo_name = field.attribute or name
            if mongo_name not in self._data:
                if callable(field.missing):
                    self._data[mongo_name] = field.missing()
                else:
                    self._data[mongo_name] = field.missing

    def required_validate(self):
        errors = {}
        for name, field in self.schema.fields.items():
            value = self._data[field.attribute or name]
            if field.required and value is ma.missing:
                errors[name] = [_("Missing data for required field.")]
            elif value is ma.missing or value is None:
                continue
            elif hasattr(field, '_required_validate'):
                try:
                    field._required_validate(value)
                except ma.ValidationError as exc:
                    errors[name] = exc.messages
        if errors:
            raise ma.ValidationError(errors)

    # Standards iterators providing oo and mongo worlds views

    def items(self):
        return (
            (key, self._data[field.attribute or key]) for key, field in self._fields.items()
        )

    def keys(self):
        return (field.attribute or key for key, field in self._fields.items())

    def values(self):
        return self._data.values()


class BaseNonStrictDataProxy(BaseDataProxy):
    """
    This data proxy will accept unknown data comming from mongo and will
    return them along with other data when ask.
    """

    __slots__ = ('_additional_data', )

    def __init__(self, data=None):
        self._additional_data = {}
        super().__init__(data=data)

    def _to_mongo(self):
        mongo_data = super()._to_mongo()
        mongo_data.update(self._additional_data)
        return mongo_data

    def from_mongo(self, data):
        self._data = {}
        for key, val in data.items():
            try:
                field = self._fields_from_mongo_key[key]
            except KeyError:
                self._additional_data[key] = val
            else:
                self._data[key] = field.deserialize_from_mongo(val)
        self.clear_modified()
        self._add_missing_fields()


def data_proxy_factory(basename, schema, strict=True):
    """
    Generate a DataProxy from the given schema.

    This way all generic informations (like schema and fields lookups)
    are kept inside the  DataProxy class and it instances are just flyweights.
    """

    cls_name = "%sDataProxy" % basename

    nmspc = {
        '__slots__': (),
        'schema': schema,
        '_fields': schema.fields,
        '_fields_from_mongo_key': {v.attribute or k: v for k, v in schema.fields.items()}
    }

    data_proxy_cls = type(cls_name, (BaseDataProxy if strict else BaseNonStrictDataProxy, ), nmspc)
    return data_proxy_cls
