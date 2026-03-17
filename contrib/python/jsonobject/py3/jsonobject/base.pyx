from collections import namedtuple, OrderedDict
import copy
import inspect
from jsonobject.exceptions import (
    DeleteNotAllowed,
    WrappingAttributeError,
)
from jsonobject.base_properties import JsonProperty, DefaultProperty
from jsonobject.utils import check_type


JsonObjectClassSettings = namedtuple('JsonObjectClassSettings', ['type_config'])

CLASS_SETTINGS_ATTR = '_$_class_settings'


def get_settings(cls):
    return getattr(cls, CLASS_SETTINGS_ATTR,
                   JsonObjectClassSettings(type_config=TypeConfig()))


def set_settings(cls, settings):
    setattr(cls, CLASS_SETTINGS_ATTR, settings)


class TypeConfig(object):
    """
    This class allows the user to configure dynamic
    type handlers and string conversions for their JsonObject.

    properties is a map from python types to JsonProperty subclasses
    string_conversions is a list or tuple of (regex, python type)-tuples

    This class is used to store the configuration but is not part of the API.
    To configure:

        class Foo(JsonObject):
            # property definitions go here
            # ...

            class Meta(object):
                update_properties = {
                    datetime.datetime: MySpecialDateTimeProperty
                }
                # this is already set by default
                # but you can override with your own modifications
                string_conversions = ((date_re, datetime.date),
                                      (datetime_re, datetime.datetime),
                                      (time_re, datetime.time),
                                      (decimal_re, decimal.Decimal))

    If you now do

        foo = Foo()
        foo.timestamp = datetime.datetime(1988, 7, 7, 11, 8, 0)

    timestamp will be governed by a MySpecialDateTimeProperty
    instead of the default.

    """
    def __init__(self, properties=None, string_conversions=None):
        self._properties = properties if properties is not None else {}

        self._string_conversions = (
            OrderedDict(string_conversions) if string_conversions is not None
            else OrderedDict()
        )
        # cache this
        self.string_conversions = self._get_string_conversions()
        self.properties = self._properties

    def replace(self, properties=None, string_conversions=None):
        return TypeConfig(
            properties=(properties if properties is not None
                        else self._properties),
            string_conversions=(string_conversions if string_conversions is not None
                                else self._string_conversions)
        )

    def updated(self, properties=None, string_conversions=None):
        """
        update properties and string_conversions with the paramenters
        keeping all non-mentioned items the same as before
        returns a new TypeConfig with these changes
        (does not modify original)

        """
        _properties = self._properties.copy()
        _string_conversions = self.string_conversions[:]
        if properties:
            _properties.update(properties)
        if string_conversions:
            _string_conversions.extend(string_conversions)
        return TypeConfig(
            properties=_properties,
            string_conversions=_string_conversions,
        )

    def _get_string_conversions(self):
        result = []
        for pattern, conversion in self._string_conversions.items():
            conversion = (
                conversion if conversion not in self._properties
                else self._properties[conversion](type_config=self).to_python
            )
            result.append((pattern, conversion))
        return result

META_ATTRS = ('properties', 'string_conversions', 'update_properties')


class JsonObjectMeta(type):

    class Meta(object):
        pass

    def __new__(mcs, name, bases, dct):
        cls = super(JsonObjectMeta, mcs).__new__(mcs, name, bases, dct)

        cls.__configure(**{key: value
                           for key, value in cls.Meta.__dict__.items()
                           if key in META_ATTRS})
        cls_settings = get_settings(cls)

        properties = {}
        properties_by_name = {}
        for key, value in dct.items():
            if isinstance(value, JsonProperty):
                properties[key] = value
            elif key.startswith('_'):
                continue
            elif type(value) in cls_settings.type_config.properties:
                property_ = cls_settings.type_config.properties[type(value)](default=value)
                properties[key] = dct[key] = property_
                setattr(cls, key, property_)

        for key, property_ in properties.items():
            property_.init_property(default_name=key,
                                    type_config=cls_settings.type_config)
            assert property_.name is not None, property_
            assert property_.name not in properties_by_name, \
                'You can only have one property named {0}'.format(
                    property_.name)
            properties_by_name[property_.name] = property_

        for base in bases:
            if getattr(base, '_properties_by_attr', None):
                for key, value in base._properties_by_attr.items():
                    if key not in properties:
                        properties[key] = value
                        properties_by_name[value.name] = value

        cls._properties_by_attr = properties
        cls._properties_by_key = properties_by_name
        return cls

    def __configure(cls, properties=None, string_conversions=None,
                    update_properties=None):
        super_settings = get_settings(super(cls, cls))
        assert not properties or not update_properties, \
            "{} {}".format(properties, update_properties)
        type_config = super_settings.type_config
        if update_properties is not None:
            type_config = type_config.updated(properties=update_properties)
        elif properties is not None:
            type_config = type_config.replace(properties=properties)
        if string_conversions is not None:
            type_config = type_config.replace(
                string_conversions=string_conversions)
        set_settings(cls, super_settings._replace(type_config=type_config))
        return cls


class _JsonObjectPrivateInstanceVariables(object):

    def __init__(self, dynamic_properties=None):
        self.dynamic_properties = dynamic_properties or {}


class JsonObjectBase(object, metaclass=JsonObjectMeta):

    _allow_dynamic_properties = True
    _validate_required_lazily = False

    _properties_by_attr = None
    _properties_by_key = None

    _string_conversions = ()

    def __init__(self, _obj=None, **kwargs):
        setattr(self, '_$', _JsonObjectPrivateInstanceVariables())

        self._obj = check_type(_obj, dict,
                               'JsonObject must wrap a dict or None')
        self._wrapped = {}

        for key, value in list(self._obj.items()):
            try:
                self.set_raw_value(key, value)
            except AttributeError:
                raise WrappingAttributeError(
                    "can't set attribute corresponding to {key!r} "
                    "on a {cls} while wrapping {data!r}".format(
                        cls=self.__class__,
                        key=key,
                        data=_obj,
                    )
                )

        for attr, value in kwargs.items():
            try:
                setattr(self, attr, value)
            except AttributeError:
                raise WrappingAttributeError(
                    "can't set attribute {key!r} "
                    "on a {cls} while wrapping {data!r}".format(
                        cls=self.__class__,
                        key=attr,
                        data=_obj,
                    )
                )

        for key, value in self._properties_by_key.items():
            if key not in self._obj:
                try:
                    d = value.default()
                except TypeError:
                    d = value.default(self)
                self[key] = d

    def set_raw_value(self, key, value):
        wrapped = self.__wrap(key, value)
        if key in self._properties_by_key:
            self[key] = wrapped
        else:
            setattr(self, key, wrapped)

    @classmethod
    def properties(cls):
        return cls._properties_by_attr.copy()

    @property
    def __dynamic_properties(self):
        return getattr(self, '_$').dynamic_properties

    @classmethod
    def wrap(cls, obj):
        self = cls(obj)
        return self

    def validate(self, required=True):
        for key, value in self._wrapped.items():
            self.__get_property(key).validate(value, required=required)

    def to_json(self):
        self.validate()
        return copy.deepcopy(self._obj)

    def __get_property(self, key):
        try:
            return self._properties_by_key[key]
        except KeyError:
            return DefaultProperty(type_config=get_settings(self).type_config)

    def __wrap(self, key, value):
        property_ = self.__get_property(key)

        if value is None:
            return None

        return property_.wrap(value)

    def __unwrap(self, key, value):
        property_ = self.__get_property(key)
        if value is None:
            wrapped, unwrapped = None, None
        else:
            wrapped, unwrapped = property_.unwrap(value)

        if isinstance(wrapped, JsonObjectBase):
            # validate containers but not objects
            recursive_kwargs = {'recursive': False}
        else:
            # omit the argument for backwards compatibility of custom properties
            # that do not contain `recursive` in their signature
            # and let the default of True shine through
            recursive_kwargs = {}
        property_.validate(
            wrapped,
            required=not self._validate_required_lazily,
            **recursive_kwargs,
        )
        return wrapped, unwrapped

    def __setitem__(self, key, value):
        wrapped, unwrapped = self.__unwrap(key, value)
        self._wrapped[key] = wrapped
        if self.__get_property(key).exclude(unwrapped):
            self._obj.pop(key, None)
        else:
            self._obj[key] = unwrapped
        if key not in self._properties_by_key:
            assert key not in self._properties_by_attr
            self.__dynamic_properties[key] = wrapped
            super(JsonObjectBase, self).__setattr__(key, wrapped)

    def __is_dynamic_property(self, name):
        return (
            name not in self._properties_by_attr and
            not name.startswith('_') and
            not inspect.isdatadescriptor(getattr(self.__class__, name, None))
        )

    def __setattr__(self, name, value):
        if self.__is_dynamic_property(name):
            if self._allow_dynamic_properties:
                self[name] = value
            else:
                raise AttributeError(
                    "{0!r} is not defined in schema "
                    "(not a valid property)".format(name)
                )
        else:
            super(JsonObjectBase, self).__setattr__(name, value)

    def __delitem__(self, key):
        if key in self._properties_by_key:
            raise DeleteNotAllowed(key)
        else:
            if not self.__is_dynamic_property(key):
                raise KeyError(key)
            del self._obj[key]
            del self._wrapped[key]
            del self.__dynamic_properties[key]
            super(JsonObjectBase, self).__delattr__(key)

    def __delattr__(self, name):
        if name in self._properties_by_attr:
            raise DeleteNotAllowed(name)
        elif self.__is_dynamic_property(name):
            del self[name]
        else:
            super(JsonObjectBase, self).__delattr__(name)

    def __repr__(self):
        name = self.__class__.__name__
        predefined_properties = self._properties_by_attr.keys()
        predefined_property_keys = set(self._properties_by_attr[p].name
                                       for p in predefined_properties)
        dynamic_properties = (set(self._wrapped.keys())
                              - predefined_property_keys)
        properties = sorted(predefined_properties) + sorted(dynamic_properties)
        return u'{name}({keyword_args})'.format(
            name=name,
            keyword_args=', '.join('{key}={value!r}'.format(
                key=key,
                value=getattr(self, key)
            ) for key in properties),
        )


class _LimitedDictInterfaceMixin(object):
    """
    mindlessly farms selected dict methods out to an internal dict

    really only a separate class from JsonObject
    to keep this mindlessness separate from the methods
    that need to be more carefully understood

    """
    _wrapped = None

    def keys(self):
        return self._wrapped.keys()

    def items(self):
        return self._wrapped.items()

    def iteritems(self):
        return self._wrapped.iteritems()

    def __contains__(self, item):
        return item in self._wrapped

    def __getitem__(self, item):
        return self._wrapped[item]

    def __iter__(self):
        return iter(self._wrapped)

    def __len__(self):
        return len(self._wrapped)


def get_dynamic_properties(obj):
    return getattr(obj, '_$').dynamic_properties.copy()
