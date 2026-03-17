from __future__ import absolute_import
import six
import inspect
from jsonobject.exceptions import BadValueError

function_name = None
if six.PY3:
    def function_name(f):
        return f.__name__
else:
    def function_name(f):
        return f.func_name


class JsonProperty(object):

    default = None
    type_config = None

    def __init__(self, default=Ellipsis, name=None, choices=None,
                 required=False, exclude_if_none=False, validators=None,
                 verbose_name=None, type_config=None):
        validators = validators or ()
        self.name = name
        if default is Ellipsis:
            default = self.default
        if callable(default):
            self.default = default
        else:
            self.default = lambda: default
        self.choices = choices
        self.choice_keys = []
        if choices:
            for choice in choices:
                if isinstance(choice, tuple):
                    choice, _ = choice
                self.choice_keys.append(choice)
        self.required = required
        self.exclude_if_none = exclude_if_none
        self._validators = validators
        self.verbose_name = verbose_name
        if type_config:
            self.type_config = type_config

    def init_property(self, default_name, type_config):
        self.name = self.name or default_name
        self.type_config = self.type_config or type_config

    def wrap(self, obj):
        raise NotImplementedError()

    def unwrap(self, obj):
        """
        must return tuple of (wrapped, unwrapped)

        If obj is already a fully wrapped object,
        it must be returned as the first element.

        For an example where the first element is relevant see ListProperty

        """
        raise NotImplementedError()

    def to_json(self, value):
        _, unwrapped = self.unwrap(value)
        return unwrapped

    def to_python(self, value):
        return self.wrap(value)

    def __get__(self, instance, owner):
        if instance:
            assert self.name in instance
            return instance[self.name]
        else:
            return self

    def __set__(self, instance, value):
        instance[self.name] = value

    def __call__(self, method):
        """
        use a property as a decorator to set its default value

        class Document(JsonObject):
            @StringProperty()
            def doc_type(self):
                return self.__class__.__name__
        """
        assert self.default() is None
        self.default = method
        self.name = self.name or function_name(method)
        return self

    def exclude(self, value):
        return self.exclude_if_none and not value

    def empty(self, value):
        return value is None

    def validate(self, value, required=True, recursive=True):
        if (self.choice_keys and value not in self.choice_keys
                and value is not None):
            raise BadValueError(
                '{0!r} not in choices: {1!r}'.format(value, self.choice_keys)
            )

        if not self.empty(value):
            self._custom_validate(value)
        elif required and self.required:
            raise BadValueError(
                'Property {0} is required.'.format(self.name)
            )
        if recursive and hasattr(value, 'validate'):
            value.validate(required=required)

    def _custom_validate(self, value):
        if self._validators:
            if hasattr(self._validators, '__iter__'):
                for validator in self._validators:
                    validator(value)
            else:
                self._validators(value)


class JsonContainerProperty(JsonProperty):

    _type = default = None
    container_class = None

    def __init__(self, item_type=None, **kwargs):
        self._item_type_deferred = item_type
        super(JsonContainerProperty, self).__init__(**kwargs)

    def init_property(self, **kwargs):
        super(JsonContainerProperty, self).init_property(**kwargs)
        if not inspect.isfunction(self._item_type_deferred):
            # trigger validation
            self.item_wrapper

    def to_item_wrapper(self, item_type):
        from jsonobject.base import JsonObjectMeta
        from .properties import ObjectProperty
        if item_type is None:
            return None
        if isinstance(item_type, JsonObjectMeta):
            return ObjectProperty(item_type, type_config=self.type_config)
        elif isinstance(item_type, JsonProperty):
            item_wrapper = item_type
            if item_wrapper.type_config is None:
                item_wrapper.type_config = self.type_config
            return item_wrapper
        elif issubclass(item_type, JsonProperty):
            return item_type(type_config=self.type_config, required=True)
        elif item_type in self.type_config.properties:
            return self.type_config.properties[item_type](type_config=self.type_config, required=True)
        else:
            for general_type, property_cls in self.type_config.properties.items():
                if issubclass(item_type, general_type):
                    return property_cls(type_config=self.type_config, required=True)
            raise ValueError("item_type {0!r} not in {1!r}".format(
                item_type,
                self.type_config.properties,
            ))

    @property
    def item_wrapper(self):
        if hasattr(self, '_item_type_deferred'):
            if inspect.isfunction(self._item_type_deferred):
                self._item_wrapper = self.to_item_wrapper(self._item_type_deferred())
            else:
                self._item_wrapper = self.to_item_wrapper(self._item_type_deferred)
            del self._item_type_deferred
        return self._item_wrapper

    def empty(self, value):
        return not value

    def wrap(self, obj):
        return self.container_class(obj, wrapper=self.item_wrapper,
                                    type_config=self.type_config)

    def unwrap(self, obj):
        if not isinstance(obj, self._type):
            raise BadValueError(
                '{0!r} is not an instance of {1!r}'.format(
                    obj, self._type.__name__)
            )
        if isinstance(obj, self.container_class):
            return obj, obj._obj
        else:
            wrapped = self.wrap(self._type())
            self._update(wrapped, obj)
            return self.unwrap(wrapped)

    def _update(self, container, extension):
        raise NotImplementedError()


class DefaultProperty(JsonProperty):

    def wrap(self, obj):
        assert self.type_config.string_conversions is not None
        value = self.value_to_python(obj)
        property_ = self.value_to_property(value)

        if property_:
            return property_.wrap(obj)

    def unwrap(self, obj):
        property_ = self.value_to_property(obj)
        if property_:
            return property_.unwrap(obj)
        else:
            return obj, None

    def value_to_property(self, value):
        map_types_properties = self.type_config.properties
        if value is None:
            return None
        elif type(value) in map_types_properties:
            return map_types_properties[type(value)](
                type_config=self.type_config)
        else:
            for value_type, prop_class in map_types_properties.items():
                if isinstance(value, value_type):
                    return prop_class(type_config=self.type_config)
            else:
                raise BadValueError(
                    'value {0!r} not in allowed types: {1!r}'.format(
                        value, map_types_properties.keys())
                )

    def value_to_python(self, value):
        """
        convert encoded string values to the proper python type

        ex:
        >>> DefaultProperty().value_to_python('2013-10-09T10:05:51Z')
        datetime.datetime(2013, 10, 9, 10, 5, 51)

        other values will be passed through unmodified
        Note: containers' items are NOT recursively converted

        """
        if isinstance(value, six.string_types):
            convert = None
            for pattern, _convert in self.type_config.string_conversions:
                if pattern.match(value):
                    convert = _convert
                    break

            if convert is not None:
                try:
                    #sometimes regex fail so return value
                    value = convert(value)
                except Exception:
                    pass
        return value


class AssertTypeProperty(JsonProperty):
    _type = None

    def assert_type(self, obj):
        if obj is None:
            return
        elif not isinstance(obj, self._type):
            raise BadValueError(
                '{0!r} not of type {1!r}'.format(obj, self._type)
            )

    def selective_coerce(self, obj):
        return obj

    def wrap(self, obj):
        obj = self.selective_coerce(obj)
        self.assert_type(obj)
        return obj

    def unwrap(self, obj):
        obj = self.selective_coerce(obj)
        self.assert_type(obj)
        return obj, obj


class AbstractDateProperty(JsonProperty):

    _type = None

    def __init__(self, exact=False, *args, **kwargs):
        super(AbstractDateProperty, self).__init__(*args, **kwargs)
        self.exact = exact

    def wrap(self, obj):
        try:
            if not isinstance(obj, six.string_types):
                raise ValueError()
            return self._wrap(obj)
        except ValueError:
            raise BadValueError('{0!r} is not a {1}-formatted string'.format(
                obj,
                self._type.__name__,
            ))

    def unwrap(self, obj):
        if not isinstance(obj, self._type):
            raise BadValueError('{0!r} is not a {1} object'.format(
                obj,
                self._type.__name__,
            ))
        return self._unwrap(obj)

    def _wrap(self, obj):
        raise NotImplementedError()

    def _unwrap(self, obj):
        raise NotImplementedError()
