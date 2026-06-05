from yt.common import YtError


class YsonType(object):
    def __getattr__(self, attribute):
        if attribute == "attributes":
            self.__dict__[attribute] = {}
            return self.__dict__[attribute]
        raise AttributeError('Attribute "{0}" not found'.format(attribute))

    def has_attributes(self):
        try:
            return "attributes" in self.__dict__ and self.attributes is not None and self.attributes != {}
        except:  # noqa
            return False

    def __eq__(self, other):
        try:
            has_attributes = other.has_attributes()
        except AttributeError:
            has_attributes = False
        if has_attributes:
            return self.attributes == other.attributes
        return not self.has_attributes()

    def __ne__(self, other):
        return not (self == other)

    def to_str(self, base_type, str_func):
        if self.has_attributes():
            return str_func({"value": base_type(self), "attributes": self.attributes})
        return str_func(base_type(self))

    def base_hash(self, type_):
        if self.has_attributes():
            raise TypeError("unhashable type: YSON has non-trivial attributes")
        return hash(type_(self))

    def get_yson_type_str(selfself):
        return None


class YsonString(bytes, YsonType):
    def __eq__(self, other):
        # COMPAT: With implicit promotion of str to unicode it can make sense
        # to compare binary YsonString to unicode string.
        if not isinstance(other, (bytes, str)):
            return NotImplemented
        return bytes(self) == bytes(other) and YsonType.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return self.base_hash(bytes)

    def __repr__(self):
        return self.to_str(bytes, repr)

    def get_yson_type_str(self):
        return "string"


class YsonUnicode(str, YsonType):
    def __eq__(self, other):
        if not isinstance(other, str):
            return NotImplemented
        return str(self) == str(other) and YsonType.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return self.base_hash(str)

    def __repr__(self):
        return self.to_str(str, repr)

    def get_yson_type_str(self):
        return "string"


class NotUnicodeError(YtError, TypeError):
    pass


def _truncate(s, length=50):
    assert isinstance(s, bytes)
    if len(s) < length:
        return s
    return s[:length] + b"..."


def _make_raise_not_unicode_error(name):
    def fun(self, *args, **kwargs):
        raise NotUnicodeError('Method "{}" is not allowed: YSON string "{}" '
                              "could not be decoded to Unicode, "
                              "see https://ytsaurus.tech/docs/en/api/python/userdoc#python3_strings"
                              .format(name, _truncate(self._bytes)))
    return fun


def proxy(cls):
    ALLOWED_METHODS = [
        "get_bytes",
        "is_unicode",
        "__hash__",
        "__eq__",
        "__ne__",
        "__repr__",
        "__format__",
        "__dict__",
        "__qualname__",
        "__class__",
        "__mro__",
        "__new__",
        "__init__",
        "__getattr__",
        "__setattr__",
        "__getattribute__",
        "__copy__",
        "__deepcopy__",
    ]

    ADDITIONAL_METHODS = [
        "__radd__",
    ]

    for name in dir(str):
        attr = getattr(str, name)
        if callable(attr) and name not in ALLOWED_METHODS:
            setattr(cls, name, _make_raise_not_unicode_error(name))
    for name in ADDITIONAL_METHODS:
        setattr(cls, name, _make_raise_not_unicode_error(name))
    return cls


# NB: This class is never returned by library in Python2.
# NB: Don't create this class by hand, it should only be returned
# from the library.
@proxy
class YsonStringProxy(YsonType):
    def __repr__(self):
        value = "<YsonStringProxy>{!r}".format(self._bytes)
        if self.has_attributes():
            return repr({"attributes": self.attributes, "value": value})
        return value

    def __format__(self, format_spec):
        return repr(self)

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self

    def __hash__(self):
        return hash(self._bytes)

    def __eq__(self, other):
        if isinstance(other, bytes):
            return self._bytes == bytes(other) and YsonType.__eq__(self, other)
        elif isinstance(other, YsonStringProxy):
            return self._bytes == other._bytes and YsonType.__eq__(self, other)
        else:
            return NotImplemented

    def __ne__(self, other):
        return not (self == other)

    def get_yson_type_str(self):
        return "string"


def is_unicode(x):
    return isinstance(x, str)


def get_bytes(x, encoding="utf8"):
    if isinstance(x, str):
        return x.encode(encoding)
    elif isinstance(x, YsonStringProxy):
        return x._bytes
    elif isinstance(x, bytes):
        return x
    else:
        raise TypeError("get_bytes() expected str, bytes or YsonStringProxy, got <{}>{!r}"
                        .format(type(x), x))


def make_byte_key(s):
    proxy = YsonStringProxy()
    proxy._bytes = s
    return proxy


_YsonIntegerBase = int


class YsonIntegerBase(_YsonIntegerBase, YsonType):
    def __eq__(self, other):
        if not isinstance(other, int):
            return NotImplemented
        return _YsonIntegerBase(self) == _YsonIntegerBase(other) and YsonType.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return self.base_hash(_YsonIntegerBase)

    def __repr__(self):
        return self.to_str(_YsonIntegerBase, repr)

    def __str__(self):
        return self.to_str(_YsonIntegerBase, str)


class YsonInt64(YsonIntegerBase):
    def get_yson_type_str(self):
        return "int64"


class YsonUint64(YsonIntegerBase):
    def get_yson_type_str(self):
        return "uint64"


class YsonDouble(float, YsonType):
    def __eq__(self, other):
        if not isinstance(other, float):
            return NotImplemented
        return float(self) == float(other) and YsonType.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return self.base_hash(float)

    def __repr__(self):
        return self.to_str(float, repr)

    def __str__(self):
        return self.to_str(float, str)

    def get_yson_type_str(self):
        return "double"


class YsonBoolean(int, YsonType):
    def __eq__(self, other):
        if not isinstance(other, int):
            return NotImplemented
        return (int(self) == 0) == (int(other) == 0) and YsonType.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return self.base_hash(bool)

    # NB: do not change this representation, because
    # this type required to be JSON serializable.
    # JSON encoder thinks that it is integer and calls str.
    def __repr__(self):
        return "true" if self else "false"

    def __str__(self):
        return self.__repr__()

    def get_yson_type_str(self):
        return "bool"


class YsonList(list, YsonType):
    def __eq__(self, other):
        if not isinstance(other, list):
            return NotImplemented
        return list(self) == list(other) and YsonType.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        raise TypeError('unhashable type "YsonList"')

    def __repr__(self):
        return self.to_str(list, repr)

    def __str__(self):
        return self.to_str(list, str)


class YsonMap(dict, YsonType):
    def __eq__(self, other):
        if not isinstance(other, dict):
            return NotImplemented
        return dict(self) == dict(other) and YsonType.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        raise TypeError('unhashable type "YsonMap"')

    def __repr__(self):
        return self.to_str(dict, repr)

    def __str__(self):
        return self.to_str(dict, str)


class YsonEntity(YsonType):
    def __init__(self, value=None):
        if value is not None:
            assert isinstance(value, YsonEntity)
            self.attributes = value.attributes

    def __eq__(self, other):
        if other is None and not self.attributes:
            return True
        if not isinstance(other, YsonEntity):
            return NotImplemented
        return YsonType.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)

    def __bool__(self):
        return False

    def __repr__(self):
        if self.attributes:
            return repr({"value": "YsonEntity", "attributes": self.attributes})
        else:
            return "YsonEntity"

    def __str__(self):
        return self.__repr__()

    __nonzero__ = __bool__
