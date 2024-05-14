import six

from abc import ABCMeta, abstractmethod


def _with_type(x):
    return "<type: {!s}>: {!r}".format(type(x), x)


def _is_utf8(s):
    if not isinstance(s, six.string_types):
        return False

    if isinstance(s, six.binary_type):
        try:
            s.decode("utf-8", errors="strict")
            return True
        except UnicodeDecodeError:
            return False
    return isinstance(s, six.text_type)


def _as_utf8(s):
    if isinstance(s, six.text_type):
        return s
    elif isinstance(s, six.binary_type):
        return s.decode("utf-8", errors="strict")
    else:
        raise TypeError("expected string or binary type, but got {}".format(type(s)))


def is_valid_type(x):
    return isinstance(x, Type) or x is Type


def validate_type(x):
    if not is_valid_type(x):
        raise ValueError("Expected type, but got {}".format(_with_type(x)))


def quote_string(s):
    return u"'{}'".format(s.replace("\\", "\\\\").replace("'", "\\'"))


class Type(six.with_metaclass(ABCMeta)):
    REQUIRED_ATTRS = ["name", "yt_type_name"]

    def __init__(self, attrs):
        assert all(key in attrs for key in self.REQUIRED_ATTRS), \
            "One or more of required arguments not found in attributes"

        for name, value in attrs.items():
            setattr(self, name, value)

    def __eq__(self, other):
        assert isinstance(self, Type)

        if not isinstance(other, Type):
            return False

        return str(self) == str(other)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return hash(str(self))

    @abstractmethod
    def __str__(self):
        pass

    @abstractmethod
    def to_yson_type(self):
        pass


class Primitive(Type):
    def __str__(self):
        return self.name

    def to_yson_type(self):
        return self.yt_type_name

    def to_yson_type_v1(self):
        return self.yt_type_name_v1


class Generic(six.with_metaclass(ABCMeta)):
    def __init__(self, name, yt_type_name=None):
        assert _is_utf8(name), "Name must be UTF-8, got {}".format(_with_type(name))
        self.name = name
        self.yt_type_name = name.lower()

    @abstractmethod
    def __getitem__(self, param):
        pass

    @abstractmethod
    def from_dict(self):
        pass


def make_primitive_type(name, yt_type_name=None, yt_type_name_v1=None):
    assert _is_utf8(name), "Name of primitive type must be UTF-8, got {}".format(_with_type(name))
    assert yt_type_name is None or _is_utf8(yt_type_name), \
        "YT type name of primitive type must be UTF-8, got {}".format(_with_type(name))

    if yt_type_name is None:
        yt_type_name = name.lower()

    if yt_type_name_v1 is None:
        yt_type_name_v1 = name.lower()

    return Primitive({
        "name": name,
        "yt_type_name": yt_type_name,
        "yt_type_name_v1": yt_type_name_v1,
    })
