from __future__ import annotations

import json
import re
from dataclasses import fields, is_dataclass
from enum import Enum
from functools import lru_cache
from ipaddress import IPv4Address, IPv4Network, IPv6Address, IPv6Network, ip_network
from typing import TYPE_CHECKING, Any, Callable, Literal, TypeVar, Union, overload
from uuid import UUID

from ._base import DirtyEquals
from ._dict import IsDict
from ._utils import Omit, plain_repr

if TYPE_CHECKING:
    from pydantic import TypeAdapter


class IsUUID(DirtyEquals[UUID]):
    """
    A class that checks if a value is a valid UUID, optionally checking UUID version.
    """

    def __init__(self, version: Literal[None, 1, 2, 3, 4, 5] = None):
        """
        Args:
            version: The version of the UUID to check, if omitted, all versions are accepted.

        ```py title="IsUUID"
        import uuid

        from dirty_equals import IsUUID

        assert 'edf9f29e-45c7-431c-99db-28ea44df9785' == IsUUID
        assert 'edf9f29e-45c7-431c-99db-28ea44df9785' == IsUUID(4)
        assert 'edf9f29e45c7431c99db28ea44df9785' == IsUUID(4)
        assert 'edf9f29e-45c7-431c-99db-28ea44df9785' != IsUUID(5)
        assert uuid.uuid4() == IsUUID(4)
        ```
        """
        self.version = version
        super().__init__(version or plain_repr(''))

    def equals(self, other: Any) -> bool:
        if isinstance(other, UUID):
            uuid = other
        elif isinstance(other, str):
            uuid = UUID(other)
            if self.version is not None and uuid.version != self.version:
                return False
        else:
            return False

        if self.version:
            return uuid.version == self.version
        else:
            return True


AnyJson = object
JsonType = TypeVar('JsonType', AnyJson, Any)


class IsJson(DirtyEquals[JsonType]):
    """
    A class that checks if a value is a JSON object, and check the contents of the JSON.
    """

    @overload
    def __init__(self, expected_value: JsonType = AnyJson): ...

    @overload
    def __init__(self, **expected_kwargs: Any): ...

    def __init__(self, expected_value: JsonType = AnyJson, **expected_kwargs: Any):
        """
        Args:
            expected_value: Value to compare the JSON to, if omitted, any JSON is accepted.
            **expected_kwargs: Keyword arguments forming a dict to compare the JSON to,
                `expected_value` and `expected_kwargs` may not be combined.

        As with any `dirty_equals` type, types can be nested to provide more complex checks.

        !!! note
            Like [`IsInstance`][dirty_equals.IsInstance], `IsJson` can be parameterized or initialised with a value -
            `IsJson[xyz]` is exactly equivalent to `IsJson(xyz)`.

            This allows usage to be analogous to type hints.


        ```py title="IsJson"
        from dirty_equals import IsJson, IsPositiveInt, IsStrictDict

        assert '{"a": 1, "b": 2}' == IsJson
        assert '{"a": 1, "b": 2}' == IsJson(a=1, b=2)
        assert '{"a": 1}' != IsJson(a=2)
        assert 'invalid json' != IsJson
        assert '{"a": 1}' == IsJson(a=IsPositiveInt)
        assert '"just a quoted string"' == IsJson('just a quoted string')

        assert '{"a": 1, "b": 2}' == IsJson[IsStrictDict(a=1, b=2)]
        assert '{"b": 2, "a": 1}' != IsJson[IsStrictDict(a=1, b=2)]
        ```
        """
        if expected_kwargs:
            if expected_value is not AnyJson:
                raise TypeError('IsJson requires either an argument or kwargs, not both')
            self.expected_value: Any = expected_kwargs
        else:
            self.expected_value = expected_value
        super().__init__(plain_repr('') if expected_value is AnyJson else expected_value)

    def __class_getitem__(cls, expected_type: JsonType) -> IsJson[JsonType]:
        return cls(expected_type)

    def equals(self, other: Any) -> bool:
        if isinstance(other, (str, bytes)):
            v = json.loads(other)
            if self.expected_value is AnyJson:
                return True
            else:
                return v == self.expected_value
        else:
            return False


class FunctionCheck(DirtyEquals[Any]):
    """
    Use a function to check if a value "equals" whatever you want to check
    """

    def __init__(self, func: Callable[[Any], bool]):
        """
        Args:
            func: callable that takes a value and returns a bool.

        ```py title="FunctionCheck"
        from dirty_equals import FunctionCheck

        def is_even(x):
            return x % 2 == 0

        assert 2 == FunctionCheck(is_even)
        assert 3 != FunctionCheck(is_even)
        ```
        """
        self.func = func
        super().__init__(plain_repr(func.__name__))

    def equals(self, other: Any) -> bool:
        return self.func(other)


T = TypeVar('T')


@lru_cache
def _build_type_adapter(ta: type[TypeAdapter[T]], schema: T) -> TypeAdapter[T]:
    return ta(schema)


_allowed_url_attribute_checks: set[str] = {
    'scheme',
    'host',
    'host_type',
    'user',
    'password',
    'port',
    'path',
    'query',
    'fragment',
}


class IsUrl(DirtyEquals[Any]):
    """
    A class that checks if a value is a valid URL, optionally checking different URL types and attributes with
    [Pydantic](https://pydantic-docs.helpmanual.io/usage/types/#urls).
    """

    def __init__(
        self,
        any_url: bool = False,
        any_http_url: bool = False,
        http_url: bool = False,
        file_url: bool = False,
        postgres_dsn: bool = False,
        ampqp_dsn: bool = False,
        redis_dsn: bool = False,
        **expected_attributes: Any,
    ):
        """
        Args:
            any_url: any scheme allowed, host required
            any_http_url: scheme http or https, host required
            http_url: scheme http or https, host required, max length 2083
            file_url: scheme file, host not required
            postgres_dsn: user info required
            ampqp_dsn: schema amqp or amqps, user info not required, host not required
            redis_dsn: scheme redis or rediss, user info not required, host not required
            **expected_attributes: Expected values for url attributes
        ```py title="IsUrl"
        from dirty_equals import IsUrl

        assert 'https://example.com' == IsUrl
        assert 'https://example.com' == IsUrl(host='example.com')
        assert 'https://example.com' == IsUrl(scheme='https')
        assert 'https://example.com' != IsUrl(scheme='http')
        assert 'postgres://user:pass@localhost:5432/app' == IsUrl(postgres_dsn=True)
        assert 'postgres://user:pass@localhost:5432/app' != IsUrl(http_url=True)
        ```
        """
        try:
            from pydantic import (
                AmqpDsn,
                AnyHttpUrl,
                AnyUrl,
                FileUrl,
                HttpUrl,
                PostgresDsn,
                RedisDsn,
                TypeAdapter,
                ValidationError,
            )

            self.ValidationError = ValidationError
        except ImportError as e:  # pragma: no cover
            raise ImportError('Pydantic V2 is not installed, run `pip install dirty-equals[pydantic]`') from e
        url_type_mappings = {
            AnyUrl: any_url,
            AnyHttpUrl: any_http_url,
            HttpUrl: http_url,
            FileUrl: file_url,
            PostgresDsn: postgres_dsn,
            AmqpDsn: ampqp_dsn,
            RedisDsn: redis_dsn,
        }
        url_types_sum = sum(url_type_mappings.values())
        if url_types_sum == 0:
            url_type: Any = AnyUrl
        elif url_types_sum == 1:
            url_type = max(url_type_mappings, key=url_type_mappings.get)  # type: ignore[arg-type]
        else:
            raise ValueError('You can only check against one Pydantic url type at a time')

        self.type_adapter = _build_type_adapter(TypeAdapter, url_type)

        for item in expected_attributes:
            if item not in _allowed_url_attribute_checks:
                raise TypeError(
                    'IsURL only checks these attributes: scheme, host, host_type, user, password, '
                    'port, path, query, fragment'
                )
        self.attribute_checks = expected_attributes
        super().__init__()

    def equals(self, other: Any) -> bool:
        try:
            other_url = self.type_adapter.validate_python(other)
        except self.ValidationError:
            raise ValueError('Invalid URL')

        # we now check that str() of the parsed URL equals its original value
        # so that invalid encodings fail
        # we remove trailing slashes since they're added by pydantic's URL parsing, but don't mean `other` is invalid
        other_url_str = str(other_url)
        if not other.endswith('/') and other_url_str.endswith('/'):
            other_url_str = other_url_str[:-1]
        equal = other_url_str == other

        if not self.attribute_checks:
            return equal

        for attribute, expected in self.attribute_checks.items():
            if getattr(other_url, attribute) != expected:
                return False
        return equal


HashTypes = Literal['md5', 'sha-1', 'sha-256']


class IsHash(DirtyEquals[str]):
    """
    A class that checks if a value is a valid common hash type, using a simple length and allowed characters regex.
    """

    def __init__(self, hash_type: HashTypes):
        """
        Args:
            hash_type: The hash type to check. Must be specified.

        ```py title="IsHash"
        from dirty_equals import IsHash

        assert 'f1e069787ece74531d112559945c6871' == IsHash('md5')
        assert b'f1e069787ece74531d112559945c6871' == IsHash('md5')
        assert 'f1e069787ece74531d112559945c6871' != IsHash('sha-256')
        assert 'F1E069787ECE74531D112559945C6871' == IsHash('md5')
        assert '40bd001563085fc35165329ea1ff5c5ecbdbbeef' == IsHash('sha-1')
        assert 'a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3' == IsHash(
            'sha-256'
        )
        ```
        """

        allowed_hashes = HashTypes.__args__  # type: ignore[attr-defined]
        if hash_type not in allowed_hashes:
            raise ValueError(f'Hash type must be one of the following values: {", ".join(allowed_hashes)}')

        self.hash_type = hash_type
        super().__init__(hash_type)

    def equals(self, other: Any) -> bool:
        if isinstance(other, str):
            s = other
        elif isinstance(other, (bytes, bytearray)):
            s = other.decode()
        else:
            return False
        hash_type_regex_patterns = {
            'md5': r'[a-fA-F\d]{32}',
            'sha-1': r'[a-fA-F\d]{40}',
            'sha-256': r'[a-fA-F\d]{64}',
        }
        return bool(re.fullmatch(hash_type_regex_patterns[self.hash_type], s))


IP = TypeVar('IP', IPv4Address, IPv4Network, IPv6Address, IPv6Network, Union[str, int, bytes])


class IsIP(DirtyEquals[IP]):
    """
    A class that checks if a value is a valid IP address, optionally checking IP version, netmask.
    """

    def __init__(self, *, version: Literal[None, 4, 6] = None, netmask: str | None = None):
        """
        Args:
            version: The version of the IP to check, if omitted, versions 4 and 6 are both accepted.
            netmask: The netmask of the IP to check, if omitted, any netmask is accepted. Requires version.

        ```py title="IsIP"
        from ipaddress import IPv4Address, IPv4Network, IPv6Address

        from dirty_equals import IsIP

        assert '179.27.154.96' == IsIP
        assert '179.27.154.96' == IsIP(version=4)
        assert '2001:0db8:0a0b:12f0:0000:0000:0000:0001' == IsIP(version=6)
        assert IPv4Address('127.0.0.1') == IsIP
        assert IPv4Network('43.48.0.0/12') == IsIP
        assert IPv6Address('::eeff:ae3f:d473') == IsIP
        assert '54.43.53.219/10' == IsIP(version=4, netmask='255.192.0.0')
        assert '54.43.53.219/10' == IsIP(version=4, netmask=4290772992)
        assert '::ffff:aebf:d473/12' == IsIP(version=6, netmask='fff0::')
        assert 3232235521 == IsIP
        ```
        """
        self.version = version
        if netmask and not self.version:
            raise TypeError('To check the netmask you must specify the IP version')
        self.netmask = netmask
        super().__init__(version=version or Omit, netmask=netmask or Omit)

    def equals(self, other: Any) -> bool:
        if isinstance(other, (IPv4Network, IPv6Network)):
            ip = other
        elif isinstance(other, (str, bytes, int, IPv4Address, IPv6Address)):
            ip = ip_network(other, strict=False)
        else:
            return False

        if self.version:
            if self.netmask:
                version_check = self.version == ip.version
                address_format = {4: IPv4Address, 6: IPv6Address}[self.version]
                netmask_check = int(address_format(self.netmask)) == int(ip.netmask)
                return version_check and netmask_check
            elif self.version != ip.version:
                return False

        return True


class IsDataclassType(DirtyEquals[Any]):
    """
    Checks that an object is a dataclass type.

    Inherits from [`DirtyEquals`][dirty_equals.DirtyEquals].

    ```py title="IsDataclassType"
    from dataclasses import dataclass
    from dirty_equals import IsDataclassType

    @dataclass
    class Foo:
        a: int
        b: int

    foo = Foo(1, 2)

    assert Foo == IsDataclassType
    assert foo != IsDataclassType
    ```
    """

    def equals(self, other: Any) -> bool:
        return is_dataclass(other) and isinstance(other, type)


class IsDataclass(DirtyEquals[Any]):
    """
    Checks that an object is an instance of a dataclass.

    Inherits from [`DirtyEquals`][dirty_equals.DirtyEquals] and it can be initialised with specific keyword arguments to
    check exactness of dataclass fields, by comparing the instance `__dict__`  with [`IsDict`][dirty_equals.IsDict].
    Moreover it is possible to check for strictness and partialness of the dataclass, by setting the `strict` and
    `partial` attributes using the `.settings(strict=..., partial=...)` method.

    Remark that passing no kwargs to `IsDataclass` initialization means fields are not checked, not that the dataclass
    is empty, namely `IsDataclass()` is the same as `IsDataclass`.

    ```py title="IsDataclass"
    from dataclasses import dataclass
    from dirty_equals import IsInt, IsDataclass

    @dataclass
    class Foo:
        a: int
        b: int
        c: str

    foo = Foo(1, 2, 'c')

    assert foo == IsDataclass
    assert foo == IsDataclass(a=IsInt, b=2, c='c')
    assert foo == IsDataclass(b=2, a=1).settings(partial=True)
    assert foo != IsDataclass(a=IsInt, b=2).settings(strict=True)
    assert foo == IsDataclass(a=IsInt, b=2).settings(strict=True, partial=True)
    assert foo != IsDataclass(b=2, a=1).settings(strict=True, partial=True)
    ```
    """

    def __init__(self, **fields: Any):
        """
        Args:
            fields: key-value pairs of field-value to check for.
        """
        self.strict = False
        self.partial = False
        self._post_init()
        super().__init__(**fields)

    def _post_init(self) -> None:
        pass

    def equals(self, other: Any) -> bool:
        if is_dataclass(other) and not isinstance(other, type):
            if self._repr_kwargs:
                return self._fields_check(other)
            else:
                return True
        else:
            return False

    def settings(
        self,
        *,
        strict: bool | None = None,
        partial: bool | None = None,
    ) -> IsDataclass:
        """Allows to customise the behaviour of `IsDataclass`, technically a new `IsDataclass` to allow chaining."""
        new_cls = self.__class__(**self._repr_kwargs)
        new_cls.__dict__ = self.__dict__.copy()

        if strict is not None:
            new_cls.strict = strict
        if partial is not None:
            new_cls.partial = partial

        return new_cls

    def _fields_check(self, other: Any) -> bool:
        """
        Checks exactness of fields using [`IsDict`][dirty_equals.IsDict] with given settings.

        Remark that if this method is called, then `other` is an instance of a dataclass, therefore we can use
        `dataclasses.fields` to get its fields. We use a shallow conversion to preserve nested dataclass instances.
        """
        other_dict = {field.name: getattr(other, field.name) for field in fields(other)}
        return other_dict == IsDict(self._repr_kwargs).settings(strict=self.strict, partial=self.partial)


class IsPartialDataclass(IsDataclass):
    """
    Inherits from [`IsDataclass`][dirty_equals.IsDataclass] with `partial=True` by default.

    ```py title="IsPartialDataclass"
    from dataclasses import dataclass
    from dirty_equals import IsInt, IsPartialDataclass

    @dataclass
    class Foo:
        a: int
        b: int
        c: str = 'c'

    foo = Foo(1, 2, 'c')

    assert foo == IsPartialDataclass
    assert foo == IsPartialDataclass(a=1)
    assert foo == IsPartialDataclass(b=2, a=IsInt)
    assert foo != IsPartialDataclass(b=2, a=IsInt).settings(strict=True)
    assert Foo != IsPartialDataclass
    ```
    """

    def _post_init(self) -> None:
        self.partial = True


class IsStrictDataclass(IsDataclass):
    """
    Inherits from [`IsDataclass`][dirty_equals.IsDataclass] with `strict=True` by default.

    ```py title="IsStrictDataclass"
    from dataclasses import dataclass
    from dirty_equals import IsInt, IsStrictDataclass

    @dataclass
    class Foo:
        a: int
        b: int
        c: str = 'c'

    foo = Foo(1, 2, 'c')

    assert foo == IsStrictDataclass
    assert foo == IsStrictDataclass(
        a=IsInt,
        b=2,
    ).settings(partial=True)
    assert foo != IsStrictDataclass(
        a=IsInt,
        b=2,
    ).settings(partial=False)
    assert foo != IsStrictDataclass(b=2, a=IsInt, c='c')
    ```
    """

    def _post_init(self) -> None:
        self.strict = True


class IsEnum(DirtyEquals[Enum]):
    """
    Checks if an instance is an Enum.

    Inherits from [`DirtyEquals`][dirty_equals.DirtyEquals].

    ```py title="IsEnum"
    from enum import Enum, auto
    from dirty_equals import IsEnum

    class ExampleEnum(Enum):
        a = auto()
        b = auto()

    a = ExampleEnum.a
    assert a == IsEnum
    assert a == IsEnum(ExampleEnum)
    ```
    """

    def __init__(self, enum_cls: type[Enum] = Enum):
        """
        Args:
            enum_cls: Enum class to check against.
        """
        self._enum_cls = enum_cls
        self._enum_values = {i.value for i in enum_cls}

    def equals(self, other: Any) -> bool:
        if isinstance(other, Enum):
            return isinstance(other, self._enum_cls)
        else:
            return other in self._enum_values
