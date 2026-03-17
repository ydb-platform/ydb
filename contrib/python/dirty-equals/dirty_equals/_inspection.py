from typing import Any, TypeVar, Union, overload

from ._base import DirtyEquals
from ._strings import IsStr
from ._utils import get_dict_arg

ExpectedType = TypeVar('ExpectedType', bound=Union[type, tuple[Union[type, tuple[Any, ...]], ...]])


class IsInstance(DirtyEquals[ExpectedType]):
    """
    A type which checks that the value is an instance of the expected type.
    """

    def __init__(self, expected_type: ExpectedType, *, only_direct_instance: bool = False):
        """
        Args:
            expected_type: The type to check against.
            only_direct_instance: whether instances of subclasses of `expected_type` should be considered equal.

        !!! note
            `IsInstance` can be parameterized or initialised with a type -
            `IsInstance[Foo]` is exactly equivalent to `IsInstance(Foo)`.

            This allows usage to be analogous to type hints.

        Example:
        ```py title="IsInstance"
        from dirty_equals import IsInstance

        class Foo:
            pass

        class Bar(Foo):
            pass

        assert Foo() == IsInstance[Foo]
        assert Foo() == IsInstance(Foo)
        assert Foo != IsInstance[Bar]

        assert Bar() == IsInstance[Foo]
        assert Foo() == IsInstance(Foo, only_direct_instance=True)
        assert Bar() != IsInstance(Foo, only_direct_instance=True)
        ```
        """
        self.expected_type = expected_type
        self.only_direct_instance = only_direct_instance
        super().__init__(expected_type)

    def __class_getitem__(cls, expected_type: ExpectedType) -> 'IsInstance[ExpectedType]':
        return cls(expected_type)

    def equals(self, other: Any) -> bool:
        if self.only_direct_instance:
            return type(other) == self.expected_type
        else:
            return isinstance(other, self.expected_type)


T = TypeVar('T')


class HasName(DirtyEquals[T]):
    """
    A type which checks that the value has the given `__name__` attribute.
    """

    def __init__(self, expected_name: Union[IsStr, str], *, allow_instances: bool = True):
        """
        Args:
            expected_name: The name to check against.
            allow_instances: whether instances of classes with the given name should be considered equal,
                (e.g. whether `other.__class__.__name__ == expected_name` should be checked).

        Example:
        ```py title="HasName"
        from dirty_equals import HasName, IsStr

        class Foo:
            pass

        assert Foo == HasName('Foo')
        assert Foo == HasName['Foo']
        assert Foo() == HasName('Foo')
        assert Foo() != HasName('Foo', allow_instances=False)
        assert Foo == HasName(IsStr(regex='F..'))
        assert Foo != HasName('Bar')
        assert int == HasName('int')
        assert int == HasName('int')
        ```
        """
        self.expected_name = expected_name
        self.allow_instances = allow_instances
        kwargs = {}
        if allow_instances:
            kwargs['allow_instances'] = allow_instances
        super().__init__(expected_name, allow_instances=allow_instances)

    def __class_getitem__(cls, expected_name: str) -> 'HasName[T]':
        return cls(expected_name)

    def equals(self, other: Any) -> bool:
        direct_name = getattr(other, '__name__', None)
        if direct_name is not None and direct_name == self.expected_name:
            return True

        if self.allow_instances:
            cls = getattr(other, '__class__', None)
            if cls is not None:  # pragma: no branch
                cls_name = getattr(cls, '__name__', None)
                if cls_name is not None and cls_name == self.expected_name:
                    return True

        return False


class HasRepr(DirtyEquals[T]):
    """
    A type which checks that the value has the given `repr()` value.
    """

    def __init__(self, expected_repr: Union[IsStr, str]):
        """
        Args:
            expected_repr: The expected repr value.

        Example:
        ```py title="HasRepr"
        from dirty_equals import HasRepr, IsStr

        class Foo:
            def __repr__(self):
                return 'This is a Foo'

        assert Foo() == HasRepr('This is a Foo')
        assert Foo() == HasRepr['This is a Foo']
        assert Foo == HasRepr(IsStr(regex='<class.+'))
        assert 42 == HasRepr('42')
        assert 43 != HasRepr('42')
        ```
        """
        self.expected_repr = expected_repr
        super().__init__(expected_repr)

    def __class_getitem__(cls, expected_repr: str) -> 'HasRepr[T]':
        return cls(expected_repr)

    def equals(self, other: Any) -> bool:
        return repr(other) == self.expected_repr


class HasAttributes(DirtyEquals[Any]):
    """
    A type which checks that the value has the given attributes.

    This is a partial check - e.g. the attributes provided to check do not need to be exhaustive.
    """

    @overload
    def __init__(self, expected: dict[Any, Any]): ...

    @overload
    def __init__(self, **expected: Any): ...

    def __init__(self, *expected_args: dict[Any, Any], **expected_kwargs: Any):
        """
        Can be created from either keyword arguments or an existing dictionary (same as `dict()`).

        Example:
        ```py title="HasAttributes"
        from dirty_equals import AnyThing, HasAttributes, IsInt, IsStr

        class Foo:
            def __init__(self, a, b):
                self.a = a
                self.b = b

            def spam(self):
                pass

        assert Foo(1, 2) == HasAttributes(a=1, b=2)
        assert Foo(1, 2) == HasAttributes(a=1)
        assert Foo(1, 's') == HasAttributes(a=IsInt, b=IsStr)
        assert Foo(1, 2) != HasAttributes(a=IsInt, b=IsStr)
        assert Foo(1, 2) != HasAttributes(a=1, b=2, c=3)
        assert Foo(1, 2) == HasAttributes(a=1, b=2, spam=AnyThing)
        ```
        """
        self.expected_attrs = get_dict_arg('HasAttributes', expected_args, expected_kwargs)
        super().__init__(**self.expected_attrs)

    def equals(self, other: Any) -> bool:
        for attr, expected_value in self.expected_attrs.items():
            # done like this to avoid problems with `AnyThing` equaling `None` or `DefaultAttr`
            try:
                value = getattr(other, attr)
            except AttributeError:
                return False
            else:
                if value != expected_value:
                    return False
        return True
