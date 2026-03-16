from typing import Any

from typing_extensions import Never, Self

from returns.primitives.exceptions import ImmutableStateError


class Immutable:
    """
    Helper type for objects that should be immutable.

    When applied, each instance becomes immutable.
    Nothing can be added or deleted from it.

    .. code:: pycon
      :force:

      >>> from returns.primitives.types import Immutable
      >>> class MyModel(Immutable):
      ...     ...

      >>> model = MyModel()
      >>> model.prop = 1
      Traceback (most recent call last):
         ...
      returns.primitives.exceptions.ImmutableStateError

    See :class:`returns.primitives.container.BaseContainer` for examples.

    """

    __slots__ = ()

    def __copy__(self) -> Self:
        """Returns itself."""
        return self

    def __deepcopy__(self, memo: dict[Any, Any]) -> Self:
        """Returns itself."""
        return self

    def __setattr__(self, attr_name: str, attr_value: Any) -> Never:
        """Makes inner state of the containers immutable for modification."""
        raise ImmutableStateError

    def __delattr__(self, attr_name: str) -> Never:  # noqa: WPS603
        """Makes inner state of the containers immutable for deletion."""
        raise ImmutableStateError
