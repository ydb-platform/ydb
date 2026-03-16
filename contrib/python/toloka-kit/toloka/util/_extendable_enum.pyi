__all__ = [
    'extend_enum',
    'ExtendableStrEnumMetaclass',
    'ExtendableStrEnum',
]
import enum


def extend_enum(
    enumeration: enum.Enum,
    name: str,
    value: str
): ...


class ExtendableStrEnumMetaclass(enum.EnumMeta):
    def __getattr__(cls, name): ...

    def __getitem__(cls, name): ...

    def __call__(
        cls,
        value,
        names=None,
        *,
        module=None,
        qualname=None,
        type=None,
        start=1
    ): ...


class ExtendableStrEnum(enum.Enum):
    """An enumeration.
    """

    ...
