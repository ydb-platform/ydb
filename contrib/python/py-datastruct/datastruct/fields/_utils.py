#  Copyright (c) Kuba Szczodrzyński 2023-1-6.

import dataclasses
from dataclasses import MISSING, Field, is_dataclass
from typing import Any, Callable

from datastruct.types import FieldMeta, FieldType


def build_field(
    ftype: FieldType,
    default: Any = ...,
    default_factory: Any = MISSING,
    *,
    public: bool = True,
    **kwargs,
) -> Field:
    if isinstance(default, (list, dict, set)) or is_dataclass(default):
        raise ValueError(
            f"Mutable default {type(default)} is not allowed: use default_factory",
        )
    if default_factory is not MISSING:
        default = MISSING
    # noinspection PyArgumentList
    return dataclasses.field(
        init=public,
        repr=public,
        compare=public,
        default=default,
        default_factory=default_factory,
        metadata=dict(
            datastruct=FieldMeta(
                validated=False,
                public=public,
                ftype=ftype,
                **kwargs,
            ),
        ),
    )


def build_wrapper(
    ftype: FieldType,
    default: Any = None,
    default_factory: Any = None,
    **kwargs,
) -> Callable[[Field], Field]:
    def wrap(base: Field):
        return build_field(
            ftype=ftype,
            default=default or base.default,
            default_factory=default_factory or base.default_factory,
            public=base.init,
            # meta
            base=base,
            **kwargs,
        )

    return wrap
