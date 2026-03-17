from adaptix._internal.retort.base_retort import BaseRetort
from adaptix._internal.retort.operating_retort import OperatingRetort
from adaptix._internal.retort.searching_retort import ProviderNotFoundError
from adaptix._internal.utils import create_deprecated_alias_getter

__all__ = (
    "BaseRetort",
    "OperatingRetort",
    "ProviderNotFoundError",
)

__getattr__ = create_deprecated_alias_getter(
    __name__,
    {
        "NoSuitableProvider": "ProviderNotFoundError",
    },
)
