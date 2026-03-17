from .fields import *  # noqa: F403
from .fields import __all__ as fields_all
from .functions import *  # noqa: F403
from .functions import __all__ as functions_all
from .migrations import *  # noqa: F403
from .migrations import __all__ as migrations_all

__all__ = [
    "patch_all",
    *fields_all,
    *functions_all,
    *migrations_all,
]


def patch_all():
    patch_functions()  # noqa: F405
    patch_fields()  # noqa: F405
    patch_migrations()  # noqa: F405
