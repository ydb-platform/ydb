from enum import Enum


class LaunchMode(Enum):
    """

    Modes of launching new dialog.

    **ROOT**:
        dialogs will be always a root dialog in stack.

        Starting such dialogs will automatically reset stack.

        Example: main menu

    **EXCLUSIVE**:
        dialogs can be only a single dialog in stack.

        Starting such dialogs will automatically reset stack.
        Starting other dialogs on top of them is forbidden

        Example: banners

    **SINGLE_TOP**:
        dialogs will not be repeated on top of stack.

        Starting the same dialog right on top of it will just replace it.

        Example: product page

    **STANDARD**:
        dialogs have no limitations themselves
    """

    STANDARD = "standard"
    ROOT = "root"
    EXCLUSIVE = "exclusive"
    SINGLE_TOP = "single_top"
