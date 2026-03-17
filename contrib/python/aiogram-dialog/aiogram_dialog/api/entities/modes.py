from enum import Enum


class ShowMode(Enum):
    """
    Modes of show dialog message when new update handled.

    **AUTO**:
        default show mode.

        Uses `SEND mode` when new message from user handled or `EDIT mode`
        when any other updated handled.

    **EDIT**:
        edit dialog message

    **SEND**:
        send new dialog message

    **DELETE_AND_SEND**:
        delete and send new dialog message

        `Attention`: Telegram's restrictions will prevent the deletion of the
        message when more than 2 days has elapsed.

    **NO_UPDATE**:
        will not update and rerender the dialog message
    """

    AUTO = "auto"
    EDIT = "edit"
    SEND = "send"
    DELETE_AND_SEND = "delete_and_send"
    NO_UPDATE = "no_update"


class StartMode(Enum):
    """
    Modes of starting a new dialog.

    **NORMAL**:
        default start mode.

        This mode continues from the current state without resetting or
        creating a new stack.

    **RESET_STACK**:
        reset the current stack.

        This mode clears the existing stack and starts fresh. It is used when
        the existing stack needs to be discarded and a new operation stack
        is required.

    **NEW_STACK**:
        start with a new stack.

        This mode initiates a new stack while retaining the old one, useful
        when a new sequence of operations is to be started alongside the
        current one.
    """

    NORMAL = "NORMAL"
    RESET_STACK = "RESET_STACK"
    NEW_STACK = "NEW_STACK"
