import logging
from collections import namedtuple
from typing import Union
from enum import Enum, unique

from ._daemon import sd_notify


log = logging.getLogger("cysystemd.daemon")
NotificationValue = namedtuple(
    "NotificationValue", ("name", "constant", "type")
)


@unique
class Notification(Enum):
    READY = NotificationValue(name="READY", constant=1, type=int)
    RELOADING = NotificationValue(name="RELOADING", constant=1, type=int)
    STOPPING = NotificationValue(name="STOPPING", constant=1, type=int)
    STATUS = NotificationValue(name="STATUS", constant=None, type=str)
    ERRNO = NotificationValue(name="ERRNO", constant=None, type=int)
    BUSERROR = NotificationValue(name="BUSERROR", constant=None, type=str)
    MAINPID = NotificationValue(name="MAINPID", constant=None, type=int)
    WATCHDOG = NotificationValue(name="WATCHDOG", constant=1, type=int)
    FDSTORE = NotificationValue(name="FDSTORE", constant=1, type=int)
    FDNAME = NotificationValue(name="FDNAME", constant=None, type=int)
    WATCHDOG_USEC = NotificationValue(
        name="WATCHDOG_USEC", constant=None, type=int
    )


def notify(
    notification: Notification,
    value: Union[str, int] = None,
    unset_environment: bool = False,
    return_exceptions: bool = True,
):

    """ Send notification to systemd daemon

    :param return_exceptions: Return exception or raise it.
    :param unset_environment: If the unset_environment parameter is non-zero,
        notify() will unset the $NOTIFY_SOCKET environment variable
        before returning (regardless of whether the function call itself
        succeeded or not). Further calls to notify() will then fail,
        but the variable is no longer inherited by child processes.
    :param notification: Notification object
    :param value: str or int value for non constant notifications
    :returns None
    """

    if not isinstance(notification, Notification):
        raise TypeError("state must be an instance of Notification")

    state = notification.value

    if state.constant is not None and value:
        raise ValueError(
            "State %s should contain only constant value %r"
            % (state.name, state.constant),
            state.name,
            state.constant,
        )

    line = "%s=%s" % (
        state.name,
        state.constant if state.constant is not None else state.type(value),
    )

    log.debug("Send %r into systemd", line)

    try:
        return sd_notify(line, unset_environment)
    except Exception as e:
        if return_exceptions:
            log.error("%s", e)
            return e
        raise


__all__ = ("notify", "Notification")
