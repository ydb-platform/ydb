import logging
import re
import traceback
import uuid
from enum import IntEnum, unique

from ._journal import send, syslog_priorities


try:
    from collections.abc import Mapping
except ImportError:
    from collections import Mapping


_priorities = syslog_priorities()


__all__ = "write", "send", "Priority", "JournaldLogHandler", "Facility"


@unique
class Priority(IntEnum):
    PANIC = _priorities["panic"]
    WARNING = _priorities["warn"]
    ALERT = _priorities["alert"]
    NONE = _priorities["none"]
    CRITICAL = _priorities["crit"]
    DEBUG = _priorities["debug"]
    INFO = _priorities["info"]
    ERROR = _priorities["error"]
    NOTICE = _priorities["notice"]


@unique
class Facility(IntEnum):
    KERN = 0
    USER = 1
    MAIL = 2
    DAEMON = 3
    AUTH = 4
    SYSLOG = 5
    LPR = 6
    NEWS = 7
    UUCP = 8
    CLOCK_DAEMON = 9
    AUTHPRIV = 10
    FTP = 11
    NTP = 12
    AUDIT = 13
    ALERT = 14
    CRON = 15
    LOCAL0 = 16
    LOCAL1 = 17
    LOCAL2 = 18
    LOCAL3 = 19
    LOCAL4 = 20
    LOCAL5 = 21
    LOCAL6 = 22
    LOCAL7 = 23


def write(message, priority=Priority.INFO):
    """ Write message into systemd journal
    :type priority: Priority
    :type message: str
    """

    priority = int(Priority(int(priority)))

    send(priority=priority, message=message)


class JournaldLogHandler(logging.Handler):
    FIELD_BADCHAR_RE = re.compile(r'\W')
    LEVELS = {
        logging.CRITICAL: Priority.CRITICAL.value,
        logging.FATAL: Priority.PANIC.value,
        logging.ERROR: Priority.ERROR.value,
        logging.WARNING: Priority.WARNING.value,
        logging.WARN: Priority.WARNING.value,
        logging.INFO: Priority.INFO.value,
        logging.DEBUG: Priority.DEBUG.value,
        logging.NOTSET: Priority.NONE.value,
    }

    __slots__ = ("__facility",)

    def __init__(self, identifier=None, facility=Facility.DAEMON):
        """

        :type identifier: Override default journald identifier
        :type facility: Facility
        """
        logging.Handler.__init__(self)
        self.__identifier = identifier
        self.__facility = int(facility)

    @staticmethod
    def _to_microsecond(ts):
        """

        :type ts: float
        """
        return int(ts * 1000 * 1000)

    def emit(self, record):
        message = str(record.getMessage())

        tb_message = ""
        if record.exc_info:
            tb_message = "\n".join(
                traceback.format_exception(*record.exc_info)
            )

        message += "\n"
        message += tb_message

        ts = self._to_microsecond(record.created)

        hash_fields = (
            message,
            record.funcName,
            record.levelno,
            record.process,
            record.processName,
            record.levelname,
            record.pathname,
            record.name,
            record.thread,
            record.lineno,
            ts,
            tb_message,
        )

        message_id = uuid.uuid3(
            uuid.NAMESPACE_OID, "$".join(str(x) for x in hash_fields)
        ).hex

        data = {
            key: value
            for key, value in record.__dict__.items()
            if not key.startswith("_") and value is not None
        }
        data["message"] = self.format(record)
        data["priority"] = self.LEVELS[data.pop("levelno")]
        data["syslog_facility"] = self.__facility

        data["code_file"] = data.pop("filename")
        data["code_line"] = data.pop("lineno")
        data["code_func"] = data.pop("funcName")
        if self.__identifier:
            data["syslog_identifier"] = self.__identifier
        else:
            data["syslog_identifier"] = data["name"]

        if "msg" in data:
            data["message_raw"] = data.pop("msg")

        data["message_id"] = message_id
        data["code_module"] = data.pop("module")
        data["logger_name"] = data.pop("name")
        data["pid"] = data.pop("process")
        data["proccess_name"] = data.pop("processName")
        data["errno"] = 0 if not record.exc_info else 255
        data["relative_ts"] = self._to_microsecond(data.pop("relativeCreated"))
        data["thread_name"] = data.pop("threadName")

        args = data.pop("args", [])
        if isinstance(args, Mapping):
            for key, value in args.items():
                key = self.FIELD_BADCHAR_RE.sub('_', key)
                data["argument_%s" % key] = value
        else:
            for idx, item in enumerate(args):
                data["argument_%d" % idx] = str(item)

        if tb_message:
            data["traceback"] = tb_message

        send(**data)


handler = JournaldLogHandler()


class JournaldLogger(logging.Logger):
    def __init__(self, level, name="root"):
        super(JournaldLogger, self).__init__(name, level)
        self.addHandler(handler)


Logger = JournaldLogger(logging.WARNING)
