from . import logger_config

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

import functools
import logging
import os
import re


def set_log_level_from_config(logger):
    if not logger_config.LOG_LEVEL:
        logger.setLevel(level=logging.__dict__["INFO"])
    else:
        if logger_config.LOG_LEVEL.upper() == "NOTSET":
            raise Exception("LOG_LEVEL couldn't be defined as NOTSET")
        # Intentionally override trace with debug for compatibility with C++ logging library.
        if logger_config.LOG_LEVEL.upper() == "TRACE":
            logger_config.LOG_LEVEL = "DEBUG"
        logger.setLevel(level=logging.__dict__[logger_config.LOG_LEVEL.upper()])


class SimpleColorizedStreamHandler(logging.StreamHandler):
    C_LCYAN = "\033[96m"
    C_LBLUE = "\033[94m"
    C_LGREEN = "\033[92m"
    C_LYELLOW = "\033[93m"
    C_LGRAY = "\033[37m"
    C_BOLD = "\033[1m"
    C_END = "\033[0m"

    KW = C_LBLUE
    URL = C_LCYAN + C_BOLD
    PARAM = C_LGRAY
    YSON_PARAM = C_LYELLOW

    RE_KW = functools.partial(lambda p, r, m: p.sub(r, m), re.compile(r"(Perform HTTP \S+ request|Response received)"), r"{}\1{}".format(KW, C_END))
    RE_HTTP = functools.partial(lambda p, r, m: p.sub(r, m), re.compile(r"(https?://\S+)"), r"{}\1{}".format(URL, C_END))
    RE_JSON = functools.partial(lambda p, r, m: p.sub(r, m), re.compile(r"(['\"][\w-]+['\"]): ?"), r"{}\1{}: ".format(PARAM, C_END))
    RE_YSON = functools.partial(lambda p, r, m: p.sub(r, m), re.compile(r"\"([^\";]+?)\"="), "\"{}\\1{}\"=".format(YSON_PARAM, C_END))

    # to disable color output
    DISABLE_COLOR = os.environ.get("YT_LOG_LEVEL") == "Debug"

    terminator = '\n'  # py2 compat

    def _colorize(self, msg):
        msg = self.RE_KW(msg)
        msg = self.RE_HTTP(msg)
        msg = self.RE_JSON(msg)
        msg = self.RE_YSON(msg)
        return msg

    def emit(self, record):
        try:
            msg = self.format(record)
            stream = self.stream
            if stream.isatty() and record.levelno == logging.DEBUG and not self.DISABLE_COLOR:
                msg = self._colorize(msg)
            stream.write(msg + self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)


formatter = None


def set_formatter(new_formatter):
    global formatter
    formatter = new_formatter
    for handler in LOGGER.handlers:
        handler.setFormatter(new_formatter)


logging.getLogger("yt.packages.requests.packages.urllib3").setLevel(logging.WARNING)

LOGGER = logging.getLogger("Yt")

LOGGER.propagate = False

set_log_level_from_config(LOGGER)

if logger_config.LOG_PATH is None:
    LOGGER.addHandler(SimpleColorizedStreamHandler())
else:
    LOGGER.addHandler(logging.FileHandler(logger_config.LOG_PATH))

BASIC_FORMATTER = logging.Formatter(logger_config.LOG_PATTERN)

set_formatter(BASIC_FORMATTER)


def debug(msg, *args, **kwargs):
    LOGGER.debug(msg, *args, **kwargs)


def info(msg, *args, **kwargs):
    LOGGER.info(msg, *args, **kwargs)


def warning(msg, *args, **kwargs):
    LOGGER.warning(msg, *args, **kwargs)


def error(msg, *args, **kwargs):
    LOGGER.error(msg, *args, **kwargs)


def exception(msg, *args, **kwargs):
    LOGGER.exception(msg, *args, **kwargs)


if hasattr(functools, 'lru_cache') and not os.environ.get("YT_LOG_NO_TIP"):
    @functools.lru_cache(maxsize=128)
    def tip(msg):
        LOGGER.debug("[TIP] " + msg)
else:
    # py2
    def tip(msg):
        pass


def log(level, msg, *args, **kwargs):
    LOGGER.log(level, msg, *args, **kwargs)
