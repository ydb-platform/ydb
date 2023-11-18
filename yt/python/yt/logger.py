from . import logger_config

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

import logging


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


logging.getLogger("yt.packages.requests.packages.urllib3").setLevel(logging.WARNING)

LOGGER = logging.getLogger("Yt")

LOGGER.propagate = False

set_log_level_from_config(LOGGER)

if logger_config.LOG_PATH is None:
    LOGGER.addHandler(logging.StreamHandler())
else:
    LOGGER.addHandler(logging.FileHandler(logger_config.LOG_PATH))

BASIC_FORMATTER = logging.Formatter(logger_config.LOG_PATTERN)

formatter = None


def set_formatter(new_formatter):
    global formatter
    formatter = new_formatter
    for handler in LOGGER.handlers:
        handler.setFormatter(new_formatter)


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


def log(level, msg, *args, **kwargs):
    LOGGER.log(level, msg, *args, **kwargs)
