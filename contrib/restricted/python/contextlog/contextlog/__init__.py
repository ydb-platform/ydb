# pylint: disable=protected-access


import sys
import os
import io
import contextlib
import traceback
import logging
import string
import pprint
import threading

import colorlog


# =====
def get_logger(name=None, depth=1, **context):
    if name is None:
        name = _get_stack_frames()[depth].f_globals["__name__"]
    context = _bind_context(depth + 1, context)
    logger = _ContextLogger(logging.getLogger(name), context=context)
    return logger


def patch_logging():
    """
        This hack is used to log the context inside standard and thirdparty libraries which
        uses usually python logging. The context inherits from caller using contextlog.
    """
    if logging.getLoggerClass() != _SlaveContextLogger:
        logging.setLoggerClass(_SlaveContextLogger)


def patch_logging_root():
    def critical(msg, *args, **kwargs):
        if len(logging.root.handlers) == 0:  # pylint: disable=len-as-condition
            logging.basicConfig()
        get_logger().critical(msg, *args, **kwargs)

    logging.fatal = logging.critical = critical

    def error(msg, *args, **kwargs):
        if len(logging.root.handlers) == 0:  # pylint: disable=len-as-condition
            logging.basicConfig()
        get_logger().error(msg, *args, **kwargs)

    logging.error = error

    def exception(msg, *args, exc_info=True, **kwargs):
        get_logger().error(msg, *args, exc_info=exc_info, **kwargs)

    logging.exception = exception

    def warning(msg, *args, **kwargs):
        if len(logging.root.handlers) == 0:  # pylint: disable=len-as-condition
            logging.basicConfig()
        get_logger().warning(msg, *args, **kwargs)

    logging.warning = logging.warn = warning

    def info(msg, *args, **kwargs):
        if len(logging.root.handlers) == 0:  # pylint: disable=len-as-condition
            logging.basicConfig()
        get_logger().info(msg, *args, **kwargs)

    logging.info = info

    def debug(msg, *args, **kwargs):
        if len(logging.root.handlers) == 0:  # pylint: disable=len-as-condition
            logging.basicConfig()
        get_logger().debug(msg, *args, **kwargs)

    logging.debug = debug

    def log(level, msg, *args, **kwargs):
        if len(logging.root.handlers) == 0:  # pylint: disable=len-as-condition
            logging.basicConfig()
        get_logger().log(level, msg, *args, **kwargs)

    logging.log = log


class _SlaveContextLogger(logging.Logger):
    def _log(self, level, msg, args, exc_info=None, extra=None, stack_info=False, stacklevel=1):  # pylint: disable=arguments-differ
        if not isinstance(extra, _ContextDict):
            # This condition is satisfied only when "extra" is not a context from _ContextLog._log().
            # If not a context - get context from a caller and merge with "extra".
            extra = _merge_contexts(_get_context(), (extra or {}))
        if sys.version_info.minor < 8:
            super()._log(level, msg, args, exc_info, extra, stack_info)
        else:
            super()._log(level, msg, args, exc_info, extra, stack_info, stacklevel)  # pylint: disable=too-many-function-args,arguments-differ

    def findCaller(self, stack_info=False, stacklevel=1):  # pylint: disable=unused-argument,arguments-differ
        # XXX: Simplified copypaste from logging/__init__.py
        # Removed py2exe and IronPython features
        frame = sys._getframe()
        while hasattr(frame, "f_code"):
            code = frame.f_code
            if os.path.normcase(code.co_filename) in (os.path.normcase(__file__), logging._srcfile):
                frame = frame.f_back
                continue
            sinfo = None
            if stack_info:
                with contextlib.closing(io.StringIO()) as sio:
                    sio.write("Stack (most recent call last):\n")  # pylint: disable=no-member
                    traceback.print_stack(frame, file=sio)
                    sinfo = sio.getvalue().strip()  # pylint: disable=no-member
            return (code.co_filename, frame.f_lineno, code.co_name, sinfo)
        return ("(unknown file)", 0, "(unknown function)", None)


def patch_threading():
    """
        Running threads have a different stack, so the context will be missing if you do
        not pass it explicitly. This hack allows you to inherit the context from the method
        that started a thread. This is useful for libraries that do not use contextlog.
    """
    if threading.Thread.start is not _thread_start:
        # We are change the methods, not a class, because some other classes can inherit it BEFORE patching
        threading.Thread.start = _thread_start
        threading.Thread._bootstrap = _thread_bootstrap


_orig_thread_start = threading.Thread.start


def _thread_start(self):
    self.__context = _get_context()  # Save context from parent thread
    _orig_thread_start(self)


_orig_thread_bootstrap = threading.Thread._bootstrap


def _thread_bootstrap(self):
    _bind_context(1, self.__context)  # Apply context inside a new thread
    _orig_thread_bootstrap(self)


# =====
class _ContextLogger(logging.Logger):
    def __init__(self, logger, context):
        super().__init__(logger.name)
        self._logger = logger
        self._context = context
        self.level = logger.level
        self.parent = logger.parent

    def get_context(self):
        return self._context

    def get_logger(self, **context):
        return _ContextLogger(self._logger, _merge_contexts(self._context, context))

    def _log(self, level, msg, args, exc_info=None, stack_info=False, **context):  # pylint: disable=arguments-differ
        context = _ContextDict(_merge_contexts(self._context, context))
        context["_extra"] = _PrettyDict(context)
        self._logger._log(level, msg, args, exc_info, context, stack_info)


class _ContextDict(dict):
    pass  # This type is needed to distinguish context from ordinary field "extra"


def _get_context():
    # Finds variable conteins context inside the stack
    for frame in _get_stack_frames()[1:]:
        if "__logger_context" in frame.f_locals:
            return frame.f_locals["__logger_context"].copy()
    return {}


def _bind_context(depth, context):
    # Creates a variable with the context in a certain depth of the stack
    context = _merge_contexts(_get_context(), context)
    _get_stack_frames()[depth].f_locals["__logger_context"] = context
    return context


def _merge_contexts(left, right):
    context = left.copy()
    context.update(right)
    return context


def _get_stack_frames():
    # Get a list of records for a frame and all higher (calling) frames.
    # XXX: We do not use inspect.stack(), because getframeinfo(), which he uses takes too much time under PyPy.
    frame = sys._getframe(1)
    frames = []
    while frame is not None:
        frames.append(frame)
        frame = frame.f_back
    return frames


class _PrettyDict(dict):
    # This type needs to log "_extra" field.
    def __format__(self, _):
        return " ".join(
            "{}={}".format(key, repr(value))
            for (key, value) in self.items()
        )


# =====
class SmartFormatter(colorlog.ColoredFormatter):
    def __init__(self, *args, **kwargs):
        self._max_vars_lines = kwargs.pop("max_vars_lines", 100)
        self._max_line_len = kwargs.pop("max_line_len", 100)
        super().__init__(*args, **kwargs)

    def formatMessage(self, record):
        return _PartialStringFormatter().format(self._style._fmt, **vars(record))

    def formatException(self, exc_info):  # pylint: disable=arguments-differ
        vars_lines = pprint.pformat(self._get_locals(exc_info)).split("\n")

        if len(vars_lines) > self._max_vars_lines:
            vars_lines = vars_lines[:self._max_vars_lines]
            vars_lines.append("...")

        for (count, line) in enumerate(vars_lines):
            # strip lines
            if len(line) > self._max_line_len:
                vars_lines[count] = line[:self._max_line_len - 3] + "..."

        output = "\n".join([
            super().formatException(exc_info),
            "\nLocals at innermost frame:\n",
        ] + vars_lines)
        return output

    def _get_locals(self, exc_info):
        tb = exc_info[2]  # This is the outermost frame of the traceback
        while tb.tb_next is not None:
            tb = tb.tb_next  # Zoom to the innermost frame
        return tb.tb_frame.f_locals


class _PartialStringFormatter(string.Formatter):
    def get_field(self, field_name, args, kwargs):
        try:
            val = super().get_field(field_name, args, kwargs)
        except (KeyError, AttributeError):
            val = ("", field_name)
        if "_extra" in kwargs:
            # In "_extra" remain only those fields that have not been explicitly requested by the formatter
            kwargs["_extra"].pop(field_name.split(".")[0], None)
        return val
