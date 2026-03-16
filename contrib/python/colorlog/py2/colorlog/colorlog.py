"""The ColoredFormatter class."""

from __future__ import absolute_import

import logging
import sys

from colorlog.escape_codes import escape_codes, parse_colors

__all__ = (
    "escape_codes",
    "default_log_colors",
    "ColoredFormatter",
    "LevelFormatter",
    "TTYColoredFormatter",
)

# The default colors to use for the debug levels
default_log_colors = {
    "DEBUG": "white",
    "INFO": "green",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "bold_red",
}

# The default format to use for each style
default_formats = {
    "%": "%(log_color)s%(levelname)s:%(name)s:%(message)s",
    "{": "{log_color}{levelname}:{name}:{message}",
    "$": "${log_color}${levelname}:${name}:${message}",
}


class ColoredRecord(object):
    """
    Wraps a LogRecord, adding named escape codes to the internal dict.

    The internal dict is used when formatting the message (by the PercentStyle,
    StrFormatStyle, and StringTemplateStyle classes).
    """

    def __init__(self, record):
        """Add attributes from the escape_codes dict and the record."""
        self.__dict__.update(escape_codes)
        self.__dict__.update(record.__dict__)

        # Keep a reference to the original record so ``__getattr__`` can
        # access functions that are not in ``__dict__``
        self.__record = record

    def __getattr__(self, name):
        return getattr(self.__record, name)


class ColoredFormatter(logging.Formatter):
    """
    A formatter that allows colors to be placed in the format string.

    Intended to help in creating more readable logging output.
    """

    def __init__(
        self,
        fmt=None,
        datefmt=None,
        style="%",
        log_colors=None,
        reset=True,
        secondary_log_colors=None,
    ):
        """
        Set the format and colors the ColoredFormatter will use.

        The ``fmt``, ``datefmt`` and ``style`` args are passed on to the
        ``logging.Formatter`` constructor.

        The ``secondary_log_colors`` argument can be used to create additional
        ``log_color`` attributes. Each key in the dictionary will set
        ``{key}_log_color``, using the value to select from a different
        ``log_colors`` set.

        :Parameters:
        - fmt (str): The format string to use
        - datefmt (str): A format string for the date
        - log_colors (dict):
            A mapping of log level names to color names
        - reset (bool):
            Implicitly append a color reset to all records unless False
        - style ('%' or '{' or '$'):
            The format style to use. (*No meaning prior to Python 3.2.*)
        - secondary_log_colors (dict):
            Map secondary ``log_color`` attributes. (*New in version 2.6.*)
        """
        if fmt is None:
            if sys.version_info > (3, 2):
                fmt = default_formats[style]
            else:
                fmt = default_formats["%"]

        if (
            sys.version_info > (3, 8)
            and isinstance(self, LevelFormatter)
            and isinstance(fmt, dict)
        ):
            super(ColoredFormatter, self).__init__(fmt, datefmt, style, validate=False)
        elif sys.version_info > (3, 2):
            super(ColoredFormatter, self).__init__(fmt, datefmt, style)
        elif sys.version_info > (2, 7):
            super(ColoredFormatter, self).__init__(fmt, datefmt)
        else:
            logging.Formatter.__init__(self, fmt, datefmt)

        self.log_colors = log_colors if log_colors is not None else default_log_colors
        self.secondary_log_colors = secondary_log_colors
        self.reset = reset

    def color(self, log_colors, level_name):
        """Return escape codes from a ``log_colors`` dict."""
        return parse_colors(log_colors.get(level_name, ""))

    def format(self, record):
        """Format a message from a record object."""
        record = ColoredRecord(record)
        record.log_color = self.color(self.log_colors, record.levelname)

        # Set secondary log colors
        if self.secondary_log_colors:
            for name, log_colors in self.secondary_log_colors.items():
                color = self.color(log_colors, record.levelname)
                setattr(record, name + "_log_color", color)

        # Format the message
        if sys.version_info > (2, 7):
            message = super(ColoredFormatter, self).format(record)
        else:
            message = logging.Formatter.format(self, record)

        # Add a reset code to the end of the message
        # (if it wasn't explicitly added in format str)
        if self.reset and not message.endswith(escape_codes["reset"]):
            message += escape_codes["reset"]

        return message


class LevelFormatter(ColoredFormatter):
    """An extension of ColoredFormatter that uses per-level format strings."""

    def __init__(
        self,
        fmt=None,
        datefmt=None,
        style="%",
        log_colors=None,
        reset=True,
        secondary_log_colors=None,
    ):
        """
        Set the per-loglevel format that will be used.

        Supports fmt as a dict. All other args are passed on to the
        ``colorlog.ColoredFormatter`` constructor.

        :Parameters:
        - fmt (dict):
            A mapping of log levels (represented as strings, e.g. 'WARNING') to
            different formatters. (*New in version 2.7.0)
        (All other parameters are the same as in colorlog.ColoredFormatter)

        Example:

        formatter = colorlog.LevelFormatter(fmt={
            'DEBUG':'%(log_color)s%(msg)s (%(module)s:%(lineno)d)',
            'INFO': '%(log_color)s%(msg)s',
            'WARNING': '%(log_color)sWARN: %(msg)s (%(module)s:%(lineno)d)',
            'ERROR': '%(log_color)sERROR: %(msg)s (%(module)s:%(lineno)d)',
            'CRITICAL': '%(log_color)sCRIT: %(msg)s (%(module)s:%(lineno)d)',
        })
        """
        if sys.version_info > (2, 7):
            super(LevelFormatter, self).__init__(
                fmt=fmt,
                datefmt=datefmt,
                style=style,
                log_colors=log_colors,
                reset=reset,
                secondary_log_colors=secondary_log_colors,
            )
        else:
            ColoredFormatter.__init__(
                self,
                fmt=fmt,
                datefmt=datefmt,
                style=style,
                log_colors=log_colors,
                reset=reset,
                secondary_log_colors=secondary_log_colors,
            )
        self.style = style
        self.fmt = fmt

    def format(self, record):
        """Customize the message format based on the log level."""
        if isinstance(self.fmt, dict):
            self._fmt = self.fmt[record.levelname]
            if sys.version_info > (3, 2):
                # Update self._style because we've changed self._fmt
                # (code based on stdlib's logging.Formatter.__init__())
                if self.style not in logging._STYLES:
                    raise ValueError(
                        "Style must be one of: %s" % ",".join(logging._STYLES.keys())
                    )
                self._style = logging._STYLES[self.style][0](self._fmt)

        if sys.version_info > (2, 7):
            message = super(LevelFormatter, self).format(record)
        else:
            message = ColoredFormatter.format(self, record)

        return message


class TTYColoredFormatter(ColoredFormatter):
    """
    Blanks all color codes if not running under a TTY.

    This is useful when you want to be able to pipe colorlog output to a file.
    """

    def __init__(self, *args, **kwargs):
        """Overwrite the `reset` argument to False if stream is not a TTY."""
        self.stream = kwargs.pop("stream")

        # Both `reset` and `isatty` must be true to insert reset codes.
        kwargs["reset"] = kwargs.get("reset", True) and self.stream.isatty()

        ColoredFormatter.__init__(self, *args, **kwargs)

    def color(self, log_colors, level_name):
        """Only returns colors if STDOUT is a TTY."""
        if not self.stream.isatty():
            log_colors = {}
        return ColoredFormatter.color(self, log_colors, level_name)
