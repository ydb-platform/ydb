from ..base import ParametrizedValue, TemplatedValue


class Encoder(ParametrizedValue):

    name_separator = ' '


class EncoderPrefix(Encoder):
    """Add a raw prefix to each log msg."""

    name = 'prefix'

    def __init__(self, value):
        """

        :param str value: Value to be used as affix

        """
        super().__init__(value)


class EncoderSuffix(EncoderPrefix):
    """Add a raw suffix to each log msg"""

    name = 'suffix'


class EncoderNewline(Encoder):
    """Add a newline char to each log msg."""

    name = 'nl'


class EncoderGzip(Encoder):
    """Compress each msg with gzip (requires zlib)."""

    name = 'gzip'


class EncoderCompress(Encoder):
    """Compress each msg with zlib compress (requires zlib)."""

    name = 'compress'


class TimeFormatter(TemplatedValue):
    """Allows user-defined time value formatting."""

    tpl = '${strftime:%s}'

    def __init__(self, fmt: str):
        """
        :param fmt: Time value format Format string (as for `strftime`)

            Aliases:
                * iso - ISO 8601: %Y-%m-%dT%H:%M:%S%z
                    2020-11-29T04:44:08+0000

        """
        if fmt == 'iso':
            fmt = '%Y-%m-%dT%H:%M:%S%z'

        super().__init__(fmt.replace('%', '%%'))


class EncoderFormat(Encoder):
    """Apply the specified format to each log msg."""

    name = 'format'

    def __init__(self, template):
        """

        :param str template: Template string.
            Available variables are listed in ``FormatEncoder.Vars``.

        """
        super().__init__(template)

    class vars:
        """Variables available to use."""

        MESSAGE = '${msg}'
        """Raw log message (newline stripped)."""

        MESSAGE_NEWLINE = '${msgnl}'
        """Raw log message (with newline)."""

        TIME = '${unix}'
        """Current unix time."""

        TIME_US = '${micros}'
        """Current unix time in microseconds."""

        TIME_MS = '${millis}'
        """Current unix time in milliseconds."""

        TIME_FORMAT = TimeFormatter
        """Current time in user-defined format."""


class EncoderJson(EncoderFormat):
    """Apply the specified format to each log msg with each variable json escaped."""

    name = 'json'
