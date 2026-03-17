from pathlib import Path
from typing import Union

from ..base import ParametrizedValue


class Logger(ParametrizedValue):

    args_joiner = ','

    def __init__(self, alias, *args):
        self.alias = alias or ''
        super().__init__(*args)


class LoggerFile(Logger):
    """Allows logging into files."""

    name = 'file'
    plugin = 'logfile'

    def __init__(self, filepath: Union[str, Path], *, alias=None):
        """
        :param str filepath: File path.

        :param str alias: Logger alias.

        """
        super().__init__(alias, str(filepath))


class LoggerFileDescriptor(Logger):
    """Allows logging using file descriptor."""

    name = 'fd'
    plugin = 'logfile'

    def __init__(self, fd: int, *, alias=None):
        """
        :param str fd: File descriptor.

        :param str alias: Logger alias.

        """
        super().__init__(alias, fd)


class LoggerStdIO(Logger):
    """Allows logging stdio."""

    name = 'stdio'
    plugin = 'logfile'

    def __init__(self, *, alias=None):
        """

        :param str alias: Logger alias.

        """
        super().__init__(alias)


class LoggerSocket(Logger):
    """Allows logging into UNIX and UDP sockets."""

    name = 'socket'
    plugin = 'logsocket'

    def __init__(self, addr_or_path: Union[str, Path], *, alias=None):
        """

        :param str addr_or_path: Remote address or filepath.

            Examples:
                * /tmp/uwsgi.logsock
                * 192.168.173.19:5050

        :param str alias: Logger alias.

        """
        super().__init__(alias, str(addr_or_path))


class LoggerSyslog(Logger):
    """Allows logging into Unix standard syslog."""

    name = 'syslog'
    plugin = 'syslog'

    def __init__(self, *, app_name=None, facility=None, alias=None):
        """

        :param str app_name:

        :param str facility:

            * https://en.wikipedia.org/wiki/Syslog#Facility

        :param str alias: Logger alias.

        """
        super().__init__(alias, app_name, facility)


class LoggerRsyslog(LoggerSyslog):
    """Allows logging into Unix standard syslog or a remote syslog."""

    name = 'rsyslog'
    plugin = 'rsyslog'

    def __init__(self, *, app_name=None, host=None, facility=None, split=None, packet_size=None, alias=None):
        """

        :param str app_name:

        :param str host: Address (host and port) or UNIX socket path.

        :param str facility:

            * https://en.wikipedia.org/wiki/Syslog#Facility

        :param bool split: Split big messages into multiple chunks if they are bigger
            than allowed packet size. Default: ``False``.

        :param int packet_size: Set maximum packet size for syslog messages. Default: 1024.

            .. warning:: using packets > 1024 breaks RFC 3164 (#4.1)

        :param str alias: Logger alias.

        """
        super().__init__(app_name=app_name, facility=facility, alias=alias)

        self.args.insert(0, host)

        self._set('rsyslog-packet-size', packet_size)
        self._set('rsyslog-split-messages', split, cast=bool)


class LoggerRedis(Logger):
    """Allows logging into Redis.

    .. note:: Consider using ``dedicate_thread`` param.

    """

    name = 'redislog'
    plugin = 'redislog'

    def __init__(self, *, host=None, command=None, prefix=None, alias=None):
        """

        :param str host: Default: 127.0.0.1:6379

        :param str command: Command to be used. Default: publish uwsgi

            Examples:
                * publish foobar
                * rpush foo

        :param str prefix: Default: <empty>

        :param str alias: Logger alias.

        """
        super().__init__(alias, host, command, prefix)


class LoggerMongo(Logger):
    """Allows logging into Mongo DB.

    .. note:: Consider using ``dedicate_thread`` param.

    """

    name = 'mongodblog'
    plugin = 'mongodblog'

    def __init__(self, *, host=None, collection=None, node=None, alias=None):
        """

        :param str host: Default: 127.0.0.1:27017

        :param str collection: Command to be used. Default: uwsgi.logs

        :param str node: An identification string for the instance
            sending logs Default: <server hostname>

        :param str alias: Logger alias.

        """
        super().__init__(alias, host, collection, node)


class LoggerZeroMq(Logger):
    """Allows logging into ZeroMQ sockets."""

    name = 'zeromq'
    plugin = 'logzmq'

    def __init__(self, connection_str, *, alias=None):
        """

        :param str connection_str:

            Examples:
                * tcp://192.168.173.18:9191

        :param str alias: Logger alias.

        """
        super().__init__(alias, connection_str)
