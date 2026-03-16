# coding: utf-8

from datetime import datetime
import codecs
import os.path
from stat import ST_DEV, ST_INO
import logging
from logging.handlers import SysLogHandler as StdSysLogHandler, SYSLOG_UDP_PORT
import re
import syslog
import socket

import six

from ylog.format import (ExceptionFormatter, FILE_FORMAT, TIME_FORMAT,
                         SYSLOG_FORMAT)


# Портировано из Python 2.6
# Разница — не поддерживается параметр delay, делающий открытие
# потока записи «ленивым», добавлен self.encoding
class PortedWatchedFileHandler(logging.FileHandler):
    """
    A handler for logging to a file, which watches the file
    to see if it has changed while in use. This can happen because of
    usage of programs such as newsyslog and logrotate which perform
    log file rotation. This handler, intended for use under Unix,
    watches the file to see if it has changed since the last emit.
    (A file has changed if its device or inode have changed.)
    If it has changed, the old file stream is closed, and the file
    opened to get a new stream.

    This handler is not appropriate for use under Windows, because
    under Windows open files cannot be moved or renamed - logging
    opens the files with exclusive locks - and so there is no need
    for such a handler. Furthermore, ST_INO is not supported under
    Windows; stat always returns zero for this value.

    This handler is based on a suggestion and patch by Chad J.
    Schroeder.
    """
    def __init__(self, filename, mode='a', encoding=None):
        logging.FileHandler.__init__(self, filename, mode, encoding)
        if not hasattr(self, 'encoding'):
            setattr(self, 'encoding', encoding)
        if not os.path.exists(self.baseFilename):
            self.dev, self.ino = -1, -1
        else:
            stat = os.stat(self.baseFilename)
            self.dev, self.ino = stat[ST_DEV], stat[ST_INO]

    def emit(self, record):
        """
        Emit a record.

        First check if the underlying file has changed, and if it
        has, close the old stream and reopen the file to get the
        current stream.
        """
        if not os.path.exists(self.baseFilename):
            stat = None
            changed = 1
        else:
            stat = os.stat(self.baseFilename)
            changed = (stat[ST_DEV] != self.dev) or (stat[ST_INO] != self.ino)
        if changed and self.stream is not None:
            self.stream.flush()
            self.stream.close()
            self.stream = self._open()
            if stat is None:
                stat = os.stat(self.baseFilename)
            self.dev, self.ino = stat[ST_DEV], stat[ST_INO]
        logging.FileHandler.emit(self, record)

    def _open(self):
        """
        Open the current base file with the (original) mode and encoding.
        Return the resulting stream.
        """
        if self.encoding is None:
            stream = open(self.baseFilename, self.mode)
        else:
            stream = codecs.open(self.baseFilename, self.mode, self.encoding)
        return stream

try:
    from logging.handlers import WatchedFileHandler
except ImportError:
    WatchedFileHandler = PortedWatchedFileHandler

# Копи-паст из logging.py
# Отличие от logging.handlers.SysLogHandler в том, что этот класс использует стандартный модуль syslog для логгирования
# Таким образом, получаем поддержку ident.
class RealSysLogHandler(logging.Handler):
    """
    Real syslog handler. Uses standard syslog module
    """

    # from <linux/sys/syslog.h>:
    # ======================================================================
    # priorities/facilities are encoded into a single 32-bit quantity, where
    # the bottom 3 bits are the priority (0-7) and the top 28 bits are the
    # facility (0-big number). Both the priorities and the facilities map
    # roughly one-to-one to strings in the syslogd(8) source code.  This
    # mapping is included in this file.
    #
    # priorities (these are ordered)

    LOG_EMERG     = 0       #  system is unusable
    LOG_ALERT     = 1       #  action must be taken immediately
    LOG_CRIT      = 2       #  critical conditions
    LOG_ERR       = 3       #  error conditions
    LOG_WARNING   = 4       #  warning conditions
    LOG_NOTICE    = 5       #  normal but significant condition
    LOG_INFO      = 6       #  informational
    LOG_DEBUG     = 7       #  debug-level messages

    #  facility codes
    LOG_KERN      = 0       #  kernel messages
    LOG_USER      = 1       #  random user-level messages
    LOG_MAIL      = 2       #  mail system
    LOG_DAEMON    = 3       #  system daemons
    LOG_AUTH      = 4       #  security/authorization messages
    LOG_SYSLOG    = 5       #  messages generated internally by syslogd
    LOG_LPR       = 6       #  line printer subsystem
    LOG_NEWS      = 7       #  network news subsystem
    LOG_UUCP      = 8       #  UUCP subsystem
    LOG_CRON      = 9       #  clock daemon
    LOG_AUTHPRIV  = 10  #  security/authorization messages (private)

    #  other codes through 15 reserved for system use
    LOG_LOCAL0    = 16      #  reserved for local use
    LOG_LOCAL1    = 17      #  reserved for local use
    LOG_LOCAL2    = 18      #  reserved for local use
    LOG_LOCAL3    = 19      #  reserved for local use
    LOG_LOCAL4    = 20      #  reserved for local use
    LOG_LOCAL5    = 21      #  reserved for local use
    LOG_LOCAL6    = 22      #  reserved for local use
    LOG_LOCAL7    = 23      #  reserved for local use

    priority_names = {
        "alert":    LOG_ALERT,
        "crit":     LOG_CRIT,
        "critical": LOG_CRIT,
        "debug":    LOG_DEBUG,
        "emerg":    LOG_EMERG,
        "err":      LOG_ERR,
        "error":    LOG_ERR,        #  DEPRECATED
        "info":     LOG_INFO,
        "notice":   LOG_NOTICE,
        "panic":    LOG_EMERG,      #  DEPRECATED
        "warn":     LOG_WARNING,    #  DEPRECATED
        "warning":  LOG_WARNING,
        }

    facility_names = {
        "auth":     LOG_AUTH,
        "authpriv": LOG_AUTHPRIV,
        "cron":     LOG_CRON,
        "daemon":   LOG_DAEMON,
        "kern":     LOG_KERN,
        "lpr":      LOG_LPR,
        "mail":     LOG_MAIL,
        "news":     LOG_NEWS,
        "security": LOG_AUTH,       #  DEPRECATED
        "syslog":   LOG_SYSLOG,
        "user":     LOG_USER,
        "uucp":     LOG_UUCP,
        "local0":   LOG_LOCAL0,
        "local1":   LOG_LOCAL1,
        "local2":   LOG_LOCAL2,
        "local3":   LOG_LOCAL3,
        "local4":   LOG_LOCAL4,
        "local5":   LOG_LOCAL5,
        "local6":   LOG_LOCAL6,
        "local7":   LOG_LOCAL7,
        }

    #The map below appears to be trivially lowercasing the key. However,
    #there's more to it than meets the eye - in some locales, lowercasing
    #gives unexpected results. See SF #1524081: in the Turkish locale,
    #"INFO".lower() != "info"
    priority_map = {
        "DEBUG" : "debug",
        "INFO" : "info",
        "WARNING" : "warning",
        "ERROR" : "error",
        "CRITICAL" : "critical"
    }

    def __init__(self, facility=LOG_USER, ident = 'syslog'):
        """
        Initialize a handler.

        """
        logging.Handler.__init__(self)

        self.facility = facility
        self.ident = ident
        self.formatter = None
        syslog.openlog(self.ident)


    def encodePriority(self, facility, priority):
        """
        Encode the facility and priority. You can pass in strings or
        integers - if strings are passed, the facility_names and
        priority_names mapping dictionaries are used to convert them to
        integers.
        """
        if isinstance(facility, six.string_types):
            facility = self.facility_names[facility]
        if isinstance(priority, six.string_types):
            priority = self.priority_names[priority]
        return (facility << 3) | priority

    def close (self):
        """
        Closes the syslog.
        """
        syslog.closelog()
        logging.Handler.close(self)

    def mapPriority(self, levelName):
        """
        Map a logging level name to a key in the priority_names map.
        This is useful in two scenarios: when custom levels are being
        used, and in the case where you can't do a straightforward
        mapping by lowercasing the logging level name because of locale-
        specific issues (see SF #1524081).
        """
        return self.priority_map.get(levelName, "warning")

    def emit(self, record):
        """
        Emit a record.

        """
        msg = self.format(record)

        #syslog can't handle unicode messages
        if isinstance(msg, six.text_type):
            msg = msg.encode('utf-8')

        encoded_priority = self.encodePriority(self.facility,
                                               self.mapPriority(record.levelname))

        try:
            syslog.syslog(encoded_priority, msg)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


class OSFileHandler(logging.Handler):

    def __init__(self, filename, encoding='utf-8', mode=0o644, *args, **kwargs):
        logging.Handler.__init__(self, *args, **kwargs)
        self.fd = os.open(filename, os.O_WRONLY | os.O_APPEND | os.O_CREAT, mode)
        self.encoding = encoding

    def emit(self, record):
        msg = self.format(record)
        if isinstance(msg, six.text_type):
            msg = msg.encode(self.encoding)
        os.write(self.fd, b'%s\n' % msg)


LOG_PATH = '/var/log/yandex/%s/%%s.log'

class ExceptionHandler(logging.Handler):
    '''
    Common handler for exceptions in production. Used to unify essential error logging
    across all Python projects. Minimal log level is WARNING.

    Accepts a single parameter `projectname` that is used in file names.
    '''
    def __init__(self, projectname):
        logging.Handler.__init__(self)
        path = LOG_PATH % projectname
        exception_file = WatchedFileHandler(filename=path % 'exception')
        exception_file.setFormatter(ExceptionFormatter(FILE_FORMAT, TIME_FORMAT))
        traceback_file = WatchedFileHandler(filename=path % 'traceback')
        traceback_file.setFormatter(ExceptionFormatter(FILE_FORMAT, TIME_FORMAT, full=True))
        self.handlers = [
            exception_file,
            traceback_file,
        ]

    def emit(self, record):
        for handler in self.handlers:
            handler.emit(record)


# Для работы с ipv6 пришлось копипастить __init__, так как хорошо его не перегрузишь
class SysLogHandlerV6(StdSysLogHandler):
    def __init__(self, address=('localhost', SYSLOG_UDP_PORT),
                 facility=StdSysLogHandler.LOG_USER, socktype=None, sockfamily=socket.AF_INET):
        """
        Initialize a handler.

        If address is specified as a string, a UNIX socket is used. To log to a
        local syslogd, "SysLogHandler(address="/dev/log")" can be used.
        If facility is not specified, LOG_USER is used. If socktype is
        specified as socket.SOCK_DGRAM or socket.SOCK_STREAM, that specific
        socket type will be used. For Unix sockets, you can also specify a
        socktype of None, in which case socket.SOCK_DGRAM will be used, falling
        back to socket.SOCK_STREAM.
        """
        logging.Handler.__init__(self)

        self.address = address
        self.facility = facility
        self.socktype = socktype

        if isinstance(address, six.string_types):
            self.unixsocket = 1
            self._connect_unixsocket(address)
        else:
            self.unixsocket = 0
            if socktype is None:
                socktype = socket.SOCK_DGRAM
            # Изменения только в следущей строке. Используется добавленный параметр sockfamily
            self.socket = socket.socket(sockfamily, socktype)
            if socktype == socket.SOCK_STREAM:
                self.socket.connect(address)
            self.socktype = socktype
        self.formatter = None


class SysLogExceptionHandler(logging.Handler):
    '''
    Like ExceptionHandler, but uses syslog.

    Accepts a single parameter `projectname` and `address`. No further configuration needed.

    If parameter `address` is specified as a string, a UNIX socket is used. To log to a
    local syslog, address="/dev/log" can be used. By default used udp address=('localhost', logging.handlers.SYSLOG_UDP_PORT).
    '''
    def __init__(self, projectname, *args, **kwargs):
        logging.Handler.__init__(self)

        exception = SysLogHandler(projectname, 'exception', *args, **kwargs)
        exception.setFormatter(ExceptionFormatter(SYSLOG_FORMAT))

        traceback = SysLogHandler(projectname, 'traceback', *args, **kwargs)
        traceback.setFormatter(ExceptionFormatter(SYSLOG_FORMAT, full=True))

        self.handlers = [
            exception,
            traceback,
        ]

    def emit(self, record):
        for handler in self.handlers:
            handler.emit(record)


class SysLogHandler(SysLogHandlerV6):
    '''
    Handler for use with syslog.

    Produces messages in the form "yandex/projectname/prefix: message", which
    would be captured by filter specified in ylog's config file for
    syslog-ng-include script, and written to /var/log/yandex/projectname/prefix.log
    file (or sent over the network to the central log storage server).

    Accepts `projectname` and `prefix` parameters and parameters of
    logging.handlers.SysLogHandler. Don't use dots in `projectname` and
    `prefix` (use only a-zA-Z0-9_/- chars), otherwise your messages would not
    be captured by syslog filter.

    Option `escaping` need to escape characters. The default value is True,
    i.e. need to escape characters.
    '''
    syslog_filter_re = re.compile('^[a-zA-Z0-9_/-]*$')

    def __init__(self, projectname, prefix=None, escaping=True, *args, **kwargs):

        if self.syslog_filter_re.match(projectname) is None or (
            prefix is not None and \
            self.syslog_filter_re.match(prefix) is None):
            raise ValueError("Don't use dots in `projectname` and `prefix` "
                             "(use only a-zA-Z0-9_/- chars), otherwise your "
                             "messages would not be captured by syslog "
                             "filter.")

        super(SysLogHandler, self).__init__(*args, **kwargs)

        if prefix is None:
            self.__prefix = 'yandex/%s: ' % projectname
        else:
            self.__prefix = 'yandex/%s/%s: ' % (projectname, prefix)

        self.__escaping = escaping

    def format(self, record):
        msg = super(SysLogHandler, self).format(record)

        if isinstance(msg, six.text_type):
            msg = msg.encode('utf-8')

        if self.__escaping:
            msg = msg.encode('string_escape')

        # trim message to 65536 bytes to amend 'Too long message' error
        # (UDP header length field is 2-bytes field)
        LIMIT = 2 ** 16 - 128
        if len(msg) > LIMIT:
            msg = '%s... message trimmed (was %d bytes long)!' % (
                msg[:LIMIT], len(msg)
            )

        return self.__prefix + msg


class ToolsLogstashHandler(SysLogHandler):

    def __init__(self, projectname, prefix=None, escaping=True, tz='UTC', *args, **kwargs):
        import pytz
        super(ToolsLogstashHandler, self).__init__(projectname, prefix=prefix, escaping=escaping, *args, **kwargs)
        self._UTC_TZ = pytz.timezone('UTC')
        self._tz = pytz.timezone(tz)
        self._host = socket.getfqdn()

    def format(self, record):
        iso_datetime = self._UTC_TZ.localize(datetime.utcnow()).astimezone(self._tz).isoformat()
        ylog_msg = super(ToolsLogstashHandler, self).format(record)
        priority = self.encodePriority(self.LOG_KERN, self.mapPriority(record.levelname))

        return '<%s>%s %s %s' % (priority, iso_datetime, self._host, ylog_msg)
