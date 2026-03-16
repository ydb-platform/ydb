# coding:utf-8

import json
import logging
import traceback
import os
import six

from ylog.context import get_log_context

POST_TRUNCATE_SIZE = 1024
IS_DEPLOY = 'DEPLOY_BOX_ID' in os.environ
CONTEXT_FIELD_WITH_REQUEST_ID = None
CONTEXT_FIELD_WITH_SUBREQUEST = None
CONTEXT_FIELD_WITH_RTLOG = None
CONTEXT_FIELD_WITH_EXPERIMENTS = None
CONTEXT_FIELD_WITH_HEADERS = None

if six.PY2:
    def smart_str(s):
        if not isinstance(s, six.string_types):
            return str(s)
        elif isinstance(s, six.text_type):
            return s.encode('utf-8', 'replace')
        else:
            return s
else:
    def smart_str(s):
        if isinstance(s, six.string_types):
            return s
        if isinstance(s, six.binary_type):
            return s.decode('utf-8', 'replace')
        return str(s)


def exception_str(value):
    '''
    Formats Exception object into a string. Unlike default str():

    - can handle unicode strings in exception arguments
    - tries to format arguments as str(), not as repr()
    '''
    try:
        return ', '.join([smart_str(b) for b in value])
    except (TypeError, AttributeError): # happens for non-iterable values
        try:
            return smart_str(value)
        except UnicodeEncodeError:
            try:
                return repr(value)
            except Exception:
                return '<Unprintable value>'

def format_post(request):
    '''
    Formats request post data into a string. Depending on content type it's
    either a dict or raw post data.
    '''
    if request.method == 'POST' and (request.META.get('CONTENT_TYPE', '').split(';')[0] not in
                                     ['application/x-www-form-urlencoded', 'multipart/form-data']):
        value = request.read()
        if len(value) > POST_TRUNCATE_SIZE:
            value = value[:POST_TRUNCATE_SIZE] + b'...'
        return value
    else:
        return str(request.POST)


def format_frame_locals(frame, max_length=512):
    ret = ['    Locals:\n']
    for key, value in six.iteritems(frame.f_locals):
        r = repr(value)
        prefix = "      %s: " % key
        if len(r) > max_length:
            infix = '...'
            suffix = ' // %d bytes' % len(r)
            l = 80 - len(prefix) - len(infix) - len(suffix)
            r = r[:(l+1)//2] + infix + r[-(l//2):] + suffix
        ret.append("%s%s\n" % (prefix, r))
    return ret

def format_locals(tb):
    ret = []
    while tb != None:
        ret.append(format_frame_locals(tb.tb_frame))
        tb = tb.tb_next
    return ret

def format_exception_with_locals(exc_type, exc_value, tb):
    try:
        exception = traceback.format_exception_only(exc_type, exc_value)
        locations = traceback.format_tb(tb)
        local_vars = format_locals(tb)
        formatted = ['Traceback (most recent call last):\n']
        for i, j in zip(locations, local_vars):
            formatted.append(i)
            formatted.extend(j)
        formatted.extend(exception)
        return formatted
    except:
        formatted = (["Couldn't print detailed traceback.\n"] +
                     traceback.format_exception(exc_type, exc_value, tb))
        return formatted


# Common usable formats for file-based logs. Used in ylog.handlers.ExceptionHandlers.
FILE_FORMAT = '%(asctime)s %(name)-15s %(levelname)-10s %(message)s'
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
# Remains here for compatibility
SYSLOG_FORMAT = '%(asctime)s %(name)-15s %(levelname)-10s %(message)s'

class FileFormatter(logging.Formatter):
    '''
    Specialiazed variant of a standard logging.Formatter using FILE_FORMAT and
    TIME_FORMAT by default. The main purpose of this is to make shorter dictConfig
    declarations.
    '''
    def __init__(self, *args, **kwargs):
        logging.Formatter.__init__(self, fmt=FILE_FORMAT, datefmt=TIME_FORMAT)


class ExceptionFormatter(logging.Formatter):
    '''
    Formats exception log events into a short useful one-line message containing
    the exception class, module name, line number and the exception value. Exception
    info triple should be passed as a record parameter record.exc_info.

    When used with Django the parameter `full=True` adds extended data to the log message:
    request path, GET and POST representations and full traceback. The original log
    message is dropped (it's usually just the string "Internal server error").

    @param show_locals: if True, then every traceback frame would contain information
                        about local variables
    @param value_length_limit: the length, to which the variable value would be
                               stripped
    @param wrap_width: width for textwrap.wrap
    '''
    def __init__(self, fmt=FILE_FORMAT, datefmt=None, full=False,
                 show_locals=False):
        self.full = full
        self.show_locals = show_locals
        logging.Formatter.__init__(self, fmt, datefmt)

    def format(self, record):

        # construct required fields in record.__dict__
        record.message = record.getMessage()
        if self._fmt.find("%(asctime)") >= 0:
            record.asctime = self.formatTime(record, self.datefmt)

        lines = [self._fmt % record.__dict__]

        if self.full:
            if hasattr(record, 'request'):
                lines.append('Path: %s' % record.request.path)
                lines.append('GET: %s' % record.request.GET)
                lines.append('POST: %s' % format_post(record.request))
            if record.exc_info is not None:
                lines.append(self.formatException(record.exc_info))
        else:
            if record.exc_info is not None:
                exception, value, tb = record.exc_info
                if tb is not None:

                    # we do not use traceback module because it doesn't provide
                    # the way to determine module_name

                    # find innermost call
                    inner = tb
                    while inner.tb_next:
                        inner = inner.tb_next

                    lineno = inner.tb_lineno
                    f_globals = inner.tb_frame.f_globals
                    module_name = f_globals.get('__name__', '<unknown>')

                    record.message = '%-20s %s:%s %s' % (exception.__name__,
                                                         module_name, lineno,
                                                         exception_str(value))
                    lines[0] = self._fmt % record.__dict__

        return '\n'.join([smart_str(line) for line in lines])

    def formatException(self, exc_info):
        if self.show_locals:
            lines = format_exception_with_locals(*exc_info)
            return ''.join(lines)
        else:
            return logging.Formatter.formatException(self, exc_info)


class QloudJsonFormatter(logging.Formatter):
    """
    https://docs.qloud.yandex-team.ru/doc/logs#json

    Форматирует сообщение как JSON в формате Qloud.

    Сообщение будет иметь следующий формат:
    {
        "msg": "message",
        "stackTrace": "your very long stacktrace \n with newlines \n and all the things",
        "level": "WARN",
        "@fields": {
            "std": {
                "funcName": "get_values",
                "lineno": 274,
                "name": "mymicroservice.dao.utils",
            },
            "context": {
                "foo": "qwerty",
                "bar": 42,
            }
        }
    }

    В словарь @fields.context попадут поля, перечисленные в log_context.

    Если приложение запущено в Deploy - в сообщение автоматически добавятся следующие поля
    {
        "levelStr": "INFO",
        "loggerName": "name-of-the-logger",
        "level": 20,
    }
    """

    LOG_RECORD_USEFUL_FIELDS = ('funcName', 'lineno', 'name')

    def format(self, record):
        record.message = record.getMessage()

        log_data = {
            'message': record.message,
            'level': record.levelname,
        }
        if IS_DEPLOY:
            log_data['levelStr'] = record.levelname
            log_data['loggerName'] = record.name
            log_data['level'] = record.levelno

        if record.exc_info:
            exc = logging.Formatter.formatException(self, record.exc_info)
            log_data['stackTrace'] = exc

        fields = {}

        standard_fields = self._get_standard_fields(record)
        if standard_fields:
            standard_fields['orig_msg'] = smart_str(record.msg)
            fields['std'] = standard_fields

        log_context_fields = get_log_context()
        if log_context_fields:
            fields['context'] = log_context_fields
            if IS_DEPLOY and CONTEXT_FIELD_WITH_REQUEST_ID:
                log_data['request_id'] = log_context_fields.get(CONTEXT_FIELD_WITH_REQUEST_ID)
            if CONTEXT_FIELD_WITH_SUBREQUEST and CONTEXT_FIELD_WITH_SUBREQUEST in log_context_fields:
                fields['subrequest'] = log_context_fields.pop(CONTEXT_FIELD_WITH_SUBREQUEST)
            if CONTEXT_FIELD_WITH_RTLOG and CONTEXT_FIELD_WITH_RTLOG in log_context_fields:
                fields['rtlog'] = log_context_fields.pop(CONTEXT_FIELD_WITH_RTLOG)
            if CONTEXT_FIELD_WITH_EXPERIMENTS and CONTEXT_FIELD_WITH_EXPERIMENTS in log_context_fields:
                fields['experiments'] = log_context_fields.pop(CONTEXT_FIELD_WITH_EXPERIMENTS)
            if CONTEXT_FIELD_WITH_HEADERS and CONTEXT_FIELD_WITH_HEADERS in log_context_fields:
                fields['headers'] = log_context_fields.pop(CONTEXT_FIELD_WITH_HEADERS)

        if fields:
            log_data['@fields'] = fields

        return json.dumps(log_data)

    def _get_standard_fields(self, record):
        return {
            field: getattr(record, field)
            for field in self.LOG_RECORD_USEFUL_FIELDS
            if hasattr(record, field)
        }
