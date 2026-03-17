# coding=utf-8
#
# Copyright © 2011-2015 Splunk, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"): you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import, division, print_function

from io import TextIOWrapper
from collections import deque, namedtuple
from splunklib import six
from collections import OrderedDict
from splunklib.six.moves import StringIO
from itertools import chain
from splunklib.six.moves import map as imap
from json import JSONDecoder, JSONEncoder
from json.encoder import encode_basestring_ascii as json_encode_string
from splunklib.six.moves import urllib

import csv
import gzip
import os
import re
import sys
import warnings

from . import environment

csv.field_size_limit(10485760)  # The default value is 128KB; upping to 10MB. See SPL-12117 for background on this issue


def set_binary_mode(fh):
    """ Helper method to set up binary mode for file handles.
    Emphasis being sys.stdin, sys.stdout, sys.stderr.
    For python3, we want to return .buffer
    For python2+windows we want to set os.O_BINARY
    """
    typefile = TextIOWrapper if sys.version_info >= (3, 0) else file
    # check for file handle
    if not isinstance(fh, typefile):
        return fh

    # check for python3 and buffer
    if sys.version_info >= (3, 0) and hasattr(fh, 'buffer'):
        return fh.buffer
    # check for python3
    elif sys.version_info >= (3, 0):
        pass
    # check for windows python2. SPL-175233 -- python3 stdout is already binary
    elif sys.platform == 'win32':
        # Work around the fact that on Windows '\n' is mapped to '\r\n'. The typical solution is to simply open files in
        # binary mode, but stdout is already open, thus this hack. 'CPython' and 'PyPy' work differently. We assume that
        # all other Python implementations are compatible with 'CPython'. This might or might not be a valid assumption.
        from platform import python_implementation
        implementation = python_implementation()
        if implementation == 'PyPy':
            return os.fdopen(fh.fileno(), 'wb', 0)
        else:
            import msvcrt
            msvcrt.setmode(fh.fileno(), os.O_BINARY)
    return fh


class CommandLineParser(object):
    r""" Parses the arguments to a search command.

    A search command line is described by the following syntax.

    **Syntax**::

       command       = command-name *[wsp option] *[wsp [dquote] field-name [dquote]]
       command-name  = alpha *( alpha / digit )
       option        = option-name [wsp] "=" [wsp] option-value
       option-name   = alpha *( alpha / digit / "_" )
       option-value  = word / quoted-string
       word          = 1*( %01-%08 / %0B / %0C / %0E-1F / %21 / %23-%FF ) ; Any character but DQUOTE and WSP
       quoted-string = dquote *( word / wsp / "\" dquote / dquote dquote ) dquote
       field-name    = ( "_" / alpha ) *( alpha / digit / "_" / "." / "-" )

    **Note:**

    This syntax is constrained to an 8-bit character set.

    **Note:**

    This syntax does not show that `field-name` values may be comma-separated when in fact they can be. This is
    because Splunk strips commas from the command line. A custom search command will never see them.

    **Example:**

    countmatches fieldname = word_count pattern = \w+ some_text_field

    Option names are mapped to properties in the targeted ``SearchCommand``. It is the responsibility of the property
    setters to validate the values they receive. Property setters may also produce side effects. For example,
    setting the built-in `log_level` immediately changes the `log_level`.

    """
    @classmethod
    def parse(cls, command, argv):
        """ Splits an argument list into an options dictionary and a fieldname
        list.

        The argument list, `argv`, must be of the form::

            *[option]... *[<field-name>]

        Options are validated and assigned to items in `command.options`. Field names are validated and stored in the
        list of `command.fieldnames`.

        #Arguments:

        :param command: Search command instance.
        :type command: ``SearchCommand``
        :param argv: List of search command arguments.
        :type argv: ``list``
        :return: ``None``

        #Exceptions:

        ``SyntaxError``: Argument list is incorrectly formed.
        ``ValueError``: Unrecognized option/field name, or an illegal field value.

        """
        debug = environment.splunklib_logger.debug
        command_class = type(command).__name__

        # Prepare

        debug('Parsing %s command line: %r', command_class, argv)
        command.fieldnames = None
        command.options.reset()
        argv = ' '.join(argv)

        command_args = cls._arguments_re.match(argv)

        if command_args is None:
            raise SyntaxError('Syntax error: {}'.format(argv))

        # Parse options

        for option in cls._options_re.finditer(command_args.group('options')):
            name, value = option.group('name'), option.group('value')
            if name not in command.options:
                raise ValueError(
                    'Unrecognized {} command option: {}={}'.format(command.name, name, json_encode_string(value)))
            command.options[name].value = cls.unquote(value)

        missing = command.options.get_missing()

        if missing is not None:
            if len(missing) > 1:
                raise ValueError(
                    'Values for these {} command options are required: {}'.format(command.name, ', '.join(missing)))
            raise ValueError('A value for {} command option {} is required'.format(command.name, missing[0]))

        # Parse field names

        fieldnames = command_args.group('fieldnames')

        if fieldnames is None:
            command.fieldnames = []
        else:
            command.fieldnames = [cls.unquote(value.group(0)) for value in cls._fieldnames_re.finditer(fieldnames)]

        debug('  %s: %s', command_class, command)

    @classmethod
    def unquote(cls, string):
        """ Removes quotes from a quoted string.

        Splunk search command quote rules are applied. The enclosing double-quotes, if present, are removed. Escaped
        double-quotes ('\"' or '""') are replaced by a single double-quote ('"').

        **NOTE**

        We are not using a json.JSONDecoder because Splunk quote rules are different than JSON quote rules. A
        json.JSONDecoder does not recognize a pair of double-quotes ('""') as an escaped quote ('"') and will
        decode single-quoted strings ("'") in addition to double-quoted ('"') strings.

        """
        if len(string) == 0:
            return ''

        if string[0] == '"':
            if len(string) == 1 or string[-1] != '"':
                raise SyntaxError('Poorly formed string literal: ' + string)
            string = string[1:-1]

        if len(string) == 0:
            return ''

        def replace(match):
            value = match.group(0)
            if value == '""':
                return '"'
            if len(value) < 2:
                raise SyntaxError('Poorly formed string literal: ' + string)
            return value[1]

        result = re.sub(cls._escaped_character_re, replace, string)
        return result

    # region Class variables

    _arguments_re = re.compile(r"""
        ^\s*
        (?P<options>     # Match a leading set of name/value pairs
            (?:
                (?:(?=\w)[^\d]\w*)                         # name
                \s*=\s*                                    # =
                (?:"(?:\\.|""|[^"])*"|(?:\\.|[^\s"])+)\s*  # value
            )*
        )\s*
        (?P<fieldnames>  # Match a trailing set of field names
            (?:
                (?:"(?:\\.|""|[^"])*"|(?:\\.|[^\s"])+)\s*
            )*
        )\s*$
        """, re.VERBOSE | re.UNICODE)

    _escaped_character_re = re.compile(r'(\\.|""|[\\"])')

    _fieldnames_re = re.compile(r"""("(?:\\.|""|[^"\\])+"|(?:\\.|[^\s"])+)""")

    _options_re = re.compile(r"""
        # Captures a set of name/value pairs when used with re.finditer
        (?P<name>(?:(?=\w)[^\d]\w*))                   # name
        \s*=\s*                                        # =
        (?P<value>"(?:\\.|""|[^"])*"|(?:\\.|[^\s"])+)  # value
        """, re.VERBOSE | re.UNICODE)

    # endregion


class ConfigurationSettingsType(type):
    """ Metaclass for constructing ConfigurationSettings classes.

    Instances of :class:`ConfigurationSettingsType` construct :class:`ConfigurationSettings` classes from classes from
    a base :class:`ConfigurationSettings` class and a dictionary of configuration settings. The settings in the
    dictionary are validated against the settings in the base class. You cannot add settings, you can only change their
    backing-field values and you cannot modify settings without backing-field values. These are considered fixed
    configuration setting values.

    This is an internal class used in two places:

    + :meth:`decorators.Configuration.__call__`

      Adds a ConfigurationSettings attribute to a :class:`SearchCommand` class.

    + :meth:`reporting_command.ReportingCommand.fix_up`

      Adds a ConfigurationSettings attribute to a :meth:`ReportingCommand.map` method, if there is one.

    """
    def __new__(mcs, module, name, bases):
        mcs = super(ConfigurationSettingsType, mcs).__new__(mcs, str(name), bases, {})
        return mcs

    def __init__(cls, module, name, bases):

        super(ConfigurationSettingsType, cls).__init__(name, bases, None)
        cls.__module__ = module

    @staticmethod
    def validate_configuration_setting(specification, name, value):
        if not isinstance(value, specification.type):
            if isinstance(specification.type, type):
                type_names = specification.type.__name__
            else:
                type_names = ', '.join(imap(lambda t: t.__name__, specification.type))
            raise ValueError('Expected {} value, not {}={}'.format(type_names, name, repr(value)))
        if specification.constraint and not specification.constraint(value):
            raise ValueError('Illegal value: {}={}'.format(name, repr(value)))
        return value

    specification = namedtuple(
        'ConfigurationSettingSpecification', (
            'type',
            'constraint',
            'supporting_protocols'))

    # P1 [ ] TODO: Review ConfigurationSettingsType.specification_matrix for completeness and correctness

    specification_matrix = {
        'clear_required_fields': specification(
            type=bool,
            constraint=None,
            supporting_protocols=[1]),
        'distributed': specification(
            type=bool,
            constraint=None,
            supporting_protocols=[2]),
        'generates_timeorder': specification(
            type=bool,
            constraint=None,
            supporting_protocols=[1]),
        'generating': specification(
            type=bool,
            constraint=None,
            supporting_protocols=[1, 2]),
        'local': specification(
            type=bool,
            constraint=None,
            supporting_protocols=[1]),
        'maxinputs': specification(
            type=int,
            constraint=lambda value: 0 <= value <= six.MAXSIZE,
            supporting_protocols=[2]),
        'overrides_timeorder': specification(
            type=bool,
            constraint=None,
            supporting_protocols=[1]),
        'required_fields': specification(
            type=(list, set, tuple),
            constraint=None,
            supporting_protocols=[1, 2]),
        'requires_preop': specification(
            type=bool,
            constraint=None,
            supporting_protocols=[1]),
        'retainsevents': specification(
            type=bool,
            constraint=None,
            supporting_protocols=[1]),
        'run_in_preview': specification(
            type=bool,
            constraint=None,
            supporting_protocols=[2]),
        'streaming': specification(
            type=bool,
            constraint=None,
            supporting_protocols=[1]),
        'streaming_preop': specification(
            type=(bytes, six.text_type),
            constraint=None,
            supporting_protocols=[1, 2]),
        'type': specification(
            type=(bytes, six.text_type),
            constraint=lambda value: value in ('events', 'reporting', 'streaming'),
            supporting_protocols=[2])}


class CsvDialect(csv.Dialect):
    """ Describes the properties of Splunk CSV streams """
    delimiter = ','
    quotechar = '"'
    doublequote = True
    skipinitialspace = False
    lineterminator = '\r\n'
    if sys.version_info >= (3, 0) and sys.platform == 'win32':
        lineterminator = '\n'
    quoting = csv.QUOTE_MINIMAL


class InputHeader(dict):
    """ Represents a Splunk input header as a collection of name/value pairs.

    """

    def __str__(self):
        return '\n'.join([name + ':' + value for name, value in six.iteritems(self)])

    def read(self, ifile):
        """ Reads an input header from an input file.

        The input header is read as a sequence of *<name>***:***<value>* pairs separated by a newline. The end of the
        input header is signalled by an empty line or an end-of-file.

        :param ifile: File-like object that supports iteration over lines.

        """
        name, value = None, None

        for line in ifile:
            if line == '\n':
                break
            item = line.split(':', 1)
            if len(item) == 2:
                # start of a new item
                if name is not None:
                    self[name] = value[:-1]  # value sans trailing newline
                name, value = item[0], urllib.parse.unquote(item[1])
            elif name is not None:
                # continuation of the current item
                value += urllib.parse.unquote(line)

        if name is not None:
            self[name] = value[:-1] if value[-1] == '\n' else value


Message = namedtuple('Message', ('type', 'text'))


class MetadataDecoder(JSONDecoder):

    def __init__(self):
        JSONDecoder.__init__(self, object_hook=self._object_hook)

    @staticmethod
    def _object_hook(dictionary):

        object_view = ObjectView(dictionary)
        stack = deque()
        stack.append((None, None, dictionary))

        while len(stack):
            instance, member_name, dictionary = stack.popleft()

            for name, value in six.iteritems(dictionary):
                if isinstance(value, dict):
                    stack.append((dictionary, name, value))

            if instance is not None:
                instance[member_name] = ObjectView(dictionary)

        return object_view


class MetadataEncoder(JSONEncoder):

    def __init__(self):
        JSONEncoder.__init__(self, separators=MetadataEncoder._separators)

    def default(self, o):
        return o.__dict__ if isinstance(o, ObjectView) else JSONEncoder.default(self, o)

    _separators = (',', ':')


class ObjectView(object):

    def __init__(self, dictionary):
        self.__dict__ = dictionary

    def __repr__(self):
        return repr(self.__dict__)

    def __str__(self):
        return str(self.__dict__)


class Recorder(object):

    def __init__(self, path, f):
        self._recording = gzip.open(path + '.gz', 'wb')
        self._file = f

    def __getattr__(self, name):
        return getattr(self._file, name)

    def __iter__(self):
        for line in self._file:
            self._recording.write(line)
            self._recording.flush()
            yield line

    def read(self, size=None):
        value = self._file.read() if size is None else self._file.read(size)
        self._recording.write(value)
        self._recording.flush()
        return value

    def readline(self, size=None):
        value = self._file.readline() if size is None else self._file.readline(size)
        if len(value) > 0:
            self._recording.write(value)
            self._recording.flush()
        return value

    def record(self, *args):
        for arg in args:
            self._recording.write(arg)

    def write(self, text):
        self._recording.write(text)
        self._file.write(text)
        self._recording.flush()


class RecordWriter(object):

    def __init__(self, ofile, maxresultrows=None):
        self._maxresultrows = 50000 if maxresultrows is None else maxresultrows

        self._ofile = set_binary_mode(ofile)
        self._fieldnames = None
        self._buffer = StringIO()

        self._writer = csv.writer(self._buffer, dialect=CsvDialect)
        self._writerow = self._writer.writerow
        self._finished = False
        self._flushed = False

        self._inspector = OrderedDict()
        self._chunk_count = 0
        self._pending_record_count = 0
        self._committed_record_count = 0
        self.custom_fields = set()

    @property
    def is_flushed(self):
        return self._flushed

    @is_flushed.setter
    def is_flushed(self, value):
        self._flushed = True if value else False

    @property
    def ofile(self):
        return self._ofile

    @ofile.setter
    def ofile(self, value):
        self._ofile = set_binary_mode(value)

    @property
    def pending_record_count(self):
        return self._pending_record_count

    @property
    def _record_count(self):
        warnings.warn(
            "_record_count will be deprecated soon. Use pending_record_count instead.",
             PendingDeprecationWarning
        )
        return self.pending_record_count

    @property
    def committed_record_count(self):
        return self._committed_record_count

    @property
    def _total_record_count(self):
        warnings.warn(
            "_total_record_count will be deprecated soon. Use committed_record_count instead.",
             PendingDeprecationWarning
        )
        return self.committed_record_count

    def write(self, data):
        bytes_type = bytes if sys.version_info >= (3, 0) else str
        if not isinstance(data, bytes_type):
            data = data.encode('utf-8')
        self.ofile.write(data)

    def flush(self, finished=None, partial=None):
        assert finished is None or isinstance(finished, bool)
        assert partial is None or isinstance(partial, bool)
        assert not (finished is None and partial is None)
        assert finished is None or partial is None
        self._ensure_validity()

    def write_message(self, message_type, message_text, *args, **kwargs):
        self._ensure_validity()
        self._inspector.setdefault('messages', []).append((message_type, message_text.format(*args, **kwargs)))

    def write_record(self, record):
        self._ensure_validity()
        self._write_record(record)

    def write_records(self, records):
        self._ensure_validity()
        records = list(records)
        write_record = self._write_record
        for record in records:
            write_record(record)

    def _clear(self):
        self._buffer.seek(0)
        self._buffer.truncate()
        self._inspector.clear()
        self._pending_record_count = 0

    def _ensure_validity(self):
        if self._finished is True:
            assert self._record_count == 0 and len(self._inspector) == 0
            raise RuntimeError('I/O operation on closed record writer')

    def _write_record(self, record):

        fieldnames = self._fieldnames

        if fieldnames is None:
            self._fieldnames = fieldnames = list(record.keys())
            self._fieldnames.extend([i for i in self.custom_fields if i not in self._fieldnames])
            value_list = imap(lambda fn: (str(fn), str('__mv_') + str(fn)), fieldnames)
            self._writerow(list(chain.from_iterable(value_list)))

        get_value = record.get
        values = []

        for fieldname in fieldnames:
            value = get_value(fieldname, None)

            if value is None:
                values += (None, None)
                continue

            value_t = type(value)

            if issubclass(value_t, (list, tuple)):

                if len(value) == 0:
                    values += (None, None)
                    continue

                if len(value) > 1:
                    value_list = value
                    sv = ''
                    mv = '$'

                    for value in value_list:

                        if value is None:
                            sv += '\n'
                            mv += '$;$'
                            continue

                        value_t = type(value)

                        if value_t is not bytes:

                            if value_t is bool:
                                value = str(value.real)
                            elif value_t is six.text_type:
                                value = value
                            elif isinstance(value, six.integer_types) or value_t is float or value_t is complex:
                                value = str(value)
                            elif issubclass(value_t, (dict, list, tuple)):
                                value = str(''.join(RecordWriter._iterencode_json(value, 0)))
                            else:
                                value = repr(value).encode('utf-8', errors='backslashreplace')

                        sv += value + '\n'
                        mv += value.replace('$', '$$') + '$;$'

                    values += (sv[:-1], mv[:-2])
                    continue

                value = value[0]
                value_t = type(value)

            if value_t is bool:
                values += (str(value.real), None)
                continue

            if value_t is bytes:
                values += (value, None)
                continue

            if value_t is six.text_type:
                if six.PY2:
                    value = value.encode('utf-8')
                values += (value, None)
                continue

            if isinstance(value, six.integer_types) or value_t is float or value_t is complex:
                values += (str(value), None)
                continue

            if issubclass(value_t, dict):
                values += (str(''.join(RecordWriter._iterencode_json(value, 0))), None)
                continue

            values += (repr(value), None)

        self._writerow(values)
        self._pending_record_count += 1

        if self.pending_record_count >= self._maxresultrows:
            self.flush(partial=True)

    try:
        # noinspection PyUnresolvedReferences
        from _json import make_encoder
    except ImportError:
        # We may be running under PyPy 2.5 which does not include the _json module
        _iterencode_json = JSONEncoder(separators=(',', ':')).iterencode
    else:
        # Creating _iterencode_json this way yields a two-fold performance improvement on Python 2.7.9 and 2.7.10
        from json.encoder import encode_basestring_ascii

        @staticmethod
        def _default(o):
            raise TypeError(repr(o) + ' is not JSON serializable')

        _iterencode_json = make_encoder(
            {},                       # markers (for detecting circular references)
            _default,                 # object_encoder
            encode_basestring_ascii,  # string_encoder
            None,                     # indent
            ':', ',',                 # separators
            False,                    # sort_keys
            False,                    # skip_keys
            True                      # allow_nan
        )

        del make_encoder


class RecordWriterV1(RecordWriter):

    def flush(self, finished=None, partial=None):

        RecordWriter.flush(self, finished, partial)  # validates arguments and the state of this instance

        if self.pending_record_count > 0 or (self._chunk_count == 0 and 'messages' in self._inspector):

            messages = self._inspector.get('messages')

            if self._chunk_count == 0:

                # Messages are written to the messages header when we write the first chunk of data
                # Guarantee: These messages are displayed by splunkweb and the job inspector

                if messages is not None:

                    message_level = RecordWriterV1._message_level.get

                    for level, text in messages:
                        self.write(message_level(level, level))
                        self.write('=')
                        self.write(text)
                        self.write('\r\n')

                self.write('\r\n')

            elif messages is not None:

                # Messages are written to the messages header when we write subsequent chunks of data
                # Guarantee: These messages are displayed by splunkweb and the job inspector, if and only if the
                # command is configured with
                #
                #       stderr_dest = message
                #
                # stderr_dest is a static configuration setting. This means that it can only be set in commands.conf.
                # It cannot be set in code.

                stderr = sys.stderr

                for level, text in messages:
                    print(level, text, file=stderr)

            self.write(self._buffer.getvalue())
            self._chunk_count += 1
            self._committed_record_count += self.pending_record_count
            self._clear()

        self._finished = finished is True

    _message_level = {
        'DEBUG': 'debug_message',
        'ERROR': 'error_message',
        'FATAL': 'error_message',
        'INFO': 'info_message',
        'WARN': 'warn_message'
    }


class RecordWriterV2(RecordWriter):

    def flush(self, finished=None, partial=None):

        RecordWriter.flush(self, finished, partial)  # validates arguments and the state of this instance

        if partial or not finished:
            # Don't flush partial chunks, since the SCP v2 protocol does not
            # provide a way to send partial chunks yet.
            return

        if not self.is_flushed:
            self.write_chunk(finished=True)

    def write_chunk(self, finished=None):
        inspector = self._inspector
        self._committed_record_count += self.pending_record_count
        self._chunk_count += 1

        # TODO: DVPL-6448: splunklib.searchcommands | Add support for partial: true when it is implemented in
        # ChunkedExternProcessor (See SPL-103525)
        #
        # We will need to replace the following block of code with this block:
        #
        # metadata = [item for item in (('inspector', inspector), ('finished', finished), ('partial', partial))]
        #
        # if partial is True:
        #     finished = False

        if len(inspector) == 0:
            inspector = None

        metadata = [item for item in (('inspector', inspector), ('finished', finished))]
        self._write_chunk(metadata, self._buffer.getvalue())
        self._clear()

    def write_metadata(self, configuration):
        self._ensure_validity()

        metadata = chain(six.iteritems(configuration), (('inspector', self._inspector if self._inspector else None),))
        self._write_chunk(metadata, '')
        self.write('\n')
        self._clear()

    def write_metric(self, name, value):
        self._ensure_validity()
        self._inspector['metric.' + name] = value

    def _clear(self):
        super(RecordWriterV2, self)._clear()
        self._fieldnames = None

    def _write_chunk(self, metadata, body):

        if metadata:
            metadata = str(''.join(self._iterencode_json(dict([(n, v) for n, v in metadata if v is not None]), 0)))
            if sys.version_info >= (3, 0):
                metadata = metadata.encode('utf-8')
            metadata_length = len(metadata)
        else:
            metadata_length = 0

        if sys.version_info >= (3, 0):
            body = body.encode('utf-8')
        body_length = len(body)

        if not (metadata_length > 0 or body_length > 0):
            return

        start_line = 'chunked 1.0,%s,%s\n' % (metadata_length, body_length)
        self.write(start_line)
        self.write(metadata)
        self.write(body)
        self._ofile.flush()
        self._flushed = True
