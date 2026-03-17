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

from __future__ import absolute_import, division, print_function, unicode_literals

# Absolute imports

from collections import namedtuple

import io

from collections import OrderedDict
from copy import deepcopy
from splunklib.six.moves import StringIO
from itertools import chain, islice
from splunklib.six.moves import filter as ifilter, map as imap, zip as izip
from splunklib import six
if six.PY2:
    from logging import _levelNames, getLevelName, getLogger
else:
    from logging import _nameToLevel as _levelNames, getLevelName, getLogger
try:
    from shutil import make_archive
except ImportError:
    # Used for recording, skip on python 2.6
    pass
from time import time
from splunklib.six.moves.urllib.parse import unquote
from splunklib.six.moves.urllib.parse import urlsplit
from warnings import warn
from xml.etree import ElementTree

import os
import sys
import re
import csv
import tempfile
import traceback

# Relative imports

from .internals import (
    CommandLineParser,
    CsvDialect,
    InputHeader,
    Message,
    MetadataDecoder,
    MetadataEncoder,
    ObjectView,
    Recorder,
    RecordWriterV1,
    RecordWriterV2,
    json_encode_string)

from . import Boolean, Option, environment
from ..client import Service


# ----------------------------------------------------------------------------------------------------------------------

# P1 [ ] TODO: Log these issues against ChunkedExternProcessor
#
# 1. Implement requires_preop configuration setting.
#    This configuration setting is currently rejected by ChunkedExternProcessor.
#
# 2. Rename type=events as type=eventing for symmetry with type=reporting and type=streaming
#    Eventing commands process records on the events pipeline. This change effects ChunkedExternProcessor.cpp,
#    eventing_command.py, and generating_command.py.
#
# 3. For consistency with SCPV1, commands.conf should not require filename setting when chunked = true
#    The SCPV1 processor uses <stanza-name>.py as the default filename. The ChunkedExternProcessor should do the same.

# P1 [ ] TODO: Verify that ChunkedExternProcessor complains if a streaming_preop has a type other than 'streaming'
# It once looked like sending type='reporting' for the streaming_preop was accepted.

# ----------------------------------------------------------------------------------------------------------------------

# P2 [ ] TODO: Consider bumping None formatting up to Option.Item.__str__


class SearchCommand(object):
    """ Represents a custom search command.

    """

    def __init__(self):

        # Variables that may be used, but not altered by derived classes

        class_name = self.__class__.__name__

        self._logger, self._logging_configuration = getLogger(class_name), environment.logging_configuration

        # Variables backing option/property values

        self._configuration = self.ConfigurationSettings(self)
        self._input_header = InputHeader()
        self._fieldnames = None
        self._finished = None
        self._metadata = None
        self._options = None
        self._protocol_version = None
        self._search_results_info = None
        self._service = None

        # Internal variables

        self._default_logging_level = self._logger.level
        self._record_writer = None
        self._records = None
        self._allow_empty_input = True

    def __str__(self):
        text = ' '.join(chain((type(self).name, str(self.options)), [] if self.fieldnames is None else self.fieldnames))
        return text

    # region Options

    @Option
    def logging_configuration(self):
        """ **Syntax:** logging_configuration=<path>

        **Description:** Loads an alternative logging configuration file for
        a command invocation. The logging configuration file must be in Python
        ConfigParser-format. Path names are relative to the app root directory.

        """
        return self._logging_configuration

    @logging_configuration.setter
    def logging_configuration(self, value):
        self._logger, self._logging_configuration = environment.configure_logging(self.__class__.__name__, value)

    @Option
    def logging_level(self):
        """ **Syntax:** logging_level=[CRITICAL|ERROR|WARNING|INFO|DEBUG|NOTSET]

        **Description:** Sets the threshold for the logger of this command invocation. Logging messages less severe than
        `logging_level` will be ignored.

        """
        return getLevelName(self._logger.getEffectiveLevel())

    @logging_level.setter
    def logging_level(self, value):
        if value is None:
            value = self._default_logging_level
        if isinstance(value, (bytes, six.text_type)):
            try:
                level = _levelNames[value.upper()]
            except KeyError:
                raise ValueError('Unrecognized logging level: {}'.format(value))
        else:
            try:
                level = int(value)
            except ValueError:
                raise ValueError('Unrecognized logging level: {}'.format(value))
        self._logger.setLevel(level)

    def add_field(self, current_record, field_name, field_value):
        self._record_writer.custom_fields.add(field_name)
        current_record[field_name] = field_value

    def gen_record(self, **record):
        self._record_writer.custom_fields |= set(record.keys())
        return record

    record = Option(doc='''
        **Syntax: record=<bool>

        **Description:** When `true`, records the interaction between the command and splunkd. Defaults to `false`.

        ''', default=False, validate=Boolean())

    show_configuration = Option(doc='''
        **Syntax:** show_configuration=<bool>

        **Description:** When `true`, reports command configuration as an informational message. Defaults to `false`.

        ''', default=False, validate=Boolean())

    # endregion

    # region Properties

    @property
    def configuration(self):
        """ Returns the configuration settings for this command.

        """
        return self._configuration

    @property
    def fieldnames(self):
        """ Returns the fieldnames specified as argument to this command.

        """
        return self._fieldnames

    @fieldnames.setter
    def fieldnames(self, value):
        self._fieldnames = value

    @property
    def input_header(self):
        """ Returns the input header for this command.

        :return: The input header for this command.
        :rtype: InputHeader

        """
        warn(
            'SearchCommand.input_header is deprecated and will be removed in a future release. '
            'Please use SearchCommand.metadata instead.', DeprecationWarning, 2)
        return self._input_header

    @property
    def logger(self):
        """ Returns the logger for this command.

        :return: The logger for this command.
        :rtype:

        """
        return self._logger

    @property
    def metadata(self):
        return self._metadata

    @property
    def options(self):
        """ Returns the options specified as argument to this command.

        """
        if self._options is None:
            self._options = Option.View(self)
        return self._options

    @property
    def protocol_version(self):
        return self._protocol_version

    @property
    def search_results_info(self):
        """ Returns the search results info for this command invocation.

        The search results info object is created from the search results info file associated with the command
        invocation.

        :return: Search results info:const:`None`, if the search results info file associated with the command
                 invocation is inaccessible.
        :rtype: SearchResultsInfo or NoneType

        """
        if self._search_results_info is not None:
            return self._search_results_info

        if self._protocol_version == 1:
            try:
                path = self._input_header['infoPath']
            except KeyError:
                return None
        else:
            assert self._protocol_version == 2

            try:
                dispatch_dir = self._metadata.searchinfo.dispatch_dir
            except AttributeError:
                return None

            path = os.path.join(dispatch_dir, 'info.csv')

        try:
            with io.open(path, 'r') as f:
                reader = csv.reader(f, dialect=CsvDialect)
                fields = next(reader)
                values = next(reader)
        except IOError as error:
            if error.errno == 2:
                self.logger.error('Search results info file {} does not exist.'.format(json_encode_string(path)))
                return
            raise

        def convert_field(field):
            return (field[1:] if field[0] == '_' else field).replace('.', '_')

        decode = MetadataDecoder().decode

        def convert_value(value):
            try:
                return decode(value) if len(value) > 0 else value
            except ValueError:
                return value

        info = ObjectView(dict(imap(lambda f_v: (convert_field(f_v[0]), convert_value(f_v[1])), izip(fields, values))))

        try:
            count_map = info.countMap
        except AttributeError:
            pass
        else:
            count_map = count_map.split(';')
            n = len(count_map)
            info.countMap = dict(izip(islice(count_map, 0, n, 2), islice(count_map, 1, n, 2)))

        try:
            msg_type = info.msgType
            msg_text = info.msg
        except AttributeError:
            pass
        else:
            messages = ifilter(lambda t_m: t_m[0] or t_m[1], izip(msg_type.split('\n'), msg_text.split('\n')))
            info.msg = [Message(message) for message in messages]
            del info.msgType

        try:
            info.vix_families = ElementTree.fromstring(info.vix_families)
        except AttributeError:
            pass

        self._search_results_info = info
        return info

    @property
    def service(self):
        """ Returns a Splunk service object for this command invocation or None.

        The service object is created from the Splunkd URI and authentication token passed to the command invocation in
        the search results info file. This data is not passed to a command invocation by default. You must request it by
        specifying this pair of configuration settings in commands.conf:

           .. code-block:: python

               enableheader = true
               requires_srinfo = true

        The :code:`enableheader` setting is :code:`true` by default. Hence, you need not set it. The
        :code:`requires_srinfo` setting is false by default. Hence, you must set it.

        :return: :class:`splunklib.client.Service`, if :code:`enableheader` and :code:`requires_srinfo` are both
            :code:`true`. Otherwise, if either :code:`enableheader` or :code:`requires_srinfo` are :code:`false`, a value
            of :code:`None` is returned.

        """
        if self._service is not None:
            return self._service

        metadata = self._metadata

        if metadata is None:
            return None

        try:
            searchinfo = self._metadata.searchinfo
        except AttributeError:
            return None

        splunkd_uri = searchinfo.splunkd_uri

        if splunkd_uri is None:
            return None

        uri = urlsplit(splunkd_uri, allow_fragments=False)

        self._service = Service(
            scheme=uri.scheme, host=uri.hostname, port=uri.port, app=searchinfo.app, token=searchinfo.session_key)

        return self._service

    # endregion

    # region Methods

    def error_exit(self, error, message=None):
        self.write_error(error.message if message is None else message)
        self.logger.error('Abnormal exit: %s', error)
        exit(1)

    def finish(self):
        """ Flushes the output buffer and signals that this command has finished processing data.

        :return: :const:`None`

        """
        self._record_writer.flush(finished=True)

    def flush(self):
        """ Flushes the output buffer.

        :return: :const:`None`

        """
        self._record_writer.flush(finished=False)

    def prepare(self):
        """ Prepare for execution.

        This method should be overridden in search command classes that wish to examine and update their configuration
        or option settings prior to execution. It is called during the getinfo exchange before command metadata is sent
        to splunkd.

        :return: :const:`None`
        :rtype: NoneType

        """
        pass

    def process(self, argv=sys.argv, ifile=sys.stdin, ofile=sys.stdout, allow_empty_input=True):
        """ Process data.

        :param argv: Command line arguments.
        :type argv: list or tuple

        :param ifile: Input data file.
        :type ifile: file

        :param ofile: Output data file.
        :type ofile: file

        :param allow_empty_input: Allow empty input records for the command, if False an Error will be returned if empty chunk body is encountered when read
        :type allow_empty_input: bool

        :return: :const:`None`
        :rtype: NoneType

        """

        self._allow_empty_input = allow_empty_input

        if len(argv) > 1:
            self._process_protocol_v1(argv, ifile, ofile)
        else:
            self._process_protocol_v2(argv, ifile, ofile)

    def _map_input_header(self):
        metadata = self._metadata
        searchinfo = metadata.searchinfo
        self._input_header.update(
            allowStream=None,
            infoPath=os.path.join(searchinfo.dispatch_dir, 'info.csv'),
            keywords=None,
            preview=metadata.preview,
            realtime=searchinfo.earliest_time != 0 and searchinfo.latest_time != 0,
            search=searchinfo.search,
            sid=searchinfo.sid,
            splunkVersion=searchinfo.splunk_version,
            truncated=None)

    def _map_metadata(self, argv):
        source = SearchCommand._MetadataSource(argv, self._input_header, self.search_results_info)

        def _map(metadata_map):
            metadata = {}

            for name, value in six.iteritems(metadata_map):
                if isinstance(value, dict):
                    value = _map(value)
                else:
                    transform, extract = value
                    if extract is None:
                        value = None
                    else:
                        value = extract(source)
                        if not (value is None or transform is None):
                            value = transform(value)
                metadata[name] = value

            return ObjectView(metadata)

        self._metadata = _map(SearchCommand._metadata_map)

    _metadata_map = {
        'action':
            (lambda v: 'getinfo' if v == '__GETINFO__' else 'execute' if v == '__EXECUTE__' else None, lambda s: s.argv[1]),
        'preview':
            (bool, lambda s: s.input_header.get('preview')),
        'searchinfo': {
            'app':
                (lambda v: v.ppc_app, lambda s: s.search_results_info),
            'args':
                (None, lambda s: s.argv),
            'dispatch_dir':
                (os.path.dirname, lambda s: s.input_header.get('infoPath')),
            'earliest_time':
                (lambda v: float(v.rt_earliest) if len(v.rt_earliest) > 0 else 0.0, lambda s: s.search_results_info),
            'latest_time':
                (lambda v: float(v.rt_latest) if len(v.rt_latest) > 0 else 0.0, lambda s: s.search_results_info),
            'owner':
                (None, None),
            'raw_args':
                (None, lambda s: s.argv),
            'search':
                (unquote, lambda s: s.input_header.get('search')),
            'session_key':
                (lambda v: v.auth_token, lambda s: s.search_results_info),
            'sid':
                (None, lambda s: s.input_header.get('sid')),
            'splunk_version':
                (None, lambda s: s.input_header.get('splunkVersion')),
            'splunkd_uri':
                (lambda v: v.splunkd_uri, lambda s: s.search_results_info),
            'username':
                (lambda v: v.ppc_user, lambda s: s.search_results_info)}}

    _MetadataSource = namedtuple('Source', ('argv', 'input_header', 'search_results_info'))

    def _prepare_protocol_v1(self, argv, ifile, ofile):

        debug = environment.splunklib_logger.debug

        # Provide as much context as possible in advance of parsing the command line and preparing for execution

        self._input_header.read(ifile)
        self._protocol_version = 1
        self._map_metadata(argv)

        debug('  metadata=%r, input_header=%r', self._metadata, self._input_header)

        try:
            tempfile.tempdir = self._metadata.searchinfo.dispatch_dir
        except AttributeError:
            raise RuntimeError('{}.metadata.searchinfo.dispatch_dir is undefined'.format(self.__class__.__name__))

        debug('  tempfile.tempdir=%r', tempfile.tempdir)

        CommandLineParser.parse(self, argv[2:])
        self.prepare()

        if self.record:
            self.record = False

            record_argv = [argv[0], argv[1], str(self._options), ' '.join(self.fieldnames)]
            ifile, ofile = self._prepare_recording(record_argv, ifile, ofile)
            self._record_writer.ofile = ofile
            ifile.record(str(self._input_header), '\n\n')

        if self.show_configuration:
            self.write_info(self.name + ' command configuration: ' + str(self._configuration))

        return ifile  # wrapped, if self.record is True

    def _prepare_recording(self, argv, ifile, ofile):

        # Create the recordings directory, if it doesn't already exist

        recordings = os.path.join(environment.splunk_home, 'var', 'run', 'splunklib.searchcommands', 'recordings')

        if not os.path.isdir(recordings):
            os.makedirs(recordings)

        # Create input/output recorders from ifile and ofile

        recording = os.path.join(recordings, self.__class__.__name__ + '-' + repr(time()) + '.' + self._metadata.action)
        ifile = Recorder(recording + '.input', ifile)
        ofile = Recorder(recording + '.output', ofile)

        # Archive the dispatch directory--if it exists--so that it can be used as a baseline in mocks)

        dispatch_dir = self._metadata.searchinfo.dispatch_dir

        if dispatch_dir is not None:  # __GETINFO__ action does not include a dispatch_dir
            root_dir, base_dir = os.path.split(dispatch_dir)
            make_archive(recording + '.dispatch_dir', 'gztar', root_dir, base_dir, logger=self.logger)

        # Save a splunk command line because it is useful for developing tests

        with open(recording + '.splunk_cmd', 'wb') as f:
            f.write('splunk cmd python '.encode())
            f.write(os.path.basename(argv[0]).encode())
            for arg in islice(argv, 1, len(argv)):
                f.write(' '.encode())
                f.write(arg.encode())

        return ifile, ofile

    def _process_protocol_v1(self, argv, ifile, ofile):

        debug = environment.splunklib_logger.debug
        class_name = self.__class__.__name__

        debug('%s.process started under protocol_version=1', class_name)
        self._record_writer = RecordWriterV1(ofile)

        # noinspection PyBroadException
        try:
            if argv[1] == '__GETINFO__':

                debug('Writing configuration settings')

                ifile = self._prepare_protocol_v1(argv, ifile, ofile)
                self._record_writer.write_record(dict(
                    (n, ','.join(v) if isinstance(v, (list, tuple)) else v) for n, v in six.iteritems(self._configuration)))
                self.finish()

            elif argv[1] == '__EXECUTE__':

                debug('Executing')

                ifile = self._prepare_protocol_v1(argv, ifile, ofile)
                self._records = self._records_protocol_v1
                self._metadata.action = 'execute'
                self._execute(ifile, None)

            else:
                message = (
                    'Command {0} appears to be statically configured for search command protocol version 1 and static '
                    'configuration is unsupported by splunklib.searchcommands. Please ensure that '
                    'default/commands.conf contains this stanza:\n'
                    '[{0}]\n'
                    'filename = {1}\n'
                    'enableheader = true\n'
                    'outputheader = true\n'
                    'requires_srinfo = true\n'
                    'supports_getinfo = true\n'
                    'supports_multivalues = true\n'
                    'supports_rawargs = true'.format(self.name, os.path.basename(argv[0])))
                raise RuntimeError(message)

        except (SyntaxError, ValueError) as error:
            self.write_error(six.text_type(error))
            self.flush()
            exit(0)

        except SystemExit:
            self.flush()
            raise

        except:
            self._report_unexpected_error()
            self.flush()
            exit(1)

        debug('%s.process finished under protocol_version=1', class_name)

    def _protocol_v2_option_parser(self, arg):
        """ Determines if an argument is an Option/Value pair, or just a Positional Argument.
            Method so different search commands can handle parsing of arguments differently.

            :param arg: A single argument provided to the command from SPL
            :type arg: str

            :return: [OptionName, OptionValue] OR [PositionalArgument]
            :rtype: List[str]

        """
        return arg.split('=', 1)

    def _process_protocol_v2(self, argv, ifile, ofile):
        """ Processes records on the `input stream optionally writing records to the output stream.

        :param ifile: Input file object.
        :type ifile: file or InputType

        :param ofile: Output file object.
        :type ofile: file or OutputType

        :return: :const:`None`

        """
        debug = environment.splunklib_logger.debug
        class_name = self.__class__.__name__

        debug('%s.process started under protocol_version=2', class_name)
        self._protocol_version = 2

        # Read search command metadata from splunkd
        # noinspection PyBroadException
        try:
            debug('Reading metadata')
            metadata, body = self._read_chunk(self._as_binary_stream(ifile))

            action = getattr(metadata, 'action', None)

            if action != 'getinfo':
                raise RuntimeError('Expected getinfo action, not {}'.format(action))

            if len(body) > 0:
                raise RuntimeError('Did not expect data for getinfo action')

            self._metadata = deepcopy(metadata)

            searchinfo = self._metadata.searchinfo

            searchinfo.earliest_time = float(searchinfo.earliest_time)
            searchinfo.latest_time = float(searchinfo.latest_time)
            searchinfo.search = unquote(searchinfo.search)

            self._map_input_header()

            debug('  metadata=%r, input_header=%r', self._metadata, self._input_header)

            try:
                tempfile.tempdir = self._metadata.searchinfo.dispatch_dir
            except AttributeError:
                raise RuntimeError('%s.metadata.searchinfo.dispatch_dir is undefined'.format(class_name))

            debug('  tempfile.tempdir=%r', tempfile.tempdir)
        except:
            self._record_writer = RecordWriterV2(ofile)
            self._report_unexpected_error()
            self.finish()
            exit(1)

        # Write search command configuration for consumption by splunkd
        # noinspection PyBroadException
        try:
            self._record_writer = RecordWriterV2(ofile, getattr(self._metadata.searchinfo, 'maxresultrows', None))
            self.fieldnames = []
            self.options.reset()

            args = self.metadata.searchinfo.args
            error_count = 0

            debug('Parsing arguments')

            if args and type(args) == list:
                for arg in args:
                    result = self._protocol_v2_option_parser(arg)
                    if len(result) == 1:
                        self.fieldnames.append(str(result[0]))
                    else:
                        name, value = result
                        name = str(name)
                        try:
                            option = self.options[name]
                        except KeyError:
                            self.write_error('Unrecognized option: {}={}'.format(name, value))
                            error_count += 1
                            continue
                        try:
                            option.value = value
                        except ValueError:
                            self.write_error('Illegal value: {}={}'.format(name, value))
                            error_count += 1
                            continue

            missing = self.options.get_missing()

            if missing is not None:
                if len(missing) == 1:
                    self.write_error('A value for "{}" is required'.format(missing[0]))
                else:
                    self.write_error('Values for these required options are missing: {}'.format(', '.join(missing)))
                error_count += 1

            if error_count > 0:
                exit(1)

            debug('  command: %s', six.text_type(self))

            debug('Preparing for execution')
            self.prepare()

            if self.record:

                ifile, ofile = self._prepare_recording(argv, ifile, ofile)
                self._record_writer.ofile = ofile

                # Record the metadata that initiated this command after removing the record option from args/raw_args

                info = self._metadata.searchinfo

                for attr in 'args', 'raw_args':
                    setattr(info, attr, [arg for arg in getattr(info, attr) if not arg.startswith('record=')])

                metadata = MetadataEncoder().encode(self._metadata)
                ifile.record('chunked 1.0,', six.text_type(len(metadata)), ',0\n', metadata)

            if self.show_configuration:
                self.write_info(self.name + ' command configuration: ' + str(self._configuration))

            debug('  command configuration: %s', self._configuration)

        except SystemExit:
            self._record_writer.write_metadata(self._configuration)
            self.finish()
            raise
        except:
            self._record_writer.write_metadata(self._configuration)
            self._report_unexpected_error()
            self.finish()
            exit(1)

        self._record_writer.write_metadata(self._configuration)

        # Execute search command on data passing through the pipeline
        # noinspection PyBroadException
        try:
            debug('Executing under protocol_version=2')
            self._metadata.action = 'execute'
            self._execute(ifile, None)
        except SystemExit:
            self.finish()
            raise
        except:
            self._report_unexpected_error()
            self.finish()
            exit(1)

        debug('%s.process completed', class_name)

    def write_debug(self, message, *args):
        self._record_writer.write_message('DEBUG', message, *args)

    def write_error(self, message, *args):
        self._record_writer.write_message('ERROR', message, *args)

    def write_fatal(self, message, *args):
        self._record_writer.write_message('FATAL', message, *args)

    def write_info(self, message, *args):
        self._record_writer.write_message('INFO', message, *args)

    def write_warning(self, message, *args):
        self._record_writer.write_message('WARN', message, *args)

    def write_metric(self, name, value):
        """ Writes a metric that will be added to the search inspector.

        :param name: Name of the metric.
        :type name: basestring

        :param value: A 4-tuple containing the value of metric ``name`` where

            value[0] = Elapsed seconds or :const:`None`.
            value[1] = Number of invocations or :const:`None`.
            value[2] = Input count or :const:`None`.
            value[3] = Output count or :const:`None`.

        The :data:`SearchMetric` type provides a convenient encapsulation of ``value``.
        The :data:`SearchMetric` type provides a convenient encapsulation of ``value``.

        :return: :const:`None`.

        """
        self._record_writer.write_metric(name, value)

    # P2 [ ] TODO: Support custom inspector values

    @staticmethod
    def _decode_list(mv):
        return [match.replace('$$', '$') for match in SearchCommand._encoded_value.findall(mv)]

    _encoded_value = re.compile(r'\$(?P<item>(?:\$\$|[^$])*)\$(?:;|$)')  # matches a single value in an encoded list

    # Note: Subclasses must override this method so that it can be called
    # called as self._execute(ifile, None)
    def _execute(self, ifile, process):
        """ Default processing loop

        :param ifile: Input file object.
        :type ifile: file

        :param process: Bound method to call in processing loop.
        :type process: instancemethod

        :return: :const:`None`.
        :rtype: NoneType

        """
        if self.protocol_version == 1:
            self._record_writer.write_records(process(self._records(ifile)))
            self.finish()
        else:
            assert self._protocol_version == 2
            self._execute_v2(ifile, process)

    @staticmethod
    def _as_binary_stream(ifile):
        naught = ifile.read(0)
        if isinstance(naught, bytes):
            return ifile

        try:
            return ifile.buffer
        except AttributeError as error:
            raise RuntimeError('Failed to get underlying buffer: {}'.format(error))

    @staticmethod
    def _read_chunk(istream):
        # noinspection PyBroadException
        assert isinstance(istream.read(0), six.binary_type), 'Stream must be binary'

        try:
            header = istream.readline()
        except Exception as error:
            raise RuntimeError('Failed to read transport header: {}'.format(error))

        if not header:
            return None

        match = SearchCommand._header.match(six.ensure_str(header))

        if match is None:
            raise RuntimeError('Failed to parse transport header: {}'.format(header))

        metadata_length, body_length = match.groups()
        metadata_length = int(metadata_length)
        body_length = int(body_length)

        try:
            metadata = istream.read(metadata_length)
        except Exception as error:
            raise RuntimeError('Failed to read metadata of length {}: {}'.format(metadata_length, error))

        decoder = MetadataDecoder()

        try:
            metadata = decoder.decode(six.ensure_str(metadata))
        except Exception as error:
            raise RuntimeError('Failed to parse metadata of length {}: {}'.format(metadata_length, error))

        # if body_length <= 0:
        #     return metadata, ''

        body = ""
        try:
            if body_length > 0:
                body = istream.read(body_length)
        except Exception as error:
            raise RuntimeError('Failed to read body of length {}: {}'.format(body_length, error))

        return metadata, six.ensure_str(body, errors="replace")

    _header = re.compile(r'chunked\s+1.0\s*,\s*(\d+)\s*,\s*(\d+)\s*\n')

    def _records_protocol_v1(self, ifile):
        return self._read_csv_records(ifile)

    def _read_csv_records(self, ifile):
        reader = csv.reader(ifile, dialect=CsvDialect)

        try:
            fieldnames = next(reader)
        except StopIteration:
            return

        mv_fieldnames = dict([(name, name[len('__mv_'):]) for name in fieldnames if name.startswith('__mv_')])

        if len(mv_fieldnames) == 0:
            for values in reader:
                yield OrderedDict(izip(fieldnames, values))
            return

        for values in reader:
            record = OrderedDict()
            for fieldname, value in izip(fieldnames, values):
                if fieldname.startswith('__mv_'):
                    if len(value) > 0:
                        record[mv_fieldnames[fieldname]] = self._decode_list(value)
                elif fieldname not in record:
                    record[fieldname] = value
            yield record

    def _execute_v2(self, ifile, process):
        istream = self._as_binary_stream(ifile)

        while True:
            result = self._read_chunk(istream)

            if not result:
                return

            metadata, body = result
            action = getattr(metadata, 'action', None)
            if action != 'execute':
                raise RuntimeError('Expected execute action, not {}'.format(action))

            self._finished = getattr(metadata, 'finished', False)
            self._record_writer.is_flushed = False

            self._execute_chunk_v2(process, result)

            self._record_writer.write_chunk(finished=self._finished)

    def _execute_chunk_v2(self, process, chunk):
            metadata, body = chunk

            if len(body) <= 0 and not self._allow_empty_input:
                raise ValueError(
                    "No records found to process. Set allow_empty_input=True in dispatch function to move forward "
                    "with empty records.")

            records = self._read_csv_records(StringIO(body))
            self._record_writer.write_records(process(records))

    def _report_unexpected_error(self):

        error_type, error, tb = sys.exc_info()
        origin = tb

        while origin.tb_next is not None:
            origin = origin.tb_next

        filename = origin.tb_frame.f_code.co_filename
        lineno = origin.tb_lineno
        message = '{0} at "{1}", line {2:d} : {3}'.format(error_type.__name__, filename, lineno, error)

        environment.splunklib_logger.error(message + '\nTraceback:\n' + ''.join(traceback.format_tb(tb)))
        self.write_error(message)

    # endregion

    # region Types

    class ConfigurationSettings(object):
        """ Represents the configuration settings common to all :class:`SearchCommand` classes.

        """
        def __init__(self, command):
            self.command = command

        def __repr__(self):
            """ Converts the value of this instance to its string representation.

            The value of this ConfigurationSettings instance is represented as a string of comma-separated
            :code:`(name, value)` pairs.

            :return: String representation of this instance

            """
            definitions = type(self).configuration_setting_definitions
            settings = imap(
                lambda setting: repr((setting.name, setting.__get__(self), setting.supporting_protocols)), definitions)
            return '[' + ', '.join(settings) + ']'

        def __str__(self):
            """ Converts the value of this instance to its string representation.

            The value of this ConfigurationSettings instance is represented as a string of comma-separated
            :code:`name=value` pairs. Items with values of :const:`None` are filtered from the list.

            :return: String representation of this instance

            """
            #text = ', '.join(imap(lambda (name, value): name + '=' + json_encode_string(unicode(value)), self.iteritems()))
            text = ', '.join(['{}={}'.format(name, json_encode_string(six.text_type(value))) for (name, value) in six.iteritems(self)])
            return text

        # region Methods

        @classmethod
        def fix_up(cls, command_class):
            """ Adjusts and checks this class and its search command class.

            Derived classes typically override this method. It is used by the :decorator:`Configuration` decorator to
            fix up the :class:`SearchCommand` class it adorns. This method is overridden by :class:`EventingCommand`,
            :class:`GeneratingCommand`, :class:`ReportingCommand`, and :class:`StreamingCommand`, the base types for
            all other search commands.

            :param command_class: Command class targeted by this class

            """
            return

        # TODO: Stop looking like a dictionary because we don't obey the semantics
        # N.B.: Does not use Python 2 dict copy semantics
        def iteritems(self):
            definitions = type(self).configuration_setting_definitions
            version = self.command.protocol_version
            return ifilter(
                lambda name_value1: name_value1[1] is not None, imap(
                    lambda setting: (setting.name, setting.__get__(self)), ifilter(
                        lambda setting: setting.is_supported_by_protocol(version), definitions)))

        # N.B.: Does not use Python 3 dict view semantics
        if not six.PY2:
            items = iteritems

        pass  # endregion

    pass  # endregion


SearchMetric = namedtuple('SearchMetric', ('elapsed_seconds', 'invocation_count', 'input_count', 'output_count'))


def dispatch(command_class, argv=sys.argv, input_file=sys.stdin, output_file=sys.stdout, module_name=None, allow_empty_input=True):
    """ Instantiates and executes a search command class

    This function implements a `conditional script stanza <https://docs.python.org/2/library/__main__.html>`_ based on the value of
    :code:`module_name`::

        if module_name is None or module_name == '__main__':
            # execute command

    Call this function at module scope with :code:`module_name=__name__`, if you would like your module to act as either
    a reusable module or a standalone program. Otherwise, if you wish this function to unconditionally instantiate and
    execute :code:`command_class`, pass :const:`None` as the value of :code:`module_name`.

    :param command_class: Search command class to instantiate and execute.
    :type command_class: type
    :param argv: List of arguments to the command.
    :type argv: list or tuple
    :param input_file: File from which the command will read data.
    :type input_file: :code:`file`
    :param output_file: File to which the command will write data.
    :type output_file: :code:`file`
    :param module_name: Name of the module calling :code:`dispatch` or :const:`None`.
    :type module_name: :code:`basestring`
    :param allow_empty_input: Allow empty input records for the command, if False an Error will be returned if empty chunk body is encountered when read
    :type allow_empty_input: bool
    :returns: :const:`None`

    **Example**

    ..  code-block:: python
        :linenos:

        #!/usr/bin/env python
        from splunklib.searchcommands import dispatch, StreamingCommand, Configuration, Option, validators
        @Configuration()
        class SomeStreamingCommand(StreamingCommand):
            ...
            def stream(records):
                ...
        dispatch(SomeStreamingCommand, module_name=__name__)

    Dispatches the :code:`SomeStreamingCommand`, if and only if :code:`__name__` is equal to :code:`'__main__'`.

    **Example**

    ..  code-block:: python
        :linenos:

        from splunklib.searchcommands import dispatch, StreamingCommand, Configuration, Option, validators
        @Configuration()
        class SomeStreamingCommand(StreamingCommand):
            ...
            def stream(records):
                ...
        dispatch(SomeStreamingCommand)

    Unconditionally dispatches :code:`SomeStreamingCommand`.

    """
    assert issubclass(command_class, SearchCommand)

    if module_name is None or module_name == '__main__':
        command_class().process(argv, input_file, output_file, allow_empty_input)
