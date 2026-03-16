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
import sys

from .decorators import ConfigurationSetting
from .search_command import SearchCommand

from splunklib import six
from splunklib.six.moves import map as imap, filter as ifilter

# P1 [O] TODO: Discuss generates_timeorder in the class-level documentation for GeneratingCommand


class GeneratingCommand(SearchCommand):
    """ Generates events based on command arguments.

    Generating commands receive no input and must be the first command on a pipeline. There are three pipelines:
    streams, events, and reports. The streams pipeline generates or processes time-ordered event records on an
    indexer or search head.

    Streaming commands filter, modify, or augment event records and can be applied to subsets of index data in a
    parallel manner. An example of a streaming command from Splunk's built-in command set is rex_ which extracts and
    adds fields to event records at search time. Records that pass through the streams pipeline move on to the events
    pipeline.

    The events pipeline generates or processes records on a search head. Eventing commands typically filter, group,
    order, or augment event records. Examples of eventing commands from Splunk's built-in command set include sort_,
    dedup_, and cluster_. Each execution of an eventing command should produce a set of event records that is
    independently usable by downstream processors. Records that pass through the events pipeline move on to the reports
    pipeline.

    The reports pipeline also runs on a search head, but yields data structures for presentation, not event records.
    Examples of streaming from Splunk's built-in command set include chart_, stats_, and contingency_.

    GeneratingCommand configuration
    ===============================

    Configure your generating command based on the pipeline that it targets. How you configure your command depends on
    the Search Command Protocol (SCP) version.

    +----------+-------------------------------------+--------------------------------------------+
    | Pipeline | SCP 1                               | SCP 2                                      |
    +==========+=====================================+============================================+
    | streams  | streaming=True[,local=[True|False]] | type='streaming'[,distributed=[true|false] |
    +----------+-------------------------------------+--------------------------------------------+
    | events   | retainsevents=True, streaming=False | type='events'                              |
    +----------+-------------------------------------+--------------------------------------------+
    | reports  | streaming=False                     | type='reporting'                           |
    +----------+-------------------------------------+--------------------------------------------+

    Only streaming commands may be distributed to indexers. By default generating commands are configured to run
    locally in the streams pipeline and will run under either SCP 1 or SCP 2.

    .. code-block:: python

        @Configuration()
        class StreamingGeneratingCommand(GeneratingCommand)
            ...

    How you configure your command to run on a different pipeline or in a distributed fashion depends on what SCP
    protocol versions you wish to support. You must be sure to configure your command consistently for each protocol,
    if you wish to support both protocol versions correctly.

    .. _chart: http://docs.splunk.com/Documentation/Splunk/latest/SearchReference/Chart
    .. _cluster: http://docs.splunk.com/Documentation/Splunk/latest/SearchReference/Cluster
    .. _contingency: http://docs.splunk.com/Documentation/Splunk/latest/SearchReference/Contingency
    .. _dedup: http://docs.splunk.com/Documentation/Splunk/latest/SearchReference/Dedup
    .. _rex: http://docs.splunk.com/Documentation/Splunk/latest/SearchReference/Rex
    .. _sort: http://docs.splunk.com/Documentation/Splunk/latest/SearchReference/Sort
    .. _stats: http://docs.splunk.com/Documentation/Splunk/latest/SearchReference/Stats

    Distributed Generating command
    ==============================

    Commands configured like this will run as the first command on search heads and/or indexers on the streams pipeline.

    +----------+---------------------------------------------------+---------------------------------------------------+
    | Pipeline | SCP 1                                             | SCP 2                                             |
    +==========+===================================================+===================================================+
    | streams  | 1. Add this line to your command's stanza in      | 1. Add this configuration setting to your code:   |
    |          |                                                   |                                                   |
    |          |    default/commands.conf::                        |    ..  code-block:: python                        |
    |          |                                                   |                                                   |
    |          |        local = false                              |        @Configuration(distributed=True)           |
    |          |                                                   |        class SomeCommand(GeneratingCommand)       |
    |          |                                                   |            ...                                    |
    |          | 2. Restart splunk                                 |                                                   |
    |          |                                                   | 2. You are good to go; no need to restart Splunk  |
    +----------+---------------------------------------------------+---------------------------------------------------+

    Eventing Generating command
    ===========================

    Generating commands configured like this will run as the first command on a search head on the events pipeline.

    +----------+---------------------------------------------------+---------------------------------------------------+
    | Pipeline | SCP 1                                             | SCP 2                                             |
    +==========+===================================================+===================================================+
    | events   | You have a choice. Add these configuration        | Add this configuration setting to your command    |
    |          | settings to your command class:                   | setting to your command class:                    |
    |          |                                                   |                                                   |
    |          | .. code-block:: python                            | .. code-block:: python                            |
    |          |                                                   |                                                   |
    |          |     @Configuration(                               |     @Configuration(type='events')                 |
    |          |         retainsevents=True, streaming=False)      |     class SomeCommand(GeneratingCommand)          |
    |          |     class SomeCommand(GeneratingCommand)          |         ...                                       |
    |          |         ...                                       |                                                   |
    |          |                                                   |                                                   |
    |          | Or add these lines to default/commands.conf:      |                                                   |
    |          |                                                   |                                                   |
    |          | ..  code-block:: text                             |                                                   |
    |          |                                                   |                                                   |
    |          |     retainsevents = true                          |                                                   |
    |          |     streaming = false                             |                                                   |
    +----------+---------------------------------------------------+---------------------------------------------------+

    Configure your command class like this, if you wish to support both protocols:

    ..  code-block:: python

        @Configuration(type='events', retainsevents=True, streaming=False)
        class SomeCommand(GeneratingCommand)
            ...

    You might also consider adding these lines to commands.conf instead of adding them to your command class:

    ..  code-block:: python

        retainsevents = false
        streaming = false

    Reporting Generating command
    ============================

    Commands configured like this will run as the first command on a search head on the reports pipeline.

    +----------+---------------------------------------------------+---------------------------------------------------+
    | Pipeline | SCP 1                                             | SCP 2                                             |
    +==========+===================================================+===================================================+
    | events   | You have a choice. Add these configuration        | Add this configuration setting to your command    |
    |          | settings to your command class:                   | setting to your command class:                    |
    |          |                                                   |                                                   |
    |          | .. code-block:: python                            | .. code-block:: python                            |
    |          |                                                   |                                                   |
    |          |     @Configuration(retainsevents=False)           |     @Configuration(type='reporting')              |
    |          |     class SomeCommand(GeneratingCommand)          |     class SomeCommand(GeneratingCommand)          |
    |          |         ...                                       |         ...                                       |
    |          |                                                   |                                                   |
    |          | Or add this lines to default/commands.conf:       |                                                   |
    |          |                                                   |                                                   |
    |          | .. code-block:: text                              |                                                   |
    |          |                                                   |                                                   |
    |          |     retainsevents = false                         |                                                   |
    |          |     streaming = false                             |                                                   |
    +----------+---------------------------------------------------+---------------------------------------------------+

    Configure your command class like this, if you wish to support both protocols:

    ..  code-block:: python

        @Configuration(type='reporting', streaming=False)
        class SomeCommand(GeneratingCommand)
            ...

    You might also consider adding these lines to commands.conf instead of adding them to your command class:

    ..  code-block:: text

        retainsevents = false
        streaming = false

    """
    # region Methods

    def generate(self):
        """ A generator that yields records to the Splunk processing pipeline

        You must override this method.

        """
        raise NotImplementedError('GeneratingCommand.generate(self)')

    def _execute(self, ifile, process):
        """ Execution loop

        :param ifile: Input file object. Unused.
        :type ifile: file

        :return: `None`.

        """
        if self._protocol_version == 2:
            self._execute_v2(ifile, self.generate())
        else:
            assert self._protocol_version == 1
            self._record_writer.write_records(self.generate())
        self.finish()

    def _execute_chunk_v2(self, process, chunk):
        count = 0
        records = []
        for row in process:
            records.append(row)
            count += 1
            if count == self._record_writer._maxresultrows:
                break

        for row in records:
            self._record_writer.write_record(row)

        if count == self._record_writer._maxresultrows:
            self._finished = False
        else:
            self._finished = True

    def process(self, argv=sys.argv, ifile=sys.stdin, ofile=sys.stdout, allow_empty_input=True):
        """ Process data.

        :param argv: Command line arguments.
        :type argv: list or tuple

        :param ifile: Input data file.
        :type ifile: file

        :param ofile: Output data file.
        :type ofile: file

        :param allow_empty_input: For generating commands, it must be true. Doing otherwise will cause an error.
        :type allow_empty_input: bool

        :return: :const:`None`
        :rtype: NoneType

        """

        # Generating commands are expected to run on an empty set of inputs as the first command being run in a search,
        # also this class implements its own separate _execute_chunk_v2 method which does not respect allow_empty_input
        # so ensure that allow_empty_input is always True

        if not allow_empty_input:
            raise ValueError("allow_empty_input cannot be False for Generating Commands")
        else:
            return super(GeneratingCommand, self).process(argv=argv, ifile=ifile, ofile=ofile, allow_empty_input=True)

    # endregion

    # region Types

    class ConfigurationSettings(SearchCommand.ConfigurationSettings):
        """ Represents the configuration settings for a :code:`GeneratingCommand` class.

        """
        # region SCP v1/v2 Properties

        generating = ConfigurationSetting(readonly=True, value=True, doc='''
            Tells Splunk that this command generates events, but does not process inputs.

            Generating commands must appear at the front of the search pipeline identified by :meth:`type`.

            Fixed: :const:`True`

            Supported by: SCP 1, SCP 2

            ''')

        # endregion

        # region SCP v1 Properties

        generates_timeorder = ConfigurationSetting(doc='''
            :const:`True`, if the command generates new events.

            Default: :const:`False`

            Supported by: SCP 1

            ''')

        local = ConfigurationSetting(doc='''
            :const:`True`, if the command should run locally on the search head.

            Default: :const:`False`

            Supported by: SCP 1

            ''')

        retainsevents = ConfigurationSetting(doc='''
            :const:`True`, if the command retains events the way the sort, dedup, and cluster commands do, or whether it
            transforms them the way the stats command does.

            Default: :const:`False`

            Supported by: SCP 1

            ''')

        streaming = ConfigurationSetting(doc='''
            :const:`True`, if the command is streamable.

            Default: :const:`True`

            Supported by: SCP 1

            ''')

        # endregion

        # region SCP v2 Properties

        distributed = ConfigurationSetting(value=False, doc='''
            True, if this command should be distributed to indexers.

            This value is ignored unless :meth:`type` is equal to :const:`streaming`. It is only this command type that
            may be distributed.

            Default: :const:`False`

            Supported by: SCP 2

            ''')

        type = ConfigurationSetting(value='streaming', doc='''
            A command type name.

            ====================  ======================================================================================
            Value                 Description
            --------------------  --------------------------------------------------------------------------------------
            :const:`'events'`     Runs as the first command in the Splunk events pipeline. Cannot be distributed.
            :const:`'reporting'`  Runs as the first command in the Splunk reports pipeline. Cannot be distributed.
            :const:`'streaming'`  Runs as the first command in the Splunk streams pipeline. May be distributed.
            ====================  ======================================================================================

            Default: :const:`'streaming'`

            Supported by: SCP 2

            ''')

        # endregion

        # region Methods

        @classmethod
        def fix_up(cls, command):
            """ Verifies :code:`command` class structure.

            """
            if command.generate == GeneratingCommand.generate:
                raise AttributeError('No GeneratingCommand.generate override')

        # TODO: Stop looking like a dictionary because we don't obey the semantics
        # N.B.: Does not use Python 2 dict copy semantics
        def iteritems(self):
            iteritems = SearchCommand.ConfigurationSettings.iteritems(self)
            version = self.command.protocol_version
            if version == 2:
                iteritems = ifilter(lambda name_value1: name_value1[0] != 'distributed', iteritems)
                if not self.distributed and self.type == 'streaming':
                    iteritems = imap(
                        lambda name_value: (name_value[0], 'stateful') if name_value[0] == 'type' else (name_value[0], name_value[1]), iteritems)
            return iteritems

        # N.B.: Does not use Python 3 dict view semantics
        if not six.PY2:
            items = iteritems

        pass
        # endregion

    pass
    # endregion
