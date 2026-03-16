# coding=utf-8
#
# Copyright © 2011-2024 Splunk, Inc.
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


from .decorators import ConfigurationSetting
from .search_command import SearchCommand


class StreamingCommand(SearchCommand):
    """Applies a transformation to search results as they travel through the streams pipeline.

    Streaming commands typically filter, augment, or update, search result records. Splunk will send them in batches of
    up to 50,000 records. Hence, a search command must be prepared to be invoked many times during the course of
    pipeline processing. Each invocation should produce a set of results independently usable by downstream processors.

    By default Splunk may choose to run a streaming command locally on a search head and/or remotely on one or more
    indexers concurrently. The size and frequency of the search result batches sent to the command will vary based
    on scheduling considerations.

    StreamingCommand configuration
    ==============================

    You can configure your command for operation under Search Command Protocol (SCP) version 1 or 2. SCP 2 requires
    Splunk 6.3 or later.

    """

    # region Methods

    def stream(self, records):
        """Generator function that processes and yields event records to the Splunk stream pipeline.

        You must override this method.

        """
        raise NotImplementedError("StreamingCommand.stream(self, records)")

    def _execute(self, ifile, process):
        SearchCommand._execute(self, ifile, self.stream)

    # endregion

    class ConfigurationSettings(SearchCommand.ConfigurationSettings):
        """Represents the configuration settings that apply to a :class:`StreamingCommand`."""

        # region SCP v1/v2 properties

        required_fields = ConfigurationSetting(
            doc="""
            List of required fields for this search which back-propagates to the generating search.

            Setting this value enables selected fields mode under SCP 2. Under SCP 1 you must also specify
            :code:`clear_required_fields=True` to enable selected fields mode. To explicitly select all fields,
            specify a value of :const:`['*']`. No error is generated if a specified field is missing.

            Default: :const:`None`, which implicitly selects all fields.

            Supported by: SCP 1, SCP 2

            """
        )

        # endregion

        # region SCP v1 properties

        clear_required_fields = ConfigurationSetting(
            doc="""
            :const:`True`, if required_fields represent the *only* fields required.

            If :const:`False`, required_fields are additive to any fields that may be required by subsequent commands.
            In most cases, :const:`False` is appropriate for streaming commands.

            Default: :const:`False`

            Supported by: SCP 1

            """
        )

        local = ConfigurationSetting(
            doc="""
            :const:`True`, if the command should run locally on the search head.

            Default: :const:`False`

            Supported by: SCP 1

            """
        )

        overrides_timeorder = ConfigurationSetting(
            doc="""
            :const:`True`, if the command changes the order of events with respect to time.

            Default: :const:`False`

            Supported by: SCP 1

            """
        )

        streaming = ConfigurationSetting(
            readonly=True,
            value=True,
            doc="""
            Specifies that the command is streamable.

            Fixed: :const:`True`

            Supported by: SCP 1

            """,
        )

        # endregion

        # region SCP v2 Properties

        distributed = ConfigurationSetting(
            value=True,
            doc="""
            :const:`True`, if this command should be distributed to indexers.

            Under SCP 1 you must either specify `local = False` or include this line in commands.conf_, if this command
            should be distributed to indexers.

            ..code:
                local = true

            Default: :const:`True`

            Supported by: SCP 2

            .. commands.conf_: http://docs.splunk.com/Documentation/Splunk/latest/Admin/Commandsconf

            """,
        )

        maxinputs = ConfigurationSetting(
            doc="""
            Specifies the maximum number of events that can be passed to the command for each invocation.

            This limit cannot exceed the value of `maxresultrows` in limits.conf. Under SCP 1 you must specify this
            value in commands.conf_.

            Default: The value of `maxresultrows`.

            Supported by: SCP 2

            """
        )

        type = ConfigurationSetting(
            readonly=True,
            value="streaming",
            doc="""
            Command type name.

            Fixed: :const:`'streaming'`

            Supported by: SCP 2

            """,
        )

        # endregion

        # region Methods

        @classmethod
        def fix_up(cls, command):
            """Verifies :code:`command` class structure."""
            if command.stream == StreamingCommand.stream:
                raise AttributeError("No StreamingCommand.stream override")

        # TODO: Stop looking like a dictionary because we don't obey the semantics
        # N.B.: Does not use Python 2 dict copy semantics
        def iteritems(self):
            iteritems = SearchCommand.ConfigurationSettings.iteritems(self)
            version = self.command.protocol_version
            if version == 1:
                if self.required_fields is None:
                    iteritems = [
                        name_value
                        for name_value in iteritems
                        if name_value[0] != "clear_required_fields"
                    ]
            else:
                iteritems = [
                    name_value2
                    for name_value2 in iteritems
                    if name_value2[0] != "distributed"
                ]
                if not self.distributed:
                    iteritems = [
                        (name_value1[0], "stateful")
                        if name_value1[0] == "type"
                        else (name_value1[0], name_value1[1])
                        for name_value1 in iteritems
                    ]
            return iteritems

        # N.B.: Does not use Python 3 dict view semantics
        items = iteritems

        # endregion
