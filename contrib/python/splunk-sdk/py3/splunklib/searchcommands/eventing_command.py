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


class EventingCommand(SearchCommand):
    """Applies a transformation to search results as they travel through the events pipeline.

    Eventing commands typically filter, group, order, and/or or augment event records. Examples of eventing commands
    from Splunk's built-in command set include sort_, dedup_, and cluster_. Each execution of an eventing command
    should produce a set of event records that is independently usable by downstream processors.

    .. _sort: http://docs.splunk.com/Documentation/Splunk/latest/SearchReference/Sort
    .. _dedup: http://docs.splunk.com/Documentation/Splunk/latest/SearchReference/Dedup
    .. _cluster: http://docs.splunk.com/Documentation/Splunk/latest/SearchReference/Cluster

    EventingCommand configuration
    ==============================

    You can configure your command for operation under Search Command Protocol (SCP) version 1 or 2. SCP 2 requires
    Splunk 6.3 or later.

    """

    # region Methods

    def transform(self, records):
        """Generator function that processes and yields event records to the Splunk events pipeline.

        You must override this method.

        """
        raise NotImplementedError("EventingCommand.transform(self, records)")

    def _execute(self, ifile, process):
        SearchCommand._execute(self, ifile, self.transform)

    # endregion

    class ConfigurationSettings(SearchCommand.ConfigurationSettings):
        """Represents the configuration settings that apply to a :class:`EventingCommand`."""

        # region SCP v1/v2 properties

        required_fields = ConfigurationSetting(
            doc="""
            List of required fields for this search which back-propagates to the generating search.

            Setting this value enables selected fields mode under SCP 2. Under SCP 1 you must also specify
            :code:`clear_required_fields=True` to enable selected fields mode. To explicitly select all fields,
            specify a value of :const:`['*']`. No error is generated if a specified field is missing.

            Default: :const:`None`, which implicitly selects all fields.

            """
        )

        # endregion

        # region SCP v1 properties

        clear_required_fields = ConfigurationSetting(
            doc="""
            :const:`True`, if required_fields represent the *only* fields required.

            If :const:`False`, required_fields are additive to any fields that may be required by subsequent commands.
            In most cases, :const:`False` is appropriate for eventing commands.

            Default: :const:`False`

            """
        )

        retainsevents = ConfigurationSetting(
            readonly=True,
            value=True,
            doc="""
            :const:`True`, if the command retains events the way the sort/dedup/cluster commands do.

            If :const:`False`, the command transforms events the way the stats command does.

            Fixed: :const:`True`

            """,
        )

        # endregion

        # region SCP v2 properties

        maxinputs = ConfigurationSetting(
            doc="""
            Specifies the maximum number of events that can be passed to the command for each invocation.

            This limit cannot exceed the value of `maxresultrows` as defined in limits.conf_. Under SCP 1 you must
            specify this value in commands.conf_.

            Default: The value of `maxresultrows`.

            Supported by: SCP 2

            .. _limits.conf: http://docs.splunk.com/Documentation/Splunk/latest/admin/Limitsconf

            """
        )

        type = ConfigurationSetting(
            readonly=True,
            value="events",
            doc="""
            Command type

            Fixed: :const:`'events'`.

            Supported by: SCP 2

            """,
        )

        # endregion

        # region Methods

        @classmethod
        def fix_up(cls, command):
            """Verifies :code:`command` class structure."""
            if command.transform == EventingCommand.transform:
                raise AttributeError("No EventingCommand.transform override")
            SearchCommand.ConfigurationSettings.fix_up(command)

        # TODO: Stop looking like a dictionary because we don't obey the semantics
        # N.B.: Does not use Python 2 dict copy semantics
        def iteritems(self):
            iteritems = SearchCommand.ConfigurationSettings.iteritems(self)
            return [
                (name_value[0], "events" if name_value[0] == "type" else name_value[1])
                for name_value in iteritems
            ]

        # N.B.: Does not use Python 3 dict view semantics

        items = iteritems

        # endregion
