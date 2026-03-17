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

from itertools import chain

from .internals import ConfigurationSettingsType, json_encode_string
from .decorators import ConfigurationSetting, Option
from .streaming_command import StreamingCommand
from .search_command import SearchCommand
from .validators import Set


class ReportingCommand(SearchCommand):
    """Processes search result records and generates a reporting data structure.

    Reporting search commands run as either reduce or map/reduce operations. The reduce part runs on a search head and
    is responsible for processing a single chunk of search results to produce the command's reporting data structure.
    The map part is called a streaming preop. It feeds the reduce part with partial results and by default runs on the
    search head and/or one or more indexers.

    You must implement a :meth:`reduce` method as a generator function that iterates over a set of event records and
    yields a reporting data structure. You may implement a :meth:`map` method as a generator function that iterates
    over a set of event records and yields :class:`dict` or :class:`list(dict)` instances.

    ReportingCommand configuration
    ==============================

    Configure the :meth:`map` operation using a Configuration decorator on your :meth:`map` method. Configure it like
    you would a :class:`StreamingCommand`. Configure the :meth:`reduce` operation using a Configuration decorator on
    your :meth:`ReportingCommand` class.

    You can configure your command for operation under Search Command Protocol (SCP) version 1 or 2. SCP 2 requires
    Splunk 6.3 or later.

    """

    # region Special methods

    def __init__(self):
        SearchCommand.__init__(self)

    # endregion

    # region Options

    phase = Option(
        doc="""
        **Syntax:** phase=[map|reduce]

        **Description:** Identifies the phase of the current map-reduce operation.

    """,
        default="reduce",
        validate=Set("map", "reduce"),
    )

    # endregion

    # region Methods

    def map(self, records):
        """Override this method to compute partial results.

        :param records:
        :type records:

        You must override this method, if :code:`requires_preop=True`.

        """
        return NotImplemented

    def _has_custom_method(self, method_name):
        method = getattr(self.__class__, method_name, None)
        base_method = getattr(ReportingCommand, method_name, None)
        return callable(method) and (method is not base_method)

    def prepare(self):
        if self.phase == "map":
            if self._has_custom_method("map"):
                phase_method = getattr(self.__class__, "map")
                self._configuration = phase_method.ConfigurationSettings(self)
            else:
                self._configuration = self.ConfigurationSettings(self)
            return

        if self.phase == "reduce":
            streaming_preop = chain(
                (self.name, 'phase="map"', str(self._options)), self.fieldnames
            )
            self._configuration.streaming_preop = " ".join(streaming_preop)
            return

        raise RuntimeError(
            f"Unrecognized reporting command phase: {json_encode_string(str(self.phase))}"
        )

    def reduce(self, records):
        """Override this method to produce a reporting data structure.

        You must override this method.

        """
        raise NotImplementedError("reduce(self, records)")

    def _execute(self, ifile, process):
        SearchCommand._execute(self, ifile, getattr(self, self.phase))

    # endregion

    # region Types

    class ConfigurationSettings(SearchCommand.ConfigurationSettings):
        """Represents the configuration settings for a :code:`ReportingCommand`."""

        # region SCP v1/v2 Properties

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

        requires_preop = ConfigurationSetting(
            doc="""
            Indicates whether :meth:`ReportingCommand.map` is required for proper command execution.

            If :const:`True`, :meth:`ReportingCommand.map` is guaranteed to be called. If :const:`False`, Splunk
            considers it to be an optimization that may be skipped.

            Default: :const:`False`

            Supported by: SCP 1, SCP 2

            """
        )

        streaming_preop = ConfigurationSetting(
            doc="""
            Denotes the requested streaming preop search string.

            Computed.

            Supported by: SCP 1, SCP 2

            """
        )

        # endregion

        # region SCP v1 Properties

        clear_required_fields = ConfigurationSetting(
            doc="""
            :const:`True`, if required_fields represent the *only* fields required.

            If :const:`False`, required_fields are additive to any fields that may be required by subsequent commands.
            In most cases, :const:`True` is appropriate for reporting commands.

            Default: :const:`True`

            Supported by: SCP 1

            """
        )

        retainsevents = ConfigurationSetting(
            readonly=True,
            value=False,
            doc="""
            Signals that :meth:`ReportingCommand.reduce` transforms _raw events to produce a reporting data structure.

            Fixed: :const:`False`

            Supported by: SCP 1

            """,
        )

        streaming = ConfigurationSetting(
            readonly=True,
            value=False,
            doc="""
            Signals that :meth:`ReportingCommand.reduce` runs on the search head.

            Fixed: :const:`False`

            Supported by: SCP 1

            """,
        )

        # endregion

        # region SCP v2 Properties

        maxinputs = ConfigurationSetting(
            doc="""
            Specifies the maximum number of events that can be passed to the command for each invocation.

            This limit cannot exceed the value of `maxresultrows` in limits.conf_. Under SCP 1 you must specify this
            value in commands.conf_.

            Default: The value of `maxresultrows`.

            Supported by: SCP 2

            .. _limits.conf: http://docs.splunk.com/Documentation/Splunk/latest/admin/Limitsconf

            """
        )

        run_in_preview = ConfigurationSetting(
            doc="""
            :const:`True`, if this command should be run to generate results for preview; not wait for final output.

            This may be important for commands that have side effects (e.g., outputlookup).

            Default: :const:`True`

            Supported by: SCP 2

            """
        )

        type = ConfigurationSetting(
            readonly=True,
            value="reporting",
            doc="""
            Command type name.

            Fixed: :const:`'reporting'`.

            Supported by: SCP 2

            """,
        )

        # endregion

        # region Methods

        @classmethod
        def fix_up(cls, command):
            """Verifies :code:`command` class structure and configures the :code:`command.map` method.

            Verifies that :code:`command` derives from :class:`ReportingCommand` and overrides
            :code:`ReportingCommand.reduce`. It then configures :code:`command.reduce`, if an overriding implementation
            of :code:`ReportingCommand.reduce` has been provided.

            :param command: :code:`ReportingCommand` class

            Exceptions:

            :code:`TypeError` :code:`command` class is not derived from :code:`ReportingCommand`
            :code:`AttributeError` No :code:`ReportingCommand.reduce` override

            """
            if not issubclass(command, ReportingCommand):
                raise TypeError(f"{command} is not a ReportingCommand")

            if command.reduce == ReportingCommand.reduce:
                raise AttributeError("No ReportingCommand.reduce override")

            if command.map == ReportingCommand.map:
                cls._requires_preop = False
                return

            f = vars(command)["map"]  # Function backing the map method

            # EXPLANATION OF PREVIOUS STATEMENT: There is no way to add custom attributes to methods. See [Why does
            # setattr fail on a method](http://stackoverflow.com/questions/7891277/why-does-setattr-fail-on-a-bound-method) for a discussion of this issue.

            try:
                settings = f._settings
            except AttributeError:
                f.ConfigurationSettings = StreamingCommand.ConfigurationSettings
                return

            # Create new StreamingCommand.ConfigurationSettings class

            module = command.__module__ + "." + command.__name__ + ".map"
            name = b"ConfigurationSettings"
            bases = (StreamingCommand.ConfigurationSettings,)

            f.ConfigurationSettings = ConfigurationSettingsType(module, name, bases)
            ConfigurationSetting.fix_up(f.ConfigurationSettings, settings)
            del f._settings

        # endregion

    # endregion
