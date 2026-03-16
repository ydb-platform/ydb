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

from abc import ABCMeta, abstractmethod
import sys
import xml.etree.ElementTree as ET
from urllib.parse import urlsplit

from ..client import Service
from .event_writer import EventWriter
from .input_definition import InputDefinition
from .validation_definition import ValidationDefinition


class Script(metaclass=ABCMeta):
    """An abstract base class for implementing modular inputs.

    Subclasses should override ``get_scheme``, ``stream_events``,
    and optionally ``validate_input`` if the modular input uses
    external validation.

    The ``run`` function is used to run modular inputs; it typically should
    not be overridden.
    """

    def __init__(self):
        self._input_definition = None
        self._service = None

    def run(self, args):
        """Runs this modular input

        :param args: List of command line arguments passed to this script.
        :returns: An integer to be used as the exit value of this program.
        """

        # call the run_script function, which handles the specifics of running
        # a modular input
        return self.run_script(args, EventWriter(), sys.stdin)

    def run_script(self, args, event_writer, input_stream):
        """Handles all the specifics of running a modular input

        :param args: List of command line arguments passed to this script.
        :param event_writer: An ``EventWriter`` object for writing events.
        :param input_stream: An input stream for reading inputs.
        :returns: An integer to be used as the exit value of this program.
        """

        try:
            if len(args) == 1:
                # This script is running as an input. Input definitions will be
                # passed on stdin as XML, and the script will write events on
                # stdout and log entries on stderr.
                self._input_definition = InputDefinition.parse(input_stream)
                self.stream_events(self._input_definition, event_writer)
                event_writer.close()
                return 0

            if str(args[1]).lower() == "--scheme":
                # Splunk has requested XML specifying the scheme for this
                # modular input Return it and exit.
                scheme = self.get_scheme()
                if scheme is None:
                    event_writer.log(
                        EventWriter.FATAL,
                        "Modular input script returned a null scheme.",
                    )
                    return 1
                event_writer.write_xml_document(scheme.to_xml())
                return 0

            if args[1].lower() == "--validate-arguments":
                validation_definition = ValidationDefinition.parse(input_stream)
                try:
                    self.validate_input(validation_definition)
                    return 0
                except Exception as e:
                    root = ET.Element("error")
                    ET.SubElement(root, "message").text = str(e)
                    event_writer.write_xml_document(root)

                    return 1
            event_writer.log(
                EventWriter.ERROR,
                "Invalid arguments to modular input script:" + " ".join(args),
            )
            return 1

        except Exception as e:
            event_writer.log_exception(str(e))
            return 1

    @property
    def service(self):
        """Returns a Splunk service object for this script invocation.

        The service object is created from the Splunkd URI and session key
        passed to the command invocation on the modular input stream. It is
        available as soon as the :code:`Script.stream_events` method is
        called.

        :return: :class:`splunklib.client.Service`. A value of None is returned,
            if you call this method before the :code:`Script.stream_events` method
            is called.

        """
        if self._service is not None:
            return self._service

        if self._input_definition is None:
            return None

        splunkd_uri = self._input_definition.metadata["server_uri"]
        session_key = self._input_definition.metadata["session_key"]

        splunkd = urlsplit(splunkd_uri, allow_fragments=False)

        self._service = Service(
            scheme=splunkd.scheme,
            host=splunkd.hostname,
            port=splunkd.port,
            token=session_key,
        )

        return self._service

    @abstractmethod
    def get_scheme(self):
        """The scheme defines the parameters understood by this modular input.

        :return: a ``Scheme`` object representing the parameters for this modular input.
        """

    def validate_input(self, definition):
        """Handles external validation for modular input kinds.

        When Splunk calls a modular input script in validation mode, it will
        pass in an XML document giving information about the Splunk instance (so
        you can call back into it if needed) and the name and parameters of the
        proposed input.

        If this function does not throw an exception, the validation is assumed
        to succeed. Otherwise any errors thrown will be turned into a string and
        logged back to Splunk.

        The default implementation always passes.

        :param definition: The parameters for the proposed input passed by splunkd.
        """

    @abstractmethod
    def stream_events(self, inputs, ew):
        """The method called to stream events into Splunk. It should do all of its output via
        EventWriter rather than assuming that there is a console attached.

        :param inputs: An ``InputDefinition`` object.
        :param ew: An object with methods to write events and log messages to Splunk.
        """
