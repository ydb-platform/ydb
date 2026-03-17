# Copyright 2011-2015 Splunk, Inc.
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

from __future__ import absolute_import
from io import TextIOBase
from splunklib.six import ensure_text

try:
    import xml.etree.cElementTree as ET
except ImportError as ie:
    import xml.etree.ElementTree as ET

class Event(object):
    """Represents an event or fragment of an event to be written by this modular input to Splunk.

    To write an input to a stream, call the ``write_to`` function, passing in a stream.
    """
    def __init__(self, data=None, stanza=None, time=None, host=None, index=None, source=None,
                 sourcetype=None, done=True, unbroken=True):
        """There are no required parameters for constructing an Event

        **Example with minimal configuration**::

            my_event = Event(
                data="This is a test of my new event.",
                stanza="myStanzaName",
                time="%.3f" % 1372187084.000
            )

        **Example with full configuration**::

            excellent_event = Event(
                data="This is a test of my excellent event.",
                stanza="excellenceOnly",
                time="%.3f" % 1372274622.493,
                host="localhost",
                index="main",
                source="Splunk",
                sourcetype="misc",
                done=True,
                unbroken=True
            )

        :param data: ``string``, the event's text.
        :param stanza: ``string``, name of the input this event should be sent to.
        :param time: ``float``, time in seconds, including up to 3 decimal places to represent milliseconds.
        :param host: ``string``, the event's host, ex: localhost.
        :param index: ``string``, the index this event is specified to write to, or None if default index.
        :param source: ``string``, the source of this event, or None to have Splunk guess.
        :param sourcetype: ``string``, source type currently set on this event, or None to have Splunk guess.
        :param done: ``boolean``, is this a complete ``Event``? False if an ``Event`` fragment.
        :param unbroken: ``boolean``, Is this event completely encapsulated in this ``Event`` object?
        """
        self.data = data
        self.done = done
        self.host = host
        self.index = index
        self.source = source
        self.sourceType = sourcetype
        self.stanza = stanza
        self.time = time
        self.unbroken = unbroken

    def write_to(self, stream):
        """Write an XML representation of self, an ``Event`` object, to the given stream.

        The ``Event`` object will only be written if its data field is defined,
        otherwise a ``ValueError`` is raised.

        :param stream: stream to write XML to.
        """
        if self.data is None:
            raise ValueError("Events must have at least the data field set to be written to XML.")

        event = ET.Element("event")
        if self.stanza is not None:
            event.set("stanza", self.stanza)
        event.set("unbroken", str(int(self.unbroken)))

        # if a time isn't set, let Splunk guess by not creating a <time> element
        if self.time is not None:
            ET.SubElement(event, "time").text = str(self.time)

        # add all other subelements to this Event, represented by (tag, text)
        subelements = [
            ("source", self.source),
            ("sourcetype", self.sourceType),
            ("index", self.index),
            ("host", self.host),
            ("data", self.data)
        ]
        for node, value in subelements:
            if value is not None:
                ET.SubElement(event, node).text = value

        if self.done:
            ET.SubElement(event, "done")

        if isinstance(stream, TextIOBase):
            stream.write(ensure_text(ET.tostring(event)))
        else:
            stream.write(ET.tostring(event))
        stream.flush()