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
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

class Scheme(object):
    """Class representing the metadata for a modular input kind.

    A ``Scheme`` specifies a title, description, several options of how Splunk should run modular inputs of this
    kind, and a set of arguments which define a particular modular input's properties.

    The primary use of ``Scheme`` is to abstract away the construction of XML to feed to Splunk.
    """

    # Constant values, do not change
    # These should be used for setting the value of a Scheme object's streaming_mode field.
    streaming_mode_simple = "SIMPLE"
    streaming_mode_xml = "XML"

    def __init__(self, title):
        """
        :param title: ``string`` identifier for this Scheme in Splunk.
        """
        self.title = title
        self.description = None
        self.use_external_validation = True
        self.use_single_instance = False
        self.streaming_mode = Scheme.streaming_mode_xml

        # list of Argument objects, each to be represented by an <arg> tag
        self.arguments = []

    def add_argument(self, arg):
        """Add the provided argument, ``arg``, to the ``self.arguments`` list.

        :param arg: An ``Argument`` object to add to ``self.arguments``.
        """
        self.arguments.append(arg)

    def to_xml(self):
        """Creates an ``ET.Element`` representing self, then returns it.

        :returns: an ``ET.Element`` representing this scheme.
        """
        root = ET.Element("scheme")

        ET.SubElement(root, "title").text = self.title

        # add a description subelement if it's defined
        if self.description is not None:
            ET.SubElement(root, "description").text = self.description

        # add all other subelements to this Scheme, represented by (tag, text)
        subelements = [
            ("use_external_validation", self.use_external_validation),
            ("use_single_instance", self.use_single_instance),
            ("streaming_mode", self.streaming_mode)
        ]
        for name, value in subelements:
            ET.SubElement(root, name).text = str(value).lower()

        endpoint = ET.SubElement(root, "endpoint")

        args = ET.SubElement(endpoint, "args")

        # add arguments as subelements to the <args> element
        for arg in self.arguments:
            arg.add_to_document(args)

        return root