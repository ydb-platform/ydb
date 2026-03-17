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


import xml.etree.ElementTree as ET

from .utils import parse_xml_data


class ValidationDefinition:
    """This class represents the XML sent by Splunk for external validation of a
    new modular input.

    **Example**::

        v = ValidationDefinition()

    """

    def __init__(self):
        self.metadata = {}
        self.parameters = {}

    def __eq__(self, other):
        if not isinstance(other, ValidationDefinition):
            return False
        return self.metadata == other.metadata and self.parameters == other.parameters

    @staticmethod
    def parse(stream):
        """Creates a ``ValidationDefinition`` from a provided stream containing XML.

        The XML typically will look like this:

        ..  code-block:: xml

            <items>
               <server_host>myHost</server_host>
                 <server_uri>https://127.0.0.1:8089</server_uri>
                 <session_key>123102983109283019283</session_key>
                 <checkpoint_dir>/opt/splunk/var/lib/splunk/modinputs</checkpoint_dir>
                 <item name="myScheme">
                   <param name="param1">value1</param>
                   <param_list name="param2">
                     <value>value2</value>
                     <value>value3</value>
                     <value>value4</value>
                   </param_list>
                 </item>
            </items>

        :param stream: ``Stream`` containing XML to parse.
        :return: A ``ValidationDefinition`` object.

        """

        definition = ValidationDefinition()

        # parse XML from the stream, then get the root node
        root = ET.parse(stream).getroot()

        for node in root:
            # lone item node
            if node.tag == "item":
                # name from item node
                definition.metadata["name"] = node.get("name")
                definition.parameters = parse_xml_data(node, "")
            else:
                # Store anything else in metadata
                definition.metadata[node.tag] = node.text

        return definition
