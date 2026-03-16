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


class Argument:
    """Class representing an argument to a modular input kind.

    ``Argument`` is meant to be used with ``Scheme`` to generate an XML
    definition of the modular input kind that Splunk understands.

    ``name`` is the only required parameter for the constructor.

        **Example with least parameters**::

            arg1 = Argument(name="arg1")

        **Example with all parameters**::

            arg2 = Argument(
                name="arg2",
                description="This is an argument with lots of parameters",
                validation="is_pos_int('some_name')",
                data_type=Argument.data_type_number,
                required_on_edit=True,
                required_on_create=True
            )
    """

    # Constant values, do not change.
    # These should be used for setting the value of an Argument object's data_type field.
    data_type_boolean = "BOOLEAN"
    data_type_number = "NUMBER"
    data_type_string = "STRING"

    def __init__(
        self,
        name,
        description=None,
        validation=None,
        data_type=data_type_string,
        required_on_edit=False,
        required_on_create=False,
        title=None,
    ):
        """
        :param name: ``string``, identifier for this argument in Splunk.
        :param description: ``string``, human-readable description of the argument.
        :param validation: ``string`` specifying how the argument should be validated, if using internal validation.
               If using external validation, this will be ignored.
        :param data_type: ``string``, data type of this field; use the class constants.
               "data_type_boolean", "data_type_number", or "data_type_string".
        :param required_on_edit: ``Boolean``, whether this arg is required when editing an existing modular input of this kind.
        :param required_on_create: ``Boolean``, whether this arg is required when creating a modular input of this kind.
        :param title: ``String``, a human-readable title for the argument.
        """
        self.name = name
        self.description = description
        self.validation = validation
        self.data_type = data_type
        self.required_on_edit = required_on_edit
        self.required_on_create = required_on_create
        self.title = title

    def add_to_document(self, parent):
        """Adds an ``Argument`` object to this ElementTree document.

        Adds an <arg> subelement to the parent element, typically <args>
        and sets up its subelements with their respective text.

        :param parent: An ``ET.Element`` to be the parent of a new <arg> subelement
        :returns: An ``ET.Element`` object representing this argument.
        """
        arg = ET.SubElement(parent, "arg")
        arg.set("name", self.name)

        if self.title is not None:
            ET.SubElement(arg, "title").text = self.title

        if self.description is not None:
            ET.SubElement(arg, "description").text = self.description

        if self.validation is not None:
            ET.SubElement(arg, "validation").text = self.validation

        # add all other subelements to this Argument, represented by (tag, text)
        subelements = [
            ("data_type", self.data_type),
            ("required_on_edit", self.required_on_edit),
            ("required_on_create", self.required_on_create),
        ]

        for name, value in subelements:
            ET.SubElement(arg, name).text = str(value).lower()

        return arg
