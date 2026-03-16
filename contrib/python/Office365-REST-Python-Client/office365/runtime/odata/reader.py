from xml.etree import ElementTree as ET

from office365.runtime.odata.model import ODataModel
from office365.runtime.odata.property import ODataProperty
from office365.runtime.odata.type import ODataType


class ODataReader(object):
    """OData reader"""

    def __init__(self, metadata_path, xml_namespaces):
        """
        :type metadata_path: string
        :type xml_namespaces: dict
        """
        self._metadata_path = metadata_path
        self._xml_namespaces = xml_namespaces

    def format_file(self):
        import xml.dom.minidom

        with open(self._metadata_path, "r", encoding="utf8") as in_file:
            metadata_content = in_file.read()

        formatted_metadata_content = xml.dom.minidom.parseString(
            metadata_content
        ).toprettyxml()

        with open(self._metadata_path, "w", encoding="utf8") as out_file:
            out_file.write(formatted_metadata_content)

    def process_schema_node(self, model):
        """
        :type model: ODataModel
        """
        root = ET.parse(self._metadata_path).getroot()
        schema_node = root.find("edmx:DataServices/xmlns:Schema", self._xml_namespaces)
        for type_node in schema_node.findall("xmlns:ComplexType", self._xml_namespaces):
            type_schema = self.process_type_node(type_node, schema_node)
            model.add_type(type_schema)

    def process_type_node(self, type_node, schema_node):
        """
        :type type_node: xml.etree.ElementTree.Element
        :type schema_node: xml.etree.ElementTree.Element
        """
        type_schema = ODataType()
        type_schema.namespace = schema_node.attrib["Namespace"]
        type_schema.name = type_node.get("Name")
        type_schema.baseType = "ComplexType"

        for prop_node in type_node.findall("xmlns:Property", self._xml_namespaces):
            prop_schema = self.process_property_node(prop_node)
            type_schema.add_property(prop_schema)

        return type_schema

    def process_method_node(self):
        pass

    def process_property_node(self, node):
        """
        :type node:  xml.etree.ElementTree.Element
        """
        prop_schema = ODataProperty()
        prop_schema.name = node.get("Name")
        return prop_schema

    def generate_model(self):
        model = ODataModel()
        self.process_schema_node(model)
        return model
