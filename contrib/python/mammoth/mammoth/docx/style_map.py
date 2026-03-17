from xml.etree import ElementTree

from ..zips import open_zip, update_zip


_style_map_path = "mammoth/style-map"
_style_map_absolute_path = "/" + _style_map_path
_relationships_path = "word/_rels/document.xml.rels"
_content_types_path = "[Content_Types].xml"


def write_style_map(fileobj, style_map):
    with open_zip(fileobj, "r") as zip_file:
        relationships_xml = _generate_relationships_xml(zip_file.read_str(_relationships_path))
        content_types_xml = _generate_content_types_xml(zip_file.read_str(_content_types_path))
    
    update_zip(fileobj, {
        _style_map_path: style_map.encode("utf8"),
        _relationships_path: relationships_xml,
        _content_types_path: content_types_xml,
    })

def _generate_relationships_xml(relationships_xml):
    schema = "http://schemas.zwobble.org/mammoth/style-map"
    relationships_uri = "http://schemas.openxmlformats.org/package/2006/relationships"
    relationship_element_name = "{" + relationships_uri + "}Relationship"
    
    relationships = ElementTree.fromstring(relationships_xml)
    _add_or_update_element(relationships, relationship_element_name, "Id", {
        "Id": "rMammothStyleMap",
        "Type": schema,
        "Target": _style_map_absolute_path,
    })

    return ElementTree.tostring(relationships, "UTF-8")


def _generate_content_types_xml(content_types_xml):
    content_types_uri = "http://schemas.openxmlformats.org/package/2006/content-types"
    override_name = "{" + content_types_uri + "}Override"
    
    types = ElementTree.fromstring(content_types_xml)
    _add_or_update_element(types, override_name, "PartName", {
        "PartName": _style_map_absolute_path,
        "ContentType": "text/prs.mammoth.style-map",
    })
    
    return ElementTree.tostring(types, "UTF-8")


def _add_or_update_element(parent, name, identifying_attribute, attributes):
    existing_child = _find_child(parent, name, identifying_attribute, attributes)
    if existing_child is None:
        ElementTree.SubElement(parent, name, attributes)
    else:
        existing_child.attrib = attributes
    

def _find_child(parent, name, identifying_attribute, attributes):
    for element in parent.iter():
        if element.tag == name and element.get(identifying_attribute) == attributes.get(identifying_attribute):
            return element


def read_style_map(fileobj):
    with open_zip(fileobj, "r") as zip_file:
        if zip_file.exists(_style_map_path):
            return zip_file.read_str(_style_map_path)


