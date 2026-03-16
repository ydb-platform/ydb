from mammoth.docx.xmlparser import element as xml_element
from mammoth.docx.relationships_xml import read_relationships_xml_element
from ..testing import assert_equal


def test_relationship_targets_can_be_found_by_id():
    element = xml_element("relationships:Relationships", {}, [
        xml_element("relationships:Relationship", {
            "Id": "rId8",
            "Type": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/hyperlink",
            "Target": "http://example.com",
        }),
        xml_element("relationships:Relationship", {
            "Id": "rId2",
            "Type": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/hyperlink",
            "Target": "http://example.net",
        }),
    ])
    relationships = read_relationships_xml_element(element)
    assert_equal(
        "http://example.com",
        relationships.find_target_by_relationship_id("rId8"),
    )


def test_relationship_targets_can_be_found_by_type():
    element = xml_element("relationships:Relationships", {}, [
        xml_element("relationships:Relationship", {
            "Id": "rId2",
            "Target": "docProps/core.xml",
            "Type": "http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties",
        }),
        xml_element("relationships:Relationship", {
            "Id": "rId1",
            "Target": "word/document.xml",
            "Type": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument",
        }),
        xml_element("relationships:Relationship", {
            "Id": "rId3",
            "Target": "word/document2.xml",
            "Type": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument",
        }),
    ])
    relationships = read_relationships_xml_element(element)
    assert_equal(
        ["word/document.xml", "word/document2.xml"],
        relationships.find_targets_by_type("http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument"),
    )


def test_when_there_are_no_relationships_of_requested_type_then_empty_list_is_returned():
    element = xml_element("relationships:Relationships", {}, [])
    relationships = read_relationships_xml_element(element)
    assert_equal(
        [],
        relationships.find_targets_by_type("http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument"),
    )
