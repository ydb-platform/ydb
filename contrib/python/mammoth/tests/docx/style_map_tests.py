import io
from zipfile import ZipFile

from mammoth.docx.style_map import write_style_map, read_style_map
from mammoth.zips import open_zip
from mammoth.docx import xmlparser as xml
from ..testing import assert_equal


def test_reading_embedded_style_map_on_document_without_embedded_style_map_returns_none():
    fileobj = _normal_docx()
    assert_equal(None, read_style_map(fileobj))


def test_writing_style_map_preserves_unrelated_files():
    fileobj = _normal_docx()
    write_style_map(fileobj, "p => h1")
    with open_zip(fileobj, "r") as zip_file:
        assert_equal("placeholder", zip_file.read_str("placeholder"))

def test_embedded_style_map_can_be_read_after_being_written():
    fileobj = _normal_docx()
    write_style_map(fileobj, "p => h1")
    assert_equal("p => h1", read_style_map(fileobj))


def test_embedded_style_map_is_written_to_separate_file():
    fileobj = _normal_docx()
    write_style_map(fileobj, "p => h1")
    with open_zip(fileobj, "r") as zip_file:
        assert_equal("p => h1", zip_file.read_str("mammoth/style-map"))


def test_embedded_style_map_is_referenced_in_relationships():
    fileobj = _normal_docx()
    write_style_map(fileobj, "p => h1")
    assert_equal(expected_relationships_xml, _read_relationships_xml(fileobj))

def test_embedded_style_map_has_override_content_type_in_content_types_xml():
    fileobj = _normal_docx()
    write_style_map(fileobj, "p => h1")
    assert_equal(expected_content_types_xml, _read_content_types_xml(fileobj))


def test_can_overwrite_existing_style_map():
    fileobj = _normal_docx()
    write_style_map(fileobj, "p => h1")
    write_style_map(fileobj, "p => h2")
    with open_zip(fileobj, "r") as zip_file:
        assert_equal("p => h2", read_style_map(fileobj))
        _assert_no_duplicates(zip_file._zip_file.namelist())
        assert_equal(expected_relationships_xml, _read_relationships_xml(fileobj))
        assert_equal(expected_content_types_xml, _read_content_types_xml(fileobj))


def _read_relationships_xml(fileobj):
    with open_zip(fileobj, "r") as zip_file:
        return xml.parse_xml(
            io.StringIO(zip_file.read_str("word/_rels/document.xml.rels")),
            [("r", "http://schemas.openxmlformats.org/package/2006/relationships")],
        )


def _read_content_types_xml(fileobj):
    with open_zip(fileobj, "r") as zip_file:
        return xml.parse_xml(
            io.StringIO(zip_file.read_str("[Content_Types].xml")),
            [("ct", "http://schemas.openxmlformats.org/package/2006/content-types")],
        )


original_relationships_xml = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
    '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">' +
    '<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/settings" Target="settings.xml"/>' +
    '</Relationships>')

expected_relationships_xml = xml.element("r:Relationships", {}, [
    xml.element("r:Relationship", {"Id": "rId3", "Type": "http://schemas.openxmlformats.org/officeDocument/2006/relationships/settings", "Target": "settings.xml"}),
    xml.element("r:Relationship", {"Id": "rMammothStyleMap", "Type": "http://schemas.zwobble.org/mammoth/style-map", "Target": "/mammoth/style-map"}),
])

original_content_types_xml = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
    '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">' +
    '<Default Extension="png" ContentType="image/png"/>' +
    '</Types>'
)

expected_content_types_xml = xml.element("ct:Types", {}, [
    xml.element("ct:Default", {"Extension": "png", "ContentType": "image/png"}),
    xml.element("ct:Override", {"PartName": "/mammoth/style-map", "ContentType": "text/prs.mammoth.style-map"}),
])


def _normal_docx():
    fileobj = io.BytesIO()
    zip_file = ZipFile(fileobj, "w")
    try:
        zip_file.writestr("placeholder", "placeholder")
        zip_file.writestr("word/_rels/document.xml.rels", original_relationships_xml)
        zip_file.writestr("[Content_Types].xml", original_content_types_xml)
        expected_relationships_xml
    finally:
        zip_file.close()
    return fileobj


def _assert_no_duplicates(values):
    counts = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    for value, count in counts.items():
        if count != 1:
            assert False, "{0} has count of {1}".format(value, count)
