from mammoth import documents
from mammoth.docx.xmlparser import element as xml_element
from mammoth.docx.notes_xml import read_footnotes_xml_element
from mammoth.docx import body_xml
from ..testing import assert_equal


def test_id_and_body_of_footnote_are_read():
    footnote_body = [xml_element("w:p")]
    footnotes = read_footnotes_xml_element(xml_element("w:footnotes", {}, [
        xml_element("w:footnote", {"w:id": "1"}, footnote_body),
    ]), body_reader=body_xml.reader())
    assert_equal(1, len(footnotes.value))
    assert isinstance(footnotes.value[0].body[0], documents.Paragraph)
    assert_equal("1", footnotes.value[0].note_id)


def test_continuation_separator_is_ignored():
    _assert_footnote_type_is_ignored("continuationSeparator")


def test_separator_is_ignored():
    _assert_footnote_type_is_ignored("separator")


def _assert_footnote_type_is_ignored(footnote_type):
    footnote_body = [xml_element("w:p")]
    footnotes = read_footnotes_xml_element(xml_element("w:footnotes", {}, [
        xml_element("w:footnote", {"w:id": "1", "w:type": footnote_type}, footnote_body),
    ]), body_reader=None)
    assert_equal(0, len(footnotes.value))

