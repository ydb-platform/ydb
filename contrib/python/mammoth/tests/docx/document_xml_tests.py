import pytest

from mammoth import documents
from mammoth.docx.xmlparser import element as xml_element, text as xml_text
from mammoth.docx.document_xml import read_document_xml_element
from mammoth.docx import body_xml
from ..testing import assert_equal


def test_when_body_element_is_present_then_body_is_read():
    text_xml = xml_element("w:t", {}, [xml_text("Hello!")])
    run_xml = xml_element("w:r", {}, [text_xml])
    paragraph_xml = xml_element("w:p", {}, [run_xml])
    body_xml = xml_element("w:body", {}, [paragraph_xml])
    document_xml = xml_element("w:document", {}, [body_xml])

    document = _read_and_get_document_xml_element(document_xml)

    assert_equal(
        documents.document([documents.paragraph([documents.run([documents.text("Hello!")])])]),
        document
    )


def test_when_body_element_is_not_present_then_error_is_raised():
    paragraph_xml = xml_element("w:p", {}, [])
    body_xml = xml_element("w:body2", {}, [paragraph_xml])
    document_xml = xml_element("w:document", {}, [body_xml])

    error = pytest.raises(ValueError, lambda: _read_and_get_document_xml_element(document_xml))

    assert_equal(str(error.value), "Could not find the body element: are you sure this is a docx file?")


def test_footnotes_of_document_are_read():
    notes = [documents.note("footnote", "4", [documents.paragraph([])])]

    body_xml = xml_element("w:body")
    document_xml = xml_element("w:document", {}, [body_xml])

    document = _read_and_get_document_xml_element(document_xml, notes=notes)
    footnote = document.notes.find_note("footnote", "4")
    assert_equal("4", footnote.note_id)
    assert isinstance(footnote.body[0], documents.Paragraph)


def _read_and_get_document_xml_element(*args, **kwargs):
    body_reader = body_xml.reader()
    result = read_document_xml_element(*args, body_reader=body_reader, **kwargs)
    assert_equal([], result.messages)
    return result.value
