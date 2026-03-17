from mammoth import documents
from mammoth.docx.xmlparser import element as xml_element
from mammoth.docx.comments_xml import read_comments_xml_element
from mammoth.docx import body_xml
from ..testing import assert_equal


def test_id_and_body_of_comment_is_read():
    body = [xml_element("w:p")]
    comments = read_comments_xml_element(xml_element("w:comments", {}, [
        xml_element("w:comment", {"w:id": "1"}, body),
    ]), body_reader=body_xml.reader())
    assert_equal(1, len(comments.value))
    assert_equal(comments.value[0].body, [documents.paragraph(children=[])])
    assert_equal("1", comments.value[0].comment_id)


def test_when_optional_attributes_of_comment_are_missing_then_they_are_read_as_none():
    comments = read_comments_xml_element(xml_element("w:comments", {}, [
        xml_element("w:comment", {"w:id": "1"}, []),
    ]), body_reader=body_xml.reader())
    comment, = comments.value
    assert_equal(None, comment.author_name)
    assert_equal(None, comment.author_initials)


def test_when_optional_attributes_of_comment_are_blank_then_they_are_read_as_none():
    comments = read_comments_xml_element(xml_element("w:comments", {}, [
        xml_element("w:comment", {"w:id": "1", "w:author": " ", "w:initials": " "}, []),
    ]), body_reader=body_xml.reader())
    comment, = comments.value
    assert_equal(None, comment.author_name)
    assert_equal(None, comment.author_initials)


def test_when_optional_attributes_of_comment_are_not_blank_then_they_are_read():
    comments = read_comments_xml_element(xml_element("w:comments", {}, [
        xml_element("w:comment", {"w:id": "1", "w:author": "The Piemaker", "w:initials": "TP"}, []),
    ]), body_reader=body_xml.reader())
    comment, = comments.value
    assert_equal("The Piemaker", comment.author_name)
    assert_equal("TP", comment.author_initials)
