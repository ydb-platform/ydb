from mammoth.docx.xmlparser import element as xml_element
from mammoth.docx.content_types_xml import read_content_types_xml_element
from ..testing import assert_equal


def test_content_type_is_based_on_default_for_extension_if_there_is_no_override():
    element = xml_element("content-types:Types", {}, [
        xml_element("content-types:Default", {
            "Extension": "png",
            "ContentType": "image/png",
        })
    ])
    content_types = read_content_types_xml_element(element)
    assert_equal(
        "image/png",
        content_types.find_content_type("word/media/hat.png"),
    )


def test_content_type_is_based_on_override_if_present():
    element = xml_element("content-types:Types", {}, [
        xml_element("content-types:Default", {
            "Extension": "png",
            "ContentType": "image/png",
        }),
        xml_element("content-types:Override", {
            "PartName": "/word/media/hat.png",
            "ContentType": "image/hat"
        }),
    ])
    content_types = read_content_types_xml_element(element)
    assert_equal(
        "image/hat",
        content_types.find_content_type("word/media/hat.png"),
    )


def test_fallback_content_types_have_common_image_types():
    element = xml_element("content-types:Types", {}, [])
    content_types = read_content_types_xml_element(element)
    assert_equal(
        "image/png",
        content_types.find_content_type("word/media/hat.png"),
    )
    assert_equal(
        "image/gif",
        content_types.find_content_type("word/media/hat.gif"),
    )
    assert_equal(
        "image/jpeg",
        content_types.find_content_type("word/media/hat.jpg"),
    )
    assert_equal(
        "image/jpeg",
        content_types.find_content_type("word/media/hat.jpeg"),
    )
    assert_equal(
        "image/bmp",
        content_types.find_content_type("word/media/hat.bmp"),
    )
    assert_equal(
        "image/tiff",
        content_types.find_content_type("word/media/hat.tif"),
    )
    assert_equal(
        "image/tiff",
        content_types.find_content_type("word/media/hat.tiff"),
    )


def test_fallback_content_types_are_case_insensitive():
    element = xml_element("content-types:Types", {}, [])
    content_types = read_content_types_xml_element(element)
    assert_equal(
        "image/png",
        content_types.find_content_type("word/media/hat.PnG"),
    )
