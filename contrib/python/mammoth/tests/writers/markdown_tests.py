from __future__ import unicode_literals

from mammoth.writers.markdown import MarkdownWriter
from ..testing import assert_equal


def test_special_markdown_characters_are_escaped():
    writer = _create_writer()
    writer.text(r"\*")
    assert_equal(r"\\\*", writer.as_string())


def test_unrecognised_elements_are_treated_as_normal_text():
    writer = _create_writer()
    writer.start("blah");
    writer.text("Hello");
    writer.end("blah");
    assert_equal("Hello", writer.as_string())


def test_paragraphs_are_terminated_with_double_new_line():
    writer = _create_writer()
    writer.start("p");
    writer.text("Hello");
    writer.end("p");
    assert_equal("Hello\n\n", writer.as_string())


def test_h1_elements_are_converted_to_heading_with_leading_hash():
    writer = _create_writer()
    writer.start("h1");
    writer.text("Hello");
    writer.end("h1");
    assert_equal("# Hello\n\n", writer.as_string())


def test_h6_elements_are_converted_to_heading_with_six_leading_hashes():
    writer = _create_writer()
    writer.start("h6");
    writer.text("Hello");
    writer.end("h6");
    assert_equal("###### Hello\n\n", writer.as_string())


def test_br_is_written_as_two_spaces_followed_by_newline():
    writer = _create_writer()
    writer.text("Hello");
    writer.self_closing("br");
    assert_equal("Hello  \n", writer.as_string())


def test_strong_text_is_surrounded_by_two_underscores():
    writer = _create_writer()
    writer.text("Hello ");
    writer.start("strong");
    writer.text("World")
    writer.end("strong")
    assert_equal("Hello __World__", writer.as_string())


def test_emphasised_text_is_surrounded_by_one_asterix():
    writer = _create_writer()
    writer.text("Hello ");
    writer.start("em");
    writer.text("World")
    writer.end("em")
    assert_equal("Hello *World*", writer.as_string())


def test_anchor_tags_are_written_as_hyperlinks():
    writer = _create_writer()
    writer.start("a", {"href": "http://example.com"});
    writer.text("Hello");
    writer.end("a");
    assert_equal("[Hello](http://example.com)", writer.as_string())


def test_anchor_tags_without_href_attribute_are_treated_as_ordinary_text():
    writer = _create_writer()
    writer.start("a");
    writer.text("Hello");
    writer.end("a");
    assert_equal("Hello", writer.as_string())


def test_elements_with_ids_have_anchor_tags_with_ids_appended_to_start_of_markdown_element():
    writer = _create_writer()
    writer.start("h1", {"id": "start"})
    writer.text("Hello")
    writer.end("h1")
    assert_equal('# <a id="start"></a>Hello\n\n', writer.as_string())


def test_links_have_anchors_before_opening_square_bracket():
    writer = _create_writer()
    writer.start("a", {"href": "http://example.com", "id": "start"})
    writer.text("Hello")
    writer.end("a")
    assert_equal('<a id="start"></a>[Hello](http://example.com)', writer.as_string())


def test_image_elements_are_written_as_markdown_images():
    writer = _create_writer()
    writer.self_closing("img", {"src": "http://example.com/image.jpg", "alt": "Alt Text"})
    assert_equal("![Alt Text](http://example.com/image.jpg)", writer.as_string())


def test_images_are_written_even_if_they_dont_have_alt_text():
    writer = _create_writer()
    writer.self_closing("img", {"src": "http://example.com/image.jpg"})
    assert_equal("![](http://example.com/image.jpg)", writer.as_string())


def test_images_are_written_even_if_they_dont_have_a_src_attribute():
    writer = _create_writer()
    writer.self_closing("img", {"alt": "Alt Text"})
    assert_equal("![Alt Text]()", writer.as_string())


def test_image_elements_are_ignored_if_they_have_no_src_and_no_alt_text():
    writer = _create_writer()
    writer.self_closing("img")
    assert_equal("", writer.as_string())


def test_list_item_outside_of_list_is_treated_as_unordered_list():
    writer = _create_writer()
    writer.start("li")
    writer.text("Fruit")
    writer.end("li")
    assert_equal("- Fruit\n", writer.as_string())


def test_ol_element_is_written_as_ordered_list_with_sequential_numbering():
    writer = _create_writer()
    writer.start("ol")
    writer.start("li")
    writer.text("Fruit")
    writer.end("li")
    writer.start("li")
    writer.text("Condiments")
    writer.end("li")
    writer.end("ol")
    assert_equal("1. Fruit\n2. Condiments\n\n", writer.as_string())


def test_ul_element_is_written_as_unordered_list_using_hyphens_as_bullets():
    writer = _create_writer()
    writer.start("ul")
    writer.start("li")
    writer.text("Fruit")
    writer.end("li")
    writer.start("li")
    writer.text("Condiments")
    writer.end("li")
    writer.end("ul")
    assert_equal("- Fruit\n- Condiments\n\n", writer.as_string())


def test_numbering_is_separate_for_nested_list_and_parent_list():
    writer = _create_writer()
    writer.start("ol")

    writer.start("li")
    writer.text("Fruit")
    writer.start("ol")
    writer.start("li")
    writer.text("Apple")
    writer.end("li")
    writer.start("li")
    writer.text("Banana")
    writer.end("li")
    writer.end("ol")
    writer.end("li")

    writer.start("li")
    writer.text("Condiments")
    writer.end("li")
    writer.end("ol")
    assert_equal("1. Fruit\n\t1. Apple\n\t2. Banana\n2. Condiments\n\n", writer.as_string())



def _create_writer():
    return MarkdownWriter()
