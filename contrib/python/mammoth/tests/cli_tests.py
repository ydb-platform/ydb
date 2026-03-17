import os
import base64

import spur
import tempman

from .testing import assert_equal, generate_test_path


_local = spur.LocalShell()


def test_html_is_printed_to_stdout_if_output_file_is_not_set():
    docx_path = generate_test_path("single-paragraph.docx")
    result = _local.run(["mammoth", docx_path])
    assert_equal(b"", result.stderr_output)
    assert_equal(b"<p>Walking on imported air</p>", result.output)


def test_html_is_written_to_file_if_output_file_is_set():
    with tempman.create_temp_dir() as temp_dir:
        output_path = os.path.join(temp_dir.path, "output.html")
        docx_path = generate_test_path("single-paragraph.docx")
        result = _local.run(["mammoth", docx_path, output_path])
        assert_equal(b"", result.stderr_output)
        assert_equal(b"", result.output)
        with open(output_path) as output_file:
            assert_equal("<p>Walking on imported air</p>", output_file.read())


_image_base_64 = b"iVBORw0KGgoAAAANSUhEUgAAAAoAAAAKCAIAAAACUFjqAAAAAXNSR0IArs4c6QAAAAlwSFlzAAAOvgAADr4B6kKxwAAAABNJREFUKFNj/M+ADzDhlWUYqdIAQSwBE8U+X40AAAAASUVORK5CYII="


def test_inline_images_are_included_in_output_if_writing_to_single_file():
    docx_path = generate_test_path("tiny-picture.docx")
    result = _local.run(["mammoth", docx_path])
    assert_equal(b"""<p><img src="data:image/png;base64,""" + _image_base_64 + b"""" /></p>""", result.output)


def test_images_are_written_to_separate_files_if_output_dir_is_set():
    with tempman.create_temp_dir() as temp_dir:
        output_path = os.path.join(temp_dir.path, "tiny-picture.html")
        image_path = os.path.join(temp_dir.path, "1.png")

        docx_path = generate_test_path("tiny-picture.docx")
        result = _local.run(["mammoth", docx_path, "--output-dir", temp_dir.path])
        assert_equal(b"", result.stderr_output)
        assert_equal(b"", result.output)
        with open(output_path) as output_file:
            assert_equal("""<p><img src="1.png" /></p>""", output_file.read())

        with open(image_path, "rb") as image_file:
            assert_equal(_image_base_64, base64.b64encode(image_file.read()))


def test_style_map_is_used_if_set():
    with tempman.create_temp_dir() as temp_dir:
        docx_path = generate_test_path("single-paragraph.docx")
        style_map_path = os.path.join(temp_dir.path, "style-map")
        with open(style_map_path, "w") as style_map_file:
            style_map_file.write("p => span:fresh")
        result = _local.run(["mammoth", docx_path, "--style-map", style_map_path])
        assert_equal(b"", result.stderr_output)
        assert_equal(b"<span>Walking on imported air</span>", result.output)


def test_output_format_markdown_option_generates_markdown_output():
    docx_path = generate_test_path("single-paragraph.docx")
    result = _local.run(["mammoth", docx_path, "--output-format=markdown"])
    assert_equal(b"", result.stderr_output)
    assert_equal(b"Walking on imported air\n\n", result.output)
