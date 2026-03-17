# Python
#
# This module implements tests for MdUtils class.
#
# This file is part of mdutils. https://github.com/didix21/mdutils
#
# MIT License: (C) 2018 Dídac Coll

from unittest import TestCase
from mdutils.mdutils import MdUtils
from mdutils.fileutils import MarkDownFile

from pathlib import Path
import os
import re


class TestMdUtils(TestCase):
    def setUp(self) -> None:
        self.expected_list = (
            "\n\n\n"
            "\n- Item 1\n"
            "- Item 2\n"
            "- Item 3\n"
            "- Item 4\n"
            "    - Item 4.1\n"
            "    - Item 4.2\n"
            "        - Item 4.2.1\n"
            "        - Item 4.2.2\n"
            "    - Item 4.3\n"
            "        - Item 4.3.1\n"
            "- Item 5\n"
        )
        self.complex_items = [
            "Item 1",
            "Item 2",
            "Item 3",
            "Item 4",
            [
                "Item 4.1",
                "Item 4.2",
                ["Item 4.2.1", "Item 4.2.2"],
                "Item 4.3",
                ["Item 4.3.1"],
            ],
            "Item 5",
        ]
        self.expected_bold_list = (
            "\n\n\n"
            "\n- **Item 1**\n"
            "- **Item 2**\n"
            "- **Item 3**\n"
            "- **Item 4**\n"
            "    - **Item 4.1**\n"
            "    - **Item 4.2**\n"
            "        - **Item 4.2.1**\n"
            "        - **Item 4.2.2**\n"
            "    - **Item 4.3**\n"
            "        - **Item 4.3.1**\n"
            "- **Item 5**\n"
        )
        self.complex_bold_items = [
            "**Item 1**",
            "**Item 2**",
            "**Item 3**",
            "**Item 4**",
            [
                "**Item 4.1**",
                "**Item 4.2**",
                ["**Item 4.2.1**", "**Item 4.2.2**"],
                "**Item 4.3**",
                ["**Item 4.3.1**"],
            ],
            "**Item 5**",
        ]

    def tearDown(self):
        md_file = Path("Test_file.md")
        if md_file.is_file():
            os.remove("Test_file.md")

    def test_create_md_file(self):
        md_file = MdUtils("Test_file")
        md_file.create_md_file()
        md_file_expect = Path("Test_file.md")
        if md_file_expect.is_file():
            os.remove("Test_file.md")
            pass
        else:
            self.fail()

    def test_new_header(self):
        file_name = "Test_file"
        md_file = MdUtils(file_name)
        string_headers_expected = (
            "\n# Header 0\n\n## Header 1\n\n### Header 2\n\n#### Header 3\n\n"
            "##### Header 4\n\n###### Header 5 {#header5}\n"
        )
        string_headers = ""
        for x in range(6):
            # The last header includes an ID
            if x == 5:
                string_headers += md_file.new_header(
                    level=(x + 1),
                    title="Header " + str(x),
                    style="atx",
                    header_id="header5",
                )
            else:
                string_headers += md_file.new_header(
                    level=(x + 1), title="Header " + str(x), style="atx"
                )

        self.assertEqual(string_headers, string_headers_expected)
        md_file.create_md_file()
        file_result = md_file.read_md_file(file_name)
        self.assertEqual(file_result, "\n\n\n" + string_headers_expected)

    def test_new_table_of_contents(self):
        # Create headers level 1 and 2.
        md_file = MdUtils(file_name="Test_file", title="Testing table of contents")
        list_headers = [
            "Header 1",
            "Header 1.1",
            "Header 2",
            "Header 2.2",
            "Header 2.3",
        ]
        table_of_content_title = MdUtils(file_name="").new_header(
            level=1, title="Index", style="setext"
        )
        md_file.new_header(level=1, title=list_headers[0])
        md_file.new_header(level=2, title=list_headers[1])
        md_file.new_header(level=1, title=list_headers[2])
        md_file.new_header(level=2, title=list_headers[3])
        md_file.new_header(level=2, title=list_headers[4])

        # Testing Depth 1
        table_of_contents_result = md_file.new_table_of_contents(
            table_title="Index", depth=1
        )
        table_of_content_expected = (
            table_of_content_title
            + "\n* ["
            + list_headers[0]
            + "](#"
            + re.sub("[^a-z0-9_\-]", "", list_headers[0].lower().replace(" ", "-"))
            + ")"
            + "\n* ["
            + list_headers[2]
            + "](#"
            + re.sub("[^a-z0-9_\-]", "", list_headers[2].lower().replace(" ", "-"))
            + ")\n"
        )
        self.assertEqual(table_of_contents_result, table_of_content_expected)
        # Testing created file
        md_file.create_md_file()
        data_file_result = MdUtils("").read_md_file("Test_file")
        data_file_expected = (
            MdUtils("").new_header(1, "Testing table of contents", "setext")
            + md_file.table_of_contents
            + md_file.file_data_text
        )
        self.assertEqual(data_file_result, data_file_expected)
        os.remove("Test_file.md")

        # Testing Depth 2
        md_file = MdUtils(file_name="Test_file", title="Testing table of contents")
        list_headers = [
            "Header 1",
            "Header 1.1",
            "Header 2",
            "Header 2.2",
            "Header 2.3",
        ]
        table_of_content_title = MdUtils(file_name="").new_header(
            level=1, title="Index", style="setext"
        )
        md_file.new_header(level=1, title=list_headers[0])
        md_file.new_header(level=2, title=list_headers[1])
        md_file.new_header(level=1, title=list_headers[2])
        md_file.new_header(level=2, title=list_headers[3])
        md_file.new_header(level=2, title=list_headers[4])

        table_of_contents_result = md_file.new_table_of_contents(
            table_title="Index", depth=2
        )
        table_of_content_expected = table_of_content_title
        for x in range(len(list_headers)):
            if x in (0, 2):
                table_of_content_expected += (
                    "\n* ["
                    + list_headers[x]
                    + "](#"
                    + re.sub(
                        "[^a-z0-9_\-]", "", list_headers[x].lower().replace(" ", "-")
                    )
                    + ")"
                )
            else:
                table_of_content_expected += (
                    "\n\t* ["
                    + list_headers[x]
                    + "](#"
                    + re.sub(
                        "[^a-z0-9_\-]", "", list_headers[x].lower().replace(" ", "-")
                    )
                    + ")"
                )
        table_of_content_expected += "\n"
        self.assertEqual(table_of_contents_result, table_of_content_expected)

        md_file.create_md_file()
        data_file_result = MdUtils("").read_md_file("Test_file")
        data_file_expected = (
            MdUtils("").new_header(1, "Testing table of contents", "setext")
            + md_file.table_of_contents
            + md_file.file_data_text
        )
        self.assertEqual(data_file_result, data_file_expected)
        os.remove("Test_file.md")

    def test_new_paragraph(self):
        md_file = MdUtils(file_name="Test_file", title="")
        created_value = md_file.new_paragraph(
            "This is a new paragraph created using new_paragraph method."
        )
        expected_value = (
            "\n\nThis is a new paragraph created using new_paragraph method."
        )
        self.assertEqual(created_value, expected_value)

    def test_new_line(self):
        md_file = MdUtils(file_name="Test_file", title="")
        created_value = md_file.new_line(
            "This is a new line created using new_line method."
        )
        expected_value = "\nThis is a new line created using new_line method."
        self.assertEqual(created_value, expected_value)

    def test_wrap_text(self):
        md_file = MdUtils(file_name="Test_file", title="")
        created_value = md_file.new_line(
            "This is a new line created using new_line method with wrapping.",
            wrap_width=25,
        )
        expected_value = (
            "\nThis is a new line \ncreated using new_line \nmethod with wrapping."
        )
        self.assertEqual(created_value, expected_value)

    def test_insert_code(self):
        md_file = MdUtils(file_name="Test_file")
        code = (
            "mdFile.new_header(level=1, title='Atx Header 1')\n"
            "mdFile.new_header(level=2, title='Atx Header 2')\n"
            "mdFile.new_header(level=3, title='Atx Header 3')\n"
            "mdFile.new_header(level=4, title='Atx Header 4')\n"
            "mdFile.new_header(level=5, title='Atx Header 5')\n"
            "mdFile.new_header(level=6, title='Atx Header 6')\n"
        )
        expects = "\n\n```\n" + code + "\n```"
        self.assertEqual(md_file.insert_code(code), expects)
        language = "python"
        expects = "\n\n```" + language + "\n" + code + "\n```"
        self.assertEqual(md_file.insert_code(code, language), expects)

    def test_new_inline_link(self):
        md_file = MdUtils(file_name="Test_file", title="")
        created_value = md_file.new_inline_link(
            link="https://github.com/didix21/mdutils", text="mdutils library"
        )
        expected_value = "[mdutils library](https://github.com/didix21/mdutils)"
        self.assertEqual(expected_value, created_value)

    def test_new_inline_link_text_empty(self):
        link = "https://github.com/didix21/mdutils"
        md_file = MdUtils(file_name="Test_file", title="")
        created_value = md_file.new_inline_link(link=link, text=link)
        expected_value = (
            "[https://github.com/didix21/mdutils](https://github.com/didix21/mdutils)"
        )

        self.assertEqual(expected_value, created_value)

    def test_new_inline_link_empty(self):
        md_file = MdUtils(file_name="Test_file", title="")
        try:
            md_file.new_inline_link()
        except TypeError:
            return

        self.fail()

    def test_new_inline_link_bold_format(self):
        link = "https://github.com/didix21/mdutils"
        text = "mdutils"
        md_file = MdUtils(file_name="Test_file", title="")
        expected_value = "[**" + text + "**](" + link + ")"
        created_value = md_file.new_inline_link(link, text, bold_italics_code="b")

        self.assertEqual(expected_value, created_value)

    def test_new_inline_link_italic_format(self):
        link = "https://github.com/didix21/mdutils"
        text = "mdutils"
        md_file = MdUtils(file_name="Test_file", title="")
        expected_value = "[*" + text + "*](" + link + ")"
        created_value = md_file.new_inline_link(link, text, bold_italics_code="i")

        self.assertEqual(expected_value, created_value)

    def test_new_inline_link_code_format(self):
        link = "https://github.com/didix21/mdutils"
        text = "mdutils"
        md_file = MdUtils(file_name="Test_file", title="")
        expected_value = "[``" + text + "``](" + link + ")"
        created_value = md_file.new_inline_link(link, text, bold_italics_code="c")

        self.assertEqual(expected_value, created_value)

    def test_new_inline_link_bold_italic_format(self):
        link = "https://github.com/didix21/mdutils"
        text = "mdutils"
        md_file = MdUtils(file_name="Test_file", title="")
        expected_value = "[***" + text + "***](" + link + ")"
        created_value = md_file.new_inline_link(link, text, bold_italics_code="bi")

        self.assertEqual(expected_value, created_value)

    def test_new_inline_link_bold_italic_code_format(self):
        link = "https://github.com/didix21/mdutils"
        text = "mdutils"
        md_file = MdUtils(file_name="Test_file", title="")
        expected_value = "[***``" + text + "``***](" + link + ")"
        created_value = md_file.new_inline_link(link, text, bold_italics_code="bic")

        self.assertEqual(expected_value, created_value)

    def test_new_inline_link_align_format(self):
        link = "https://github.com/didix21/mdutils"
        text = "mdutils"
        md_file = MdUtils(file_name="Test_file", title="")
        expected_value = "[<center>" + text + "</center>](" + link + ")"
        created_value = md_file.new_inline_link(link, text, align="center")

        self.assertEqual(expected_value, created_value)

    def test_new_reference_link(self):
        md_file = MdUtils(file_name="Test_file", title="")
        text = "mdutils library"
        reference_tag = "mdutils library"
        expected_value = "[" + text + "][" + reference_tag + "]"
        created_value = md_file.new_reference_link(
            link="https://github.com/didix21/mdutils",
            text=text,
            reference_tag=reference_tag,
        )

        self.assertEqual(expected_value, created_value)

    def test_new_reference_link_when_reference_tag_not_defined_and_bold_italics_code_is_defined(
        self,
    ):
        md_file = MdUtils(file_name="Test_file", title="")
        text = "mdutils library"
        try:
            md_file.new_reference_link(
                link="https://github.com/didix21/mdutils",
                text=text,
                bold_italics_code="b",
            )
        except TypeError:
            return

        self.fail()

    def test_new_reference_link_when_reference_tag_not_defined_and_align_is_defined(
        self,
    ):
        md_file = MdUtils(file_name="Test_file", title="")
        text = "mdutils library"
        try:
            md_file.new_reference_link(
                link="https://github.com/didix21/mdutils", text=text, align="center"
            )
        except TypeError:
            return

        self.fail()

    def test_new_bold_reference_link(self):
        md_file = MdUtils(file_name="Test_file", title="")
        text = "mdutils library"
        reference_tag = "mdutils library"
        expected_value = "[**" + text + "**][" + reference_tag + "]"
        created_value = md_file.new_reference_link(
            link="https://github.com/didix21/mdutils",
            text=text,
            reference_tag=reference_tag,
            bold_italics_code="b",
        )

        self.assertEqual(expected_value, created_value)

    def test_new_italics_reference_link(self):
        md_file = MdUtils(file_name="Test_file", title="")
        text = "mdutils library"
        reference_tag = "mdutils library"
        expected_value = "[*" + text + "*][" + reference_tag + "]"
        created_value = md_file.new_reference_link(
            link="https://github.com/didix21/mdutils",
            text=text,
            reference_tag=reference_tag,
            bold_italics_code="i",
        )

        self.assertEqual(expected_value, created_value)

    def test_new_code_reference_link(self):
        md_file = MdUtils(file_name="Test_file", title="")
        text = "mdutils library"
        reference_tag = "mdutils library"
        expected_value = "[``" + text + "``][" + reference_tag + "]"
        created_value = md_file.new_reference_link(
            link="https://github.com/didix21/mdutils",
            text=text,
            reference_tag=reference_tag,
            bold_italics_code="c",
        )

        self.assertEqual(expected_value, created_value)

    def test_references_placed_in_markdown_file(self):
        md_file = MdUtils(file_name="Test_file", title="")

        text = "mdutils library"
        reference_tag = "mdutils library"
        link = "https://github.com/didix21/mdutils"

        expected_value = (
            "\n\n\n[mdutils library0][mdutils library0]\n"
            "[mdutils library1][mdutils library1]\n"
            "[mdutils library2][mdutils library2]\n"
            "[mdutils library3][mdutils library3]\n"
            "\n\n\n"
            "[mdutils library0]: https://github.com/didix21/mdutils0\n"
            "[mdutils library1]: https://github.com/didix21/mdutils1\n"
            "[mdutils library2]: https://github.com/didix21/mdutils2\n"
            "[mdutils library3]: https://github.com/didix21/mdutils3\n"
        )

        for i in range(4):
            md_file.write(
                md_file.new_reference_link(
                    link=link + str(i),
                    text=text + str(i),
                    reference_tag=reference_tag + str(i),
                )
            )
            md_file.write("\n")

        md_file.create_md_file()

        created_data = MarkDownFile.read_file("Test_file.md")

        self.assertEqual(expected_value, created_data)

    def test_new_inline_image(self):
        md_file = MdUtils(file_name="Test_file", title="")
        expected_image = "![image](../image.png)"
        created_image = md_file.new_inline_image(text="image", path="../image.png")

        self.assertEqual(expected_image, created_image)

    def test_new_reference_image(self):
        md_file = MdUtils(file_name="Test_file", title="")
        expected_image = "![image][reference]"
        created_image = md_file.new_reference_image(
            text="image", path="../image.png", reference_tag="reference"
        )

        self.assertEqual(expected_image, created_image)

    def test_new_reference_image_markdown_data(self):
        md_file = MdUtils(file_name="Test_file", title="")
        expected_image_1 = "![image_1][reference]"
        expected_image_2 = "![image_2]"
        image_1 = md_file.new_reference_image(
            text="image_1", path="../image.png", reference_tag="reference"
        )
        image_2 = md_file.new_reference_image(text="image_2", path="../image_2.png")

        expected_created_data = (
            "\n\n\n"
            "\n{}".format(expected_image_1)
            + "\n{}".format(expected_image_2)
            + "\n\n\n"
            "[image_2]: ../image_2.png\n"
            "[reference]: ../image.png\n"
        )

        md_file.new_line(image_1)
        md_file.new_line(image_2)
        md_file.create_md_file()

        actual_created_data = MarkDownFile.read_file("Test_file")

        self.assertEqual(expected_created_data, actual_created_data)

    def test_new_list(self):
        md_file = MdUtils(file_name="Test_file", title="")
        md_file.new_list(self.complex_items)
        md_file.create_md_file()

        self.assertEqual(self.expected_list, MarkDownFile.read_file("Test_file.md"))
    
    def test_new_list_bold_items(self):
        md_file = MdUtils(file_name="Test_file", title="")
        md_file.new_list(self.complex_bold_items)
        md_file.create_md_file()

        self.assertEqual(self.expected_bold_list, MarkDownFile.read_file("Test_file.md"))

    def test_new_checkbox_list(self):
        md_file = MdUtils(file_name="Test_file", title="")
        md_file.new_checkbox_list(self.complex_items)
        md_file.create_md_file()

        self.assertEqual(
            self.expected_list.replace("-", "- [ ]"),
            MarkDownFile.read_file("Test_file.md"),
        )

    def test_new_checkbox_checked_list(self):
        md_file = MdUtils(file_name="Test_file", title="")
        md_file.new_checkbox_list(self.complex_items, checked=True)
        md_file.create_md_file()

        self.assertEqual(
            self.expected_list.replace("-", "- [x]"),
            MarkDownFile.read_file("Test_file.md"),
        )
