from __future__ import annotations

import pytest

from prettytable import PrettyTable, from_mediawiki


class TestMediaWikiOutput:
    def test_mediawiki_output(self, helper_table: PrettyTable) -> None:
        assert (
            helper_table.get_mediawiki_string(header=True).strip()
            == """
{| class="wikitable"
|-
!  !! Field 1 !! Field 2 !! Field 3
|-
| 1 || value 1 || value2 || value3
|-
| 4 || value 4 || value5 || value6
|-
| 7 || value 7 || value8 || value9
|}
""".strip()
        )

    def test_mediawiki_output_without_header(self, helper_table: PrettyTable) -> None:
        assert (
            helper_table.get_mediawiki_string(header=False).strip()
            == """
{| class="wikitable"
|-
| 1 || value 1 || value2 || value3
|-
| 4 || value 4 || value5 || value6
|-
| 7 || value 7 || value8 || value9
|}
""".strip()
        )

    def test_mediawiki_output_with_caption(self, helper_table: PrettyTable) -> None:
        assert (
            helper_table.get_mediawiki_string(
                title="Optional caption", header=True
            ).strip()
            == """
{| class="wikitable"
|+ Optional caption
|-
!  !! Field 1 !! Field 2 !! Field 3
|-
| 1 || value 1 || value2 || value3
|-
| 4 || value 4 || value5 || value6
|-
| 7 || value 7 || value8 || value9
|}
""".strip()
        )

    def test_mediawiki_output_with_attributes(self, helper_table: PrettyTable) -> None:
        assert (
            helper_table.get_mediawiki_string(
                attributes={"class": "mytable", "id": "table1"}, header=True
            ).strip()
            == """
{| class="mytable" id="table1"
|-
!  !! Field 1 !! Field 2 !! Field 3
|-
| 1 || value 1 || value2 || value3
|-
| 4 || value 4 || value5 || value6
|-
| 7 || value 7 || value8 || value9
|}
            """.strip()
        )

    def test_mediawiki_output_with_fields_option(
        self, helper_table: PrettyTable
    ) -> None:
        assert (
            helper_table.get_mediawiki_string(
                fields=["Field 1", "Field 3"], header=True
            ).strip()
            == """
{| class="wikitable"
|-
! Field 1 !! Field 3
|-
| value 1 || value3
|-
| value 4 || value6
|-
| value 7 || value9
|}
            """.strip()
        )


class TestMediaWikiConstructor:
    def test_mediawiki_and_back(self, city_data: PrettyTable) -> None:
        mediawiki_string = city_data.get_mediawiki_string()
        new_table = from_mediawiki(mediawiki_string)
        assert new_table.get_string() == city_data.get_string()

    def test_from_mediawiki_ignores_non_table_text(
        self, city_data: PrettyTable
    ) -> None:
        wiki_table = city_data.get_mediawiki_string()
        wiki_text = f"Before table text.\n{wiki_table}\nAfter table text."
        table = from_mediawiki(wiki_text)
        output = table.get_string()

        for header in city_data.field_names:
            assert header in output
        for row in city_data._rows:
            for cell in row:
                assert str(cell) in output
        assert "Before table text." not in output
        assert "After table text." not in output

    def test_from_mediawiki_ignores_caption(self) -> None:
        wiki_text = """
{| class="wikitable"
|+ Optional caption
|-
! Field 1 !! Field 2
|-
| value 1 || value2
|-
| value 4 || value5
|}
    """
        table = from_mediawiki(wiki_text)
        output = table.get_string()
        assert "Optional caption" not in output
        assert "value 1" in output

    def test_from_mediawiki_no_header(self) -> None:
        wiki_text = """
{| class="wikitable"
|-
| value 1 || value2
|-
| value 4 || value5
|-
| value 7 || value8
|}
    """
        with pytest.raises(
            ValueError, match="No valid header found in the MediaWiki table."
        ):
            from_mediawiki(wiki_text)

    def test_from_mediawiki_row_length_mismatch(self) -> None:
        wiki_text = """
{| class="wikitable"
|-
! Field 1 !! Field 2
|-
| value 1 || value2 || value3
|-
| value 4 || value5 || value6
|}
    """
        with pytest.raises(
            ValueError, match="Row length mismatch between header and body."
        ):
            from_mediawiki(wiki_text)
