from __future__ import annotations

from prettytable import PrettyTable, from_json


class TestJSONOutput:
    def test_json_output(self, helper_table: PrettyTable) -> None:
        result = helper_table.get_json_string()
        assert (
            result.strip()
            == """
[
    [
        "",
        "Field 1",
        "Field 2",
        "Field 3"
    ],
    {
        "": 1,
        "Field 1": "value 1",
        "Field 2": "value2",
        "Field 3": "value3"
    },
    {
        "": 4,
        "Field 1": "value 4",
        "Field 2": "value5",
        "Field 3": "value6"
    },
    {
        "": 7,
        "Field 1": "value 7",
        "Field 2": "value8",
        "Field 3": "value9"
    }
]""".strip()
        )
        options = {"fields": ["Field 1", "Field 3"]}
        result = helper_table.get_json_string(**options)
        assert (
            result.strip()
            == """
[
    [
        "Field 1",
        "Field 3"
    ],
    {
        "Field 1": "value 1",
        "Field 3": "value3"
    },
    {
        "Field 1": "value 4",
        "Field 3": "value6"
    },
    {
        "Field 1": "value 7",
        "Field 3": "value9"
    }
]""".strip()
        )

    def test_json_output_options(self, helper_table: PrettyTable) -> None:
        result = helper_table.get_json_string(
            header=False, indent=None, separators=(",", ":")
        )
        assert (
            result
            == """[{"":1,"Field 1":"value 1","Field 2":"value2","Field 3":"value3"},"""
            """{"":4,"Field 1":"value 4","Field 2":"value5","Field 3":"value6"},"""
            """{"":7,"Field 1":"value 7","Field 2":"value8","Field 3":"value9"}]"""
        )


class TestJSONConstructor:
    def test_json_and_back(self, city_data: PrettyTable) -> None:
        json_string = city_data.get_json_string()
        new_table = from_json(json_string)
        assert new_table.get_string() == city_data.get_string()
