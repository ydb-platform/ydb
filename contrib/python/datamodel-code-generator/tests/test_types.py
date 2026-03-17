from __future__ import annotations

import pytest

from datamodel_code_generator.types import _remove_none_from_union, get_optional_type


@pytest.mark.parametrize(
    ("input_", "use_union_operator", "expected"),
    [
        ("List[str]", False, "Optional[List[str]]"),
        ("List[str, int, float]", False, "Optional[List[str, int, float]]"),
        ("List[str, int, None]", False, "Optional[List[str, int, None]]"),
        ("Union[str]", False, "Optional[str]"),
        ("Union[str, int, float]", False, "Optional[Union[str, int, float]]"),
        ("Union[str, int, None]", False, "Optional[Union[str, int]]"),
        ("Union[str, int, None, None]", False, "Optional[Union[str, int]]"),
        (
            "Union[str, int, List[str, int, None], None]",
            False,
            "Optional[Union[str, int, List[str, int, None]]]",
        ),
        (
            "Union[str, int, List[str, Dict[int, str | None]], None]",
            False,
            "Optional[Union[str, int, List[str, Dict[int, str | None]]]]",
        ),
        ("List[str]", True, "List[str] | None"),
        ("List[str | int | float]", True, "List[str | int | float] | None"),
        ("List[str | int | None]", True, "List[str | int | None] | None"),
        ("str", True, "str | None"),
        ("str | int | float", True, "str | int | float | None"),
        ("str | int | None", True, "str | int | None"),
        ("str | int | None | None", True, "str | int | None"),
        (
            "str | int | List[str | Dict[int | Union[str | None]]] | None",
            True,
            "str | int | List[str | Dict[int | Union[str | None]]] | None",
        ),
    ],
)
def test_get_optional_type(input_: str, use_union_operator: bool, expected: str) -> None:
    assert get_optional_type(input_, use_union_operator) == expected


@pytest.mark.parametrize(
    ("type_str", "use_union_operator", "expected"),
    [
        # Traditional Union syntax
        ("Union[str, None]", False, "str"),
        ("Union[str, int, None]", False, "Union[str, int]"),
        ("Union[None, str]", False, "str"),
        ("Union[None]", False, "None"),
        ("Union[None, None]", False, "None"),
        ("Union[Union[str, None], int]", False, "Union[str, int]"),
        # Union for constraint strings with pattern or regex
        (
            "Union[constr(pattern=r'^a,b$'), None]",
            False,
            "constr(pattern=r'^a,b$')",
        ),
        (
            "Union[constr(regex=r'^a,b$'), None]",
            False,
            "constr(regex=r'^a,b$')",
        ),
        (
            "Union[constr(pattern=r'^\\d+,\\w+$'), None]",
            False,
            "constr(pattern=r'^\\d+,\\w+$')",
        ),
        (
            "Union[constr(regex=r'^\\d+,\\w+$'), None]",
            False,
            "constr(regex=r'^\\d+,\\w+$')",
        ),
        # Union operator syntax
        ("str | None", True, "str"),
        ("int | str | None", True, "int | str"),
        ("None | str", True, "str"),
        ("None | None", True, "None"),
        ("constr(pattern='0|1') | None", True, "constr(pattern='0|1')"),
        ("constr(pattern='0  |1') | int | None", True, "constr(pattern='0  |1') | int"),
        # Complex nested types - traditional syntax
        ("Union[str, int] | None", True, "Union[str, int]"),
        (
            "Optional[List[Dict[str, Any]]] | None",
            True,
            "Optional[List[Dict[str, Any]]]",
        ),
        # Union for constraint strings with pattern or regex on nested types
        (
            "Union[constr(pattern=r'\\['), Union[str, None], int]",
            False,
            "Union[constr(pattern=r'\\['), str, int]",
        ),
        (
            "Union[constr(regex=r'\\['), Union[str, None], int]",
            False,
            "Union[constr(regex=r'\\['), str, int]",
        ),
        # Complex nested types - union operator syntax
        ("List[str | None] | None", True, "List[str | None]"),
        (
            "List[constr(pattern='0|1') | None] | None",
            True,
            "List[constr(pattern='0|1') | None]",
        ),
        (
            "List[constr(pattern='0 | 1') | None] | None",
            True,
            "List[constr(pattern='0 | 1') | None]",
        ),
        (
            "List[constr(pattern='0  | 1') | None] | None",
            True,
            "List[constr(pattern='0  | 1') | None]",
        ),
        ("Dict[str, int] | None | List[str]", True, "Dict[str, int] | List[str]"),
        # Edge cases that test the fixed regex pattern issue
        ("List[str] | None", True, "List[str]"),
        ("Dict[str, int] | None", True, "Dict[str, int]"),
        ("Tuple[int, ...] | None", True, "Tuple[int, ...]"),
        ("Callable[[int], str] | None", True, "Callable[[int], str]"),
        # Non-union types (should be returned as-is)
        ("str", False, "str"),
        ("List[str]", False, "List[str]"),
    ],
)
def test_remove_none_from_union(type_str: str, use_union_operator: bool, expected: str) -> None:
    assert _remove_none_from_union(type_str, use_union_operator=use_union_operator) == expected
