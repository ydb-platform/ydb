from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from datamodel_code_generator.imports import Import
from datamodel_code_generator.model.pydantic.imports import (
    IMPORT_CONDECIMAL,
    IMPORT_CONFLOAT,
    IMPORT_CONINT,
    IMPORT_CONSTR,
    IMPORT_NEGATIVE_FLOAT,
    IMPORT_NEGATIVE_INT,
    IMPORT_NON_NEGATIVE_FLOAT,
    IMPORT_NON_NEGATIVE_INT,
    IMPORT_NON_POSITIVE_FLOAT,
    IMPORT_NON_POSITIVE_INT,
    IMPORT_POSITIVE_FLOAT,
    IMPORT_POSITIVE_INT,
)
from datamodel_code_generator.model.pydantic.types import DataTypeManager
from datamodel_code_generator.types import DataType, Types, UnionIntFloat


@pytest.mark.parametrize(
    ("types", "use_non_positive_negative_number_constrained_types", "params", "data_type"),
    [
        (Types.integer, False, {}, {"type": "int"}),
        (
            Types.integer,
            False,
            {"maximum": 10},
            {
                "type": "conint",
                "is_func": True,
                "kwargs": {"le": 10},
                "import_": IMPORT_CONINT,
            },
        ),
        (
            Types.integer,
            False,
            {"exclusiveMaximum": 10},
            {
                "type": "conint",
                "is_func": True,
                "kwargs": {"lt": 10},
                "import_": IMPORT_CONINT,
            },
        ),
        (
            Types.integer,
            False,
            {"minimum": 10},
            {
                "type": "conint",
                "is_func": True,
                "kwargs": {"ge": 10},
                "import_": IMPORT_CONINT,
            },
        ),
        (
            Types.integer,
            False,
            {"exclusiveMinimum": 10},
            {
                "type": "conint",
                "is_func": True,
                "kwargs": {"gt": 10},
                "import_": IMPORT_CONINT,
            },
        ),
        (
            Types.integer,
            False,
            {"multipleOf": 10},
            {
                "type": "conint",
                "is_func": True,
                "kwargs": {"multiple_of": 10},
                "import_": IMPORT_CONINT,
            },
        ),
        (
            Types.integer,
            False,
            {"exclusiveMinimum": 0},
            {"type": "PositiveInt", "import_": IMPORT_POSITIVE_INT},
        ),
        (
            Types.integer,
            False,
            {"exclusiveMaximum": 0},
            {"type": "NegativeInt", "import_": IMPORT_NEGATIVE_INT},
        ),
        (
            Types.integer,
            True,
            {"minimum": 0},
            {"type": "NonNegativeInt", "import_": IMPORT_NON_NEGATIVE_INT},
        ),
        (
            Types.integer,
            True,
            {"maximum": 0},
            {"type": "NonPositiveInt", "import_": IMPORT_NON_POSITIVE_INT},
        ),
        (
            Types.integer,
            False,
            {"minimum": 0},
            {
                "type": "conint",
                "is_func": True,
                "kwargs": {"ge": 0},
                "import_": IMPORT_CONINT,
            },
        ),
        (
            Types.integer,
            False,
            {"maximum": 0},
            {
                "type": "conint",
                "is_func": True,
                "kwargs": {"le": 0},
                "import_": IMPORT_CONINT,
            },
        ),
    ],
)
def test_get_data_int_type(
    types: Types,
    use_non_positive_negative_number_constrained_types: bool,
    params: dict[str, Any],
    data_type: dict[str, Any],
) -> None:
    data_type_manager = DataTypeManager(
        use_non_positive_negative_number_constrained_types=use_non_positive_negative_number_constrained_types
    )
    assert (
        data_type_manager.get_data_int_type(types, **params).dict() == data_type_manager.data_type(**data_type).dict()
    )


@pytest.mark.parametrize(
    ("types", "use_non_positive_negative_number_constrained_types", "params", "data_type"),
    [
        (Types.float, False, {}, {"type": "float"}),
        (
            Types.float,
            False,
            {"maximum": 10},
            {
                "type": "confloat",
                "is_func": True,
                "kwargs": {"le": 10},
                "import_": IMPORT_CONFLOAT,
            },
        ),
        (
            Types.float,
            False,
            {"exclusiveMaximum": 10},
            {
                "type": "confloat",
                "is_func": True,
                "kwargs": {"lt": 10.0},
                "import_": IMPORT_CONFLOAT,
            },
        ),
        (
            Types.float,
            False,
            {"minimum": 10},
            {
                "type": "confloat",
                "is_func": True,
                "kwargs": {"ge": 10.0},
                "import_": IMPORT_CONFLOAT,
            },
        ),
        (
            Types.float,
            False,
            {"exclusiveMinimum": 10},
            {
                "type": "confloat",
                "is_func": True,
                "kwargs": {"gt": 10.0},
                "import_": IMPORT_CONFLOAT,
            },
        ),
        (
            Types.float,
            False,
            {"multipleOf": 10},
            {
                "type": "confloat",
                "is_func": True,
                "kwargs": {"multiple_of": 10.0},
                "import_": IMPORT_CONFLOAT,
            },
        ),
        (
            Types.float,
            False,
            {"exclusiveMinimum": 0},
            {"type": "PositiveFloat", "import_": IMPORT_POSITIVE_FLOAT},
        ),
        (
            Types.float,
            False,
            {"exclusiveMaximum": 0},
            {"type": "NegativeFloat", "import_": IMPORT_NEGATIVE_FLOAT},
        ),
        (
            Types.float,
            True,
            {"maximum": 0},
            {"type": "NonPositiveFloat", "import_": IMPORT_NON_POSITIVE_FLOAT},
        ),
        (
            Types.float,
            True,
            {"minimum": 0},
            {"type": "NonNegativeFloat", "import_": IMPORT_NON_NEGATIVE_FLOAT},
        ),
        (
            Types.float,
            False,
            {"maximum": 0},
            {
                "type": "confloat",
                "is_func": True,
                "kwargs": {"le": 0.0},
                "import_": IMPORT_CONFLOAT,
            },
        ),
        (
            Types.float,
            False,
            {"minimum": 0},
            {
                "type": "confloat",
                "is_func": True,
                "kwargs": {"ge": 0.0},
                "import_": IMPORT_CONFLOAT,
            },
        ),
    ],
)
def test_get_data_float_type(
    types: Types,
    use_non_positive_negative_number_constrained_types: bool,
    params: dict[str, Any],
    data_type: dict[str, Any],
) -> None:
    data_type_manager = DataTypeManager(
        use_non_positive_negative_number_constrained_types=use_non_positive_negative_number_constrained_types
    )
    assert data_type_manager.get_data_float_type(types, **params) == data_type_manager.data_type(**data_type)


@pytest.mark.parametrize(
    ("types", "params", "data_type"),
    [
        (
            Types.decimal,
            {},
            {"type": "Decimal", "import_": Import(from_="decimal", import_="Decimal")},
        ),
        (
            Types.decimal,
            {"maximum": 10},
            {
                "type": "condecimal",
                "is_func": True,
                "kwargs": {"le": 10},
                "import_": IMPORT_CONDECIMAL,
            },
        ),
        (
            Types.decimal,
            {"exclusiveMaximum": 10},
            {
                "type": "condecimal",
                "is_func": True,
                "kwargs": {"lt": 10},
                "import_": IMPORT_CONDECIMAL,
            },
        ),
        (
            Types.decimal,
            {"minimum": 10},
            {
                "type": "condecimal",
                "is_func": True,
                "kwargs": {"ge": 10},
                "import_": IMPORT_CONDECIMAL,
            },
        ),
        (
            Types.decimal,
            {"exclusiveMinimum": 10},
            {
                "type": "condecimal",
                "is_func": True,
                "kwargs": {"gt": 10},
                "import_": IMPORT_CONDECIMAL,
            },
        ),
        (
            Types.decimal,
            {"multipleOf": 10},
            {
                "type": "condecimal",
                "is_func": True,
                "kwargs": {"multiple_of": 10},
                "import_": IMPORT_CONDECIMAL,
            },
        ),
        (
            Types.decimal,
            {"minimum": UnionIntFloat(10.01)},
            {
                "type": "condecimal",
                "is_func": True,
                "kwargs": {"ge": Decimal("10.01")},
                "import_": IMPORT_CONDECIMAL,
            },
        ),
    ],
)
def test_get_data_decimal_type(types: Types, params: dict[str, Any], data_type: dict[str, Any]) -> None:
    data_type_manager = DataTypeManager()
    assert data_type_manager.get_data_decimal_type(types, **params) == data_type_manager.data_type(**data_type)


@pytest.mark.parametrize(
    ("types", "params", "data_type"),
    [
        (Types.string, {}, {"type": "str"}),
        (
            Types.string,
            {"pattern": "^abc"},
            {
                "type": "constr",
                "is_func": True,
                "kwargs": {"regex": "r'^abc'"},
                "import_": IMPORT_CONSTR,
            },
        ),
        (
            Types.string,
            {"minLength": 10},
            {
                "type": "constr",
                "is_func": True,
                "kwargs": {"min_length": 10},
                "import_": IMPORT_CONSTR,
            },
        ),
        (
            Types.string,
            {"maxLength": 10},
            {
                "type": "constr",
                "is_func": True,
                "kwargs": {"max_length": 10},
                "import_": IMPORT_CONSTR,
            },
        ),
    ],
)
def test_get_data_str_type(types: Types, params: dict[str, Any], data_type: dict[str, Any]) -> None:
    data_type_manager = DataTypeManager()
    assert data_type_manager.get_data_str_type(types, **params) == data_type_manager.data_type(**data_type)


@pytest.mark.parametrize(
    ("types", "data_type"),
    [
        (Types.string, {"type": "str"}),
        (Types.integer, {"type": "int"}),
        (Types.float, {"type": "float"}),
        (Types.boolean, {"type": "bool"}),
        (
            Types.decimal,
            {"type": "Decimal", "import_": Import(from_="decimal", import_="Decimal")},
        ),
    ],
)
def test_get_data_type(types: Types, data_type: dict[str, str]) -> None:
    data_type_manager = DataTypeManager()
    assert data_type_manager.get_data_type(types) == data_type_manager.data_type(**data_type)


def test_data_type_type_hint() -> None:
    assert DataType(type="str").type_hint == "str"
    assert DataType(type="constr", is_func=True).type_hint == "constr()"
    assert DataType(type="constr", is_func=True, kwargs={"min_length": 10}).type_hint == "constr(min_length=10)"


@pytest.mark.parametrize(
    ("types", "data_type"),
    [
        ("string", {"type": "str"}),
        (10, {"type": "int"}),
        (20.3, {"type": "float"}),
        (True, {"type": "bool"}),
    ],
)
def test_get_data_type_from_value(types: Any, data_type: dict[str, str]) -> None:
    data_type_manager = DataTypeManager()
    assert data_type_manager.get_data_type_from_value(types) == data_type_manager.data_type(**data_type)


@pytest.mark.parametrize(
    ("types", "data_type"),
    [
        (
            [1, 2, 3],
            ("typing.List", False),
        ),
        (
            {"a": 1, "b": 2, "c": 3},
            ("typing.Dict", False),
        ),
        (None, ("typing.Any", False)),
    ],
)
def test_get_data_type_from_full_path(types: Any, data_type: tuple[str, bool]) -> None:
    data_type_manager = DataTypeManager()
    assert data_type_manager.get_data_type_from_value(types) == data_type_manager.get_data_type_from_full_path(
        *data_type
    )
