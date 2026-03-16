from __future__ import annotations

from datamodel_code_generator.format import PythonVersionMin
from datamodel_code_generator.imports import (
    IMPORT_LITERAL,
    IMPORT_OPTIONAL,
)
from datamodel_code_generator.types import DataType


def test_imports_with_literal_one() -> None:
    """Test imports for a DataType with single literal value"""
    data_type = DataType(literals=[""], python_version=PythonVersionMin)

    # Convert iterator to list for assertion
    imports = list(data_type.imports)
    assert IMPORT_LITERAL in imports
    assert len(imports) == 1


def test_imports_with_literal_one_and_optional() -> None:
    """Test imports for an optional DataType with single literal value"""
    data_type = DataType(literals=[""], is_optional=True, python_version=PythonVersionMin)

    imports = list(data_type.imports)
    assert IMPORT_LITERAL in imports
    assert IMPORT_OPTIONAL in imports
    assert len(imports) == 2


def test_imports_with_literal_empty() -> None:
    """Test imports for a DataType with no literal values"""
    data_type = DataType(literals=[], python_version=PythonVersionMin)

    imports = list(data_type.imports)
    assert len(imports) == 0


def test_imports_with_nested_dict_key() -> None:
    """Test imports for a DataType with dict_key containing literals"""
    dict_key_type = DataType(literals=["key"], python_version=PythonVersionMin)

    data_type = DataType(python_version=PythonVersionMin, dict_key=dict_key_type)

    imports = list(data_type.imports)
    assert IMPORT_LITERAL in imports
    assert len(imports) == 1


def test_imports_without_duplicate_literals() -> None:
    """Test that literal import is not duplicated"""
    dict_key_type = DataType(literals=["key1"], python_version=PythonVersionMin)

    data_type = DataType(
        literals=["key2"],
        python_version=PythonVersionMin,
        dict_key=dict_key_type,
    )

    imports = list(data_type.imports)
    assert IMPORT_LITERAL in imports
