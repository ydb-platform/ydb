from __future__ import annotations

from argparse import Namespace
from typing import TYPE_CHECKING

import black
import isort
import pytest
from freezegun import freeze_time

from datamodel_code_generator.__main__ import Exit, main
from __tests__.main.test_main_general import DATA_PATH, EXPECTED_MAIN_PATH

if TYPE_CHECKING:
    from pathlib import Path

GRAPHQL_DATA_PATH: Path = DATA_PATH / "graphql"
EXPECTED_GRAPHQL_PATH: Path = EXPECTED_MAIN_PATH / "graphql"


@pytest.fixture(autouse=True)
def reset_namespace(monkeypatch: pytest.MonkeyPatch) -> None:
    namespace_ = Namespace(no_color=False)
    monkeypatch.setattr("datamodel_code_generator.__main__.namespace", namespace_)
    monkeypatch.setattr("datamodel_code_generator.arguments.namespace", namespace_)


@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic.BaseModel",
            "simple_star_wars.py",
        ),
        (
            "dataclasses.dataclass",
            "simple_star_wars_dataclass.py",
        ),
    ],
)
@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_graphql_simple_star_wars(output_model: str, expected_output: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(GRAPHQL_DATA_PATH / "simple-star-wars.graphql"),
        "--output",
        str(output_file),
        "--input-file-type",
        "graphql",
        "--output-model",
        output_model,
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_GRAPHQL_PATH / expected_output).read_text()


@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_graphql_different_types_of_fields(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(GRAPHQL_DATA_PATH / "different-types-of-fields.graphql"),
        "--output",
        str(output_file),
        "--input-file-type",
        "graphql",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8") == (EXPECTED_GRAPHQL_PATH / "different_types_of_fields.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_use_default_kwarg(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(GRAPHQL_DATA_PATH / "annotated.graphql"),
        "--output",
        str(output_file),
        "--input-file-type",
        "graphql",
        "--use-default-kwarg",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_GRAPHQL_PATH / "annotated_use_default_kwarg.py").read_text()
    )


@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_graphql_custom_scalar_types(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(GRAPHQL_DATA_PATH / "custom-scalar-types.graphql"),
        "--output",
        str(output_file),
        "--input-file-type",
        "graphql",
        "--extra-template-data",
        str(GRAPHQL_DATA_PATH / "custom-scalar-types.json"),
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_GRAPHQL_PATH / "custom_scalar_types.py").read_text()


@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_graphql_field_aliases(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(GRAPHQL_DATA_PATH / "field-aliases.graphql"),
        "--output",
        str(output_file),
        "--input-file-type",
        "graphql",
        "--aliases",
        str(GRAPHQL_DATA_PATH / "field-aliases.json"),
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_GRAPHQL_PATH / "field_aliases.py").read_text()


@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_graphql_enums(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(GRAPHQL_DATA_PATH / "enums.graphql"),
        "--output",
        str(output_file),
        "--input-file-type",
        "graphql",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_GRAPHQL_PATH / "enums.py").read_text()


@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_graphql_union(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(GRAPHQL_DATA_PATH / "union.graphql"),
        "--output",
        str(output_file),
        "--input-file-type",
        "graphql",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_GRAPHQL_PATH / "union.py").read_text()


@pytest.mark.skipif(
    not isort.__version__.startswith("4."),
    reason="See https://github.com/PyCQA/isort/issues/1600 for example",
)
@freeze_time("2019-07-26")
def test_main_graphql_additional_imports_isort_4(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(GRAPHQL_DATA_PATH / "additional-imports.graphql"),
        "--output",
        str(output_file),
        "--input-file-type",
        "graphql",
        "--extra-template-data",
        str(GRAPHQL_DATA_PATH / "additional-imports-types.json"),
        "--additional-imports",
        "datetime.datetime,datetime.date,mymodule.myclass.MyCustomPythonClass",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8") == (EXPECTED_GRAPHQL_PATH / "additional_imports_isort4.py").read_text()
    )


@pytest.mark.skipif(
    isort.__version__.startswith("4."),
    reason="See https://github.com/PyCQA/isort/issues/1600 for example",
)
@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_graphql_additional_imports_isort_5_or_6(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(GRAPHQL_DATA_PATH / "additional-imports.graphql"),
        "--output",
        str(output_file),
        "--input-file-type",
        "graphql",
        "--extra-template-data",
        str(GRAPHQL_DATA_PATH / "additional-imports-types.json"),
        "--additional-imports",
        "datetime.datetime,datetime.date,mymodule.myclass.MyCustomPythonClass",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8") == (EXPECTED_GRAPHQL_PATH / "additional_imports_isort5.py").read_text()
    )


@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_graphql_custom_formatters(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(GRAPHQL_DATA_PATH / "custom-scalar-types.graphql"),
        "--output",
        str(output_file),
        "--input-file-type",
        "graphql",
        "--custom-formatters",
        "tests.data.python.custom_formatters.add_comment",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_GRAPHQL_PATH / "custom_formatters.py").read_text()


@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_graphql_use_standard_collections(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(GRAPHQL_DATA_PATH / "use-standard-collections.graphql"),
        "--output",
        str(output_file),
        "--input-file-type",
        "graphql",
        "--use-standard-collections",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8") == (EXPECTED_GRAPHQL_PATH / "use_standard_collections.py").read_text()
    )


@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_graphql_use_union_operator(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(GRAPHQL_DATA_PATH / "use-union-operator.graphql"),
        "--output",
        str(output_file),
        "--input-file-type",
        "graphql",
        "--use-union-operator",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_GRAPHQL_PATH / "use_union_operator.py").read_text()


@freeze_time("2019-07-26")
def test_main_graphql_extra_fields_allow(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(GRAPHQL_DATA_PATH / "simple-star-wars.graphql"),
        "--output",
        str(output_file),
        "--input-file-type",
        "graphql",
        "--extra-fields",
        "allow",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_GRAPHQL_PATH / "simple_star_wars_extra_fields_allow.py").read_text()
    )
