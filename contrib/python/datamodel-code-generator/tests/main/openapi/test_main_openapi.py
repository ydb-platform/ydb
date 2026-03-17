from __future__ import annotations

import contextlib
import json
import platform
import shutil
from argparse import Namespace
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import Mock, call

import black
import isort
import pydantic
import pytest
from freezegun import freeze_time
from packaging import version

with contextlib.suppress(ImportError):
    pass

from datamodel_code_generator import (
    MIN_VERSION,
    DataModelType,
    InputFileType,
    OpenAPIScope,
    PythonVersionMin,
    chdir,
    generate,
    get_version,
    inferred_message,
)
from datamodel_code_generator.__main__ import Exit, main
from __tests__.main.test_main_general import DATA_PATH, EXPECTED_MAIN_PATH, TIMESTAMP

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

OPEN_API_DATA_PATH: Path = DATA_PATH / "openapi"
EXPECTED_OPENAPI_PATH: Path = EXPECTED_MAIN_PATH / "openapi"


@pytest.fixture(autouse=True)
def reset_namespace(monkeypatch: pytest.MonkeyPatch) -> None:
    namespace_ = Namespace(no_color=False)
    monkeypatch.setattr("datamodel_code_generator.__main__.namespace", namespace_)
    monkeypatch.setattr("datamodel_code_generator.arguments.namespace", namespace_)


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--output",
        str(output_file),
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "general.py").read_text()


@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_openapi_discriminator_enum(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "discriminator_enum.yaml"),
        "--output",
        str(output_file),
        "--target-python-version",
        "3.10",
        "--output-model-type",
        "pydantic_v2.BaseModel",
        "--input-file-type",
        "openapi",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "discriminator" / "enum.py").read_text()


@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_openapi_discriminator_enum_duplicate(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "discriminator_enum_duplicate.yaml"),
        "--output",
        str(output_file),
        "--target-python-version",
        "3.10",
        "--output-model-type",
        "pydantic_v2.BaseModel",
        "--input-file-type",
        "openapi",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_OPENAPI_PATH / "discriminator" / "enum_duplicate.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_openapi_discriminator_with_properties(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "discriminator_with_properties.yaml"),
        "--output",
        str(output_file),
        "--output-model-type",
        "pydantic_v2.BaseModel",
    ])
    assert return_code == Exit.OK

    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_OPENAPI_PATH / "discriminator" / "with_properties.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_pydantic_basemodel(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--output",
        str(output_file),
        "--output-model-type",
        "pydantic.BaseModel",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "general.py").read_text()


@freeze_time("2019-07-26")
def test_main_base_class(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    shutil.copy(DATA_PATH / "pyproject.toml", tmp_path / "pyproject.toml")
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--output",
        str(output_file),
        "--base-class",
        "custom_module.Base",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "base_class.py").read_text()


@freeze_time("2019-07-26")
def test_target_python_version(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--output",
        str(output_file),
        "--target-python-version",
        f"3.{MIN_VERSION}",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "target_python_version.py").read_text()


@pytest.mark.benchmark
def test_main_modular(tmp_path: Path) -> None:
    """Test main function on modular file."""
    input_filename = OPEN_API_DATA_PATH / "modular.yaml"
    output_path = tmp_path / "model"

    with freeze_time(TIMESTAMP):
        main(["--input", str(input_filename), "--output", str(output_path)])
    main_modular_dir = EXPECTED_OPENAPI_PATH / "modular"
    for path in main_modular_dir.rglob("*.py"):
        result = output_path.joinpath(path.relative_to(main_modular_dir)).read_text()
        assert result == path.read_text()


def test_main_modular_reuse_model(tmp_path: Path) -> None:
    """Test main function on modular file."""
    input_filename = OPEN_API_DATA_PATH / "modular.yaml"
    output_path = tmp_path / "model"

    with freeze_time(TIMESTAMP):
        main([
            "--input",
            str(input_filename),
            "--output",
            str(output_path),
            "--reuse-model",
        ])
    main_modular_dir = EXPECTED_OPENAPI_PATH / "modular_reuse_model"
    for path in main_modular_dir.rglob("*.py"):
        result = output_path.joinpath(path.relative_to(main_modular_dir)).read_text()
        assert result == path.read_text()


def test_main_modular_no_file() -> None:
    """Test main function on modular file with no output name."""

    input_filename = OPEN_API_DATA_PATH / "modular.yaml"

    assert main(["--input", str(input_filename)]) == Exit.ERROR


def test_main_modular_filename(tmp_path: Path) -> None:
    """Test main function on modular file with filename."""
    input_filename = OPEN_API_DATA_PATH / "modular.yaml"
    output_filename = tmp_path / "model.py"

    assert main(["--input", str(input_filename), "--output", str(output_filename)]) == Exit.ERROR


@pytest.mark.xfail
def test_main_openapi_no_file(capsys: pytest.CaptureFixture, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test main function on non-modular file with no output name."""
    monkeypatch.chdir(tmp_path)
    input_filename = OPEN_API_DATA_PATH / "api.yaml"

    with freeze_time(TIMESTAMP):
        main(["--input", str(input_filename)])

    captured = capsys.readouterr()
    assert captured.out == (EXPECTED_OPENAPI_PATH / "no_file.py").read_text()
    assert captured.err == inferred_message.format("openapi") + "\n"


@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic.BaseModel",
            "extra_template_data_config.py",
        ),
        (
            "pydantic_v2.BaseModel",
            "extra_template_data_config_pydantic_v2.py",
        ),
    ],
)
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
@pytest.mark.xfail
def test_main_openapi_extra_template_data_config(
    capsys: pytest.CaptureFixture,
    output_model: str,
    expected_output: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test main function with custom config data in extra template."""

    monkeypatch.chdir(tmp_path)
    input_filename = OPEN_API_DATA_PATH / "api.yaml"
    extra_template_data = OPEN_API_DATA_PATH / "extra_data.json"

    with freeze_time(TIMESTAMP):
        main([
            "--input",
            str(input_filename),
            "--extra-template-data",
            str(extra_template_data),
            "--output-model",
            output_model,
        ])

    captured = capsys.readouterr()
    assert captured.out == (EXPECTED_OPENAPI_PATH / expected_output).read_text()
    assert captured.err == inferred_message.format("openapi") + "\n"


@pytest.mark.xfail
def test_main_custom_template_dir_old_style(
    capsys: pytest.CaptureFixture, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test main function with custom template directory."""

    monkeypatch.chdir(tmp_path)
    input_filename = OPEN_API_DATA_PATH / "api.yaml"
    custom_template_dir = DATA_PATH / "templates_old_style"
    extra_template_data = OPEN_API_DATA_PATH / "extra_data.json"

    with freeze_time(TIMESTAMP):
        main([
            "--input",
            str(input_filename),
            "--custom-template-dir",
            str(custom_template_dir),
            "--extra-template-data",
            str(extra_template_data),
        ])

    captured = capsys.readouterr()
    assert captured.out == (EXPECTED_OPENAPI_PATH / "custom_template_dir.py").read_text()
    assert captured.err == inferred_message.format("openapi") + "\n"


@pytest.mark.xfail
def test_main_openapi_custom_template_dir(
    capsys: pytest.CaptureFixture, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    """Test main function with custom template directory."""

    input_filename = OPEN_API_DATA_PATH / "api.yaml"
    custom_template_dir = DATA_PATH / "templates"
    extra_template_data = OPEN_API_DATA_PATH / "extra_data.json"

    with freeze_time(TIMESTAMP):
        main([
            "--input",
            str(input_filename),
            "--custom-template-dir",
            str(custom_template_dir),
            "--extra-template-data",
            str(extra_template_data),
        ])

    captured = capsys.readouterr()
    assert captured.out == (EXPECTED_OPENAPI_PATH / "custom_template_dir.py").read_text()
    assert captured.err == inferred_message.format("openapi") + "\n"


@pytest.mark.skipif(
    black.__version__.split(".")[0] >= "24",
    reason="Installed black doesn't support the old style",
)
@freeze_time("2019-07-26")
def test_pyproject(tmp_path: Path) -> None:
    if platform.system() == "Windows":

        def get_path(path: str) -> str:
            return str(path).replace("\\", "\\\\")

    else:

        def get_path(path: str) -> str:
            return str(path)

    with chdir(tmp_path):
        output_file: Path = tmp_path / "output.py"
        pyproject_toml_path = Path(DATA_PATH) / "project" / "pyproject.toml"
        pyproject_toml = (
            pyproject_toml_path.read_text()
            .replace("INPUT_PATH", get_path(OPEN_API_DATA_PATH / "api.yaml"))
            .replace("OUTPUT_PATH", get_path(output_file))
            .replace("ALIASES_PATH", get_path(OPEN_API_DATA_PATH / "empty_aliases.json"))
            .replace(
                "EXTRA_TEMPLATE_DATA_PATH",
                get_path(OPEN_API_DATA_PATH / "empty_data.json"),
            )
            .replace("CUSTOM_TEMPLATE_DIR_PATH", get_path(tmp_path))
        )
        (tmp_path / "pyproject.toml").write_text(pyproject_toml)

        return_code: Exit = main([])
        assert return_code == Exit.OK
        assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "pyproject.py").read_text()


@freeze_time("2019-07-26")
def test_pyproject_not_found(tmp_path: Path) -> None:
    with chdir(tmp_path):
        output_file: Path = tmp_path / "output.py"
        return_code: Exit = main([
            "--input",
            str(OPEN_API_DATA_PATH / "api.yaml"),
            "--output",
            str(output_file),
        ])
        assert return_code == Exit.OK
        assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "pyproject_not_found.py").read_text()


@freeze_time("2019-07-26")
def test_stdin(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    monkeypatch.setattr("sys.stdin", (OPEN_API_DATA_PATH / "api.yaml").open())
    return_code: Exit = main([
        "--output",
        str(output_file),
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "stdin.py").read_text()


@freeze_time("2019-07-26")
def test_validation(mocker: MockerFixture, tmp_path: Path) -> None:
    mock_prance = mocker.patch("prance.BaseParser")

    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--output",
        str(output_file),
        "--validation",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "validation.py").read_text()
    mock_prance.assert_called_once()


@freeze_time("2019-07-26")
def test_validation_failed(mocker: MockerFixture, tmp_path: Path) -> None:
    mock_prance = mocker.patch("prance.BaseParser", side_effect=Exception("error"))
    output_file: Path = tmp_path / "output.py"
    assert (
        main([
            "--input",
            str(OPEN_API_DATA_PATH / "invalid.yaml"),
            "--output",
            str(output_file),
            "--input-file-type",
            "openapi",
            "--validation",
        ])
        == Exit.ERROR
    )
    mock_prance.assert_called_once()


@pytest.mark.parametrize(
    ("output_model", "expected_output", "args"),
    [
        ("pydantic.BaseModel", "with_field_constraints.py", []),
        (
            "pydantic.BaseModel",
            "with_field_constraints_use_unique_items_as_set.py",
            ["--use-unique-items-as-set"],
        ),
        ("pydantic_v2.BaseModel", "with_field_constraints_pydantic_v2.py", []),
        (
            "pydantic_v2.BaseModel",
            "with_field_constraints_pydantic_v2_use_generic_container_types.py",
            ["--use-generic-container-types"],
        ),
        (
            "pydantic_v2.BaseModel",
            "with_field_constraints_pydantic_v2_use_generic_container_types_set.py",
            ["--use-generic-container-types", "--use-unique-items-as-set"],
        ),
        (
            "pydantic_v2.BaseModel",
            "with_field_constraints_pydantic_v2_use_standard_collections.py",
            [
                "--use-standard-collections",
            ],
        ),
        (
            "pydantic_v2.BaseModel",
            "with_field_constraints_pydantic_v2_use_standard_collections_set.py",
            ["--use-standard-collections", "--use-unique-items-as-set"],
        ),
    ],
)
@freeze_time("2019-07-26")
def test_main_with_field_constraints(output_model: str, expected_output: str, args: list[str], tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api_constrained.yaml"),
        "--output",
        str(output_file),
        "--field-constraints",
        "--output-model-type",
        output_model,
        *args,
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / expected_output).read_text()


@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic.BaseModel",
            "without_field_constraints.py",
        ),
        (
            "pydantic_v2.BaseModel",
            "without_field_constraints_pydantic_v2.py",
        ),
    ],
)
@freeze_time("2019-07-26")
def test_main_without_field_constraints(output_model: str, expected_output: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api_constrained.yaml"),
        "--output",
        str(output_file),
        "--output-model-type",
        output_model,
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / expected_output).read_text()


@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic.BaseModel",
            "with_aliases.py",
        ),
        (
            "msgspec.Struct",
            "with_aliases_msgspec.py",
        ),
    ],
)
@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_with_aliases(output_model: str, expected_output: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--aliases",
        str(OPEN_API_DATA_PATH / "aliases.json"),
        "--target-python",
        "3.9",
        "--output-model",
        output_model,
        "--output",
        str(output_file),
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / expected_output).read_text()


def test_main_with_bad_aliases(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--aliases",
        str(OPEN_API_DATA_PATH / "not.json"),
        "--output",
        str(output_file),
    ])
    assert return_code == Exit.ERROR


def test_main_with_more_bad_aliases(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--aliases",
        str(OPEN_API_DATA_PATH / "list.json"),
        "--output",
        str(output_file),
    ])
    assert return_code == Exit.ERROR


def test_main_with_bad_extra_data(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--extra-template-data",
        str(OPEN_API_DATA_PATH / "not.json"),
        "--output",
        str(output_file),
    ])
    assert return_code == Exit.ERROR


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_with_snake_case_field(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--output",
        str(output_file),
        "--snake-case-field",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "with_snake_case_field.py").read_text()


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_with_strip_default_none(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--output",
        str(output_file),
        "--strip-default-none",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "with_strip_default_none.py").read_text()


def test_disable_timestamp(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--output",
        str(output_file),
        "--disable-timestamp",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "disable_timestamp.py").read_text()


@freeze_time("2019-07-26")
def test_enable_version_header(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--output",
        str(output_file),
        "--enable-version-header",
    ])
    assert return_code == Exit.OK
    expected = (EXPECTED_OPENAPI_PATH / "enable_version_header.py").read_text()
    expected = expected.replace("#   version:   0.0.0", f"#   version:   {get_version()}")
    assert output_file.read_text(encoding="utf-8") == expected


@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic.BaseModel",
            "allow_population_by_field_name.py",
        ),
        (
            "pydantic_v2.BaseModel",
            "allow_population_by_field_name_pydantic_v2.py",
        ),
    ],
)
@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_allow_population_by_field_name(output_model: str, expected_output: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--output",
        str(output_file),
        "--allow-population-by-field-name",
        "--output-model-type",
        output_model,
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / expected_output).read_text()


@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic.BaseModel",
            "allow_extra_fields.py",
        ),
        (
            "pydantic_v2.BaseModel",
            "allow_extra_fields_pydantic_v2.py",
        ),
    ],
)
@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_allow_extra_fields(output_model: str, expected_output: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--output",
        str(output_file),
        "--allow-extra-fields",
        "--output-model-type",
        output_model,
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / expected_output).read_text()


@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic.BaseModel",
            "enable_faux_immutability.py",
        ),
        (
            "pydantic_v2.BaseModel",
            "enable_faux_immutability_pydantic_v2.py",
        ),
    ],
)
@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_enable_faux_immutability(output_model: str, expected_output: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--output",
        str(output_file),
        "--enable-faux-immutability",
        "--output-model-type",
        output_model,
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / expected_output).read_text()


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_use_default(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--output",
        str(output_file),
        "--use-default",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "use_default.py").read_text()


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_force_optional(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--output",
        str(output_file),
        "--force-optional",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "force_optional.py").read_text()


@freeze_time("2019-07-26")
def test_main_with_exclusive(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "exclusive.yaml"),
        "--output",
        str(output_file),
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "with_exclusive.py").read_text()


@freeze_time("2019-07-26")
def test_main_subclass_enum(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "subclass_enum.json"),
        "--output",
        str(output_file),
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "subclass_enum.py").read_text()


def test_main_use_standard_collections(tmp_path: Path) -> None:
    input_filename = OPEN_API_DATA_PATH / "modular.yaml"
    output_path = tmp_path / "model"

    with freeze_time(TIMESTAMP):
        main([
            "--input",
            str(input_filename),
            "--output",
            str(output_path),
            "--use-standard-collections",
        ])
    main_use_standard_collections_dir = EXPECTED_OPENAPI_PATH / "use_standard_collections"
    for path in main_use_standard_collections_dir.rglob("*.py"):
        result = output_path.joinpath(path.relative_to(main_use_standard_collections_dir)).read_text()
        assert result == path.read_text()


@pytest.mark.skipif(
    black.__version__.split(".")[0] >= "24",
    reason="Installed black doesn't support the old style",
)
def test_main_use_generic_container_types(tmp_path: Path) -> None:
    input_filename = OPEN_API_DATA_PATH / "modular.yaml"
    output_path = tmp_path / "model"

    with freeze_time(TIMESTAMP):
        main([
            "--input",
            str(input_filename),
            "--output",
            str(output_path),
            "--use-generic-container-types",
        ])
    main_use_generic_container_types_dir = EXPECTED_OPENAPI_PATH / "use_generic_container_types"
    for path in main_use_generic_container_types_dir.rglob("*.py"):
        result = output_path.joinpath(path.relative_to(main_use_generic_container_types_dir)).read_text()
        assert result == path.read_text()


@pytest.mark.skipif(
    black.__version__.split(".")[0] >= "24",
    reason="Installed black doesn't support the old style",
)
@pytest.mark.benchmark
def test_main_use_generic_container_types_standard_collections(
    tmp_path: Path,
) -> None:
    input_filename = OPEN_API_DATA_PATH / "modular.yaml"
    output_path = tmp_path / "model"

    with freeze_time(TIMESTAMP):
        main([
            "--input",
            str(input_filename),
            "--output",
            str(output_path),
            "--use-generic-container-types",
            "--use-standard-collections",
        ])
    main_use_generic_container_types_standard_collections_dir = (
        EXPECTED_OPENAPI_PATH / "use_generic_container_types_standard_collections"
    )
    for path in main_use_generic_container_types_standard_collections_dir.rglob("*.py"):
        result = output_path.joinpath(
            path.relative_to(main_use_generic_container_types_standard_collections_dir)
        ).read_text()
        assert result == path.read_text()


def test_main_original_field_name_delimiter_without_snake_case_field(capsys: pytest.CaptureFixture) -> None:
    input_filename = OPEN_API_DATA_PATH / "modular.yaml"

    return_code: Exit = main([
        "--input",
        str(input_filename),
        "--original-field-name-delimiter",
        "-",
    ])
    captured = capsys.readouterr()
    assert return_code == Exit.ERROR
    assert captured.err == "`--original-field-name-delimiter` can not be used without `--snake-case-field`.\n"


@freeze_time("2019-07-26")
@pytest.mark.parametrize(
    ("output_model", "expected_output", "date_type"),
    [
        ("pydantic.BaseModel", "datetime.py", "AwareDatetime"),
        ("pydantic_v2.BaseModel", "datetime_pydantic_v2.py", "AwareDatetime"),
        ("pydantic_v2.BaseModel", "datetime_pydantic_v2_datetime.py", "datetime"),
        ("dataclasses.dataclass", "datetime_dataclass.py", "datetime"),
        ("msgspec.Struct", "datetime_msgspec.py", "datetime"),
    ],
)
def test_main_openapi_aware_datetime(output_model: str, expected_output: str, date_type: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "datetime.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--output-datetime-class",
        date_type,
        "--output-model",
        output_model,
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / expected_output).read_text()


@freeze_time("2019-07-26")
@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic.BaseModel",
            "datetime.py",
        ),
        (
            "pydantic_v2.BaseModel",
            "datetime_pydantic_v2.py",
        ),
    ],
)
def test_main_openapi_datetime(output_model: str, expected_output: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "datetime.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--output-model",
        output_model,
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / expected_output).read_text()


@freeze_time("2019-07-26")
def test_main_models_not_found(capsys: pytest.CaptureFixture, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "no_components.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
    ])
    captured = capsys.readouterr()
    assert return_code == Exit.ERROR
    assert captured.err == "Models not found in the input data\n"


@pytest.mark.skipif(
    version.parse(pydantic.VERSION) < version.parse("1.9.0"),
    reason="Require Pydantic version 1.9.0 or later ",
)
@freeze_time("2019-07-26")
def test_main_openapi_enum_models_as_literal_one(min_version: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "enum_models.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--enum-field-as-literal",
        "one",
        "--target-python-version",
        min_version,
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "enum_models" / "one.py").read_text()


@pytest.mark.skipif(
    version.parse(pydantic.VERSION) < version.parse("1.9.0"),
    reason="Require Pydantic version 1.9.0 or later ",
)
@freeze_time("2019-07-26")
def test_main_openapi_use_one_literal_as_default(min_version: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "enum_models.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--enum-field-as-literal",
        "one",
        "--target-python-version",
        min_version,
        "--use-one-literal-as-default",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_OPENAPI_PATH / "enum_models" / "one_literal_as_default.py").read_text()
    )


@pytest.mark.skipif(
    version.parse(pydantic.VERSION) < version.parse("1.9.0"),
    reason="Require Pydantic version 1.9.0 or later ",
)
@pytest.mark.skipif(
    black.__version__.split(".")[0] >= "24",
    reason="Installed black doesn't support the old style",
)
@freeze_time("2019-07-26")
def test_main_openapi_enum_models_as_literal_all(min_version: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "enum_models.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--enum-field-as-literal",
        "all",
        "--target-python-version",
        min_version,
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "enum_models" / "all.py").read_text()


@pytest.mark.skipif(
    version.parse(pydantic.VERSION) < version.parse("1.9.0"),
    reason="Require Pydantic version 1.9.0 or later ",
)
@pytest.mark.skipif(
    black.__version__.split(".")[0] >= "24",
    reason="Installed black doesn't support the old style",
)
@freeze_time("2019-07-26")
def test_main_openapi_enum_models_as_literal(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "enum_models.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--enum-field-as-literal",
        "all",
        "--target-python-version",
        f"3.{MIN_VERSION}",
    ])

    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "enum_models" / "as_literal.py").read_text()
    )


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_openapi_all_of_required(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "allof_required.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "allof_required.py").read_text()


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_openapi_nullable(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "nullable.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "nullable.py").read_text()


@freeze_time("2019-07-26")
def test_main_openapi_nullable_strict_nullable(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "nullable.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--strict-nullable",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "nullable_strict_nullable.py").read_text()
    )


@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic.BaseModel",
            "general.py",
        ),
        (
            "pydantic_v2.BaseModel",
            "pydantic_v2.py",
        ),
        (
            "msgspec.Struct",
            "msgspec_pattern.py",
        ),
    ],
)
@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_openapi_pattern(output_model: str, expected_output: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "pattern.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--target-python",
        "3.9",
        "--output-model-type",
        output_model,
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (
        EXPECTED_OPENAPI_PATH / "pattern" / expected_output
    ).read_text().replace("pattern.json", "pattern.yaml")


@pytest.mark.parametrize(
    ("expected_output", "args"),
    [
        ("pattern_with_lookaround_pydantic_v2.py", []),
        (
            "pattern_with_lookaround_pydantic_v2_field_constraints.py",
            ["--field-constraints"],
        ),
    ],
)
@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] < "22",
    reason="Installed black doesn't support Python version 3.10",
)
def test_main_openapi_pattern_with_lookaround_pydantic_v2(
    expected_output: str, args: list[str], tmp_path: Path
) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "pattern_lookaround.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--target-python",
        "3.9",
        "--output-model-type",
        "pydantic_v2.BaseModel",
        *args,
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / expected_output).read_text()


@freeze_time("2019-07-26")
def test_main_generate_custom_class_name_generator_modular(
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "model"
    main_modular_custom_class_name_dir = EXPECTED_OPENAPI_PATH / "modular_custom_class_name"

    def custom_class_name_generator(name: str) -> str:
        return f"Custom{name[0].upper() + name[1:]}"

    with freeze_time(TIMESTAMP):
        input_ = (OPEN_API_DATA_PATH / "modular.yaml").relative_to(Path.cwd(), walk_up=True)
        assert not input_.is_absolute()
        generate(
            input_=input_,
            input_file_type=InputFileType.OpenAPI,
            output=output_path,
            custom_class_name_generator=custom_class_name_generator,
        )

        for path in main_modular_custom_class_name_dir.rglob("*.py"):
            result = output_path.joinpath(path.relative_to(main_modular_custom_class_name_dir)).read_text()
            assert result == path.read_text()


@freeze_time("2019-07-26")
def test_main_http_openapi(mocker: MockerFixture, tmp_path: Path) -> None:
    def get_mock_response(path: str) -> Mock:
        mock = mocker.Mock()
        mock.text = (OPEN_API_DATA_PATH / path).read_text()
        return mock

    httpx_get_mock = mocker.patch(
        "httpx.get",
        side_effect=[
            get_mock_response("refs.yaml"),
            get_mock_response("definitions.yaml"),
        ],
    )

    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--url",
        "https://example.com/refs.yaml",
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "http_refs.py").read_text()
    httpx_get_mock.assert_has_calls([
        call(
            "https://example.com/refs.yaml",
            headers=None,
            verify=True,
            follow_redirects=True,
            params=None,
        ),
        call(
            "https://teamdigitale.github.io/openapi/0.0.6/definitions.yaml",
            headers=None,
            verify=True,
            follow_redirects=True,
            params=None,
        ),
    ])


@freeze_time("2019-07-26")
def test_main_disable_appending_item_suffix(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api_constrained.yaml"),
        "--output",
        str(output_file),
        "--field-constraints",
        "--disable-appending-item-suffix",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_OPENAPI_PATH / "disable_appending_item_suffix.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_openapi_body_and_parameters(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "body_and_parameters.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--openapi-scopes",
        "paths",
        "schemas",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_OPENAPI_PATH / "body_and_parameters" / "general.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_openapi_body_and_parameters_remote_ref(mocker: MockerFixture, tmp_path: Path) -> None:
    input_path = OPEN_API_DATA_PATH / "body_and_parameters_remote_ref.yaml"
    person_response = mocker.Mock()
    person_response.text = input_path.read_text()
    httpx_get_mock = mocker.patch("httpx.get", side_effect=[person_response])

    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(input_path),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--openapi-scopes",
        "paths",
        "schemas",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_OPENAPI_PATH / "body_and_parameters" / "remote_ref.py").read_text()
    )
    httpx_get_mock.assert_has_calls([
        call(
            "https://schema.example",
            headers=None,
            verify=True,
            follow_redirects=True,
            params=None,
        ),
    ])


@freeze_time("2019-07-26")
def test_main_openapi_body_and_parameters_only_paths(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "body_and_parameters.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--openapi-scopes",
        "paths",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_OPENAPI_PATH / "body_and_parameters" / "only_paths.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_openapi_body_and_parameters_only_schemas(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "body_and_parameters.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--openapi-scopes",
        "schemas",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_OPENAPI_PATH / "body_and_parameters" / "only_schemas.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_openapi_content_in_parameters(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "content_in_parameters.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "content_in_parameters.py").read_text()


@freeze_time("2019-07-26")
def test_main_openapi_oas_response_reference(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "oas_response_reference.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--openapi-scopes",
        "paths",
        "schemas",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "oas_response_reference.py").read_text()


@freeze_time("2019-07-26")
def test_main_openapi_json_pointer(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "json_pointer.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "json_pointer.py").read_text()


@freeze_time("2019-07-26")
@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        ("pydantic.BaseModel", "use_annotated_with_field_constraints.py"),
        (
            "pydantic_v2.BaseModel",
            "use_annotated_with_field_constraints_pydantic_v2.py",
        ),
    ],
)
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_use_annotated_with_field_constraints(
    output_model: str, expected_output: str, min_version: str, tmp_path: Path
) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api_constrained.yaml"),
        "--output",
        str(output_file),
        "--field-constraints",
        "--use-annotated",
        "--target-python-version",
        min_version,
        "--output-model",
        output_model,
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / expected_output).read_text()


@freeze_time("2019-07-26")
def test_main_nested_enum(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "nested_enum.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "nested_enum.py").read_text()


@freeze_time("2019-07-26")
def test_openapi_special_yaml_keywords(mocker: MockerFixture, tmp_path: Path) -> None:
    mock_prance = mocker.patch("prance.BaseParser")

    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "special_yaml_keywords.yaml"),
        "--output",
        str(output_file),
        "--validation",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "special_yaml_keywords.py").read_text()
    mock_prance.assert_called_once()


@pytest.mark.skipif(
    black.__version__.split(".")[0] < "22",
    reason="Installed black doesn't support Python version 3.10",
)
@freeze_time("2019-07-26")
def test_main_openapi_nullable_use_union_operator(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "nullable.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--use-union-operator",
        "--strict-nullable",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_OPENAPI_PATH / "nullable_strict_nullable_use_union_operator.py").read_text()
    )


@freeze_time("2019-07-26")
def test_external_relative_ref(tmp_path: Path) -> None:
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "external_relative_ref" / "model_b"),
        "--output",
        str(tmp_path),
    ])
    assert return_code == Exit.OK
    main_modular_dir = EXPECTED_OPENAPI_PATH / "external_relative_ref"
    for path in main_modular_dir.rglob("*.py"):
        result = tmp_path.joinpath(path.relative_to(main_modular_dir)).read_text()
        assert result == path.read_text()


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_collapse_root_models(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "not_real_string.json"),
        "--output",
        str(output_file),
        "--collapse-root-models",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "collapse_root_models.py").read_text()


@freeze_time("2019-07-26")
def test_main_collapse_root_models_field_constraints(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "not_real_string.json"),
        "--output",
        str(output_file),
        "--collapse-root-models",
        "--field-constraints",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_OPENAPI_PATH / "collapse_root_models_field_constraints.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_collapse_root_models_with_references_to_flat_types(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "flat_type.jsonschema"),
        "--output",
        str(output_file),
        "--collapse-root-models",
    ])

    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_OPENAPI_PATH / "collapse_root_models_with_references_to_flat_types.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_openapi_max_items_enum(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "max_items_enum.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "max_items_enum.py").read_text()


@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic.BaseModel",
            "const.py",
        ),
        (
            "pydantic_v2.BaseModel",
            "const_pydantic_v2.py",
        ),
    ],
)
@freeze_time("2019-07-26")
def test_main_openapi_const(output_model: str, expected_output: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "const.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--output-model",
        output_model,
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / expected_output).read_text()


@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic.BaseModel",
            "const_field.py",
        ),
        (
            "pydantic_v2.BaseModel",
            "const_field_pydantic_v2.py",
        ),
        (
            "msgspec.Struct",
            "const_field_msgspec.py",
        ),
        (
            "typing.TypedDict",
            "const_field_typed_dict.py",
        ),
        (
            "dataclasses.dataclass",
            "const_field_dataclass.py",
        ),
    ],
)
@freeze_time("2019-07-26")
def test_main_openapi_const_field(output_model: str, expected_output: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "const.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--output-model",
        output_model,
        "--collapse-root-models",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / expected_output).read_text()


@freeze_time("2019-07-26")
def test_main_openapi_complex_reference(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "complex_reference.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "complex_reference.py").read_text()


@freeze_time("2019-07-26")
def test_main_openapi_reference_to_object_properties(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "reference_to_object_properties.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_OPENAPI_PATH / "reference_to_object_properties.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_openapi_reference_to_object_properties_collapse_root_models(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "reference_to_object_properties.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--collapse-root-models",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_OPENAPI_PATH / "reference_to_object_properties_collapse_root_models.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_openapi_override_required_all_of_field(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "override_required_all_of.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--collapse-root-models",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "override_required_all_of.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_use_default_kwarg(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "nullable.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--use-default-kwarg",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "use_default_kwarg.py").read_text()


@pytest.mark.parametrize(
    ("input_", "output"),
    [
        (
            "discriminator.yaml",
            "general.py",
        ),
        (
            "discriminator_without_mapping.yaml",
            "without_mapping.py",
        ),
    ],
)
@freeze_time("2019-07-26")
def test_main_openapi_discriminator(input_: str, output: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / input_),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "discriminator" / output).read_text()


@freeze_time("2023-07-27")
@pytest.mark.parametrize(
    ("kind", "option", "expected"),
    [
        (
            "anyOf",
            "--collapse-root-models",
            "in_array_collapse_root_models.py",
        ),
        (
            "oneOf",
            "--collapse-root-models",
            "in_array_collapse_root_models.py",
        ),
        ("anyOf", None, "in_array.py"),
        ("oneOf", None, "in_array.py"),
    ],
)
def test_main_openapi_discriminator_in_array(kind: str, option: str | None, expected: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    input_file = f"discriminator_in_array_{kind.lower()}.yaml"
    return_code: Exit = main([
        a
        for a in [
            "--input",
            str(OPEN_API_DATA_PATH / input_file),
            "--output",
            str(output_file),
            "--input-file-type",
            "openapi",
            option,
        ]
        if a
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (
        EXPECTED_OPENAPI_PATH / "discriminator" / expected
    ).read_text().replace("discriminator_in_array.yaml", input_file)


@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic.BaseModel",
            "default_object",
        ),
        (
            "pydantic_v2.BaseModel",
            "pydantic_v2_default_object",
        ),
        (
            "msgspec.Struct",
            "msgspec_default_object",
        ),
    ],
)
@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_openapi_default_object(output_model: str, expected_output: str, tmp_path: Path) -> None:
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "default_object.yaml"),
        "--output",
        str(tmp_path),
        "--output-model",
        output_model,
        "--input-file-type",
        "openapi",
        "--target-python-version",
        "3.9",
    ])
    assert return_code == Exit.OK

    main_modular_dir = EXPECTED_OPENAPI_PATH / expected_output
    for path in main_modular_dir.rglob("*.py"):
        result = tmp_path.joinpath(path.relative_to(main_modular_dir)).read_text()
        assert result == path.read_text(), path


@freeze_time("2019-07-26")
def test_main_dataclass(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--output",
        str(output_file),
        "--output-model-type",
        "dataclasses.dataclass",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "dataclass.py").read_text()


@freeze_time("2019-07-26")
def test_main_dataclass_base_class(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--output",
        str(output_file),
        "--output-model-type",
        "dataclasses.dataclass",
        "--base-class",
        "custom_base.Base",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "dataclass_base_class.py").read_text()


@freeze_time("2019-07-26")
def test_main_openapi_reference_same_hierarchy_directory(tmp_path: Path) -> None:
    with chdir(OPEN_API_DATA_PATH / "reference_same_hierarchy_directory"):
        output_file: Path = tmp_path / "output.py"
        return_code: Exit = main([
            "--input",
            "./public/entities.yaml",
            "--output",
            str(output_file),
            "--input-file-type",
            "openapi",
        ])
        assert return_code == Exit.OK
        assert (
            output_file.read_text(encoding="utf-8")
            == (EXPECTED_OPENAPI_PATH / "reference_same_hierarchy_directory.py").read_text()
        )


@freeze_time("2019-07-26")
def test_main_multiple_required_any_of(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "multiple_required_any_of.yaml"),
        "--output",
        str(output_file),
        "--collapse-root-models",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "multiple_required_any_of.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_openapi_max_min(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "max_min_number.yaml"),
        "--output",
        str(output_file),
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "max_min_number.py").read_text()


@freeze_time("2019-07-26")
def test_main_openapi_use_operation_id_as_name(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--output",
        str(output_file),
        "--use-operation-id-as-name",
        "--openapi-scopes",
        "paths",
        "schemas",
        "parameters",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "use_operation_id_as_name.py").read_text()
    )


@freeze_time("2019-07-26")
@pytest.mark.xfail
def test_main_openapi_use_operation_id_as_name_not_found_operation_id(
    capsys: pytest.CaptureFixture, tmp_path: Path
) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "body_and_parameters.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--use-operation-id-as-name",
        "--openapi-scopes",
        "paths",
        "schemas",
        "parameters",
    ])
    captured = capsys.readouterr()
    assert return_code == Exit.ERROR
    assert (
        captured.err == "All operations must have an operationId when --use_operation_id_as_name is set."
        "The following path was missing an operationId: pets\n"
    )


@freeze_time("2019-07-26")
def test_main_unsorted_optional_fields(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "unsorted_optional_fields.yaml"),
        "--output",
        str(output_file),
        "--output-model-type",
        "dataclasses.dataclass",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "unsorted_optional_fields.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_typed_dict(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--output",
        str(output_file),
        "--output-model-type",
        "typing.TypedDict",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "typed_dict.py").read_text()


@freeze_time("2019-07-26")
def test_main_typed_dict_py(min_version: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--output",
        str(output_file),
        "--output-model-type",
        "typing.TypedDict",
        "--target-python-version",
        min_version,
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "typed_dict_py.py").read_text()


@pytest.mark.skipif(
    version.parse(black.__version__) < version.parse("23.3.0"),
    reason="Require Black version 23.3.0 or later ",
)
def test_main_modular_typed_dict(tmp_path: Path) -> None:
    """Test main function on modular file."""

    input_filename = OPEN_API_DATA_PATH / "modular.yaml"
    output_path = tmp_path / "model"

    with freeze_time(TIMESTAMP):
        main([
            "--input",
            str(input_filename),
            "--output",
            str(output_path),
            "--output-model-type",
            "typing.TypedDict",
            "--target-python-version",
            "3.11",
        ])
    main_modular_dir = EXPECTED_OPENAPI_PATH / "modular_typed_dict"
    for path in main_modular_dir.rglob("*.py"):
        result = output_path.joinpath(path.relative_to(main_modular_dir)).read_text()
        assert result == path.read_text()


@pytest.mark.skipif(
    version.parse(black.__version__) < version.parse("23.3.0"),
    reason="Require Black version 23.3.0 or later ",
)
@freeze_time("2019-07-26")
def test_main_typed_dict_nullable(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "nullable.yaml"),
        "--output",
        str(output_file),
        "--output-model-type",
        "typing.TypedDict",
        "--target-python-version",
        "3.11",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "typed_dict_nullable.py").read_text()


@pytest.mark.skipif(
    version.parse(black.__version__) < version.parse("23.3.0"),
    reason="Require Black version 23.3.0 or later ",
)
@freeze_time("2019-07-26")
def test_main_typed_dict_nullable_strict_nullable(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "nullable.yaml"),
        "--output",
        str(output_file),
        "--output-model-type",
        "typing.TypedDict",
        "--target-python-version",
        "3.11",
        "--strict-nullable",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_OPENAPI_PATH / "typed_dict_nullable_strict_nullable.py").read_text()
    )


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_openapi_nullable_31(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "nullable_31.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--output-model-type",
        "pydantic_v2.BaseModel",
        "--strip-default-none",
        "--use-union-operator",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "nullable_31.py").read_text()


@freeze_time("2019-07-26")
def test_main_custom_file_header_path(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--output",
        str(output_file),
        "--custom-file-header-path",
        str(DATA_PATH / "custom_file_header.txt"),
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "custom_file_header.py").read_text()


@freeze_time("2019-07-26")
def test_main_custom_file_header_duplicate_options(capsys: pytest.CaptureFixture, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--output",
        str(output_file),
        "--custom-file-header-path",
        str(DATA_PATH / "custom_file_header.txt"),
        "--custom-file-header",
        "abc",
    ])

    captured = capsys.readouterr()
    assert return_code == Exit.ERROR
    assert captured.err == "`--custom_file_header_path` can not be used with `--custom_file_header`.\n"


@freeze_time("2019-07-26")
def test_main_pydantic_v2(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--output",
        str(output_file),
        "--output-model-type",
        "pydantic_v2.BaseModel",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "pydantic_v2.py").read_text()


@freeze_time("2019-07-26")
def test_main_openapi_custom_id_pydantic_v2(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "custom_id.yaml"),
        "--output",
        str(output_file),
        "--output-model-type",
        "pydantic_v2.BaseModel",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "custom_id_pydantic_v2.py").read_text()


@pytest.mark.skipif(
    not isort.__version__.startswith("4."),
    reason="isort 5.x don't sort pydantic modules",
)
@freeze_time("2019-07-26")
def test_main_openapi_custom_id_pydantic_v2_custom_base(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "custom_id.yaml"),
        "--output",
        str(output_file),
        "--output-model-type",
        "pydantic_v2.BaseModel",
        "--base-class",
        "custom_base.Base",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_OPENAPI_PATH / "custom_id_pydantic_v2_custom_base.py").read_text()
    )


@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_openapi_all_of_with_relative_ref(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "all_of_with_relative_ref" / "openapi.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--output-model-type",
        "pydantic_v2.BaseModel",
        "--keep-model-order",
        "--collapse-root-models",
        "--field-constraints",
        "--use-title-as-name",
        "--field-include-all-keys",
        "--use-field-description",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "all_of_with_relative_ref.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_openapi_msgspec_struct(min_version: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--output",
        str(output_file),
        "--target-python-version",
        min_version,
        "--output-model-type",
        "msgspec.Struct",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "msgspec_struct.py").read_text()


@freeze_time("2019-07-26")
def test_main_openapi_msgspec_struct_snake_case(min_version: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api_ordered_required_fields.yaml"),
        "--output",
        str(output_file),
        "--target-python-version",
        min_version,
        "--snake-case-field",
        "--output-model-type",
        "msgspec.Struct",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "msgspec_struct_snake_case.py").read_text()
    )


@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_openapi_msgspec_use_annotated_with_field_constraints(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api_constrained.yaml"),
        "--output",
        str(output_file),
        "--field-constraints",
        "--target-python-version",
        "3.9",
        "--output-model-type",
        "msgspec.Struct",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_OPENAPI_PATH / "msgspec_use_annotated_with_field_constraints.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_openapi_discriminator_one_literal_as_default(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "discriminator_enum.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--output-model-type",
        "pydantic_v2.BaseModel",
        "--use-one-literal-as-default",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_OPENAPI_PATH / "discriminator" / "enum_one_literal_as_default.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_openapi_discriminator_one_literal_as_default_dataclass(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "discriminator_enum.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--output-model-type",
        "dataclasses.dataclass",
        "--use-one-literal-as-default",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_OPENAPI_PATH / "discriminator" / "dataclass_enum_one_literal_as_default.py").read_text()
    )


@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_openapi_keyword_only_dataclass(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "inheritance.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--output-model-type",
        "dataclasses.dataclass",
        "--keyword-only",
        "--target-python-version",
        "3.10",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "dataclass_keyword_only.py").read_text()


@freeze_time("2019-07-26")
def test_main_openapi_keyword_only_dataclass_with_python_3_9(capsys: pytest.CaptureFixture) -> None:
    return_code = main([
        "--input",
        str(OPEN_API_DATA_PATH / "inheritance.yaml"),
        "--input-file-type",
        "openapi",
        "--output-model-type",
        "dataclasses.dataclass",
        "--keyword-only",
        "--target-python-version",
        "3.9",
    ])
    assert return_code == Exit.ERROR
    captured = capsys.readouterr()
    assert not captured.out
    assert captured.err == "`--keyword-only` requires `--target-python-version` 3.10 or higher.\n"


@freeze_time("2019-07-26")
def test_main_openapi_dataclass_with_naive_datetime(capsys: pytest.CaptureFixture) -> None:
    return_code = main([
        "--input",
        str(OPEN_API_DATA_PATH / "inheritance.yaml"),
        "--input-file-type",
        "openapi",
        "--output-model-type",
        "dataclasses.dataclass",
        "--output-datetime-class",
        "NaiveDatetime",
    ])
    assert return_code == Exit.ERROR
    captured = capsys.readouterr()
    assert not captured.out
    assert (
        captured.err
        == '`--output-datetime-class` only allows "datetime" for `--output-model-type` dataclasses.dataclass\n'
    )


@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_openapi_keyword_only_msgspec(min_version: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "inheritance.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--output-model-type",
        "msgspec.Struct",
        "--keyword-only",
        "--target-python-version",
        min_version,
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "msgspec_keyword_only.py").read_text()


@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_openapi_keyword_only_msgspec_with_extra_data(min_version: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "inheritance.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--output-model-type",
        "msgspec.Struct",
        "--keyword-only",
        "--target-python-version",
        min_version,
        "--extra-template-data",
        str(OPEN_API_DATA_PATH / "extra_data_msgspec.json"),
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_OPENAPI_PATH / "msgspec_keyword_only_omit_defaults.py").read_text()
    )


@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_generate_openapi_keyword_only_msgspec_with_extra_data(tmp_path: Path) -> None:
    extra_data = json.loads((OPEN_API_DATA_PATH / "extra_data_msgspec.json").read_text())
    output_file: Path = tmp_path / "output.py"
    generate(
        input_=OPEN_API_DATA_PATH / "inheritance.yaml",
        output=output_file,
        input_file_type=InputFileType.OpenAPI,
        output_model_type=DataModelType.MsgspecStruct,
        keyword_only=True,
        target_python_version=PythonVersionMin,
        extra_template_data=defaultdict(dict, extra_data),
        # Following values are defaults in the CLI, but not in the API
        openapi_scopes=[OpenAPIScope.Schemas],
        # Following values are implied by `msgspec.Struct` in the CLI
        use_annotated=True,
        field_constraints=True,
    )
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_OPENAPI_PATH / "msgspec_keyword_only_omit_defaults.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_openapi_referenced_default(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "referenced_default.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--output-model-type",
        "pydantic_v2.BaseModel",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "referenced_default.py").read_text()


@freeze_time("2019-07-26")
def test_duplicate_models(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "duplicate_models2.yaml"),
        "--output",
        str(output_file),
        "--use-operation-id-as-name",
        "--openapi-scopes",
        "paths",
        "schemas",
        "parameters",
        "--output-model-type",
        "pydantic_v2.BaseModel",
        "--parent-scoped-naming",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "duplicate_models2.py").read_text()


@freeze_time("2019-07-26")
def test_main_openapi_shadowed_imports(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "shadowed_imports.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--output-model-type",
        "pydantic_v2.BaseModel",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "shadowed_imports.py").read_text()


@freeze_time("2019-07-26")
def test_main_openapi_extra_fields_forbid(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "additional_properties.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
        "--extra-fields",
        "forbid",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "additional_properties.py").read_text()


@freeze_time("2019-07-26")
def test_main_openapi_same_name_objects(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "same_name_objects.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "openapi",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_OPENAPI_PATH / "same_name_objects.py").read_text()
