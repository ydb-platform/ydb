from __future__ import annotations

import shutil
from argparse import Namespace
from pathlib import Path

import black
import pytest
from freezegun import freeze_time

from datamodel_code_generator import MIN_VERSION, chdir, inferred_message
from datamodel_code_generator.__main__ import Exit, main

import yatest.common as yc
DATA_PATH: Path = Path(yc.source_path(__file__)).parent / "data"
OPEN_API_DATA_PATH: Path = DATA_PATH / "openapi"
EXPECTED_MAIN_KR_PATH = DATA_PATH / "expected" / "main_kr"


TIMESTAMP = "1985-10-26T01:21:00-07:00"


@pytest.fixture(autouse=True)
def reset_namespace(monkeypatch: pytest.MonkeyPatch) -> None:
    namespace_ = Namespace(no_color=False)
    monkeypatch.setattr("datamodel_code_generator.__main__.namespace", namespace_)
    monkeypatch.setattr("datamodel_code_generator.arguments.namespace", namespace_)


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
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_MAIN_KR_PATH / "main" / "output.py").read_text()


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
    assert (
        output_file.read_text(encoding="utf-8") == (EXPECTED_MAIN_KR_PATH / "main_base_class" / "output.py").read_text()
    )


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
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_MAIN_KR_PATH / "target_python_version" / "output.py").read_text()
    )


def test_main_modular(tmp_path: Path) -> None:
    """Test main function on modular file."""
    input_filename = OPEN_API_DATA_PATH / "modular.yaml"
    output_path = tmp_path / "model"

    with freeze_time(TIMESTAMP):
        main(["--input", str(input_filename), "--output", str(output_path)])
    main_modular_dir = EXPECTED_MAIN_KR_PATH / "main_modular"
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
def test_main_no_file(capsys: pytest.CaptureFixture, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    """Test main function on non-modular file with no output name."""

    input_filename = OPEN_API_DATA_PATH / "api.yaml"

    with freeze_time(TIMESTAMP):
        main(["--input", str(input_filename)])

    captured = capsys.readouterr()
    assert captured.out == (EXPECTED_MAIN_KR_PATH / "main_no_file" / "output.py").read_text()

    assert captured.err == inferred_message.format("openapi") + "\n"


@pytest.mark.xfail(reason="Relative paths in testing")
def test_main_custom_template_dir(
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
    assert captured.out == (EXPECTED_MAIN_KR_PATH / "main_custom_template_dir" / "output.py").read_text()
    assert captured.err == inferred_message.format("openapi") + "\n"


@pytest.mark.skipif(
    black.__version__.split(".")[0] >= "24",
    reason="Installed black doesn't support the old style",
)
@freeze_time("2019-07-26")
def test_pyproject(tmp_path: Path) -> None:
    pyproject_toml = DATA_PATH / "project" / "pyproject.toml"
    shutil.copy(pyproject_toml, tmp_path)
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api.yaml"),
        "--output",
        str(output_file),
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_MAIN_KR_PATH / "pyproject" / "output.py").read_text()


@pytest.mark.parametrize("language", ["UK", "US"])
def test_pyproject_respects_both_spellings_of_capitalize_enum_members_flag(language: str, tmp_path: Path) -> None:
    pyproject_toml_data = f"""
[tool.datamodel-codegen]
capitali{"s" if language == "UK" else "z"}e-enum-members = true
enable-version-header = false
input-file-type = "jsonschema"
"""
    with (tmp_path / "pyproject.toml").open("w") as f:
        f.write(pyproject_toml_data)

        input_data = """
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "definitions": {
    "MyEnum": {
      "enum": [
        "MEMBER_1",
        "member_2"
      ]
    }
  }
}
"""
    input_file = tmp_path / "schema.json"
    with input_file.open("w") as f:
        f.write(input_data)

    expected_output = """# generated by datamodel-codegen:
#   filename:  schema.json

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel


class Model(BaseModel):
    __root__: Any


class MyEnum(Enum):
    MEMBER_1 = 'MEMBER_1'
    member_2 = 'member_2'
"""

    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--disable-timestamp",
        "--input",
        input_file.as_posix(),
        "--output",
        output_file.as_posix(),
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == expected_output, (
        f"\nExpected  output:\n{expected_output}\n\nGenerated output:\n{output_file.read_text(encoding='utf-8')}"
    )


@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
@freeze_time("2019-07-26")
def test_pyproject_with_tool_section(tmp_path: Path) -> None:
    """Test that a pyproject.toml with a [tool.datamodel-codegen] section is
    found and its configuration applied.
    """
    pyproject_toml = """
[tool.datamodel-codegen]
target-python-version = "3.10"
strict-types = ["str"]
"""
    (tmp_path / "pyproject.toml").write_text(pyproject_toml)
    output_file: Path = tmp_path / "output.py"

    # Run main from within the output directory so we can find our
    # pyproject.toml.
    with chdir(tmp_path):
        return_code: Exit = main([
            "--input",
            str((OPEN_API_DATA_PATH / "api.yaml").resolve()),
            "--output",
            str(output_file.resolve()),
        ])

    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        # We expect the output to use pydantic.StrictStr in place of str
        == (EXPECTED_MAIN_KR_PATH / "pyproject" / "output.strictstr.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_use_schema_description(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api_multiline_docstrings.yaml"),
        "--output",
        str(output_file),
        "--use-schema-description",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_MAIN_KR_PATH / "main_use_schema_description" / "output.py").read_text()
    )


@freeze_time("2022-11-11")
def test_main_use_field_description(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(OPEN_API_DATA_PATH / "api_multiline_docstrings.yaml"),
        "--output",
        str(output_file),
        "--use-field-description",
    ])
    assert return_code == Exit.OK
    generated = output_file.read_text(encoding="utf-8")
    expected = (EXPECTED_MAIN_KR_PATH / "main_use_field_description" / "output.py").read_text()
    assert generated == expected


def test_capitalise_enum_members(tmp_path: Path) -> None:
    """capitalise-enum-members not working since v0.28.5

    From https://github.com/koxudaxi/datamodel-code-generator/issues/2370
    """
    input_data = """
openapi: 3.0.3
info:
  version: X.Y.Z
  title: example schema
servers:
  - url: "https://acme.org"
paths: {}
components:
  schemas:
    EnumSystems:
      type: enum
      enum:
        - linux
        - osx
        - windows
"""
    input_file = tmp_path / "myschema.yaml"
    input_file.write_text(input_data, encoding="utf_8")

    expected_output = """# generated by datamodel-codegen:
#   filename:  myschema.yaml

from __future__ import annotations

from enum import Enum


class EnumSystems(Enum):
    LINUX = 'linux'
    OSX = 'osx'
    WINDOWS = 'windows'
"""

    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--output-model-type",
        "pydantic_v2.BaseModel",
        "--disable-timestamp",
        "--capitalise-enum-members",
        "--snake-case-field",
        "--input",
        str(input_file),
        "--output",
        str(output_file),
    ])
    assert return_code == Exit.OK
    output_file_read_text = output_file.read_text(encoding="utf_8")
    assert output_file_read_text == expected_output, (
        f"\nExpected  output:\n{expected_output}\n\nGenerated output:\n{output_file_read_text}"
    )


def test_capitalise_enum_members_and_use_subclass_enum(tmp_path: Path) -> None:
    """Combination of capitalise-enum-members and use-subclass-enum not working since v0.28.5

    From https://github.com/koxudaxi/datamodel-code-generator/issues/2395
    """
    input_data = """
openapi: 3.0.3
info:
  version: X.Y.Z
  title: example schema
servers:
  - url: "https://acme.org"
paths: {}
components:
  schemas:
    EnumSystems:
      type: string
      enum:
        - linux
        - osx
        - windows
"""
    input_file = tmp_path / "myschema.yaml"
    input_file.write_text(input_data, encoding="utf_8")

    expected_output = """# generated by datamodel-codegen:
#   filename:  myschema.yaml

from __future__ import annotations

from enum import Enum


class EnumSystems(str, Enum):
    LINUX = 'linux'
    OSX = 'osx'
    WINDOWS = 'windows'
"""

    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--output-model-type",
        "pydantic_v2.BaseModel",
        "--disable-timestamp",
        "--capitalise-enum-members",
        "--snake-case-field",
        "--use-subclass-enum",
        "--input",
        str(input_file),
        "--output",
        str(output_file),
    ])
    assert return_code == Exit.OK
    output_file_read_text = output_file.read_text(encoding="utf_8")
    assert output_file_read_text == expected_output, (
        f"\nExpected  output:\n{expected_output}\n\nGenerated output:\n{output_file_read_text}"
    )
