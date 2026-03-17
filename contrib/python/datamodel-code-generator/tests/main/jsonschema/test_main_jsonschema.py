from __future__ import annotations

import contextlib
import json
import shutil
from argparse import Namespace
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import call

import black
import isort
import pytest
from freezegun import freeze_time
from packaging import version

from datamodel_code_generator import (
    MIN_VERSION,
    DataModelType,
    InputFileType,
    PythonVersionMin,
    chdir,
    generate,
)
from datamodel_code_generator.__main__ import Exit, main
from __tests__.main.test_main_general import DATA_PATH, EXPECTED_MAIN_PATH, TIMESTAMP

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

with contextlib.suppress(ImportError):
    pass


FixtureRequest = pytest.FixtureRequest


JSON_SCHEMA_DATA_PATH: Path = DATA_PATH / "jsonschema"
EXPECTED_JSON_SCHEMA_PATH: Path = EXPECTED_MAIN_PATH / "jsonschema"


@pytest.fixture(autouse=True)
def reset_namespace(monkeypatch: pytest.MonkeyPatch) -> None:
    namespace_ = Namespace(no_color=False)
    monkeypatch.setattr("datamodel_code_generator.__main__.namespace", namespace_)
    monkeypatch.setattr("datamodel_code_generator.arguments.namespace", namespace_)


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_inheritance_forward_ref(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    shutil.copy(DATA_PATH / "pyproject.toml", tmp_path / "pyproject.toml")
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "inheritance_forward_ref.json"),
        "--output",
        str(output_file),
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "inheritance_forward_ref.py").read_text()
    )


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_inheritance_forward_ref_keep_model_order(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    shutil.copy(DATA_PATH / "pyproject.toml", tmp_path / "pyproject.toml")
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "inheritance_forward_ref.json"),
        "--output",
        str(output_file),
        "--keep-model-order",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "inheritance_forward_ref_keep_model_order.py").read_text()
    )


@pytest.mark.skip(reason="pytest-xdist does not support the test")
@freeze_time("2019-07-26")
def test_main_without_arguments() -> None:
    with pytest.raises(SystemExit):
        main()


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_autodetect(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "person.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "auto",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "autodetect.py").read_text()


@freeze_time("2019-07-26")
def test_main_autodetect_failed(tmp_path: Path) -> None:
    input_file: Path = tmp_path / "input.yaml"
    output_file: Path = tmp_path / "output.py"

    input_file.write_text(":", encoding="utf-8")

    return_code: Exit = main([
        "--input",
        str(input_file),
        "--output",
        str(output_file),
        "--input-file-type",
        "auto",
    ])
    assert return_code == Exit.ERROR


@freeze_time("2019-07-26")
def test_main_jsonschema(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "person.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "general.py").read_text()


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_jsonschema_nested_deep(tmp_path: Path) -> None:
    output_init_file: Path = tmp_path / "__init__.py"
    output_nested_file: Path = tmp_path / "nested/deep.py"
    output_empty_parent_nested_file: Path = tmp_path / "empty_parent/nested/deep.py"

    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "nested_person.json"),
        "--output",
        str(tmp_path),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert (
        output_init_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "nested_deep" / "__init__.py").read_text()
    )

    assert (
        output_nested_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "nested_deep" / "nested" / "deep.py").read_text()
    )
    assert (
        output_empty_parent_nested_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "nested_deep" / "empty_parent" / "nested" / "deep.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_jsonschema_nested_skip(tmp_path: Path) -> None:
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "nested_skip.json"),
        "--output",
        str(tmp_path),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    nested_skip_dir = EXPECTED_JSON_SCHEMA_PATH / "nested_skip"
    for path in nested_skip_dir.rglob("*.py"):
        result = tmp_path.joinpath(path.relative_to(nested_skip_dir)).read_text()
        assert result == path.read_text()


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_jsonschema_external_files(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "external_parent_root.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "external_files.py").read_text()


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_jsonschema_collapsed_external_references(tmp_path: Path) -> None:
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "external_reference"),
        "--output",
        str(tmp_path),
        "--input-file-type",
        "jsonschema",
        "--collapse-root-models",
    ])
    assert return_code == Exit.OK
    assert (tmp_path / "ref0.py").read_text() == (EXPECTED_JSON_SCHEMA_PATH / "external_ref0.py").read_text()
    assert (tmp_path / "other/ref2.py").read_text() == (
        EXPECTED_JSON_SCHEMA_PATH / "external_other_ref2.py"
    ).read_text()


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_jsonschema_multiple_files(tmp_path: Path) -> None:
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "multiple_files"),
        "--output",
        str(tmp_path),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    main_modular_dir = EXPECTED_JSON_SCHEMA_PATH / "multiple_files"
    for path in main_modular_dir.rglob("*.py"):
        result = tmp_path.joinpath(path.relative_to(main_modular_dir)).read_text()
        assert result == path.read_text()


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_jsonschema_no_empty_collapsed_external_model(tmp_path: Path) -> None:
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "external_collapse"),
        "--output",
        str(tmp_path),
        "--input-file-type",
        "jsonschema",
        "--collapse-root-models",
    ])
    assert return_code == Exit.OK
    assert not (tmp_path / "child.py").exists()
    assert (tmp_path / "__init__.py").exists()


@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic.BaseModel",
            "null_and_array.py",
        ),
        (
            "pydantic_v2.BaseModel",
            "null_and_array_v2.py",
        ),
    ],
)
@freeze_time("2019-07-26")
def test_main_null_and_array(output_model: str, expected_output: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "null_and_array.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--output-model",
        output_model,
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / expected_output).read_text()


@freeze_time("2019-07-26")
def test_use_default_pydantic_v2_with_json_schema_const(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "use_default_with_const.json"),
        "--output",
        str(output_file),
        "--output-model-type",
        "pydantic_v2.BaseModel",
        "--use-default",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "use_default_with_const.py").read_text()
    )


@freeze_time("2019-07-26")
@pytest.mark.parametrize(
    ("output_model", "expected_output", "option"),
    [
        (
            "pydantic.BaseModel",
            "complicated_enum_default_member.py",
            "--set-default-enum-member",
        ),
        (
            "dataclasses.dataclass",
            "complicated_enum_default_member_dataclass.py",
            "--set-default-enum-member",
        ),
        (
            "dataclasses.dataclass",
            "complicated_enum_default_member_dataclass.py",
            None,
        ),
    ],
)
def test_main_complicated_enum_default_member(
    output_model: str, expected_output: str, option: str | None, tmp_path: Path
) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        a
        for a in [
            "--input",
            str(JSON_SCHEMA_DATA_PATH / "complicated_enum.json"),
            "--output",
            str(output_file),
            option,
            "--output-model",
            output_model,
        ]
        if a
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / expected_output).read_text()


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_json_reuse_enum_default_member(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "duplicate_enum.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--reuse-model",
        "--set-default-enum-member",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "json_reuse_enum_default_member.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_invalid_model_name_failed(capsys: pytest.CaptureFixture, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "invalid_model_name.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--class-name",
        "with",
    ])
    captured = capsys.readouterr()
    assert return_code == Exit.ERROR
    assert captured.err == "title='with' is invalid class name. You have to set `--class-name` option\n"


@freeze_time("2019-07-26")
def test_main_invalid_model_name_converted(capsys: pytest.CaptureFixture, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "invalid_model_name.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    captured = capsys.readouterr()
    assert return_code == Exit.ERROR
    assert captured.err == "title='1Xyz' is invalid class name. You have to set `--class-name` option\n"


@freeze_time("2019-07-26")
def test_main_invalid_model_name(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "invalid_model_name.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--class-name",
        "ValidModelName",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "invalid_model_name.py").read_text()


@freeze_time("2019-07-26")
def test_main_root_id_jsonschema_with_local_file(mocker: MockerFixture, tmp_path: Path) -> None:
    root_id_response = mocker.Mock()
    root_id_response.text = "dummy"
    person_response = mocker.Mock()
    person_response.text = (JSON_SCHEMA_DATA_PATH / "person.json").read_text()
    httpx_get_mock = mocker.patch("httpx.get", side_effect=[person_response])

    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "root_id.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "root_id.py").read_text()
    httpx_get_mock.assert_not_called()


@freeze_time("2019-07-26")
def test_main_root_id_jsonschema_with_remote_file(mocker: MockerFixture, tmp_path: Path) -> None:
    root_id_response = mocker.Mock()
    root_id_response.text = "dummy"
    person_response = mocker.Mock()
    person_response.text = (JSON_SCHEMA_DATA_PATH / "person.json").read_text()
    httpx_get_mock = mocker.patch("httpx.get", side_effect=[person_response])

    input_file = tmp_path / "root_id.json"
    shutil.copy(JSON_SCHEMA_DATA_PATH / "root_id.json", input_file)
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(input_file),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "root_id.py").read_text()
    httpx_get_mock.assert_has_calls([
        call(
            "https://example.com/person.json",
            headers=None,
            verify=True,
            follow_redirects=True,
            params=None,
        ),
    ])


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_root_id_jsonschema_self_refs_with_local_file(mocker: MockerFixture, tmp_path: Path) -> None:
    person_response = mocker.Mock()
    person_response.text = (JSON_SCHEMA_DATA_PATH / "person.json").read_text()
    httpx_get_mock = mocker.patch("httpx.get", side_effect=[person_response])

    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "root_id_self_ref.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "root_id.py").read_text().replace(
        "filename:  root_id.json", "filename:  root_id_self_ref.json"
    )
    httpx_get_mock.assert_not_called()


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_root_id_jsonschema_self_refs_with_remote_file(mocker: MockerFixture, tmp_path: Path) -> None:
    person_response = mocker.Mock()
    person_response.text = (JSON_SCHEMA_DATA_PATH / "person.json").read_text()
    httpx_get_mock = mocker.patch("httpx.get", side_effect=[person_response])

    input_file = tmp_path / "root_id_self_ref.json"
    shutil.copy(JSON_SCHEMA_DATA_PATH / "root_id_self_ref.json", input_file)
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(input_file),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "root_id.py").read_text().replace(
        "filename:  root_id.json", "filename:  root_id_self_ref.json"
    )
    httpx_get_mock.assert_has_calls([
        call(
            "https://example.com/person.json",
            headers=None,
            verify=True,
            follow_redirects=True,
            params=None,
        ),
    ])


@freeze_time("2019-07-26")
def test_main_root_id_jsonschema_with_absolute_remote_file(mocker: MockerFixture, tmp_path: Path) -> None:
    root_id_response = mocker.Mock()
    root_id_response.text = "dummy"
    person_response = mocker.Mock()
    person_response.text = (JSON_SCHEMA_DATA_PATH / "person.json").read_text()
    httpx_get_mock = mocker.patch("httpx.get", side_effect=[person_response])

    input_file = tmp_path / "root_id_absolute_url.json"
    shutil.copy(JSON_SCHEMA_DATA_PATH / "root_id_absolute_url.json", input_file)
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(input_file),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "root_id_absolute_url.py").read_text()
    )
    httpx_get_mock.assert_has_calls([
        call(
            "https://example.com/person.json",
            headers=None,
            verify=True,
            follow_redirects=True,
            params=None,
        ),
    ])


@freeze_time("2019-07-26")
def test_main_root_id_jsonschema_with_absolute_local_file(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "root_id_absolute_url.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "root_id_absolute_url.py").read_text()
    )


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_jsonschema_id(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "id.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "id.py").read_text()


@freeze_time("2019-07-26")
def test_main_jsonschema_id_as_stdin(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    monkeypatch.setattr("sys.stdin", (JSON_SCHEMA_DATA_PATH / "id.json").open())
    return_code: Exit = main([
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "id_stdin.py").read_text()


def test_main_jsonschema_ids(tmp_path: Path) -> None:
    input_filename = JSON_SCHEMA_DATA_PATH / "ids" / "Organization.schema.json"
    output_path = tmp_path / "model"

    with freeze_time(TIMESTAMP):
        main([
            "--input",
            str(input_filename),
            "--output",
            str(output_path),
            "--input-file-type",
            "jsonschema",
        ])
    main_jsonschema_ids_dir = EXPECTED_JSON_SCHEMA_PATH / "ids"
    for path in main_jsonschema_ids_dir.rglob("*.py"):
        result = output_path.joinpath(path.relative_to(main_jsonschema_ids_dir)).read_text()
        assert result == path.read_text()


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_external_definitions(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "external_definitions_root.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "external_definitions.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_external_files_in_directory(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "external_files_in_directory" / "person.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "external_files_in_directory.py").read_text()
    )


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_nested_directory(tmp_path: Path) -> None:
    output_path = tmp_path / "model"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "external_files_in_directory"),
        "--output",
        str(output_path),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    main_nested_directory = EXPECTED_JSON_SCHEMA_PATH / "nested_directory"

    for path in main_nested_directory.rglob("*.py"):
        result = output_path.joinpath(path.relative_to(main_nested_directory)).read_text()
        assert result == path.read_text()


@freeze_time("2019-07-26")
def test_main_circular_reference(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "circular_reference.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "circular_reference.py").read_text()


@freeze_time("2019-07-26")
def test_main_invalid_enum_name(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "invalid_enum_name.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "invalid_enum_name.py").read_text()


@freeze_time("2019-07-26")
def test_main_invalid_enum_name_snake_case_field(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "invalid_enum_name.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--snake-case-field",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "invalid_enum_name_snake_case_field.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_json_reuse_enum(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "duplicate_enum.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--reuse-model",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "json_reuse_enum.py").read_text()


@freeze_time("2019-07-26")
def test_main_json_capitalise_enum_members(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "many_case_enum.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--capitalise-enum-members",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "json_capitalise_enum_members.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_json_capitalise_enum_members_without_enum(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "person.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--capitalise-enum-members",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "autodetect.py").read_text()


@freeze_time("2019-07-26")
def test_main_similar_nested_array(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "similar_nested_array.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "similar_nested_array.py").read_text()
    )


@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic.BaseModel",
            "require_referenced_field",
        ),
        (
            "pydantic_v2.BaseModel",
            "require_referenced_field_pydantic_v2",
        ),
    ],
)
@freeze_time("2019-07-26")
def test_main_require_referenced_field(output_model: str, expected_output: str, tmp_path: Path) -> None:
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "require_referenced_field/"),
        "--output",
        str(tmp_path),
        "--input-file-type",
        "jsonschema",
        "--output-datetime-class",
        "AwareDatetime",
        "--output-model-type",
        output_model,
    ])
    assert return_code == Exit.OK

    assert (tmp_path / "referenced.py").read_text() == (
        EXPECTED_JSON_SCHEMA_PATH / expected_output / "referenced.py"
    ).read_text()
    assert (tmp_path / "required.py").read_text() == (
        EXPECTED_JSON_SCHEMA_PATH / expected_output / "required.py"
    ).read_text()


@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic.BaseModel",
            "require_referenced_field",
        ),
        (
            "pydantic_v2.BaseModel",
            "require_referenced_field_naivedatetime",
        ),
    ],
)
@freeze_time("2019-07-26")
def test_main_require_referenced_field_naive_datetime(output_model: str, expected_output: str, tmp_path: Path) -> None:
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "require_referenced_field/"),
        "--output",
        str(tmp_path),
        "--input-file-type",
        "jsonschema",
        "--output-datetime-class",
        "NaiveDatetime",
        "--output-model-type",
        output_model,
    ])
    assert return_code == Exit.OK

    assert (tmp_path / "referenced.py").read_text() == (
        EXPECTED_JSON_SCHEMA_PATH / expected_output / "referenced.py"
    ).read_text()
    assert (tmp_path / "required.py").read_text() == (
        EXPECTED_JSON_SCHEMA_PATH / expected_output / "required.py"
    ).read_text()


@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic.BaseModel",
            "require_referenced_field",
        ),
        (
            "pydantic_v2.BaseModel",
            "require_referenced_field_pydantic_v2",
        ),
        (
            "msgspec.Struct",
            "require_referenced_field_msgspec",
        ),
    ],
)
@freeze_time("2019-07-26")
def test_main_require_referenced_field_datetime(output_model: str, expected_output: str, tmp_path: Path) -> None:
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "require_referenced_field/"),
        "--output",
        str(object=tmp_path),
        "--input-file-type",
        "jsonschema",
        "--output-model-type",
        output_model,
    ])
    assert return_code == Exit.OK

    assert (tmp_path / "referenced.py").read_text() == (
        EXPECTED_JSON_SCHEMA_PATH / expected_output / "referenced.py"
    ).read_text()
    assert (tmp_path / "required.py").read_text() == (
        EXPECTED_JSON_SCHEMA_PATH / expected_output / "required.py"
    ).read_text()


@freeze_time("2019-07-26")
def test_main_json_pointer(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "json_pointer.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "json_pointer.py").read_text()


@freeze_time("2019-07-26")
def test_main_nested_json_pointer(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "nested_json_pointer.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "nested_json_pointer.py").read_text()


@freeze_time("2019-07-26")
def test_main_jsonschema_multiple_files_json_pointer(tmp_path: Path) -> None:
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "multiple_files_json_pointer"),
        "--output",
        str(tmp_path),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    main_modular_dir = EXPECTED_JSON_SCHEMA_PATH / "multiple_files_json_pointer"
    for path in main_modular_dir.rglob("*.py"):
        result = tmp_path.joinpath(path.relative_to(main_modular_dir)).read_text()
        assert result == path.read_text()


@freeze_time("2019-07-26")
def test_main_root_model_with_additional_properties(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "root_model_with_additional_properties.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "root_model_with_additional_properties.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_root_model_with_additional_properties_use_generic_container_types(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "root_model_with_additional_properties.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--use-generic-container-types",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (
            EXPECTED_JSON_SCHEMA_PATH / "root_model_with_additional_properties_use_generic_container_types.py"
        ).read_text()
    )


@freeze_time("2019-07-26")
def test_main_root_model_with_additional_properties_use_standard_collections(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "root_model_with_additional_properties.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--use-standard-collections",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "root_model_with_additional_properties_use_standard_collections.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_root_model_with_additional_properties_literal(min_version: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "root_model_with_additional_properties.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--enum-field-as-literal",
        "all",
        "--target-python-version",
        min_version,
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "root_model_with_additional_properties_literal.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_jsonschema_multiple_files_ref(tmp_path: Path) -> None:
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "multiple_files_self_ref"),
        "--output",
        str(tmp_path),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    main_modular_dir = EXPECTED_JSON_SCHEMA_PATH / "multiple_files_self_ref"
    for path in main_modular_dir.rglob("*.py"):
        result = tmp_path.joinpath(path.relative_to(main_modular_dir)).read_text()
        assert result == path.read_text()


@freeze_time("2019-07-26")
def test_main_jsonschema_multiple_files_ref_test_json(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    with chdir(JSON_SCHEMA_DATA_PATH / "multiple_files_self_ref"):
        return_code: Exit = main([
            "--input",
            "test.json",
            "--output",
            str(output_file),
            "--input-file-type",
            "jsonschema",
        ])
        assert return_code == Exit.OK
        assert (
            output_file.read_text(encoding="utf-8")
            == (EXPECTED_JSON_SCHEMA_PATH / "multiple_files_self_ref_single.py").read_text()
        )


@freeze_time("2019-07-26")
def test_main_space_field_enum_snake_case_field(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    with chdir(JSON_SCHEMA_DATA_PATH / "space_field_enum.json"):
        return_code: Exit = main([
            "--input",
            "space_field_enum.json",
            "--output",
            str(output_file),
            "--input-file-type",
            "jsonschema",
            "--snake-case-field",
            "--original-field-name-delimiter",
            " ",
        ])
        assert return_code == Exit.OK
        assert (
            output_file.read_text(encoding="utf-8")
            == (EXPECTED_JSON_SCHEMA_PATH / "space_field_enum_snake_case_field.py").read_text()
        )


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_all_of_ref(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    with chdir(JSON_SCHEMA_DATA_PATH / "all_of_ref"):
        return_code: Exit = main([
            "--input",
            "test.json",
            "--output",
            str(output_file),
            "--input-file-type",
            "jsonschema",
            "--class-name",
            "Test",
        ])
        assert return_code == Exit.OK
        assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "all_of_ref.py").read_text()


@freeze_time("2019-07-26")
def test_main_all_of_with_object(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    with chdir(JSON_SCHEMA_DATA_PATH):
        return_code: Exit = main([
            "--input",
            "all_of_with_object.json",
            "--output",
            str(output_file),
            "--input-file-type",
            "jsonschema",
        ])
        assert return_code == Exit.OK
        assert (
            output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "all_of_with_object.py").read_text()
        )


@pytest.mark.skipif(
    black.__version__.split(".")[0] >= "24",
    reason="Installed black doesn't support the old style",
)
@freeze_time("2019-07-26")
def test_main_combined_array(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    with chdir(JSON_SCHEMA_DATA_PATH):
        return_code: Exit = main([
            "--input",
            "combined_array.json",
            "--output",
            str(output_file),
            "--input-file-type",
            "jsonschema",
        ])
        assert return_code == Exit.OK
        assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "combined_array.py").read_text()


@freeze_time("2019-07-26")
def test_main_jsonschema_pattern(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "pattern.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "pattern.py").read_text()


@freeze_time("2019-07-26")
def test_main_generate(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    input_ = (JSON_SCHEMA_DATA_PATH / "person.json").relative_to(Path.cwd(), walk_up=True)
    assert not input_.is_absolute()
    generate(
        input_=input_,
        input_file_type=InputFileType.JsonSchema,
        output=output_file,
    )

    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "general.py").read_text()


@freeze_time("2019-07-26")
def test_main_generate_non_pydantic_output(tmp_path: Path) -> None:
    """
    See https://github.com/koxudaxi/datamodel-code-generator/issues/1452.
    """
    output_file: Path = tmp_path / "output.py"
    input_ = (JSON_SCHEMA_DATA_PATH / "simple_string.json").relative_to(Path.cwd(), walk_up=True)
    assert not input_.is_absolute()
    generate(
        input_=input_,
        input_file_type=InputFileType.JsonSchema,
        output=output_file,
        output_model_type=DataModelType.DataclassesDataclass,
    )

    file = EXPECTED_JSON_SCHEMA_PATH / "generate_non_pydantic_output.py"
    assert output_file.read_text(encoding="utf-8") == file.read_text()


@freeze_time("2019-07-26")
def test_main_generate_from_directory(tmp_path: Path) -> None:
    input_ = (JSON_SCHEMA_DATA_PATH / "external_files_in_directory").relative_to(Path.cwd(), walk_up=True)
    assert not input_.is_absolute()
    assert input_.is_dir()
    generate(
        input_=input_,
        input_file_type=InputFileType.JsonSchema,
        output=tmp_path,
    )

    main_nested_directory = EXPECTED_JSON_SCHEMA_PATH / "nested_directory"

    for path in main_nested_directory.rglob("*.py"):
        result = tmp_path.joinpath(path.relative_to(main_nested_directory)).read_text()
        assert result == path.read_text()


@freeze_time("2019-07-26")
def test_main_generate_custom_class_name_generator(tmp_path: Path) -> None:
    def custom_class_name_generator(title: str) -> str:
        return f"Custom{title}"

    output_file: Path = tmp_path / "output.py"
    input_ = (JSON_SCHEMA_DATA_PATH / "person.json").relative_to(Path.cwd(), walk_up=True)
    assert not input_.is_absolute()
    generate(
        input_=input_,
        input_file_type=InputFileType.JsonSchema,
        output=output_file,
        custom_class_name_generator=custom_class_name_generator,
    )

    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "general.py").read_text().replace(
        "Person", "CustomPerson"
    )


@freeze_time("2019-07-26")
def test_main_generate_custom_class_name_generator_additional_properties(tmp_path: Path) -> None:
    output_file = tmp_path / "models.py"

    def custom_class_name_generator(name: str) -> str:
        return f"Custom{name[0].upper() + name[1:]}"

    input_ = (JSON_SCHEMA_DATA_PATH / "root_model_with_additional_properties.json").relative_to(Path.cwd(), walk_up=True)
    assert not input_.is_absolute()
    generate(
        input_=input_,
        input_file_type=InputFileType.JsonSchema,
        output=output_file,
        custom_class_name_generator=custom_class_name_generator,
    )

    assert (
        output_file.read_text()
        == (EXPECTED_JSON_SCHEMA_PATH / "root_model_with_additional_properties_custom_class_name.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_http_jsonschema(mocker: MockerFixture, tmp_path: Path) -> None:
    external_directory = JSON_SCHEMA_DATA_PATH / "external_files_in_directory"

    def get_mock_response(path: str) -> mocker.Mock:
        mock = mocker.Mock()
        mock.text = (external_directory / path).read_text()
        return mock

    httpx_get_mock = mocker.patch(
        "httpx.get",
        side_effect=[
            get_mock_response("person.json"),
            get_mock_response("definitions/relative/animal/pet/pet.json"),
            get_mock_response("definitions/relative/animal/fur.json"),
            get_mock_response("definitions/friends.json"),
            get_mock_response("definitions/food.json"),
            get_mock_response("definitions/machine/robot.json"),
            get_mock_response("definitions/drink/coffee.json"),
            get_mock_response("definitions/drink/tea.json"),
        ],
    )
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--url",
        "https://example.com/external_files_in_directory/person.json",
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (
        EXPECTED_JSON_SCHEMA_PATH / "external_files_in_directory.py"
    ).read_text().replace(
        "#   filename:  person.json",
        "#   filename:  https://example.com/external_files_in_directory/person.json",
    )
    httpx_get_mock.assert_has_calls([
        call(
            "https://example.com/external_files_in_directory/person.json",
            headers=None,
            verify=True,
            follow_redirects=True,
            params=None,
        ),
        call(
            "https://example.com/external_files_in_directory/definitions/relative/animal/pet/pet.json",
            headers=None,
            verify=True,
            follow_redirects=True,
            params=None,
        ),
        call(
            "https://example.com/external_files_in_directory/definitions/relative/animal/fur.json",
            headers=None,
            verify=True,
            follow_redirects=True,
            params=None,
        ),
        call(
            "https://example.com/external_files_in_directory/definitions/friends.json",
            headers=None,
            verify=True,
            follow_redirects=True,
            params=None,
        ),
        call(
            "https://example.com/external_files_in_directory/definitions/food.json",
            headers=None,
            verify=True,
            follow_redirects=True,
            params=None,
        ),
        call(
            "https://example.com/external_files_in_directory/definitions/machine/robot.json",
            headers=None,
            verify=True,
            follow_redirects=True,
            params=None,
        ),
        call(
            "https://example.com/external_files_in_directory/definitions/drink/coffee.json",
            headers=None,
            verify=True,
            follow_redirects=True,
            params=None,
        ),
        call(
            "https://example.com/external_files_in_directory/definitions/drink/tea.json",
            headers=None,
            verify=True,
            follow_redirects=True,
            params=None,
        ),
    ])


@freeze_time("2019-07-26")
@pytest.mark.parametrize(
    (
        "headers_arguments",
        "headers_requests",
        "query_parameters_arguments",
        "query_parameters_requests",
        "http_ignore_tls",
    ),
    [
        (
            ("Authorization: Basic dXNlcjpwYXNz",),
            [("Authorization", "Basic dXNlcjpwYXNz")],
            ("key=value",),
            [("key", "value")],
            False,
        ),
        (
            ("Authorization: Basic dXNlcjpwYXNz", "X-API-key: abcefg"),
            [("Authorization", "Basic dXNlcjpwYXNz"), ("X-API-key", "abcefg")],
            ("key=value", "newkey=newvalue"),
            [("key", "value"), ("newkey", "newvalue")],
            True,
        ),
    ],
)
def test_main_http_jsonschema_with_http_headers_and_http_query_parameters_and_ignore_tls(
    mocker: MockerFixture,
    headers_arguments: tuple[str, str],
    headers_requests: list[tuple[str, str]],
    query_parameters_arguments: tuple[str, ...],
    query_parameters_requests: list[tuple[str, str]],
    http_ignore_tls: bool,
    tmp_path: Path,
) -> None:
    external_directory = JSON_SCHEMA_DATA_PATH / "external_files_in_directory"

    def get_mock_response(path: str) -> mocker.Mock:
        mock = mocker.Mock()
        mock.text = (external_directory / path).read_text()
        return mock

    httpx_get_mock = mocker.patch(
        "httpx.get",
        side_effect=[
            get_mock_response("person.json"),
            get_mock_response("definitions/relative/animal/pet/pet.json"),
            get_mock_response("definitions/relative/animal/fur.json"),
            get_mock_response("definitions/friends.json"),
            get_mock_response("definitions/food.json"),
            get_mock_response("definitions/machine/robot.json"),
            get_mock_response("definitions/drink/coffee.json"),
            get_mock_response("definitions/drink/tea.json"),
        ],
    )
    output_file: Path = tmp_path / "output.py"
    args = [
        "--url",
        "https://example.com/external_files_in_directory/person.json",
        "--http-headers",
        *headers_arguments,
        "--http-query-parameters",
        *query_parameters_arguments,
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ]
    if http_ignore_tls:
        args.append("--http-ignore-tls")

    return_code: Exit = main(args)
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (
        EXPECTED_JSON_SCHEMA_PATH / "external_files_in_directory.py"
    ).read_text().replace(
        "#   filename:  person.json",
        "#   filename:  https://example.com/external_files_in_directory/person.json",
    )
    httpx_get_mock.assert_has_calls([
        call(
            "https://example.com/external_files_in_directory/person.json",
            headers=headers_requests,
            verify=bool(not http_ignore_tls),
            follow_redirects=True,
            params=query_parameters_requests,
        ),
        call(
            "https://example.com/external_files_in_directory/definitions/relative/animal/pet/pet.json",
            headers=headers_requests,
            verify=bool(not http_ignore_tls),
            follow_redirects=True,
            params=query_parameters_requests,
        ),
        call(
            "https://example.com/external_files_in_directory/definitions/relative/animal/fur.json",
            headers=headers_requests,
            verify=bool(not http_ignore_tls),
            follow_redirects=True,
            params=query_parameters_requests,
        ),
        call(
            "https://example.com/external_files_in_directory/definitions/friends.json",
            headers=headers_requests,
            verify=bool(not http_ignore_tls),
            follow_redirects=True,
            params=query_parameters_requests,
        ),
        call(
            "https://example.com/external_files_in_directory/definitions/food.json",
            headers=headers_requests,
            verify=bool(not http_ignore_tls),
            follow_redirects=True,
            params=query_parameters_requests,
        ),
        call(
            "https://example.com/external_files_in_directory/definitions/machine/robot.json",
            headers=headers_requests,
            verify=bool(not http_ignore_tls),
            follow_redirects=True,
            params=query_parameters_requests,
        ),
        call(
            "https://example.com/external_files_in_directory/definitions/drink/coffee.json",
            headers=headers_requests,
            verify=bool(not http_ignore_tls),
            follow_redirects=True,
            params=query_parameters_requests,
        ),
        call(
            "https://example.com/external_files_in_directory/definitions/drink/tea.json",
            headers=headers_requests,
            verify=bool(not http_ignore_tls),
            follow_redirects=True,
            params=query_parameters_requests,
        ),
    ])


@freeze_time("2019-07-26")
def test_main_self_reference(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "self_reference.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "self_reference.py").read_text()


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_strict_types(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "strict_types.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "strict_types.py").read_text()


@pytest.mark.skipif(
    black.__version__.split(".")[0] >= "24",
    reason="Installed black doesn't support the old style",
)
@freeze_time("2019-07-26")
def test_main_strict_types_all(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "strict_types.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--strict-types",
        "str",
        "bytes",
        "int",
        "float",
        "bool",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "strict_types_all.py").read_text()


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_strict_types_all_with_field_constraints(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "strict_types.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--strict-types",
        "str",
        "bytes",
        "int",
        "float",
        "bool",
        "--field-constraints",
    ])

    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "strict_types_all_field_constraints.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_jsonschema_special_enum(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "special_enum.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "special_enum.py").read_text()


@freeze_time("2019-07-26")
def test_main_jsonschema_special_enum_special_field_name_prefix(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "special_enum.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--special-field-name-prefix",
        "special",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "special_enum_special_field_name_prefix.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_jsonschema_special_enum_special_field_name_prefix_keep_private(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "special_enum.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--special-field-name-prefix",
        "",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "special_enum_special_field_name_prefix_keep_private.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_jsonschema_special_model_remove_special_field_name_prefix(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "special_prefix_model.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--remove-special-field-name-prefix",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "special_model_remove_special_field_name_prefix.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_jsonschema_subclass_enum(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "subclass_enum.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--use-subclass-enum",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "subclass_enum.py").read_text()


@freeze_time("2019-07-26")
def test_main_jsonschema_special_enum_empty_enum_field_name(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "special_enum.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--empty-enum-field-name",
        "empty",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "special_enum_empty_enum_field_name.py").read_text()
    )


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_jsonschema_special_field_name(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "special_field_name.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "special_field_name.py").read_text()


@freeze_time("2019-07-26")
def test_main_jsonschema_complex_one_of(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "complex_one_of.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "complex_one_of.py").read_text()


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_jsonschema_complex_any_of(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "complex_any_of.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "complex_any_of.py").read_text()


@freeze_time("2019-07-26")
def test_main_jsonschema_combine_one_of_object(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "combine_one_of_object.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "combine_one_of_object.py").read_text()
    )


@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
@pytest.mark.parametrize(
    ("union_mode", "output_model", "expected_output"),
    [
        (None, "pydantic.BaseModel", "combine_any_of_object.py"),
        (None, "pydantic_v2.BaseModel", "combine_any_of_object_v2.py"),
        (
            "left_to_right",
            "pydantic_v2.BaseModel",
            "combine_any_of_object_left_to_right.py",
        ),
    ],
)
@freeze_time("2019-07-26")
def test_main_jsonschema_combine_any_of_object(
    union_mode: str | None, output_model: str, expected_output: str, tmp_path: Path
) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main(
        [
            "--input",
            str(JSON_SCHEMA_DATA_PATH / "combine_any_of_object.json"),
            "--output",
            str(output_file),
            "--input-file-type",
            "jsonschema",
            "--output-model",
            output_model,
        ]
        + ([] if union_mode is None else ["--union-mode", union_mode])
    )
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / expected_output).read_text()


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_jsonschema_field_include_all_keys(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "person.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--field-include-all-keys",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "general.py").read_text()


@freeze_time("2019-07-26")
@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic.BaseModel",
            "field_extras_field_include_all_keys.py",
        ),
        (
            "pydantic_v2.BaseModel",
            "field_extras_field_include_all_keys_v2.py",
        ),
    ],
)
def test_main_jsonschema_field_extras_field_include_all_keys(
    output_model: str, expected_output: str, tmp_path: Path
) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "extras.json"),
        "--output",
        str(output_file),
        "--output-model",
        output_model,
        "--input-file-type",
        "jsonschema",
        "--field-include-all-keys",
        "--field-extra-keys-without-x-prefix",
        "x-repr",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / expected_output).read_text()


@freeze_time("2019-07-26")
@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic.BaseModel",
            "field_extras_field_extra_keys.py",
        ),
        (
            "pydantic_v2.BaseModel",
            "field_extras_field_extra_keys_v2.py",
        ),
    ],
)
def test_main_jsonschema_field_extras_field_extra_keys(output_model: str, expected_output: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "extras.json"),
        "--output",
        str(output_file),
        "--output-model",
        output_model,
        "--input-file-type",
        "jsonschema",
        "--field-extra-keys",
        "key2",
        "invalid-key-1",
        "--field-extra-keys-without-x-prefix",
        "x-repr",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / expected_output).read_text()


@freeze_time("2019-07-26")
@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic.BaseModel",
            "field_extras.py",
        ),
        (
            "pydantic_v2.BaseModel",
            "field_extras_v2.py",
        ),
    ],
)
def test_main_jsonschema_field_extras(output_model: str, expected_output: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "extras.json"),
        "--output",
        str(output_file),
        "--output-model",
        output_model,
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / expected_output).read_text()


@pytest.mark.skipif(
    not isort.__version__.startswith("4."),
    reason="isort 5.x don't sort pydantic modules",
)
@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic.BaseModel",
            "custom_type_path.py",
        ),
        (
            "pydantic_v2.BaseModel",
            "custom_type_path_pydantic_v2.py",
        ),
    ],
)
@freeze_time("2019-07-26")
def test_main_jsonschema_custom_type_path(output_model: str, expected_output: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "custom_type_path.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--output-model",
        output_model,
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / expected_output).read_text()


@freeze_time("2019-07-26")
def test_main_jsonschema_custom_base_path(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "custom_base_path.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "custom_base_path.py").read_text()


@freeze_time("2019-07-26")
def test_long_description(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "long_description.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "long_description.py").read_text()


@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_long_description_wrap_string_literal(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "long_description.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--wrap-string-literal",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "long_description_wrap_string_literal.py").read_text()
    )


def test_version(capsys: pytest.CaptureFixture) -> None:
    with pytest.raises(SystemExit) as e:
        main(["--version"])
    assert e.value.code == Exit.OK
    captured = capsys.readouterr()
    assert captured.out != "0.0.0\n"
    assert not captured.err


@freeze_time("2019-07-26")
def test_jsonschema_pattern_properties(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "pattern_properties.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "pattern_properties.py").read_text()


@freeze_time("2019-07-26")
def test_jsonschema_pattern_properties_field_constraints(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "pattern_properties.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--field-constraints",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "pattern_properties_field_constraints.py").read_text()
    )


@freeze_time("2019-07-26")
def test_jsonschema_titles(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "titles.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "titles.py").read_text()


@freeze_time("2019-07-26")
def test_jsonschema_titles_use_title_as_name(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "titles.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--use-title-as-name",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "titles_use_title_as_name.py").read_text()
    )


@freeze_time("2019-07-26")
def test_jsonschema_without_titles_use_title_as_name(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "without_titles.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--use-title-as-name",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "without_titles_use_title_as_name.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_jsonschema_has_default_value(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "has_default_value.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "has_default_value.py").read_text()


@freeze_time("2019-07-26")
def test_main_jsonschema_boolean_property(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "boolean_property.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "boolean_property.py").read_text()


@freeze_time("2019-07-26")
def test_main_jsonschema_modular_default_enum_member(
    tmp_path: Path,
) -> None:
    input_filename = JSON_SCHEMA_DATA_PATH / "modular_default_enum_member"
    output_path = tmp_path / "model"

    with freeze_time(TIMESTAMP):
        main([
            "--input",
            str(input_filename),
            "--output",
            str(output_path),
            "--set-default-enum-member",
        ])
    main_modular_dir = EXPECTED_JSON_SCHEMA_PATH / "modular_default_enum_member"
    for path in main_modular_dir.rglob("*.py"):
        result = output_path.joinpath(path.relative_to(main_modular_dir)).read_text()
        assert result == path.read_text()


@pytest.mark.skipif(
    black.__version__.split(".")[0] < "22",
    reason="Installed black doesn't support Python version 3.10",
)
@freeze_time("2019-07-26")
def test_main_use_union_operator(tmp_path: Path) -> None:
    output_path = tmp_path / "model"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "external_files_in_directory"),
        "--output",
        str(output_path),
        "--input-file-type",
        "jsonschema",
        "--use-union-operator",
    ])
    assert return_code == Exit.OK
    main_nested_directory = EXPECTED_JSON_SCHEMA_PATH / "use_union_operator"

    for path in main_nested_directory.rglob("*.py"):
        result = output_path.joinpath(path.relative_to(main_nested_directory)).read_text()
        assert result == path.read_text()


@freeze_time("2019-07-26")
@pytest.mark.parametrize("as_module", [True, False])
def test_treat_dot_as_module(as_module: bool, tmp_path: Path) -> None:
    if as_module:
        return_code: Exit = main([
            "--input",
            str(JSON_SCHEMA_DATA_PATH / "treat_dot_as_module"),
            "--output",
            str(tmp_path),
            "--treat-dot-as-module",
        ])
    else:
        return_code: Exit = main([
            "--input",
            str(JSON_SCHEMA_DATA_PATH / "treat_dot_as_module"),
            "--output",
            str(tmp_path),
        ])
    assert return_code == Exit.OK
    path_extension = "treat_dot_as_module" if as_module else "treat_dot_not_as_module"
    main_modular_dir = EXPECTED_JSON_SCHEMA_PATH / path_extension
    for path in main_modular_dir.rglob("*.py"):
        result = tmp_path.joinpath(path.relative_to(main_modular_dir)).read_text()
        if as_module:
            assert str(path.relative_to(main_modular_dir)).count(".") == 1
        assert result == path.read_text()


@freeze_time("2019-07-26")
def test_main_jsonschema_duplicate_name(tmp_path: Path) -> None:
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "duplicate_name"),
        "--output",
        str(tmp_path),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    main_modular_dir = EXPECTED_JSON_SCHEMA_PATH / "duplicate_name"
    for path in main_modular_dir.rglob("*.py"):
        result = tmp_path.joinpath(path.relative_to(main_modular_dir)).read_text()
        assert result == path.read_text()


@freeze_time("2019-07-26")
def test_main_jsonschema_items_boolean(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "items_boolean.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "items_boolean.py").read_text()


@freeze_time("2019-07-26")
def test_main_jsonschema_array_in_additional_properites(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "array_in_additional_properties.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "array_in_additional_properties.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_jsonschema_object_with_only_additional_properties(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "string_dict.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "string_dict.py").read_text()


@freeze_time("2019-07-26")
def test_main_jsonschema_nullable_object(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "nullable_object.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "nullable_object.py").read_text()


@freeze_time("2019-07-26")
def test_main_jsonschema_object_has_one_of(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "object_has_one_of.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "object_has_one_of.py").read_text()


@freeze_time("2019-07-26")
def test_main_jsonschema_json_pointer_array(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "json_pointer_array.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "json_pointer_array.py").read_text()


@pytest.mark.filterwarnings("error")
def test_main_disable_warnings_config(capsys: pytest.CaptureFixture, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "person.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--use-union-operator",
        "--target-python-version",
        f"3.{MIN_VERSION}",
        "--disable-warnings",
    ])
    captured = capsys.readouterr()
    assert return_code == Exit.OK
    assert not captured.err


@pytest.mark.filterwarnings("error")
def test_main_disable_warnings(capsys: pytest.CaptureFixture, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "all_of_with_object.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--disable-warnings",
    ])
    captured = capsys.readouterr()
    assert return_code == Exit.OK
    assert not captured.err


@freeze_time("2019-07-26")
def test_main_jsonschema_pattern_properties_by_reference(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "pattern_properties_by_reference.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "pattern_properties_by_reference.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_dataclass_field(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "user.json"),
        "--output",
        str(output_file),
        "--output-model-type",
        "dataclasses.dataclass",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "dataclass_field.py").read_text()


@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_jsonschema_enum_root_literal(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "enum_in_root" / "enum_in_root.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--use-schema-description",
        "--use-title-as-name",
        "--field-constraints",
        "--target-python-version",
        "3.9",
        "--allow-population-by-field-name",
        "--strip-default-none",
        "--use-default",
        "--enum-field-as-literal",
        "all",
        "--snake-case-field",
        "--collapse-root-models",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "root_in_enum.py").read_text()


@freeze_time("2019-07-26")
def test_main_nullable_any_of(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "nullable_any_of.json"),
        "--output",
        str(output_file),
        "--field-constraints",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "nullable_any_of.py").read_text()


@freeze_time("2019-07-26")
def test_main_nullable_any_of_use_union_operator(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "nullable_any_of.json"),
        "--output",
        str(output_file),
        "--field-constraints",
        "--use-union-operator",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "nullable_any_of_use_union_operator.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_nested_all_of(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "nested_all_of.json"),
        "--output",
        str(output_file),
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "nested_all_of.py").read_text()


@freeze_time("2019-07-26")
def test_main_all_of_any_of(tmp_path: Path) -> None:
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "all_of_any_of"),
        "--output",
        str(tmp_path),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    all_of_any_of_dir = EXPECTED_JSON_SCHEMA_PATH / "all_of_any_of"
    for path in all_of_any_of_dir.rglob("*.py"):
        result = tmp_path.joinpath(path.relative_to(all_of_any_of_dir)).read_text()
        assert result == path.read_text()


@freeze_time("2019-07-26")
def test_main_all_of_one_of(tmp_path: Path) -> None:
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "all_of_one_of"),
        "--output",
        str(tmp_path),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    all_of_any_of_dir = EXPECTED_JSON_SCHEMA_PATH / "all_of_one_of"
    for path in all_of_any_of_dir.rglob("*.py"):
        result = tmp_path.joinpath(path.relative_to(all_of_any_of_dir)).read_text()
        assert result == path.read_text()


@freeze_time("2019-07-26")
def test_main_null(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "null.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "null.py").read_text()


@pytest.mark.skipif(
    version.parse(black.__version__) < version.parse("23.3.0"),
    reason="Require Black version 23.3.0 or later ",
)
@freeze_time("2019-07-26")
def test_main_typed_dict_special_field_name_with_inheritance_model(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "special_field_name_with_inheritance_model.json"),
        "--output",
        str(output_file),
        "--output-model-type",
        "typing.TypedDict",
        "--target-python-version",
        "3.11",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "typed_dict_special_field_name_with_inheritance_model.py").read_text()
    )


@pytest.mark.skipif(
    version.parse(black.__version__) < version.parse("23.3.0"),
    reason="Require Black version 23.3.0 or later ",
)
@freeze_time("2019-07-26")
def test_main_typed_dict_not_required_nullable(tmp_path: Path) -> None:
    """Test main function writing to TypedDict, with combos of Optional/NotRequired."""
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "not_required_nullable.json"),
        "--output",
        str(output_file),
        "--output-model-type",
        "typing.TypedDict",
        "--target-python-version",
        "3.11",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "typed_dict_not_required_nullable.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_typed_dict_const(tmp_path: Path) -> None:
    """Test main function writing to TypedDict with const fields."""
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "const.json"),
        "--output",
        str(output_file),
        "--output-model-type",
        "typing.TypedDict",
        "--target-python-version",
        "3.10",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "typed_dict_const.py").read_text()


@pytest.mark.skipif(
    black.__version__.split(".")[0] < "24",
    reason="Installed black doesn't support the new style",
)
@freeze_time("2019-07-26")
def test_main_typed_dict_additional_properties(tmp_path: Path) -> None:
    """Test main function writing to TypedDict with additional properties, and no other fields."""
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "string_dict.json"),
        "--output",
        str(output_file),
        "--output-model-type",
        "typing.TypedDict",
        "--target-python-version",
        "3.11",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "typed_dict_with_only_additional_properties.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_dataclass_const(tmp_path: Path) -> None:
    """Test main function writing to dataclass with const fields."""
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "const.json"),
        "--output",
        str(output_file),
        "--output-model-type",
        "dataclasses.dataclass",
        "--target-python-version",
        "3.10",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "dataclass_const.py").read_text()


@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic_v2.BaseModel",
            "discriminator_literals.py",
        ),
        (
            "msgspec.Struct",
            "discriminator_literals_msgspec.py",
        ),
    ],
)
@freeze_time("2019-07-26")
@pytest.mark.skipif(
    int(black.__version__.split(".")[0]) < 24,
    reason="Installed black doesn't support the new style",
)
def test_main_jsonschema_discriminator_literals(
    output_model: str, expected_output: str, min_version: str, tmp_path: Path
) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "discriminator_literals.json"),
        "--output",
        str(output_file),
        "--output-model-type",
        output_model,
        "--target-python",
        min_version,
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / expected_output).read_text()


@freeze_time("2019-07-26")
@pytest.mark.skipif(
    int(black.__version__.split(".")[0]) < 24,
    reason="Installed black doesn't support the new style",
)
def test_main_jsonschema_discriminator_literals_with_no_mapping(min_version: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "discriminator_no_mapping.json"),
        "--output",
        str(output_file),
        "--output-model-type",
        "pydantic_v2.BaseModel",
        "--target-python",
        min_version,
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "discriminator_no_mapping.py").read_text()
    )


@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic_v2.BaseModel",
            "discriminator_with_external_reference.py",
        ),
        (
            "msgspec.Struct",
            "discriminator_with_external_reference_msgspec.py",
        ),
    ],
)
@freeze_time("2019-07-26")
def test_main_jsonschema_external_discriminator(
    output_model: str, expected_output: str, min_version: str, tmp_path: Path
) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "discriminator_with_external_reference" / "inner_folder" / "schema.json"),
        "--output",
        str(output_file),
        "--output-model-type",
        output_model,
        "--target-python",
        min_version,
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / expected_output).read_text(), (
        EXPECTED_JSON_SCHEMA_PATH / expected_output
    )


@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic.BaseModel",
            "discriminator_with_external_references_folder",
        ),
        (
            "msgspec.Struct",
            "discriminator_with_external_references_folder_msgspec",
        ),
    ],
)
@freeze_time("2019-07-26")
def test_main_jsonschema_external_discriminator_folder(
    output_model: str, expected_output: str, min_version: str, tmp_path: Path
) -> None:
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "discriminator_with_external_reference"),
        "--output",
        str(tmp_path),
        "--output-model-type",
        output_model,
        "--target-python",
        min_version,
    ])
    assert return_code == Exit.OK
    main_modular_dir = EXPECTED_JSON_SCHEMA_PATH / expected_output
    for path in main_modular_dir.rglob("*.py"):
        result = tmp_path.joinpath(path.relative_to(main_modular_dir)).read_text()
        assert result == path.read_text(), path


@freeze_time("2019-07-26")
def test_main_duplicate_field_constraints(tmp_path: Path) -> None:
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "duplicate_field_constraints"),
        "--output",
        str(tmp_path),
        "--input-file-type",
        "jsonschema",
        "--collapse-root-models",
        "--output-model-type",
        "pydantic_v2.BaseModel",
    ])
    assert return_code == Exit.OK
    main_modular_dir = EXPECTED_JSON_SCHEMA_PATH / "duplicate_field_constraints"
    for path in main_modular_dir.rglob("*.py"):
        result = tmp_path.joinpath(path.relative_to(main_modular_dir)).read_text()
        assert result == path.read_text()


@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_duplicate_field_constraints_msgspec(min_version: str, tmp_path: Path) -> None:
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "duplicate_field_constraints"),
        "--output",
        str(tmp_path),
        "--input-file-type",
        "jsonschema",
        "--output-model-type",
        "msgspec.Struct",
        "--target-python-version",
        min_version,
    ])
    assert return_code == Exit.OK
    main_modular_dir = EXPECTED_JSON_SCHEMA_PATH / "duplicate_field_constraints_msgspec"
    for path in main_modular_dir.rglob("*.py"):
        result = tmp_path.joinpath(path.relative_to(main_modular_dir)).read_text()
        assert result == path.read_text()


@freeze_time("2019-07-26")
def test_main_dataclass_field_defs(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "user_defs.json"),
        "--output",
        str(output_file),
        "--output-model-type",
        "dataclasses.dataclass",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (
        EXPECTED_JSON_SCHEMA_PATH / "dataclass_field.py"
    ).read_text().replace("filename:  user.json", "filename:  user_defs.json")


@freeze_time("2019-07-26")
def test_main_dataclass_default(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "user_default.json"),
        "--output",
        str(output_file),
        "--output-model-type",
        "dataclasses.dataclass",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "dataclass_field_default.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_all_of_ref_self(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "all_of_ref_self.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "all_of_ref_self.py").read_text()


@freeze_time("2019-07-26")
@pytest.mark.skipif(
    black.__version__.split(".")[0] == "19",
    reason="Installed black doesn't support the old style",
)
def test_main_array_field_constraints(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "array_field_constraints.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--target-python-version",
        "3.9",
        "--field-constraints",
        "--collapse-root-models",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "array_field_constraints.py").read_text()
    )


@freeze_time("2019-07-26")
def test_all_of_use_default(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "all_of_default.json"),
        "--output",
        str(output_file),
        "--use-default",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "all_of_use_default.py").read_text()


@freeze_time("2019-07-26")
def test_main_root_one_of(tmp_path: Path) -> None:
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "root_one_of"),
        "--output",
        str(tmp_path),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    expected_directory = EXPECTED_JSON_SCHEMA_PATH / "root_one_of"
    for path in expected_directory.rglob("*.py"):
        result = tmp_path.joinpath(path.relative_to(expected_directory)).read_text()
        assert result == path.read_text()


@freeze_time("2019-07-26")
def test_one_of_with_sub_schema_array_item(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "one_of_with_sub_schema_array_item.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--output-model-type",
        "pydantic_v2.BaseModel",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "one_of_with_sub_schema_array_item.py").read_text()
    )


@freeze_time("2019-07-26")
@pytest.mark.xfail
def test_main_jsonschema_with_custom_formatters(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    formatter_config = {
        "license_file": str(Path(__file__).parent.parent.parent / "data/python/custom_formatters/license_example.txt")
    }
    formatter_config_path = tmp_path / "formatter_config"
    formatter_config_path.write_text(json.dumps(formatter_config))
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "person.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--custom-formatters",
        "tests.data.python.custom_formatters.add_license",
        "--custom-formatters-kwargs",
        str(formatter_config_path),
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "custom_formatters.py").read_text()


@freeze_time("2019-07-26")
def test_main_imports_correct(tmp_path: Path) -> None:
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "imports_correct"),
        "--output",
        str(tmp_path),
        "--output-model-type",
        "pydantic_v2.BaseModel",
    ])
    assert return_code == Exit.OK
    main_modular_dir = EXPECTED_JSON_SCHEMA_PATH / "imports_correct"
    for path in main_modular_dir.rglob("*.py"):
        result = tmp_path.joinpath(path.relative_to(main_modular_dir)).read_text()
        assert result == path.read_text()


@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic_v2.BaseModel",
            "duration_pydantic_v2.py",
        ),
        (
            "msgspec.Struct",
            "duration_msgspec.py",
        ),
    ],
)
@freeze_time("2019-07-26")
def test_main_jsonschema_duration(output_model: str, expected_output: str, min_version: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "duration.json"),
        "--output",
        str(output_file),
        "--output-model-type",
        output_model,
        "--target-python",
        min_version,
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / expected_output).read_text()


@freeze_time("2019-07-26")
@pytest.mark.skipif(
    int(black.__version__.split(".")[0]) < 24,
    reason="Installed black doesn't support the new style",
)
def test_main_jsonschema_keyword_only_msgspec(min_version: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "discriminator_literals.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--output-model-type",
        "msgspec.Struct",
        "--keyword-only",
        "--target-python-version",
        min_version,
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "discriminator_literals_msgspec_keyword_only.py").read_text()
    )


@freeze_time("2019-07-26")
@pytest.mark.skipif(
    int(black.__version__.split(".")[0]) < 24,
    reason="Installed black doesn't support the new style",
)
def test_main_jsonschema_keyword_only_msgspec_with_extra_data(min_version: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "discriminator_literals.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--output-model-type",
        "msgspec.Struct",
        "--keyword-only",
        "--target-python-version",
        min_version,
        "--extra-template-data",
        str(JSON_SCHEMA_DATA_PATH / "extra_data_msgspec.json"),
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "discriminator_literals_msgspec_keyword_only_omit_defaults.py").read_text()
    )


@freeze_time("2019-07-26")
@pytest.mark.skipif(
    int(black.__version__.split(".")[0]) < 24,
    reason="Installed black doesn't support the new style",
)
def test_main_jsonschema_openapi_keyword_only_msgspec_with_extra_data(tmp_path: Path) -> None:
    extra_data = json.loads((JSON_SCHEMA_DATA_PATH / "extra_data_msgspec.json").read_text())
    output_file: Path = tmp_path / "output.py"
    generate(
        input_=JSON_SCHEMA_DATA_PATH / "discriminator_literals.json",
        output=output_file,
        input_file_type=InputFileType.JsonSchema,
        output_model_type=DataModelType.MsgspecStruct,
        keyword_only=True,
        target_python_version=PythonVersionMin,
        extra_template_data=defaultdict(dict, extra_data),
        # Following values are implied by `msgspec.Struct` in the CLI
        use_annotated=True,
        field_constraints=True,
    )
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "discriminator_literals_msgspec_keyword_only_omit_defaults.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_invalid_import_name(tmp_path: Path) -> None:
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "invalid_import_name"),
        "--output",
        str(tmp_path),
        "--output-model-type",
        "pydantic_v2.BaseModel",
    ])
    assert return_code == Exit.OK
    main_modular_dir = EXPECTED_JSON_SCHEMA_PATH / "invalid_import_name"
    for path in main_modular_dir.rglob("*.py"):
        result = tmp_path.joinpath(path.relative_to(main_modular_dir)).read_text()
        assert result == path.read_text()


@pytest.mark.parametrize(
    ("output_model", "expected_output"),
    [
        (
            "pydantic_v2.BaseModel",
            "field_has_same_name_v2.py",
        ),
        (
            "pydantic.BaseModel",
            "field_has_same_name.py",
        ),
    ],
)
@freeze_time("2019-07-26")
def test_main_jsonschema_field_has_same_name(output_model: str, expected_output: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "field_has_same_name.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--output-model-type",
        output_model,
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / expected_output).read_text()


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_jsonschema_required_and_any_of_required(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "required_and_any_of_required.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8")
        == (EXPECTED_JSON_SCHEMA_PATH / "required_and_any_of_required.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_json_pointer_escaped_segments(tmp_path: Path) -> None:
    schema = {
        "definitions": {
            "foo/bar": {"type": "object", "properties": {"value": {"type": "string"}}},
            "baz~qux": {"type": "object", "properties": {"value": {"type": "integer"}}},
        },
        "properties": {
            "foo_bar": {"$ref": "#/definitions/foo~1bar"},
            "baz_qux": {"$ref": "#/definitions/baz~0qux"},
        },
        "type": "object",
    }
    expected = (
        "# generated by datamodel-codegen:\n"
        "#   filename: input.json\n"
        "#   timestamp: 2019-07-26T00:00:00+00:00\n\n"
        "from __future__ import annotations\n\n"
        "from typing import Optional\n\n"
        "from pydantic import BaseModel\n\n"
        "class FooBar(BaseModel):\n    value: Optional[str] = None\n\n"
        "class BazQux(BaseModel):\n    value: Optional[int] = None\n\n"
        "class Baz0qux(BaseModel):\n    value: Optional[int] = None\n\n"
        "class Foo1bar(BaseModel):\n    value: Optional[str] = None\n\n"
        "class Model(BaseModel):\n    foo_bar: Optional[Foo1bar] = None\n    baz_qux: Optional[Baz0qux] = None\n"
    )

    input_file = tmp_path / "input.json"
    output_file = tmp_path / "output.py"
    input_file.write_text(json.dumps(schema))
    return_code: Exit = main([
        "--input",
        str(input_file),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    result = output_file.read_text()
    # Normalize whitespace for comparison
    assert "".join(result.split()) == "".join(expected.split())


@freeze_time("2019-07-26")
def test_main_json_pointer_percent_encoded_segments(tmp_path: Path) -> None:
    schema = {
        "definitions": {
            "foo/bar": {"type": "object", "properties": {"value": {"type": "string"}}},
            "baz~qux": {"type": "object", "properties": {"value": {"type": "integer"}}},
            "space key": {"type": "object", "properties": {"value": {"type": "boolean"}}},
        },
        "properties": {
            "foo_bar": {"$ref": "#/definitions/foo%2Fbar"},
            "baz_qux": {"$ref": "#/definitions/baz%7Equx"},
            "space_key": {"$ref": "#/definitions/space%20key"},
        },
        "type": "object",
    }
    expected = (
        "# generated by datamodel-codegen:\n"
        "#   filename: input.json\n"
        "#   timestamp: 2019-07-26T00:00:00+00:00\n\n"
        "from __future__ import annotations\n\n"
        "from typing import Optional\n\n"
        "from pydantic import BaseModel\n\n"
        "class FooBar(BaseModel):\n    value: Optional[str] = None\n\n"
        "class BazQux(BaseModel):\n    value: Optional[int] = None\n\n"
        "class SpaceKey(BaseModel):\n    value: Optional[bool] = None\n\n"
        "class Baz7Equx(BaseModel):\n    value: Optional[int] = None\n\n"
        "class Foo2Fbar(BaseModel):\n    value: Optional[str] = None\n\n"
        "class Space20key(BaseModel):\n    value: Optional[bool] = None\n\n"
        "class Model(BaseModel):\n    foo_bar: Optional[Foo2Fbar] = None\n"
        "    baz_qux: Optional[Baz7Equx] = None\n"
        "    space_key: Optional[Space20key] = None\n"
    )

    input_file = tmp_path / "input.json"
    output_file = tmp_path / "output.py"
    input_file.write_text(json.dumps(schema))
    return_code: Exit = main([
        "--input",
        str(input_file),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    result = output_file.read_text()
    # Normalize whitespace for comparison
    assert "".join(result.split()) == "".join(expected.split())


@pytest.mark.parametrize(
    ("extra_fields", "output_model", "expected_output"),
    [
        (
            "allow",
            "pydantic.BaseModel",
            "extra_fields_allow.py",
        ),
        (
            "forbid",
            "pydantic.BaseModel",
            "extra_fields_forbid.py",
        ),
        (
            "ignore",
            "pydantic.BaseModel",
            "extra_fields_ignore.py",
        ),
        (
            "allow",
            "pydantic_v2.BaseModel",
            "extra_fields_v2_allow.py",
        ),
        (
            "forbid",
            "pydantic_v2.BaseModel",
            "extra_fields_v2_forbid.py",
        ),
        (
            "ignore",
            "pydantic_v2.BaseModel",
            "extra_fields_v2_ignore.py",
        ),
    ],
)
@freeze_time("2019-07-26")
def test_main_extra_fields(extra_fields: str, output_model: str, expected_output: str, tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "extra_fields.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
        "--extra-fields",
        extra_fields,
        "--output-model-type",
        output_model,
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / expected_output).read_text()


@freeze_time("2019-07-26")
def test_main_jsonschema_same_name_objects(tmp_path: Path) -> None:
    """
    See: https://github.com/koxudaxi/datamodel-code-generator/issues/2460
    """
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "same_name_objects.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "jsonschema",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_SCHEMA_PATH / "same_name_objects.py").read_text()


@freeze_time("2019-07-26")
def test_main_jsonschema_forwarding_reference_collapse_root(tmp_path: Path) -> None:
    """
    See: https://github.com/koxudaxi/datamodel-code-generator/issues/1466
    """
    return_code: Exit = main([
        "--input",
        str(JSON_SCHEMA_DATA_PATH / "forwarding_reference"),
        "--output",
        str(tmp_path),
        "--input-file-type",
        "jsonschema",
        "--collapse-root-models",
    ])
    assert return_code == Exit.OK
    main_modular_dir = EXPECTED_JSON_SCHEMA_PATH / "forwarding_reference"
    for path in main_modular_dir.rglob("*.py"):
        result = tmp_path.joinpath(path.relative_to(main_modular_dir)).read_text()
        assert result == path.read_text()
