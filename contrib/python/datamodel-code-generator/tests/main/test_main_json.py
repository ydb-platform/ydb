from __future__ import annotations

from argparse import Namespace
from typing import TYPE_CHECKING
from unittest.mock import call

import black
import pytest
from freezegun import freeze_time
from packaging import version

from datamodel_code_generator import (
    chdir,
)
from datamodel_code_generator.__main__ import Exit, main
from __tests__.main.test_main_general import DATA_PATH, EXPECTED_MAIN_PATH

if TYPE_CHECKING:
    from pathlib import Path

    from pytest_mock import MockerFixture

FixtureRequest = pytest.FixtureRequest


JSON_DATA_PATH: Path = DATA_PATH / "json"
EXPECTED_JSON_PATH: Path = EXPECTED_MAIN_PATH / "json"


@pytest.fixture(autouse=True)
def reset_namespace(monkeypatch: pytest.MonkeyPatch) -> None:
    namespace_ = Namespace(no_color=False)
    monkeypatch.setattr("datamodel_code_generator.__main__.namespace", namespace_)
    monkeypatch.setattr("datamodel_code_generator.arguments.namespace", namespace_)


@freeze_time("2019-07-26")
def test_main_json(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_DATA_PATH / "pet.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "json",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_PATH / "general.py").read_text()


@freeze_time("2019-07-26")
def test_space_and_special_characters_json(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_DATA_PATH / "space_and_special_characters.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "json",
    ])
    assert return_code == Exit.OK
    assert (
        output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_PATH / "space_and_special_characters.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_json_failed(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_DATA_PATH / "broken.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "json",
    ])
    assert return_code == Exit.ERROR


@freeze_time("2019-07-26")
def test_main_json_array_include_null(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_DATA_PATH / "array_include_null.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "json",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_PATH / "json_array_include_null.py").read_text()


@freeze_time("2019-07-26")
def test_main_json_reuse_model(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_DATA_PATH / "duplicate_models.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "json",
        "--reuse-model",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_PATH / "json_reuse_model.py").read_text()


@freeze_time("2019-07-26")
def test_main_json_reuse_model_pydantic2(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_DATA_PATH / "duplicate_models.json"),
        "--output-model-type",
        "pydantic_v2.BaseModel",
        "--output",
        str(output_file),
        "--input-file-type",
        "json",
        "--reuse-model",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_PATH / "json_reuse_model_pydantic2.py").read_text()


@freeze_time("2019-07-26")
def test_simple_json_snake_case_field(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    with chdir(JSON_DATA_PATH / "simple.json"):
        return_code: Exit = main([
            "--input",
            "simple.json",
            "--output",
            str(output_file),
            "--input-file-type",
            "json",
            "--snake-case-field",
        ])
        assert return_code == Exit.OK
        assert (
            output_file.read_text(encoding="utf-8")
            == (EXPECTED_JSON_PATH / "simple_json_snake_case_field.py").read_text()
        )


@freeze_time("2019-07-26")
def test_main_http_json(mocker: MockerFixture, tmp_path: Path) -> None:
    def get_mock_response(path: str) -> mocker.Mock:
        mock = mocker.Mock()
        mock.text = (JSON_DATA_PATH / path).read_text()
        return mock

    httpx_get_mock = mocker.patch(
        "httpx.get",
        side_effect=[
            get_mock_response("pet.json"),
        ],
    )
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--url",
        "https://example.com/pet.json",
        "--output",
        str(output_file),
        "--input-file-type",
        "json",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_PATH / "general.py").read_text().replace(
        "#   filename:  pet.json",
        "#   filename:  https://example.com/pet.json",
    )
    httpx_get_mock.assert_has_calls([
        call(
            "https://example.com/pet.json",
            headers=None,
            verify=True,
            follow_redirects=True,
            params=None,
        ),
    ])


@pytest.mark.skipif(
    version.parse(black.__version__) < version.parse("23.3.0"),
    reason="Require Black version 23.3.0 or later ",
)
@freeze_time("2019-07-26")
def test_main_typed_dict_space_and_special_characters(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_DATA_PATH / "space_and_special_characters.json"),
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
        == (EXPECTED_JSON_PATH / "typed_dict_space_and_special_characters.py").read_text()
    )


@freeze_time("2019-07-26")
def test_main_json_snake_case_field(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(JSON_DATA_PATH / "snake_case.json"),
        "--output",
        str(output_file),
        "--input-file-type",
        "json",
        "--snake-case-field",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_JSON_PATH / "json_snake_case_field.py").read_text()
