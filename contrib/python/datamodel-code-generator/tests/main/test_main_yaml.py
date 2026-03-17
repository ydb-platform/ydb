from __future__ import annotations

from argparse import Namespace
from typing import TYPE_CHECKING

import pytest
from freezegun import freeze_time

from datamodel_code_generator.__main__ import Exit, main
from __tests__.main.test_main_general import DATA_PATH, EXPECTED_MAIN_PATH

if TYPE_CHECKING:
    from pathlib import Path

YAML_DATA_PATH: Path = DATA_PATH / "yaml"


@pytest.fixture(autouse=True)
def reset_namespace(monkeypatch: pytest.MonkeyPatch) -> None:
    namespace_ = Namespace(no_color=False)
    monkeypatch.setattr("datamodel_code_generator.__main__.namespace", namespace_)
    monkeypatch.setattr("datamodel_code_generator.arguments.namespace", namespace_)


@pytest.mark.benchmark
@freeze_time("2019-07-26")
def test_main_yaml(tmp_path: Path) -> None:
    output_file: Path = tmp_path / "output.py"
    return_code: Exit = main([
        "--input",
        str(YAML_DATA_PATH / "pet.yaml"),
        "--output",
        str(output_file),
        "--input-file-type",
        "yaml",
    ])
    assert return_code == Exit.OK
    assert output_file.read_text(encoding="utf-8") == (EXPECTED_MAIN_PATH / "yaml.py").read_text()
