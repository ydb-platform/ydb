from __future__ import annotations

from pathlib import Path

import pytest

from datamodel_code_generator import Error, InputFileType, infer_input_type

import yatest.common as yc
DATA_PATH: Path = Path(yc.source_path(__file__)).parent / "data"


def test_infer_input_type() -> None:
    def assert_infer_input_type(file: Path, raw_data_type: InputFileType) -> None:
        __tracebackhide__ = True
        if file.is_dir():
            return
        if file.suffix not in {".yaml", ".json"}:
            return
        result = infer_input_type(file.read_text(encoding="utf-8"))
        assert result == raw_data_type, f"{file} was the wrong type!"

    def assert_invalid_infer_input_type(file: Path) -> None:
        with pytest.raises(
            Error,
            match=(
                r"Can't infer input file type from the input data. "
                r"Please specify the input file type explicitly with --input-file-type option."
            ),
        ):
            infer_input_type(file.read_text(encoding="utf-8"))

    for file in (DATA_PATH / "csv").rglob("*"):
        assert_infer_input_type(file, InputFileType.CSV)

    for file in (DATA_PATH / "json").rglob("*"):
        if file.name.endswith("broken.json"):
            continue
        assert_infer_input_type(file, InputFileType.Json)
    for file in (DATA_PATH / "jsonschema").rglob("*"):
        if file.name.endswith((
            "external_child.json",
            "external_child.yaml",
            "extra_data_msgspec.json",
        )):
            continue
        assert_infer_input_type(file, InputFileType.JsonSchema)
    for file in (DATA_PATH / "openapi").rglob("*"):
        if "all_of_with_relative_ref" in file.parts:
            continue
        if "reference_same_hierarchy_directory" in file.parts:
            continue
        if file.name.endswith((
            "aliases.json",
            "extra_data.json",
            "extra_data_msgspec.json",
            "invalid.yaml",
            "list.json",
            "empty_data.json",
            "root_model.yaml",
            "json_pointer.yaml",
            "const.json",
            "array_called_fields_with_oneOf_items.yaml",
        )):
            continue

        if file.name.endswith("not.json"):
            assert_invalid_infer_input_type(file)
            continue
        assert_infer_input_type(file, InputFileType.OpenAPI)
