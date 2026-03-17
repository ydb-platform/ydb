"""Test validation against JSON examples.

These examples are organised in groups (sharing the same schema).

Each example group is a folder under the `tests/examples/` directory
that contains a single `.schema.json` file and as many examples (simple
`.json` files, whithout `.schema`) as necessary.

If an example is meant to fail validation, a text file with the same name/path
but replacing the extension from `.json` to `.error` is expected to exist.

The content of this `.error` file should be a piece of the message of the
associated JsonSchemaException (i.e. a substring of `str(exception_object)`).

All files should be encoded with UTF-8.
"""
import json
from pathlib import Path

import pytest

from fastjsonschema import JsonSchemaException, compile

HERE = Path(__file__).parent
EXAMPLES = HERE / "examples"
EXAMPLE_GROUPS = [x.name for x in EXAMPLES.glob("*/")]


@pytest.mark.parametrize("example_group", EXAMPLE_GROUPS)
def test_validate(example_group):
    example_dir = EXAMPLES / example_group
    schema = next(example_dir.glob("*.schema.json"))
    validator = compile(json.loads(schema.read_text(encoding="utf-8")))
    examples = (
        (e, e.with_suffix(".error"), json.loads(e.read_text(encoding="utf-8")))
        for e in example_dir.glob("*.json")
        if not str(e).endswith(".schema.json")
    )
    for example_file, err_file, example in examples:
        if err_file.exists():
            with pytest.raises(JsonSchemaException) as exc_info:
                validator(example)
            assert err_file.read_text(encoding="utf-8").strip() in str(exc_info.value).strip()
        else:
            validator(example)  # valid
