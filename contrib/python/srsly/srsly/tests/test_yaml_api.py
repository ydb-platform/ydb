from io import StringIO
from pathlib import Path
import pytest

from srsly._yaml_api import yaml_dumps, yaml_loads, read_yaml, write_yaml
from srsly._yaml_api import is_yaml_serializable
from srsly.ruamel_yaml.comments import CommentedMap
from .util import make_tempdir


def test_yaml_dumps():
    data = {"a": [1, "hello"], "b": {"foo": "bar", "baz": [10.5, 120]}}
    result = yaml_dumps(data)
    expected = "a:\n  - 1\n  - hello\nb:\n  foo: bar\n  baz:\n    - 10.5\n    - 120\n"
    assert result == expected


def test_yaml_dumps_indent():
    data = {"a": [1, "hello"], "b": {"foo": "bar", "baz": [10.5, 120]}}
    result = yaml_dumps(data, indent_mapping=2, indent_sequence=2, indent_offset=0)
    expected = "a:\n- 1\n- hello\nb:\n  foo: bar\n  baz:\n  - 10.5\n  - 120\n"
    assert result == expected


def test_yaml_loads():
    data = "a:\n- 1\n- hello\nb:\n  foo: bar\n  baz:\n  - 10.5\n  - 120\n"
    result = yaml_loads(data)
    # Check that correct loader is used and result is regular dict, not the
    # custom ruamel.yaml "ordereddict" class
    assert not isinstance(result, CommentedMap)
    assert result == {"a": [1, "hello"], "b": {"foo": "bar", "baz": [10.5, 120]}}


def test_read_yaml_file():
    file_contents = "a:\n- 1\n- hello\nb:\n  foo: bar\n  baz:\n  - 10.5\n  - 120\n"
    with make_tempdir({"tmp.yaml": file_contents}) as temp_dir:
        file_path = temp_dir / "tmp.yaml"
        assert file_path.exists()
        data = read_yaml(file_path)
    assert len(data) == 2
    assert data["a"] == [1, "hello"]


def test_read_yaml_file_invalid():
    file_contents = "a: - 1\n- hello\nb:\n  foo: bar\n  baz:\n    - 10.5\n    - 120\n"
    with make_tempdir({"tmp.yaml": file_contents}) as temp_dir:
        file_path = temp_dir / "tmp.yaml"
        assert file_path.exists()
        with pytest.raises(ValueError):
            read_yaml(file_path)


def test_read_yaml_stdin(monkeypatch):
    input_data = "a:\n  - 1\n  - hello\nb:\n  foo: bar\n  baz:\n    - 10.5\n    - 120\n"
    monkeypatch.setattr("sys.stdin", StringIO(input_data))
    data = read_yaml("-")
    assert len(data) == 2
    assert data["a"] == [1, "hello"]


def test_write_yaml_file():
    data = {"hello": "world", "test": [123, 456]}
    expected = "hello: world\ntest:\n  - 123\n  - 456\n"
    with make_tempdir() as temp_dir:
        file_path = temp_dir / "tmp.yaml"
        write_yaml(file_path, data)
        with Path(file_path).open("r", encoding="utf8") as f:
            assert f.read() == expected


def test_write_yaml_stdout(capsys):
    data = {"hello": "world", "test": [123, 456]}
    expected = "hello: world\ntest:\n  - 123\n  - 456\n\n"
    write_yaml("-", data)
    captured = capsys.readouterr()
    assert captured.out == expected


@pytest.mark.parametrize(
    "obj,expected",
    [
        (["a", "b", 1, 2], True),
        ({"a": "b", "c": 123}, True),
        ("hello", True),
        (lambda x: x, False),
        ({"a": lambda x: x}, False),
    ],
)
def test_is_yaml_serializable(obj, expected):
    assert is_yaml_serializable(obj) == expected
    # Check again to be sure it's consistent
    assert is_yaml_serializable(obj) == expected
