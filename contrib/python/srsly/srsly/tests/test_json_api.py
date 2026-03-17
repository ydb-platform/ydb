import pytest
from io import StringIO
from pathlib import Path
import gzip
import numpy

from srsly._json_api import (
    read_json,
    write_json,
    read_jsonl,
    write_jsonl,
    read_gzip_jsonl,
    write_gzip_jsonl,
)
from srsly._json_api import write_gzip_json, json_dumps, is_json_serializable
from srsly._json_api import json_loads
from srsly.util import force_string
from .util import make_tempdir


def test_json_dumps_sort_keys():
    data = {"a": 1, "c": 3, "b": 2}
    result = json_dumps(data, sort_keys=True)
    assert result == '{"a":1,"b":2,"c":3}'


def test_read_json_file():
    file_contents = '{\n    "hello": "world"\n}'
    with make_tempdir({"tmp.json": file_contents}) as temp_dir:
        file_path = temp_dir / "tmp.json"
        assert file_path.exists()
        data = read_json(file_path)
    assert len(data) == 1
    assert data["hello"] == "world"


def test_read_json_file_invalid():
    file_contents = '{\n    "hello": world\n}'
    with make_tempdir({"tmp.json": file_contents}) as temp_dir:
        file_path = temp_dir / "tmp.json"
        assert file_path.exists()
        with pytest.raises(ValueError):
            read_json(file_path)


def test_read_json_stdin(monkeypatch):
    input_data = '{\n    "hello": "world"\n}'
    monkeypatch.setattr("sys.stdin", StringIO(input_data))
    data = read_json("-")
    assert len(data) == 1
    assert data["hello"] == "world"


def test_write_json_file():
    data = {"hello": "world", "test": 123}
    # Provide two expected options, depending on how keys are ordered
    expected = [
        '{\n  "hello":"world",\n  "test":123\n}',
        '{\n  "test":123,\n  "hello":"world"\n}',
    ]
    with make_tempdir() as temp_dir:
        file_path = temp_dir / "tmp.json"
        write_json(file_path, data)
        with Path(file_path).open("r", encoding="utf8") as f:
            assert f.read() in expected


def test_write_json_file_gzip():
    data = {"hello": "world", "test": 123}
    # Provide two expected options, depending on how keys are ordered
    expected = [
        '{\n  "hello":"world",\n  "test":123\n}',
        '{\n  "test":123,\n  "hello":"world"\n}',
    ]
    with make_tempdir() as temp_dir:
        file_path = force_string(temp_dir / "tmp.json")
        write_gzip_json(file_path, data)
        with gzip.open(file_path, "r") as f:
            assert f.read().decode("utf8") in expected


def test_write_json_stdout(capsys):
    data = {"hello": "world", "test": 123}
    # Provide two expected options, depending on how keys are ordered
    expected = [
        '{\n  "hello":"world",\n  "test":123\n}\n',
        '{\n  "test":123,\n  "hello":"world"\n}\n',
    ]
    write_json("-", data)
    captured = capsys.readouterr()
    assert captured.out in expected


def test_read_jsonl_file():
    file_contents = '{"hello": "world"}\n{"test": 123}'
    with make_tempdir({"tmp.json": file_contents}) as temp_dir:
        file_path = temp_dir / "tmp.json"
        assert file_path.exists()
        data = read_jsonl(file_path)
        # Make sure this returns a generator, not just a list
        assert not hasattr(data, "__len__")
        data = list(data)
    assert len(data) == 2
    assert len(data[0]) == 1
    assert len(data[1]) == 1
    assert data[0]["hello"] == "world"
    assert data[1]["test"] == 123


def test_read_jsonl_file_invalid():
    file_contents = '{"hello": world}\n{"test": 123}'
    with make_tempdir({"tmp.json": file_contents}) as temp_dir:
        file_path = temp_dir / "tmp.json"
        assert file_path.exists()
        with pytest.raises(ValueError):
            data = list(read_jsonl(file_path))
        data = list(read_jsonl(file_path, skip=True))
    assert len(data) == 1
    assert len(data[0]) == 1
    assert data[0]["test"] == 123


def test_read_jsonl_stdin(monkeypatch):
    input_data = '{"hello": "world"}\n{"test": 123}'
    monkeypatch.setattr("sys.stdin", StringIO(input_data))
    data = read_jsonl("-")
    # Make sure this returns a generator, not just a list
    assert not hasattr(data, "__len__")
    data = list(data)
    assert len(data) == 2
    assert len(data[0]) == 1
    assert len(data[1]) == 1
    assert data[0]["hello"] == "world"
    assert data[1]["test"] == 123


def test_write_jsonl_file():
    data = [{"hello": "world"}, {"test": 123}]
    with make_tempdir() as temp_dir:
        file_path = temp_dir / "tmp.json"
        write_jsonl(file_path, data)
        with Path(file_path).open("r", encoding="utf8") as f:
            assert f.read() == '{"hello":"world"}\n{"test":123}\n'


def test_write_jsonl_file_append():
    data = [{"hello": "world"}, {"test": 123}]
    expected = '{"hello":"world"}\n{"test":123}\n\n{"hello":"world"}\n{"test":123}\n'
    with make_tempdir() as temp_dir:
        file_path = temp_dir / "tmp.json"
        write_jsonl(file_path, data)
        write_jsonl(file_path, data, append=True)
        with Path(file_path).open("r", encoding="utf8") as f:
            assert f.read() == expected


def test_write_jsonl_file_append_no_new_line():
    data = [{"hello": "world"}, {"test": 123}]
    expected = '{"hello":"world"}\n{"test":123}\n{"hello":"world"}\n{"test":123}\n'
    with make_tempdir() as temp_dir:
        file_path = temp_dir / "tmp.json"
        write_jsonl(file_path, data)
        write_jsonl(file_path, data, append=True, append_new_line=False)
        with Path(file_path).open("r", encoding="utf8") as f:
            assert f.read() == expected


def test_write_jsonl_stdout(capsys):
    data = [{"hello": "world"}, {"test": 123}]
    write_jsonl("-", data)
    captured = capsys.readouterr()
    assert captured.out == '{"hello":"world"}\n{"test":123}\n'


@pytest.mark.parametrize(
    "obj,expected",
    [
        (["a", "b", 1, 2], True),
        ({"a": "b", "c": 123}, True),
        ("hello", True),
        (lambda x: x, False),
    ],
)
def test_is_json_serializable(obj, expected):
    assert is_json_serializable(obj) == expected


@pytest.mark.parametrize(
    "obj,expected",
    [
        ("-32", -32),
        ("32", 32),
        ("0", 0),
        ("-0", 0),
    ],
)
def test_json_loads_number_string(obj, expected):
    assert json_loads(obj) == expected


@pytest.mark.parametrize(
    "obj",
    ["HI", "-", "-?", "?!", "THIS IS A STRING"],
)
def test_json_loads_raises(obj):
    with pytest.raises(ValueError):
        json_loads(obj)


def test_unsupported_type_error():
    f = numpy.float32()
    with pytest.raises(TypeError):
        s = json_dumps(f)


def test_write_jsonl_gzip():
    """Tests writing data to a gzipped .jsonl file."""
    data = [{"hello": "world"}, {"test": 123}]
    expected = ['{"hello":"world"}\n', '{"test":123}\n']

    with make_tempdir() as temp_dir:
        file_path = temp_dir / "tmp.json"
        write_gzip_jsonl(file_path, data)
        with gzip.open(file_path, "r") as f:
            assert [line.decode("utf8") for line in f.readlines()] == expected


def test_write_jsonl_gzip_append():
    """Tests appending data to a gzipped .jsonl file."""
    data = [{"hello": "world"}, {"test": 123}]
    expected = [
        '{"hello":"world"}\n',
        '{"test":123}\n',
        "\n",
        '{"hello":"world"}\n',
        '{"test":123}\n',
    ]
    with make_tempdir() as temp_dir:
        file_path = temp_dir / "tmp.json"
        write_gzip_jsonl(file_path, data)
        write_gzip_jsonl(file_path, data, append=True)
        with gzip.open(file_path, "r") as f:
            assert [line.decode("utf8") for line in f.readlines()] == expected


def test_read_jsonl_gzip():
    """Tests reading data from a gzipped .jsonl file."""
    file_contents = [{"hello": "world"}, {"test": 123}]
    with make_tempdir() as temp_dir:
        file_path = temp_dir / "tmp.json"
        with gzip.open(file_path, "w") as f:
            f.writelines(
                [(json_dumps(line) + "\n").encode("utf-8") for line in file_contents]
            )
        assert file_path.exists()
        data = read_gzip_jsonl(file_path)
        # Make sure this returns a generator, not just a list
        assert not hasattr(data, "__len__")
        data = list(data)
    assert len(data) == 2
    assert len(data[0]) == 1
    assert len(data[1]) == 1
    assert data[0]["hello"] == "world"
    assert data[1]["test"] == 123
