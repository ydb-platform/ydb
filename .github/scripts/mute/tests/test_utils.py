"""Tests for mute.utils."""
import os
import tempfile

from mute.utils import parse_mute_file


def test_parse_mute_file():
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
        f.write("a b\n# comment\nc d\n")
        path = f.name
    try:
        result = parse_mute_file(path)
        assert result == {"a b", "c d"}
    finally:
        os.unlink(path)


def test_parse_mute_file_empty():
    assert parse_mute_file(None) == set()
    assert parse_mute_file("") == set()
