import pytest

from yarl._path import normalize_path

PATHS = [
    # No dots
    ("", ""),
    ("/", "/"),
    ("//", "//"),
    ("///", "///"),
    ("path", "path"),
    # Single-dot
    ("path/to", "path/to"),
    ("././path/to", "path/to"),
    ("path/./to", "path/to"),
    ("path/././to", "path/to"),
    ("path/to/.", "path/to/"),
    ("path/to/./.", "path/to/"),
    ("/path/to/.", "/path/to/"),
    # Double-dots
    ("../path/to", "path/to"),
    ("path/../to", "to"),
    ("path/../../to", "to"),
    # absolute path root / is maintained; tests based on two
    # tests from web-platform-tests project's urltestdata.json
    ("/foo/../../../ton", "/ton"),
    ("/foo/../../../..bar", "/..bar"),
    # Non-ASCII characters
    ("μονοπάτι/../../να/ᴜɴɪ/ᴄᴏᴅᴇ", "να/ᴜɴɪ/ᴄᴏᴅᴇ"),
    ("μονοπάτι/../../να/𝕦𝕟𝕚/𝕔𝕠𝕕𝕖/.", "να/𝕦𝕟𝕚/𝕔𝕠𝕕𝕖/"),
]


@pytest.mark.parametrize("original,expected", PATHS)
def test_normalize_path(original: str, expected: str) -> None:
    assert normalize_path(original) == expected
