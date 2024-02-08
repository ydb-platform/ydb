import pytest

from yarl import URL

PATHS = [
    # No dots
    ("", ""),
    ("/", "/"),
    ("//", "//"),
    ("///", "///"),
    # Single-dot
    ("path/to", "path/to"),
    ("././path/to", "path/to"),
    ("path/./to", "path/to"),
    ("path/././to", "path/to"),
    ("path/to/.", "path/to/"),
    ("path/to/./.", "path/to/"),
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
def test__normalize_path(original, expected):
    assert URL._normalize_path(original) == expected
