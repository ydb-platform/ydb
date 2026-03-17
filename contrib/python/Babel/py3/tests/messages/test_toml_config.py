from __future__ import annotations

import pathlib
from io import BytesIO

import pytest

from babel.messages import frontend

import yatest.common as yc
toml_test_cases_path = pathlib.Path(yc.source_path(__file__)).parent / "toml-test-cases"
assert toml_test_cases_path.is_dir(), "toml-test-cases directory not found"


def parse_toml(cfg: bytes | str):
    if isinstance(cfg, str):
        cfg = cfg.encode("utf-8")
    return frontend._parse_mapping_toml(BytesIO(cfg))


def test_toml_mapping_multiple_patterns():
    """
    Test that patterns may be specified as a list in TOML,
    and are expanded to multiple entries in the method map.
    """
    method_map, options_map = parse_toml("""
[[mappings]]
method = "python"
pattern = ["xyz/**.py", "foo/**.py"]
""")
    assert method_map == [
        ('xyz/**.py', 'python'),
        ('foo/**.py', 'python'),
    ]


@pytest.mark.parametrize(
    ("keywords_val", "expected"),
    [
        pytest.param('"foo bar quz"', {'bar': None, 'foo': None, 'quz': None}, id='string'),
        pytest.param('["foo", "bar", "quz"]', {'bar': None, 'foo': None, 'quz': None}, id='list'),
        pytest.param('"foo:1,2 bar quz"', {'bar': None, 'foo': (1, 2), 'quz': None}, id='s-args'),
        pytest.param('["bar", "foo:1,2", "quz"]', {'bar': None, 'foo': (1, 2), 'quz': None}, id='l-args'),
        pytest.param('[]', None, id='empty'),
    ],
)
def test_toml_mapping_keywords_parsing(keywords_val, expected):
    method_map, options_map = parse_toml(f"""
[[mappings]]
method = "python"
pattern = ["**.py"]
keywords = {keywords_val}
""")
    assert options_map['**.py'].get('keywords') == expected


@pytest.mark.parametrize(
    ("add_comments_val", "expected"),
    [
        ('"SPECIAL SAUCE"', ['SPECIAL SAUCE']),  # TOML will allow this as a single string
        ('["SPECIAL", "SAUCE"]', ['SPECIAL', 'SAUCE']),
        ('[]', None),
    ],
)
def test_toml_mapping_add_comments_parsing(add_comments_val, expected):
    method_map, options_map = parse_toml(f"""
[[mappings]]
method = "python"
pattern = ["**.py"]
add_comments = {add_comments_val}
""")
    assert options_map['**.py'].get('add_comments') == expected


@pytest.mark.parametrize("test_case", toml_test_cases_path.glob("bad.*.toml"), ids=lambda p: p.name)
def test_bad_toml_test_case(test_case: pathlib.Path):
    """
    Test that bad TOML files raise a ValueError.
    """
    with pytest.raises(frontend.ConfigurationError):
        with test_case.open("rb") as f:
            frontend._parse_mapping_toml(
                f,
                filename=test_case.name,
                style="pyproject.toml" if "pyproject" in test_case.name else "standalone",
            )
