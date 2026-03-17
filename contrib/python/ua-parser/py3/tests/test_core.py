"""Tests UAP-Python using the UAP-core test suite"""

import dataclasses
import logging
import pathlib
from operator import attrgetter
from typing import cast

import pytest  # type: ignore

try:
    from yaml import (
        CSafeLoader as SafeLoader,
        load,
    )
except ImportError:
    logging.getLogger(__name__).warning(
        "PyYaml C extension not available to run tests, this will result "
        "in tests slowdown."
    )
    from yaml import SafeLoader, load  # type: ignore

from ua_parser import (
    BasicResolver,
    Device,
    OS,
    Parser,
    Result,
    UserAgent,
    load_builtins,
    load_lazy_builtins,
    loaders,
)
from ua_parser.matchers import UserAgentMatcher

import yatest.common as yc
CORE_DIR = pathlib.Path(yc.work_path("test_data/"))


data = cast(loaders.FileLoader, loaders.load_yaml)(CORE_DIR / "regexes.yaml")
data_lazy = cast(loaders.FileLoader, loaders.load_yaml)(
    CORE_DIR / "regexes.yaml", loader=loaders.load_lazy
)
PARSERS = [
    pytest.param(Parser(BasicResolver(load_builtins())), id="basic"),
    pytest.param(Parser(BasicResolver(load_lazy_builtins())), id="lazy"),
    pytest.param(Parser(BasicResolver(data)), id="basic-yaml"),
    pytest.param(Parser(BasicResolver(data_lazy)), id="lazy-yaml"),
]
try:
    from ua_parser import re2
except ImportError:
    PARSERS.append(
        pytest.param(
            None, id="re2", marks=pytest.mark.skip(reason="re2 parser not available")
        )
    )
else:
    PARSERS.append(pytest.param(Parser(re2.Resolver(data)), id="re2"))

try:
    from ua_parser import regex
except ImportError:
    PARSERS.append(
        pytest.param(
            None,
            id="regex",
            marks=pytest.mark.skip(reason="regex parser not available"),
        )
    )
else:
    PARSERS.append(pytest.param(Parser(regex.Resolver(data)), id="regex"))

UA_FIELDS = {f.name for f in dataclasses.fields(UserAgent)}


@pytest.mark.parametrize("parser", PARSERS)
@pytest.mark.parametrize(
    "test_file",
    [
        CORE_DIR / "tests" / "test_ua.yaml",
        CORE_DIR / "test_resources" / "firefox_user_agent_strings.yaml",
        CORE_DIR / "test_resources" / "pgts_browser_list.yaml",
    ],
    ids=attrgetter("stem"),
)
def test_ua(parser, test_file):
    with test_file.open("rb") as f:
        contents = load(f, Loader=SafeLoader)

    for test_case in contents["test_cases"]:
        res = {k: v for k, v in test_case.items() if k in UA_FIELDS}
        # there seems to be broken test cases which have a patch_minor
        # of null where it's not, as well as the reverse, so we can't
        # test patch_minor (ua-parser/uap-core#562)
        res.pop("patch_minor", None)
        r = parser.parse_user_agent(test_case["user_agent_string"]) or UserAgent()
        assert dataclasses.asdict(r).items() >= res.items()


OS_FIELDS = {f.name for f in dataclasses.fields(OS)}


@pytest.mark.parametrize("parser", PARSERS)
@pytest.mark.parametrize(
    "test_file",
    [
        pytest.param(CORE_DIR / "tests" / "test_os.yaml", marks=pytest.mark.skip),
        CORE_DIR / "test_resources" / "additional_os_tests.yaml",
    ],
    ids=attrgetter("stem"),
)
def test_os(parser, test_file):
    with test_file.open("rb") as f:
        contents = load(f, Loader=SafeLoader)

    for test_case in contents["test_cases"]:
        res = {k: v for k, v in test_case.items() if k in OS_FIELDS}
        r = parser.parse_os(test_case["user_agent_string"]) or OS()
        assert dataclasses.asdict(r) == res


DEVICE_FIELDS = {f.name for f in dataclasses.fields(Device)}


@pytest.mark.parametrize("parser", PARSERS)
@pytest.mark.parametrize(
    "test_file",
    [
        CORE_DIR / "tests" / "test_device.yaml",
    ],
    ids=attrgetter("stem"),
)
def test_devices(parser, test_file):
    with test_file.open("rb") as f:
        contents = load(f, Loader=SafeLoader)

    for test_case in contents["test_cases"]:
        res = {k: v for k, v in test_case.items() if k in DEVICE_FIELDS}
        r = parser.parse_device(test_case["user_agent_string"]) or Device()
        assert dataclasses.asdict(r) == res


def test_results():
    p = Parser(BasicResolver(([UserAgentMatcher("(x)")], [], [])))

    assert p.parse_user_agent("x") == UserAgent("x")
    assert p.parse_user_agent("y") is None

    assert p.parse("x") == Result(
        user_agent=UserAgent("x"),
        os=None,
        device=None,
        string="x",
    )
    assert p.parse("y") == Result(
        user_agent=None,
        os=None,
        device=None,
        string="y",
    )
