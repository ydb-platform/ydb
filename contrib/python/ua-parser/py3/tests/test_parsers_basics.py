import io

from ua_parser import (
    BasicResolver,
    Domain,
    PartialResult,
    UserAgent,
)
from ua_parser.loaders import load_yaml
from ua_parser.matchers import UserAgentMatcher


def test_trivial_matching():
    p = BasicResolver(([UserAgentMatcher("(a)")], [], []))

    assert p("x", Domain.ALL) == PartialResult(
        string="x",
        domains=Domain.ALL,
        user_agent=None,
        os=None,
        device=None,
    )

    assert p("a", Domain.ALL) == PartialResult(
        string="a",
        domains=Domain.ALL,
        user_agent=UserAgent("a"),
        os=None,
        device=None,
    )


def test_partial():
    p = BasicResolver(([UserAgentMatcher("(a)")], [], []))

    assert p("x", Domain.USER_AGENT) == PartialResult(
        string="x",
        domains=Domain.USER_AGENT,
        user_agent=None,
        os=None,
        device=None,
    )

    assert p("a", Domain.USER_AGENT) == PartialResult(
        string="a",
        domains=Domain.USER_AGENT,
        user_agent=UserAgent("a"),
        os=None,
        device=None,
    )


def test_init_yaml():
    assert load_yaml
    f = io.BytesIO(
        b"""\
user_agent_parsers:
- regex: (a)
os_parsers: []
device_parsers: []
"""
    )
    p = BasicResolver(load_yaml(f))

    assert p("x", Domain.USER_AGENT) == PartialResult(
        string="x",
        domains=Domain.USER_AGENT,
        user_agent=None,
        os=None,
        device=None,
    )

    assert p("a", Domain.USER_AGENT) == PartialResult(
        string="a",
        domains=Domain.USER_AGENT,
        user_agent=UserAgent("a"),
        os=None,
        device=None,
    )
