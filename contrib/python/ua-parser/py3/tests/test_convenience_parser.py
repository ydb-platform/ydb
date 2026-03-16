import ua_parser
from ua_parser import Domain, Parser, PartialResult, Result


def test_parser_memoized() -> None:
    """The global parser should be lazily instantiated but memoized"""
    # ensure there is no global parser
    vars(ua_parser).pop("parser", None)

    p1 = ua_parser.parser
    p2 = ua_parser.parser

    assert p1 is p2

    # force the creation of a clean parser
    del ua_parser.parser
    p3 = ua_parser.parser
    assert p3 is not p1


def resolver(s: str, d: Domain) -> PartialResult:
    return PartialResult(d, None, None, None, s)


def test_parser_utility() -> None:
    """Tests that ``Parser``'s methods to behave as procedural
    helpers, for users who may not wish to instantiate a parser or
    something.

    """

    r = Parser.parse(resolver, "a")
    assert r == Result(None, None, None, "a")

    os = Parser.parse_os(resolver, "a")
    assert os is None
