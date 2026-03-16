import pickle
from datetime import datetime

import parse


def test_tz_compare_to_None():
    utc = parse.FixedTzOffset(0, "UTC")
    assert utc is not None
    assert utc != "spam"


def test_named_date_issue7():
    r = parse.parse("on {date:ti}", "on 2012-09-17")
    assert r["date"] == datetime(2012, 9, 17, 0, 0, 0)

    # fix introduced regressions
    r = parse.parse("a {:ti} b", "a 1997-07-16T19:20 b")
    assert r[0] == datetime(1997, 7, 16, 19, 20, 0)
    r = parse.parse("a {:ti} b", "a 1997-07-16T19:20Z b")
    utc = parse.FixedTzOffset(0, "UTC")
    assert r[0] == datetime(1997, 7, 16, 19, 20, tzinfo=utc)
    r = parse.parse("a {date:ti} b", "a 1997-07-16T19:20Z b")
    assert r["date"] == datetime(1997, 7, 16, 19, 20, tzinfo=utc)


def test_dotted_type_conversion_pull_8():
    # test pull request 8 which fixes type conversion related to dotted
    # names being applied correctly
    r = parse.parse("{a.b:d}", "1")
    assert r["a.b"] == 1
    r = parse.parse("{a_b:w} {a.b:d}", "1 2")
    assert r["a_b"] == "1"
    assert r["a.b"] == 2


def test_pm_overflow_issue16():
    r = parse.parse("Meet at {:tg}", "Meet at 1/2/2011 12:45 PM")
    assert r[0] == datetime(2011, 2, 1, 12, 45)


def test_pm_handling_issue57():
    r = parse.parse("Meet at {:tg}", "Meet at 1/2/2011 12:15 PM")
    assert r[0] == datetime(2011, 2, 1, 12, 15)
    r = parse.parse("Meet at {:tg}", "Meet at 1/2/2011 12:15 AM")
    assert r[0] == datetime(2011, 2, 1, 0, 15)


def test_user_type_with_group_count_issue60():
    @parse.with_pattern(r"((\w+))", regex_group_count=2)
    def parse_word_and_covert_to_uppercase(text):
        return text.strip().upper()

    @parse.with_pattern(r"\d+")
    def parse_number(text):
        return int(text)

    # -- CASE: Use named (OK)
    type_map = {"Name": parse_word_and_covert_to_uppercase, "Number": parse_number}
    r = parse.parse(
        "Hello {name:Name} {number:Number}", "Hello Alice 42", extra_types=type_map
    )
    assert r.named == {"name": "ALICE", "number": 42}

    # -- CASE: Use unnamed/fixed (problematic)
    r = parse.parse("Hello {:Name} {:Number}", "Hello Alice 42", extra_types=type_map)
    assert r[0] == "ALICE"
    assert r[1] == 42


def test_unmatched_brace_doesnt_match():
    r = parse.parse("{who.txt", "hello")
    assert r is None


def test_pickling_bug_110():
    p = parse.compile("{a:d}")
    # prior to the fix, this would raise an AttributeError
    pickle.dumps(p)


def test_unused_centered_alignment_bug():
    r = parse.parse("{:^2S}", "foo")
    assert r[0] == "foo"
    r = parse.search("{:^2S}", "foo")
    assert r[0] == "foo"

    # specifically test for the case in issue #118 as well
    r = parse.parse("Column {:d}:{:^}", "Column 1: Timestep")
    assert r[0] == 1
    assert r[1] == "Timestep"


def test_unused_left_alignment_bug():
    r = parse.parse("{:<2S}", "foo")
    assert r[0] == "foo"
    r = parse.search("{:<2S}", "foo")
    assert r[0] == "foo"


def test_match_trailing_newline():
    r = parse.parse("{}", "test\n")
    assert r[0] == "test\n"
