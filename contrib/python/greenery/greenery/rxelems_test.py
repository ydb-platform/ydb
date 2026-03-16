from __future__ import annotations

import pickle
import time

import pytest

from .charclass import Charclass
from .fsm import Fsm
from .parse import parse
from .rxelems import from_fsm

# pylint: disable=compare-to-empty-string
# pylint: disable=invalid-name
# pylint: disable=too-many-lines

if __name__ == "__main__":
    raise RuntimeError(
        "Test files can't be run directly. Use `python -m pytest greenery`"
    )


###############################################################################
# Stringification tests


def test_charclass_str() -> None:
    # Arbitrary ranges
    assert str(parse("[\\w:;<=>?@\\[\\\\\\]\\^`]")) == "[0-z]"
    # pylint: disable-next=fixme
    # TODO: what if \d is a proper subset of `chars`?

    # escape sequences are not preserved
    assert str(parse("\\x09")) == "\\t"

    # Printing ASCII control characters? You should get hex escapes
    assert str(parse("\\x00")) == "\\x00"


def test_parse_str_round_trip() -> None:
    # not "a[ab]b"
    assert str(parse("a.b")) == "a.b"
    assert str(parse("\\d{4}")) == "\\d{4}"
    assert str(parse("a.b()()")) == "a.b()()"


###############################################################################
# Test to_fsm() and alphabet-related functionality


def test_pattern_fsm() -> None:
    # "a[^a]"
    anota = parse("a[^a]").to_fsm()
    assert len(anota.states) == 4
    assert not anota.accepts("a")
    assert not anota.accepts("b")
    assert not anota.accepts("aa")
    assert anota.accepts("ab")
    assert anota.accepts("ac")
    assert not anota.accepts("ba")
    assert not anota.accepts("bb")

    # "0\\d"
    zeroD = parse("0\\d").to_fsm()
    assert zeroD.accepts("01")
    assert not zeroD.accepts("10")

    # "\\d{2}"
    d2 = parse("\\d{2}").to_fsm()
    assert not d2.accepts("")
    assert not d2.accepts("1")
    assert d2.accepts("11")
    assert not d2.accepts("111")

    # abc|def(ghi|jkl)
    conventional = parse("abc|def(ghi|jkl)").to_fsm()
    assert not conventional.accepts("a")
    assert not conventional.accepts("ab")
    assert conventional.accepts("abc")
    assert not conventional.accepts("abcj")
    assert conventional.accepts("defghi")
    assert conventional.accepts("defjkl")


def test_fsm() -> None:
    # You should be able to to_fsm() a single regular expression element
    # without supplying a specific alphabet. That should be determinable from
    # context.
    assert parse("a.b").to_fsm().accepts("acb")

    bad = parse("0{2}|1{2}").to_fsm()
    assert bad.accepts("00")
    assert bad.accepts("11")
    assert not bad.accepts("01")

    bad = parse("0{2}|1{2}").to_fsm()
    assert bad.accepts("00")
    assert bad.accepts("11")
    assert not bad.accepts("01")


def test_bug_28() -> None:
    # Starification was broken in FSMs
    assert not parse("(ab*)").to_fsm().star().accepts("bb")
    assert not parse("(ab*)*").to_fsm().accepts("bb")


###############################################################################
# Test matches(). Quite sparse at the moment


def test_wildcards_in_charclasses() -> None:
    # Allow "\w", "\d" and "\s" in `Charclass`es
    assert parse("[\\w~]*").matches("a0~")
    assert parse("[\\da]*").matches("0129a")
    assert parse("[\\s]+").matches(" \t \t ")


def test_block_comment_regex() -> None:
    # I went through several incorrect regexes for C block comments. Here we
    # show why the first few attempts were incorrect
    a = parse("/\\*(([^*]|\\*+[^*/])*)\\*/")
    assert a.matches("/**/")
    assert not a.matches("/***/")
    assert not a.matches("/****/")

    b = parse("/\\*(([^*]|\\*[^/])*)\\*/")
    assert b.matches("/**/")
    assert not b.matches("/***/")
    assert b.matches("/****/")

    c = parse("/\\*(([^*]|\\*+[^*/])*)\\*+/")
    assert c.matches("/**/")
    assert c.matches("/***/")
    assert c.matches("/****/")


def test_named_groups() -> None:
    a = parse("(?P<ng1>abc)")
    assert a.matches("abc")


def test_in() -> None:
    assert "a" in parse("a")
    assert "abcdsasda" in parse("\\w{4,10}")
    assert "abc" in parse("abc|def(ghi|jkl)")


###############################################################################
# Test string generators


def test_charclass_gen() -> None:
    gen = parse("[xyz]").strings()
    assert next(gen) == "x"
    assert next(gen) == "y"
    assert next(gen) == "z"

    with pytest.raises(StopIteration):
        next(gen)


def test_mult_gen() -> None:
    # One term
    gen = parse("[ab]").strings()
    assert next(gen) == "a"
    assert next(gen) == "b"

    with pytest.raises(StopIteration):
        next(gen)

    # No terms
    gen = parse("[ab]{0}").strings()
    assert next(gen) == ""

    with pytest.raises(StopIteration):
        next(gen)

    # Many terms
    gen = parse("[ab]*").strings()
    assert next(gen) == ""
    assert next(gen) == "a"
    assert next(gen) == "b"
    assert next(gen) == "aa"
    assert next(gen) == "ab"
    assert next(gen) == "ba"
    assert next(gen) == "bb"
    assert next(gen) == "aaa"


def test_conc_generator() -> None:
    gen = parse("[ab][cd]").strings()
    assert next(gen) == "ac"
    assert next(gen) == "ad"
    assert next(gen) == "bc"
    assert next(gen) == "bd"

    with pytest.raises(StopIteration):
        next(gen)


def test_pattern_generator() -> None:
    gen = parse("[ab]|[cde]").strings()
    assert next(gen) == "a"
    assert next(gen) == "b"
    assert next(gen) == "c"
    assert next(gen) == "d"
    assert next(gen) == "e"

    with pytest.raises(StopIteration):
        next(gen)

    # more complex
    gen = parse("abc|def(ghi|jkl)").strings()
    assert next(gen) == "abc"
    assert next(gen) == "defghi"
    assert next(gen) == "defjkl"

    gen = parse("[0-9a-fA-F]{3,10}").strings()
    assert next(gen) == "000"
    assert next(gen) == "001"
    assert next(gen) == "002"


def test_infinite_generation() -> None:
    # Infinite generator, flummoxes both depth-first and breadth-first searches
    gen = parse("a*b*").strings()
    assert next(gen) == ""
    assert next(gen) == "a"
    assert next(gen) == "b"
    assert next(gen) == "aa"
    assert next(gen) == "ab"
    assert next(gen) == "bb"
    assert next(gen) == "aaa"
    assert next(gen) == "aab"
    assert next(gen) == "abb"
    assert next(gen) == "bbb"
    assert next(gen) == "aaaa"


def test_wildcard_generator() -> None:
    # Generator needs to handle wildcards as well. Wildcards come last.
    gen = parse("a.b").strings(otherchar="*")
    assert next(gen) == "aab"
    assert next(gen) == "abb"
    assert next(gen) == "a*b"

    with pytest.raises(StopIteration):
        next(gen)


def test_forin() -> None:
    assert tuple(parse("abc|def(ghi|jkl)")) == ("abc", "defghi", "defjkl")


###############################################################################
# Test cardinality() and len()


def test_cardinality() -> None:
    # pylint: disable-next=compare-to-zero
    assert parse("[]").cardinality() == 0
    assert parse("[ab]{3}").cardinality() == 8
    assert parse("[ab]{2,3}").cardinality() == 12
    assert len(parse("abc|def(ghi|jkl)")) == 3
    with pytest.raises(OverflowError):
        len(parse(".*"))


def test_cardinality_harder() -> None:
    assert parse("[]?").cardinality() == 1
    assert parse("[]{0,6}").cardinality() == 1
    assert parse("[ab]{0,3}").cardinality() == 15


###############################################################################


def test_copy() -> None:
    x = parse("abc|def(ghi|jkl)")
    assert x.copy() == x


###############################################################################
# Test from_fsm()


def test_dot() -> None:
    # not "a[ab]b"
    assert str(from_fsm(parse("a.b").to_fsm())) == "a.b"


def test_abstar() -> None:
    # Buggggs.
    abstar = Fsm(
        alphabet={Charclass("a"), ~Charclass("ab"), Charclass("b")},
        states={0, 1},
        initial=0,
        finals={0},
        map={
            0: {Charclass("a"): 0, ~Charclass("ab"): 1, Charclass("b"): 0},
            1: {Charclass("a"): 1, ~Charclass("ab"): 1, Charclass("b"): 1},
        },
    )
    assert str(from_fsm(abstar)) == "[ab]*"


def test_adotb() -> None:
    adotb = Fsm(
        alphabet={Charclass("a"), ~Charclass("ab"), Charclass("b")},
        states={0, 1, 2, 3, 4},
        initial=0,
        finals={4},
        map={
            0: {Charclass("a"): 2, ~Charclass("ab"): 1, Charclass("b"): 1},
            1: {Charclass("a"): 1, ~Charclass("ab"): 1, Charclass("b"): 1},
            2: {Charclass("a"): 3, ~Charclass("ab"): 3, Charclass("b"): 3},
            3: {Charclass("a"): 1, ~Charclass("ab"): 1, Charclass("b"): 4},
            4: {Charclass("a"): 1, ~Charclass("ab"): 1, Charclass("b"): 1},
        },
    )
    assert str(from_fsm(adotb)) == "a.b"


def test_rxelems_recursion_error() -> None:
    # Catch a recursion error
    assert (
        str(
            from_fsm(
                Fsm(
                    alphabet={Charclass("0"), Charclass("1"), ~Charclass("01")},
                    states={0, 1, 2, 3},
                    initial=3,
                    finals={1},
                    map={
                        0: {Charclass("0"): 1, Charclass("1"): 1, ~Charclass("01"): 2},
                        1: {Charclass("0"): 2, Charclass("1"): 2, ~Charclass("01"): 2},
                        2: {Charclass("0"): 2, Charclass("1"): 2, ~Charclass("01"): 2},
                        3: {Charclass("0"): 0, Charclass("1"): 2, ~Charclass("01"): 2},
                    },
                )
            )
        )
        == "0[01]"
    )


def test_even_star_bug1() -> None:
    # Bug fix. This is a(a{2})* (i.e. accepts an odd number of "a" chars in a
    # row), but when from_fsm() is called, the result is "a+". Turned out to be
    # a fault in the rxelems.multiplier.__mul__() routine
    elesscomplex = Fsm(
        alphabet={Charclass("a"), ~Charclass("a")},
        states={0, 1, 2},
        initial=0,
        finals={1},
        map={
            0: {Charclass("a"): 1, ~Charclass("a"): 2},
            1: {Charclass("a"): 0, ~Charclass("a"): 2},
            2: {Charclass("a"): 2, ~Charclass("a"): 2},
        },
    )
    assert not elesscomplex.accepts("")
    assert elesscomplex.accepts("a")
    assert not elesscomplex.accepts("aa")
    assert elesscomplex.accepts("aaa")
    elesscomplex_pat = from_fsm(elesscomplex)
    assert str(elesscomplex_pat) in {"a(a{2})*", "(a{2})*a"}
    elesscomplex = elesscomplex_pat.to_fsm()
    assert not elesscomplex.accepts("")
    assert elesscomplex.accepts("a")
    assert not elesscomplex.accepts("aa")
    assert elesscomplex.accepts("aaa")
    gen = elesscomplex.strings([])
    assert next(gen) == "a"
    assert next(gen) == "aaa"
    assert next(gen) == "aaaaa"
    assert next(gen) == "aaaaaaa"


def test_binary_3() -> None:
    # Binary numbers divisible by 3.
    # Disallows the empty string
    # Allows "0" on its own, but not leading zeroes.
    div3 = from_fsm(
        Fsm(
            alphabet={Charclass("0"), Charclass("1"), ~Charclass("01")},
            states={-2, -1, 0, 1, 2, 3},
            initial=-2,
            finals={-1, 0},
            map={
                -2: {Charclass("0"): -1, Charclass("1"): 1, ~Charclass("01"): 3},
                -1: {Charclass("0"): 3, Charclass("1"): 3, ~Charclass("01"): 3},
                0: {Charclass("0"): 0, Charclass("1"): 1, ~Charclass("01"): 3},
                1: {Charclass("0"): 2, Charclass("1"): 0, ~Charclass("01"): 3},
                2: {Charclass("0"): 1, Charclass("1"): 2, ~Charclass("01"): 3},
                3: {Charclass("0"): 3, Charclass("1"): 3, ~Charclass("01"): 3},
            },
        )
    )
    assert str(parse("(0|1)").reduce()) == "[01]"
    assert str(parse("(0|12)").reduce()) == "0|12"
    assert str(parse("(0|1(01*0|10*1)*10*)").reduce()) == "0|1(01*0|10*1)*10*"
    assert str(div3) == "0|1(01*0|10*1)*10*"
    gen = div3.strings()
    assert next(gen) == "0"
    assert next(gen) == "11"
    assert next(gen) == "110"
    assert next(gen) == "1001"
    assert next(gen) == "1100"


def test_base_N() -> None:
    # Machine accepts only numbers in selected base (e.g. 2, 10) that are
    # divisible by N (e.g. 3, 7).
    # "0" alone is acceptable, but leading zeroes (e.g. "00", "07") are not
    base = 2
    N = 3
    assert base <= 10
    anything_else = ~Charclass("".join(str(i) for i in range(base)))
    divN = from_fsm(
        Fsm(
            alphabet=({Charclass(str(i)) for i in range(base)} | {anything_else}),
            states=frozenset(range(N)) | {-2, -1, N},
            initial=-2,
            finals={-1, 0},
            map={
                -2: {
                    Charclass("0"): -1,
                    **{Charclass(str(j)): j % N for j in range(1, base)},
                    anything_else: N,
                },
                -1: {
                    **{Charclass(str(j)): N for j in range(base)},
                    anything_else: N,
                },
                **{
                    i: {
                        **{Charclass(str(j)): (i * base + j) % N for j in range(base)},
                        anything_else: N,
                    }
                    for i in range(N)
                },
                N: {
                    **{Charclass(str(j)): N for j in range(base)},
                    anything_else: N,
                },
            },
        )
    )
    gen = divN.strings()
    a = next(gen)
    assert a == "0"
    for i in range(7):
        b = next(gen)
        assert int(a, base) + N == int(b, base)
        a = b


def test_dead_default() -> None:
    blockquote = from_fsm(
        Fsm(
            alphabet={Charclass("/"), Charclass("*"), ~Charclass("/*")},
            states={0, 1, 2, 3, 4, 5},
            initial=0,
            finals={4},
            map={
                0: {Charclass("/"): 1, ~Charclass("/*"): 5, Charclass("*"): 5},
                1: {Charclass("/"): 5, ~Charclass("/*"): 5, Charclass("*"): 2},
                2: {Charclass("/"): 2, ~Charclass("/*"): 2, Charclass("*"): 3},
                3: {Charclass("/"): 4, ~Charclass("/*"): 2, Charclass("*"): 3},
                4: {Charclass("/"): 5, ~Charclass("/*"): 5, Charclass("*"): 5},
                5: {Charclass("/"): 5, ~Charclass("/*"): 5, Charclass("*"): 5},
            },
        )
    )
    assert str(blockquote) == "/\\*([^*]|\\*+[^*/])*\\*+/"


###############################################################################
# charclass set operations


def test_charclass_union() -> None:
    assert (parse("[ab]") | parse("[bc]")).reduce() == parse("[abc]")
    assert (parse("[ab]") | parse("[^bc]")).reduce() == parse("[^c]")
    assert (parse("[^ab]") | parse("[bc]")).reduce() == parse("[^a]")
    assert (parse("[^ab]") | parse("[^bc]")).reduce() == parse("[^b]")


def test_charclass_intersection() -> None:
    assert parse("[ab]") & parse("[bc]") == parse("b")
    assert parse("[ab]") & parse("[^bc]") == parse("a")
    assert parse("[^ab]") & parse("[bc]") == parse("c")
    assert parse("[^ab]") & parse("[^bc]") == parse("[^abc]")


###############################################################################
# Emptiness detection


def test_empty() -> None:
    assert not parse("a{0}").empty()
    assert parse("[]").empty()
    assert not parse("[]?").empty()
    assert parse("a[]").empty()
    assert not parse("a[]?").empty()
    assert not parse("a{0}").empty()
    assert not parse("[]?").empty()


###############################################################################
# Test everythingbut()


def test_everythingbut() -> None:
    # Regexes are usually gibberish but we make a few claims
    a = parse("a")
    notA = a.everythingbut().to_fsm()
    assert notA.accepts("")
    assert not notA.accepts("a")
    assert notA.accepts("aa")

    # everythingbut(), called twice, should take us back to where we started.
    beer = parse("beer")
    notBeer = beer.everythingbut()
    beer2 = notBeer.everythingbut()
    assert str(beer2) == "be{2}r"

    # ".*" becomes "[]" and vice versa under this call.
    assert str(parse(".*").everythingbut()) == "[]"
    assert str(parse("[]").everythingbut()) == ".*"


def test_isinstance_bug() -> None:
    # Problem relating to isinstance(). The class `Mult` was occurring as both
    # rxelems.Mult and as __main__.Mult and apparently these count as different
    # classes for some reason, so isinstance(m, Mult) was returning false.
    var = str(parse("").everythingbut()) + "aa" + str(parse("").everythingbut())
    assert var == ".+aa.+"

    starfree = parse(var).everythingbut()
    assert str(starfree) == "(.(a?[^a])*a{0,2})?"


###############################################################################


def test_equivalence() -> None:
    assert parse("aa*").equivalent(parse("a*a"))
    assert parse("([ab]*a|[bc]*c)?b*").equivalent(parse("b*(a[ab]*|c[bc]*)?"))


###############################################################################
# Test reversed()


def test_regex_reversal() -> None:
    assert parse("b").reversed() == parse("b")
    assert parse("e*").reversed() == parse("e*")
    assert parse("bear").reversed() == parse("raeb")
    assert parse("beer").reversed() == parse("reeb")
    assert parse("abc|def|ghi").reversed() == parse("cba|fed|ihg")
    assert parse("(abc)*d").reversed() == parse("d(cba)*")


###############################################################################
# Tests for some more set operations


def test_set_ops() -> None:
    assert parse("[abcd]") - parse("a") == parse("[bcd]")
    assert parse("[abcd]") ^ parse("[cdef]") == parse("[abef]")


###############################################################################
# Test methods for finding common parts of regular expressions.


def test_pattern_commonconc() -> None:
    # pylint: disable=protected-access
    assert str(parse("aa|aa")._commonconc()) == "aa"
    assert str(parse("abc|aa")._commonconc()) == "a"
    assert str(parse("a|bc")._commonconc()) == ""
    assert str(parse("cf{1,2}|cf")._commonconc()) == "cf"
    assert str(parse("ZA|ZB|ZC")._commonconc()) == "Z"
    assert str(parse("Z+A|ZB|ZZC")._commonconc()) == "Z"
    assert str(parse("a{2}b|a+c")._commonconc()) == "a"


def test_pattern_commonconc_suffix() -> None:
    # pylint: disable=protected-access
    assert str(parse("a|bc")._commonconc(suffix=True)) == ""
    assert str(parse("aa|bca")._commonconc(suffix=True)) == "a"
    assert str(parse("xyza|abca|a")._commonconc(suffix=True)) == "a"
    assert str(parse("f{2,3}c|fc")._commonconc(suffix=True)) == "fc"
    assert str(parse("aa")._commonconc(suffix=True)) == "aa"


###############################################################################
# Basic concatenation reduction tests


def test_reduce_concatenations() -> None:
    assert str(parse("aa").reduce()) == "a{2}"
    assert str(parse("bb").reduce()) == "b{2}"
    assert str(parse("b*b").reduce()) == "b+"
    assert str(parse("aa{2,}").reduce()) == "a{3,}"
    assert str(parse("a*a{2}").reduce()) == "a{2,}"
    assert str(parse("aa{0,8}").reduce()) == "a{1,9}"
    assert str(parse("b{0,8}b").reduce()) == "b{1,9}"
    assert str(parse("aab").reduce()) == "a{2}b"
    assert str(parse("abb").reduce()) == "ab{2}"
    assert str(parse("abb*").reduce()) == "ab+"
    assert str(parse("abbc").reduce()) == "ab{2}c"
    assert str(parse("a?ab").reduce()) == "a{1,2}b"
    assert str(parse("(ac{2}|bc+)c").reduce()) == "(ac|bc*)c{2}"
    assert str(parse("a(a{2}b|a+c)").reduce()) == "a{2}(a*c|ab)"
    assert str(parse("a{2,3}(a{2}b|a+c)").reduce()) == "a{3,4}(a*c|ab)"
    assert str(parse("(ba{2}|ca+)a{2,3}").reduce()) == "(ba|ca*)a{3,4}"
    assert str(parse("za{2,3}(a{2}b|a+c)").reduce()) == "za{3,4}(a*c|ab)"
    assert str(parse("(ba{2}|ca+)a{2,3}z").reduce()) == "(ba|ca*)a{3,4}z"
    assert str(parse("(a|bc)(a|bc)").reduce()) == "(a|bc){2}"
    assert str(parse("a+[ab]+").reduce()) == "a[ab]+"
    assert str(parse("a{3,8}[ab]+").reduce()) == "a{3}[ab]+"
    assert str(parse("[ab]+b+").reduce()) == "[ab]+b"
    assert str(parse("[ab]+a{3,8}").reduce()) == "[ab]+a{3}"
    assert str(parse("\\d+\\w+").reduce()) == "\\d\\w+"
    assert str(parse("[ab]+a?").reduce()) == "[ab]+"


###############################################################################
# Multiplication tests


def test_mult_multiplication() -> None:
    assert parse("(a{2,3}){1,1}").reduce() == parse("a{2,3}").reduce()
    assert parse("(a{2,3}){1}").reduce() == parse("a{2,3}").reduce()
    assert parse("(a{2,3})").reduce() == parse("a{2,3}").reduce()
    assert parse("(a{2,3}){4,5}").reduce() == parse("a{8,15}").reduce()
    assert parse("(a{2,}){2,}").reduce() == parse("a{4,}").reduce()


def test_even_star_bug2() -> None:
    # Defect: (a{2})* should NOT reduce to a*
    assert parse("(a{2})*").reduce() != parse("a*").reduce()


def test_two_or_more_qm_bug() -> None:
    assert str(parse("(a{2,})?").reduce()) == "(a{2,})?"


def test_two_two_bug() -> None:
    assert str(parse("(a{2}){2}").reduce()) == "a{4}"


###############################################################################
# Test intersection (&)


def test_mult_intersection() -> None:
    assert str(parse("a") & parse("a")) == "a"
    assert str(parse("a*") & parse("a")) == "a"
    assert str(parse("a") & parse("a?")) == "a"
    assert str(parse("a{2}") & parse("a{2,}")) == "a{2}"
    assert str(parse("a*") & parse("a+")) == "a+"
    assert str(parse("a{2}") & parse("a{4}")) == "[]"
    assert str(parse("a{3,}") & parse("a{3,}")) == "a{3,}"


def test_parse_regex_intersection() -> None:
    assert str(parse("a*") & parse("b*")) == ""
    assert str(parse("a") & parse("b")) == "[]"
    assert str(parse("\\d") & parse(".")) == "\\d"
    assert str(parse("\\d{2}") & parse("0.")) == "0\\d"
    assert str(parse("\\d{2}") & parse("19.*")) == "19"
    assert str(parse("\\d{3}") & parse("19.*")) == "19\\d"
    assert str(parse("abc...") & parse("...def")) == "abcdef"
    assert str(parse("[bc]*[ab]*") & parse("[ab]*[bc]*")) in {
        "([ab]*a|[bc]*c)?b*",
        "b*(a[ab]*|c[bc]*)?",
    }
    assert str(parse("\\W*")) == "\\W*"
    assert str(parse("[a-g0-8$%\\^]+")) == "[$%0-8\\^a-g]+"
    assert str(parse("[^d]{2,8}")) == "[^d]{2,8}"
    assert str(parse("\\W*") & parse("[a-g0-8$%\\^]+")) == "[$%\\^]+"
    assert str(parse("[ab]{1,2}") & parse("[^a]{1,2}")) == "b{1,2}"
    assert str(parse("[ab]?") & parse("[^a]?")) == "b?"
    assert parse("a{0,2}").matches("")
    assert parse("[ab]{0,2}").matches("")
    assert parse("[^a]{0,2}").matches("")
    assert parse("b{0,2}").matches("")
    assert str(parse("[ab]{0,2}") & parse("[^a]{0,2}")) == "b{0,2}"
    assert str(parse("[ab]{0,4}") & parse("[^a]{0,4}")) == "b{0,4}"
    assert str(parse("[abc]{0,8}") & parse("[^a]{0,8}")) == "[bc]{0,8}"
    assert (
        str(parse("[a-g0-8$%\\^]{0,8}") & parse("[^d]{0,8}")) == "[$%0-8\\^abcefg]{0,8}"
    )
    assert str(parse("[a-g0-8$%\\^]+") & parse("[^d]{0,8}")) == "[$%0-8\\^abcefg]{1,8}"
    assert str(parse("[a-g0-8$%\\^]+") & parse("[^d]{2,8}")) == "[$%0-8\\^abcefg]{2,8}"
    assert (
        str(parse("\\W*") & parse("[a-g0-8$%\\^]+") & parse("[^d]{2,8}"))
        == "[$%\\^]{2,8}"
    )
    assert (
        str(parse("\\d{4}-\\d{2}-\\d{2}") & parse("19.*")) == "19\\d{2}-\\d{2}-\\d{2}"
    )


def test_complexify() -> None:
    # Complexify!
    gen = (parse("[bc]*[ab]*") & parse("[ab]*[bc]*")).strings()
    assert next(gen) == ""
    assert next(gen) == "a"
    assert next(gen) == "b"
    assert next(gen) == "c"
    assert next(gen) == "aa"
    assert next(gen) == "ab"
    # no "ac"
    assert next(gen) == "ba"
    assert next(gen) == "bb"
    assert next(gen) == "bc"
    # no "ca"
    assert next(gen) == "cb"
    assert next(gen) == "cc"
    assert next(gen) == "aaa"


def test_silly_reduction() -> None:
    # This one is horrendous and we have to jump through some hoops to get to
    # a sensible result. Probably not a good unit test actually.
    long = (
        "(aa|bb*aa)a*|((ab|bb*ab)|(aa|bb*aa)a*b)"
        + "((ab|bb*ab)|(aa|bb*aa)a*b)*"
        + "(aa|bb*aa)a*|((ab|bb*ab)|(aa|bb*aa)a*b)"
        + "((ab|bb*ab)|(aa|bb*aa)a*b)*"
    )
    long_pat1 = parse(long)
    long_fsm = long_pat1.to_fsm().reversed()
    long_pat2 = from_fsm(long_fsm).reversed()
    assert str(long_pat2) == "[ab]*a[ab]"
    short = "[ab]*a?b*|[ab]*b?a*"
    assert str(parse(".*") & parse(short)) == "[ab]*"


###############################################################################
# reduce() tests


def test_mult_reduction_easy() -> None:
    assert str(parse("a").reduce()) == "a"
    assert str(parse("a").reduce()) == "a"
    assert str(parse("a?").reduce()) == "a?"
    assert str(parse("a{0}").reduce()) == ""
    assert str(parse("[]").reduce()) == "[]"
    assert str(parse("[]?").reduce()) == ""
    assert str(parse("[]{0}").reduce()) == ""
    assert str(parse("[]{0,5}").reduce()) == ""


def test_conc_reduction_basic() -> None:
    assert str(parse("a").reduce()) == "a"
    assert str(parse("a{3,4}").reduce()) == "a{3,4}"
    assert str(parse("ab").reduce()) == "ab"
    assert str(parse("a[]b").reduce()) == "[]"


def test_pattern_reduce_basic() -> None:
    assert str(parse("ab|cd").reduce()) == "ab|cd"
    assert str(parse("a{2}b{2}").reduce()) == "a{2}b{2}"
    assert str(parse("a{2}").reduce()) == "a{2}"
    assert str(parse("a").reduce()) == "a"
    assert str(parse("(((a)))").reduce()) == "a"


def test_empty_conc_suppression() -> None:
    assert str(parse("[]0\\d").reduce()) == "[]"


def test_nested_pattern_reduction() -> None:
    # a(d(ab|a*c)) -> ad(ab|a*c)
    assert str(parse("a(d(ab|a*c))").reduce()) == "ad(a*c|ab)"


def test_mult_factor_out_qm() -> None:
    # `Mult` contains a `Pattern` containing an empty `Conc`? Pull the empty
    # part out where it's external
    assert str(parse("a|b*|").reduce()) == "a|b*"
    assert str(parse("(a|b*|)").reduce()) == "a|b*"
    assert str(parse("(a|b*|)c").reduce()) == "(a|b*)c"
    # This happens even if `EMPTYSTRING` is the only thing left inside the
    # `Mult`
    assert str(parse("()").reduce()) == ""
    assert str(parse("([$%\\^]|){1}").reduce()) == "[$%\\^]?"


def test_remove_unnecessary_parens() -> None:
    # `Mult` contains a `Pattern` containing a single `Conc` containing a
    # single `Mult`? That can be reduced greatly
    assert str(parse("(a){2}b").reduce()) == "a{2}b"
    assert str(parse("(a?)+b").reduce()) == "a*b"
    assert str(parse("([ab])*").reduce()) == "[ab]*"
    assert str(parse("(c{1,2}){3,4}").reduce()) == "c{3,8}"


def test_obvious_reduction() -> None:
    assert str(parse("(a|b)*").reduce()) == "[ab]*"


def test_mult_squoosh() -> None:
    # sequence squooshing of mults within a `Conc`
    assert str(parse("[0-9a-fA-F][0-9a-fA-F][0-9a-fA-F]").reduce()) == "[0-9A-Fa-f]{3}"
    assert str(parse("[$%\\^]?[$%\\^]").reduce()) == "[$%\\^]{1,2}"
    assert (
        str(
            parse(
                "(|(|(|(|(|[$%\\^])[$%\\^])[$%\\^])[$%\\^])[$%\\^])[$%\\^][$%\\^]"
            ).reduce()
        )
        == "[$%\\^]{2,7}"
    )


def test_bad_reduction_bug() -> None:
    # DEFECT: "0{2}|1{2}" was erroneously reduced() to "[01]{2}"
    assert parse("0{2}|1{2}").reduce() != parse("[01]{2}")
    assert parse("0|[1-9]|ab").reduce() == parse("\\d|ab")
    assert parse("0|[1-9]|a{5,7}").reduce() == parse("\\d|a{5,7}")
    assert parse("0|(0|[1-9]|a{5,7})").reduce() == parse("0|(\\d|a{5,7})")
    # pylint: disable-next=fixme
    # TODO: should do better than this! Merge that 0


def test_common_prefix_pattern_reduction() -> None:
    assert str(parse("a{2}b|a+c").reduce()) == "a(a*c|ab)"


def test_epsilon_reduction() -> None:
    assert str(parse("|(ab)*|def").reduce()) == "(ab)*|def"
    assert str(parse("|(ab)+|def").reduce()) == "(ab)*|def"
    assert str(parse("|.+").reduce()) == ".*"
    assert str(parse("|a+|b+").reduce()) in {"a+|b*", "a*|b+"}


def test_charclass_intersection_2() -> None:
    assert (parse("[A-z]") & parse("[^g]")).reduce() == parse("[A-fh-z]").reduce()


def test_reduce_boom() -> None:
    # make sure recursion problem in reduce() has gone away
    assert str(parse("([1-9]|0)").reduce()) == "\\d"


def test_new_reduce() -> None:
    # The @reduce_after decorator has been removed from many methods since it
    # takes unnecessary time which the user may not wish to spend.
    # This alters the behaviour of several methods and also exposes a new
    # opportunity for `Conc.reduce()`
    assert str(parse("a()").reduce()) == "a"
    assert str(parse("a()()").reduce()) == "a"
    assert str(parse("a.b()()").reduce()) == "a.b"


def test_main_bug() -> None:
    assert str(parse("a*").reduce()) == "a*"
    assert str(parse("a|a*").reduce()) == "a*"
    assert str(parse("a{1,2}|a{3,4}|bc").reduce()) == "a{1,4}|bc"
    assert str(parse("a{1,2}|bc|a{3,4}").reduce()) == "a{1,4}|bc"
    assert str(parse("a{1,2}|a{3,4}|a{5,6}|bc").reduce()) == "a{1,6}|bc"
    assert str(parse("a{1,2}|a{3}|a{5,6}").reduce()) == "a{1,2}(a?|a{4})"
    assert str(parse("a{1,2}|a{3}|a{5,6}|bc").reduce()) == "a{1,3}|a{5,6}|bc"
    assert str(parse("a{1,2}|a{4}|a{5,6}").reduce()) == "a{1,2}(a{3,4})?"
    assert str(parse("a{1,2}|a{4}|a{5,6}|bc").reduce()) == "a{1,2}|a{4,6}|bc"
    assert str((parse("a") | parse("a*")).reduce()) == "a*"


def test_bug_28_b() -> None:
    # Defect in rxelems.to_fsm()
    assert not parse("(ab*)*").to_fsm().accepts("bb")


def test_derive() -> None:
    assert str(parse("a+").derive("a")) == "a*"
    assert str(parse("a+|b+").derive("a")) == "a*"
    assert str(parse("abc|ade").derive("a")) == "bc|de"
    assert str(parse("abc|ade").derive("ab")) == "c"
    assert str(parse("abc|ade").derive("c")) == "[]"


def test_bug_36_1() -> None:
    etc1 = parse(".*").to_fsm()
    etc2 = parse("s.*").to_fsm()
    assert etc1.accepts("s")
    assert etc2.accepts("s")
    assert not etc1.isdisjoint(etc2)
    assert not etc2.isdisjoint(etc1)


def test_bug_36_2() -> None:
    etc1 = parse("/etc/.*").to_fsm()
    etc2 = parse("/etc/something.*").to_fsm()
    assert etc1.accepts("/etc/something")
    assert etc2.accepts("/etc/something")
    assert not etc1.isdisjoint(etc2)
    assert not etc2.isdisjoint(etc1)


def test_isdisjoint() -> None:
    xyzzy = parse("xyz(zy)?")
    xyz = parse("xyz")
    blippy = parse("blippy")
    assert xyzzy.isdisjoint(blippy)
    assert not xyzzy.isdisjoint(xyz)


def test_bug_slow() -> None:
    # issue #43
    m = Fsm(
        alphabet={
            Charclass("R"),
            Charclass("L"),
            Charclass("U"),
            Charclass("D"),
            ~Charclass("RLUD"),
        },
        states={
            0,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
        },
        initial=0,
        finals={20},
        map={
            0: {
                Charclass("R"): 21,
                Charclass("L"): 21,
                Charclass("U"): 2,
                Charclass("D"): 1,
                ~Charclass("RLUD"): 21,
            },
            1: {
                Charclass("R"): 21,
                Charclass("L"): 3,
                Charclass("U"): 21,
                Charclass("D"): 21,
                ~Charclass("RLUD"): 21,
            },
            2: {
                Charclass("R"): 21,
                Charclass("L"): 4,
                Charclass("U"): 21,
                Charclass("D"): 21,
                ~Charclass("RLUD"): 21,
            },
            3: {
                Charclass("R"): 21,
                Charclass("L"): 21,
                Charclass("U"): 5,
                Charclass("D"): 21,
                ~Charclass("RLUD"): 21,
            },
            4: {
                Charclass("R"): 21,
                Charclass("L"): 21,
                Charclass("U"): 21,
                Charclass("D"): 6,
                ~Charclass("RLUD"): 21,
            },
            5: {
                Charclass("R"): 7,
                Charclass("L"): 21,
                Charclass("U"): 21,
                Charclass("D"): 21,
                ~Charclass("RLUD"): 21,
            },
            6: {
                Charclass("R"): 8,
                Charclass("L"): 21,
                Charclass("U"): 21,
                Charclass("D"): 21,
                ~Charclass("RLUD"): 21,
            },
            7: {
                Charclass("R"): 21,
                Charclass("L"): 21,
                Charclass("U"): 9,
                Charclass("D"): 21,
                ~Charclass("RLUD"): 21,
            },
            8: {
                Charclass("R"): 21,
                Charclass("L"): 21,
                Charclass("U"): 21,
                Charclass("D"): 10,
                ~Charclass("RLUD"): 21,
            },
            9: {
                Charclass("R"): 21,
                Charclass("L"): 11,
                Charclass("U"): 21,
                Charclass("D"): 21,
                ~Charclass("RLUD"): 21,
            },
            10: {
                Charclass("R"): 21,
                Charclass("L"): 12,
                Charclass("U"): 21,
                Charclass("D"): 21,
                ~Charclass("RLUD"): 21,
            },
            11: {
                Charclass("R"): 21,
                Charclass("L"): 13,
                Charclass("U"): 21,
                Charclass("D"): 21,
                ~Charclass("RLUD"): 21,
            },
            12: {
                Charclass("R"): 21,
                Charclass("L"): 14,
                Charclass("U"): 21,
                Charclass("D"): 21,
                ~Charclass("RLUD"): 21,
            },
            13: {
                Charclass("R"): 21,
                Charclass("L"): 21,
                Charclass("U"): 21,
                Charclass("D"): 15,
                ~Charclass("RLUD"): 21,
            },
            14: {
                Charclass("R"): 21,
                Charclass("L"): 21,
                Charclass("U"): 16,
                Charclass("D"): 21,
                ~Charclass("RLUD"): 21,
            },
            15: {
                Charclass("R"): 17,
                Charclass("L"): 21,
                Charclass("U"): 21,
                Charclass("D"): 21,
                ~Charclass("RLUD"): 21,
            },
            16: {
                Charclass("R"): 18,
                Charclass("L"): 21,
                Charclass("U"): 21,
                Charclass("D"): 21,
                ~Charclass("RLUD"): 21,
            },
            17: {
                Charclass("R"): 21,
                Charclass("L"): 21,
                Charclass("U"): 21,
                Charclass("D"): 19,
                ~Charclass("RLUD"): 21,
            },
            18: {
                Charclass("R"): 21,
                Charclass("L"): 21,
                Charclass("U"): 19,
                Charclass("D"): 21,
                ~Charclass("RLUD"): 21,
            },
            19: {
                Charclass("R"): 21,
                Charclass("L"): 20,
                Charclass("U"): 21,
                Charclass("D"): 21,
                ~Charclass("RLUD"): 21,
            },
            20: {
                Charclass("R"): 21,
                Charclass("L"): 21,
                Charclass("U"): 21,
                Charclass("D"): 21,
                ~Charclass("RLUD"): 21,
            },
            21: {
                Charclass("R"): 21,
                Charclass("L"): 21,
                Charclass("U"): 21,
                Charclass("D"): 21,
                ~Charclass("RLUD"): 21,
            },
        },
    )
    t1 = time.time()
    ll = from_fsm(m)
    t2 = time.time()
    assert (t2 - t1) < 60  # should finish in way under 1s
    assert ll == parse("(DLURULLDRD|ULDRDLLURU)L").reduce()


def test_bug_48_simpler() -> None:
    assert (
        str(
            from_fsm(
                Fsm(
                    alphabet={Charclass("d"), ~Charclass("d")},
                    states={0, 1, 2},
                    initial=0,
                    finals={1},
                    map={
                        0: {Charclass("d"): 1, ~Charclass("d"): 2},
                        1: {Charclass("d"): 2, ~Charclass("d"): 2},
                        2: {Charclass("d"): 2, ~Charclass("d"): 2},
                    },
                )
            )
        )
        == "d"
    )


def test_bug_48() -> None:
    machine = Fsm(
        alphabet={
            Charclass("_"),
            Charclass("a"),
            Charclass("d"),
            Charclass("e"),
            Charclass("g"),
            Charclass("m"),
            Charclass("n"),
            Charclass("o"),
            Charclass("p"),
            ~Charclass("_adegmnop"),
        },
        states={0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13},
        initial=0,
        finals={12},
        map={
            0: {
                Charclass("_"): 13,
                Charclass("a"): 13,
                Charclass("d"): 1,
                Charclass("e"): 13,
                Charclass("g"): 13,
                Charclass("m"): 13,
                Charclass("n"): 13,
                Charclass("o"): 13,
                Charclass("p"): 13,
                ~Charclass("_adegmnop"): 13,
            },
            1: {
                Charclass("_"): 13,
                Charclass("a"): 2,
                Charclass("d"): 13,
                Charclass("e"): 13,
                Charclass("g"): 13,
                Charclass("m"): 13,
                Charclass("n"): 13,
                Charclass("o"): 13,
                Charclass("p"): 13,
                ~Charclass("_adegmnop"): 13,
            },
            2: {
                Charclass("_"): 13,
                Charclass("a"): 13,
                Charclass("d"): 13,
                Charclass("e"): 13,
                Charclass("g"): 13,
                Charclass("m"): 3,
                Charclass("n"): 13,
                Charclass("o"): 13,
                Charclass("p"): 13,
                ~Charclass("_adegmnop"): 13,
            },
            3: {
                Charclass("_"): 13,
                Charclass("a"): 4,
                Charclass("d"): 13,
                Charclass("e"): 13,
                Charclass("g"): 13,
                Charclass("m"): 13,
                Charclass("n"): 13,
                Charclass("o"): 13,
                Charclass("p"): 13,
                ~Charclass("_adegmnop"): 13,
            },
            4: {
                Charclass("_"): 13,
                Charclass("a"): 13,
                Charclass("d"): 13,
                Charclass("e"): 13,
                Charclass("g"): 5,
                Charclass("m"): 13,
                Charclass("n"): 13,
                Charclass("o"): 13,
                Charclass("p"): 13,
                ~Charclass("_adegmnop"): 13,
            },
            5: {
                Charclass("_"): 13,
                Charclass("a"): 13,
                Charclass("d"): 13,
                Charclass("e"): 6,
                Charclass("g"): 13,
                Charclass("m"): 13,
                Charclass("n"): 13,
                Charclass("o"): 13,
                Charclass("p"): 13,
                ~Charclass("_adegmnop"): 13,
            },
            6: {
                Charclass("_"): 7,
                Charclass("a"): 13,
                Charclass("d"): 13,
                Charclass("e"): 13,
                Charclass("g"): 13,
                Charclass("m"): 13,
                Charclass("n"): 13,
                Charclass("o"): 13,
                Charclass("p"): 13,
                ~Charclass("_adegmnop"): 13,
            },
            7: {
                Charclass("_"): 13,
                Charclass("a"): 13,
                Charclass("d"): 13,
                Charclass("e"): 13,
                Charclass("g"): 13,
                Charclass("m"): 13,
                Charclass("n"): 13,
                Charclass("o"): 8,
                Charclass("p"): 13,
                ~Charclass("_adegmnop"): 13,
            },
            8: {
                Charclass("_"): 13,
                Charclass("a"): 13,
                Charclass("d"): 13,
                Charclass("e"): 13,
                Charclass("g"): 13,
                Charclass("m"): 13,
                Charclass("n"): 9,
                Charclass("o"): 13,
                Charclass("p"): 13,
                ~Charclass("_adegmnop"): 13,
            },
            9: {
                Charclass("_"): 10,
                Charclass("a"): 13,
                Charclass("d"): 13,
                Charclass("e"): 13,
                Charclass("g"): 13,
                Charclass("m"): 13,
                Charclass("n"): 13,
                Charclass("o"): 13,
                Charclass("p"): 13,
                ~Charclass("_adegmnop"): 13,
            },
            10: {
                Charclass("_"): 13,
                Charclass("a"): 13,
                Charclass("d"): 13,
                Charclass("e"): 13,
                Charclass("g"): 13,
                Charclass("m"): 11,
                Charclass("n"): 13,
                Charclass("o"): 13,
                Charclass("p"): 13,
                ~Charclass("_adegmnop"): 13,
            },
            11: {
                Charclass("_"): 13,
                Charclass("a"): 13,
                Charclass("d"): 13,
                Charclass("e"): 13,
                Charclass("g"): 13,
                Charclass("m"): 13,
                Charclass("n"): 13,
                Charclass("o"): 13,
                Charclass("p"): 12,
                ~Charclass("_adegmnop"): 13,
            },
            12: {
                Charclass("_"): 13,
                Charclass("a"): 13,
                Charclass("d"): 13,
                Charclass("e"): 13,
                Charclass("g"): 13,
                Charclass("m"): 13,
                Charclass("n"): 13,
                Charclass("o"): 13,
                Charclass("p"): 13,
                ~Charclass("_adegmnop"): 13,
            },
            13: {
                Charclass("_"): 13,
                Charclass("a"): 13,
                Charclass("d"): 13,
                Charclass("e"): 13,
                Charclass("g"): 13,
                Charclass("m"): 13,
                Charclass("n"): 13,
                Charclass("o"): 13,
                Charclass("p"): 13,
                ~Charclass("_adegmnop"): 13,
            },
        },
    )

    rex = from_fsm(machine)
    assert str(rex) == "damage_on_mp"


def test_pickle() -> None:
    f1 = parse("a{0,4}").to_fsm()
    f2 = parse("a{0,3}").to_fsm()

    assert f2 < f1

    f1_unpickled = pickle.loads(pickle.dumps(f1))
    f2_unpickled = pickle.loads(pickle.dumps(f2))

    assert f2_unpickled < f1_unpickled
