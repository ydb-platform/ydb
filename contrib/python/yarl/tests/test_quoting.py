import pytest

from yarl._quoting import NO_EXTENSIONS
from yarl._quoting_py import _Quoter as _PyQuoter
from yarl._quoting_py import _Unquoter as _PyUnquoter

if not NO_EXTENSIONS:
    from yarl._quoting_c import _Quoter as _CQuoter
    from yarl._quoting_c import _Unquoter as _CUnquoter

    @pytest.fixture(params=[_PyQuoter, _CQuoter], ids=["py_quoter", "c_quoter"])
    def quoter(request):
        return request.param

    @pytest.fixture(params=[_PyUnquoter, _CUnquoter], ids=["py_unquoter", "c_unquoter"])
    def unquoter(request):
        return request.param

else:

    @pytest.fixture(params=[_PyQuoter], ids=["py_quoter"])
    def quoter(request):
        return request.param

    @pytest.fixture(params=[_PyUnquoter], ids=["py_unquoter"])
    def unquoter(request):
        return request.param


def hexescape(char):
    """Escape char as RFC 2396 specifies"""
    hex_repr = hex(ord(char))[2:].upper()
    if len(hex_repr) == 1:
        hex_repr = "0%s" % hex_repr
    return "%" + hex_repr


def test_quote_not_allowed_non_strict(quoter):
    assert quoter()("%HH") == "%25HH"


def test_quote_unfinished_tail_percent_non_strict(quoter):
    assert quoter()("%") == "%25"


def test_quote_unfinished_tail_digit_non_strict(quoter):
    assert quoter()("%2") == "%252"


def test_quote_unfinished_tail_safe_non_strict(quoter):
    assert quoter()("%x") == "%25x"


def test_quote_unfinished_tail_unsafe_non_strict(quoter):
    assert quoter()("%#") == "%25%23"


def test_quote_unfinished_tail_non_ascii_non_strict(quoter):
    assert quoter()("%ß") == "%25%C3%9F"


def test_quote_unfinished_tail_non_ascii2_non_strict(quoter):
    assert quoter()("%€") == "%25%E2%82%AC"


def test_quote_unfinished_tail_non_ascii3_non_strict(quoter):
    assert quoter()("%🐍") == "%25%F0%9F%90%8D"


def test_quote_from_bytes(quoter):
    assert quoter()("archaeological arcana") == "archaeological%20arcana"
    assert quoter()("") == ""


def test_quote_ignore_broken_unicode(quoter):
    s = quoter()(
        "j\u001a\udcf4q\udcda/\udc97g\udcee\udccb\u000ch\udccb"
        "\u0018\udce4v\u001b\udce2\udcce\udccecom/y\udccepj\u0016"
    )

    assert s == "j%1Aq%2Fg%0Ch%18v%1Bcom%2Fypj%16"
    assert quoter()(s) == s


def test_unquote_to_bytes(unquoter):
    assert unquoter()("abc%20def") == "abc def"
    assert unquoter()("") == ""


def test_never_quote(quoter):
    # Make sure quote() does not quote letters, digits, and "_,.-~"
    do_not_quote = (
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ" "abcdefghijklmnopqrstuvwxyz" "0123456789" "_.-~"
    )
    assert quoter()(do_not_quote) == do_not_quote
    assert quoter(qs=True)(do_not_quote) == do_not_quote


def test_safe(quoter):
    # Test setting 'safe' parameter does what it should do
    quote_by_default = "<>"
    assert quoter(safe=quote_by_default)(quote_by_default) == quote_by_default

    ret = quoter(safe=quote_by_default, qs=True)(quote_by_default)
    assert ret == quote_by_default


_SHOULD_QUOTE = [chr(num) for num in range(32)]
_SHOULD_QUOTE.append(r'<>#"{}|\^[]`')
_SHOULD_QUOTE.append(chr(127))  # For 0x7F
SHOULD_QUOTE = "".join(_SHOULD_QUOTE)


@pytest.mark.parametrize("char", SHOULD_QUOTE)
def test_default_quoting(char, quoter):
    # Make sure all characters that should be quoted are by default sans
    # space (separate test for that).
    result = quoter()(char)
    assert hexescape(char) == result
    result = quoter(qs=True)(char)
    assert hexescape(char) == result


# TODO: should it encode percent?
def test_default_quoting_percent(quoter):
    result = quoter()("%25")
    assert "%25" == result
    result = quoter(qs=True)("%25")
    assert "%25" == result
    result = quoter(requote=False)("%25")
    assert "%2525" == result


def test_default_quoting_partial(quoter):
    partial_quote = "ab[]cd"
    expected = "ab%5B%5Dcd"
    result = quoter()(partial_quote)
    assert expected == result
    result = quoter(qs=True)(partial_quote)
    assert expected == result


def test_quoting_space(quoter):
    # Make sure quote() and quote_plus() handle spaces as specified in
    # their unique way
    result = quoter()(" ")
    assert result == hexescape(" ")
    result = quoter(qs=True)(" ")
    assert result == "+"

    given = "a b cd e f"
    expect = given.replace(" ", hexescape(" "))
    result = quoter()(given)
    assert expect == result
    expect = given.replace(" ", "+")
    result = quoter(qs=True)(given)
    assert expect == result


def test_quoting_plus(quoter):
    assert quoter(qs=False)("alpha+beta gamma") == "alpha+beta%20gamma"
    assert quoter(qs=True)("alpha+beta gamma") == "alpha%2Bbeta+gamma"
    assert quoter(safe="+", qs=True)("alpha+beta gamma") == "alpha+beta+gamma"


def test_quote_with_unicode(quoter):
    # Characters in Latin-1 range, encoded by default in UTF-8
    given = "\u00a2\u00d8ab\u00ff"
    expect = "%C2%A2%C3%98ab%C3%BF"
    result = quoter()(given)
    assert expect == result
    # Characters in BMP, encoded by default in UTF-8
    given = "\u6f22\u5b57"  # "Kanji"
    expect = "%E6%BC%A2%E5%AD%97"
    result = quoter()(given)
    assert expect == result


def test_quote_plus_with_unicode(quoter):
    # Characters in Latin-1 range, encoded by default in UTF-8
    given = "\u00a2\u00d8ab\u00ff"
    expect = "%C2%A2%C3%98ab%C3%BF"
    result = quoter(qs=True)(given)
    assert expect == result
    # Characters in BMP, encoded by default in UTF-8
    given = "\u6f22\u5b57"  # "Kanji"
    expect = "%E6%BC%A2%E5%AD%97"
    result = quoter(qs=True)(given)
    assert expect == result


@pytest.mark.parametrize("num", list(range(128)))
def test_unquoting(num, unquoter):
    # Make sure unquoting of all ASCII values works
    given = hexescape(chr(num))
    expect = chr(num)
    result = unquoter()(given)
    assert expect == result
    if expect not in "+=&;":
        result = unquoter(qs=True)(given)
        assert expect == result


# Expected value should be the same as given.
# See https://url.spec.whatwg.org/#percent-encoded-bytes
@pytest.mark.parametrize(
    ("input", "expected"),
    [
        ("%", "%"),
        ("%2", "%2"),
        ("%x", "%x"),
        ("%€", "%€"),
        ("%2x", "%2x"),
        ("%2 ", "%2 "),
        ("% 2", "% 2"),
        ("%xa", "%xa"),
        ("%%", "%%"),
        ("%%3f", "%?"),
        ("%2%", "%2%"),
        ("%2%3f", "%2?"),
        ("%x%3f", "%x?"),
        ("%€%3f", "%€?"),
    ],
)
def test_unquoting_bad_percent_escapes(unquoter, input, expected):
    assert unquoter()(input) == expected


@pytest.mark.xfail(
    reason="""
    FIXME: After conversion to bytes, should not cause UTF-8 decode fail.
    See https://url.spec.whatwg.org/#percent-encoded-bytes

    Refs:
    * https://github.com/aio-libs/yarl/pull/216
    * https://github.com/aio-libs/yarl/pull/214
    * https://github.com/aio-libs/yarl/pull/7
    """,
)
@pytest.mark.parametrize("urlencoded_string", ("%AB", "%AB%AB"))
def test_unquoting_invalid_utf8_sequence(unquoter, urlencoded_string):
    with pytest.raises(ValueError):
        unquoter()(urlencoded_string)


def test_unquoting_mixed_case_percent_escapes(unquoter):
    expected = "𝕦"
    assert expected == unquoter()("%F0%9D%95%A6")
    assert expected == unquoter()("%F0%9d%95%a6")
    assert expected == unquoter()("%f0%9D%95%a6")
    assert expected == unquoter()("%f0%9d%95%a6")


def test_unquoting_parts(unquoter):
    # Make sure unquoting works when have non-quoted characters
    # interspersed
    given = "ab" + hexescape("c") + "d"
    expect = "abcd"
    result = unquoter()(given)
    assert expect == result
    result = unquoter(qs=True)(given)
    assert expect == result


def test_quote_None(quoter):
    assert quoter()(None) is None


def test_unquote_None(unquoter):
    assert unquoter()(None) is None


def test_quote_empty_string(quoter):
    assert quoter()("") == ""


def test_unquote_empty_string(unquoter):
    assert unquoter()("") == ""


def test_quote_bad_types(quoter):
    with pytest.raises(TypeError):
        quoter()(123)


def test_unquote_bad_types(unquoter):
    with pytest.raises(TypeError):
        unquoter()(123)


def test_quote_lowercase(quoter):
    assert quoter()("%d1%84") == "%D1%84"


def test_quote_unquoted(quoter):
    assert quoter()("%41") == "A"


def test_quote_space(quoter):
    assert quoter()(" ") == "%20"  # NULL


# test to see if this would work to fix
# coverage on this file.
def test_quote_percent_last_character(quoter):
    # % is last character in this case.
    assert quoter()("%") == "%25"


def test_unquote_unsafe(unquoter):
    assert unquoter(unsafe="@")("%40") == "%40"


def test_unquote_unsafe2(unquoter):
    assert unquoter(unsafe="@")("%40abc") == "%40abc"


def test_unquote_unsafe3(unquoter):
    assert unquoter(qs=True)("a%2Bb=?%3D%2B%26") == "a%2Bb=?%3D%2B%26"


def test_unquote_unsafe4(unquoter):
    assert unquoter(unsafe="@")("a@b") == "a%40b"


@pytest.mark.parametrize(
    ("input", "expected"),
    [
        ("%e2%82", "%e2%82"),
        ("%e2%82ac", "%e2%82ac"),
        ("%e2%82%f8", "%e2%82%f8"),
        ("%e2%82%2b", "%e2%82+"),
        ("%e2%82%e2%82%ac", "%e2%82€"),
        ("%e2%82%e2%82", "%e2%82%e2%82"),
    ],
)
def test_unquote_non_utf8(unquoter, input, expected):
    assert unquoter()(input) == expected


def test_unquote_unsafe_non_utf8(unquoter):
    assert unquoter(unsafe="\n")("%e2%82%0a") == "%e2%82%0A"


def test_unquote_plus_non_utf8(unquoter):
    assert unquoter(qs=True)("%e2%82%2b") == "%e2%82%2B"


def test_quote_non_ascii(quoter):
    assert quoter()("%F8") == "%F8"


def test_quote_non_ascii2(quoter):
    assert quoter()("a%F8b") == "a%F8b"


def test_quote_percent_percent_encoded(quoter):
    assert quoter()("%%3f") == "%25%3F"


def test_quote_percent_digit_percent_encoded(quoter):
    assert quoter()("%2%3f") == "%252%3F"


def test_quote_percent_safe_percent_encoded(quoter):
    assert quoter()("%x%3f") == "%25x%3F"


def test_quote_percent_unsafe_percent_encoded(quoter):
    assert quoter()("%#%3f") == "%25%23%3F"


def test_quote_percent_non_ascii_percent_encoded(quoter):
    assert quoter()("%ß%3f") == "%25%C3%9F%3F"


def test_quote_percent_non_ascii2_percent_encoded(quoter):
    assert quoter()("%€%3f") == "%25%E2%82%AC%3F"


def test_quote_percent_non_ascii3_percent_encoded(quoter):
    assert quoter()("%🐍%3f") == "%25%F0%9F%90%8D%3F"


class StrLike(str):
    pass


def test_quote_str_like(quoter):
    assert quoter()(StrLike("abc")) == "abc"


def test_unquote_str_like(unquoter):
    assert unquoter()(StrLike("abc")) == "abc"


def test_quote_sub_delims(quoter):
    assert quoter()("!$&'()*+,;=") == "!$&'()*+,;="


def test_requote_sub_delims(quoter):
    assert quoter()("%21%24%26%27%28%29%2A%2B%2C%3B%3D") == "!$&'()*+,;="


def test_unquoting_plus(unquoter):
    assert unquoter(qs=False)("a+b") == "a+b"


def test_unquote_plus_to_space(unquoter):
    assert unquoter(qs=True)("a+b") == "a b"


def test_unquote_plus_to_space_unsafe(unquoter):
    assert unquoter(unsafe="+", qs=True)("a+b") == "a+b"


def test_quote_qs_with_colon(quoter):
    s = quoter(safe="=+&?/:@", qs=True)("next=http%3A//example.com/")
    assert s == "next=http://example.com/"


def test_quote_protected(quoter):
    s = quoter(protected="/")("/path%2fto/three")
    assert s == "/path%2Fto/three"


def test_quote_fastpath_safe(quoter):
    s1 = "/path/to"
    s2 = quoter(safe="/")(s1)
    assert s1 is s2


def test_quote_fastpath_pct(quoter):
    s1 = "abc%A0"
    s2 = quoter()(s1)
    assert s1 is s2


def test_quote_very_large_string(quoter):
    # more than 8 KiB
    s = "abcфух%30%0a" * 1024
    assert quoter()(s) == "abc%D1%84%D1%83%D1%850%0A" * 1024


def test_space(quoter):
    s = "% A"
    assert quoter()(s) == "%25%20A"


def test_quoter_path_with_plus(quoter):
    s = "/test/x+y%2Bz/:+%2B/"
    assert "/test/x+y%2Bz/:+%2B/" == quoter(safe="@:", protected="/+")(s)


def test_unquoter_path_with_plus(unquoter):
    s = "/test/x+y%2Bz/:+%2B/"
    assert "/test/x+y+z/:++/" == unquoter(unsafe="+")(s)
