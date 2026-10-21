"""Property-based quoting tests.

Split out from ``test_quoting.py`` so the wheel-build test run can execute the
rest of the suite without installing ``hypothesis`` (its tests are excluded
there via ``-m "not hypothesis"``). ``importorskip`` skips this module when
``hypothesis`` is not installed.
"""

import re
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl, quote, quote_plus, unquote_plus

import pytest

from yarl import query_to_pairs
from yarl._quoting import NO_EXTENSIONS
from yarl._quoting_py import _Quoter as _PyQuoter
from yarl._quoting_py import _Unquoter as _PyUnquoter

if TYPE_CHECKING:
    from hypothesis import assume, example, given, note
    from hypothesis import strategies as st
else:
    pytest.importorskip("hypothesis")

    from hypothesis import assume, example, given, note
    from hypothesis import strategies as st

if not NO_EXTENSIONS:
    from yarl._quoting_c import _Quoter as _CQuoter  # type: ignore[import-not-found]
    from yarl._quoting_c import _Unquoter as _CUnquoter

    quoters = [_PyQuoter, _CQuoter]
    quoter_ids = ["PyQuoter", "CQuoter"]
    unquoters = [_PyUnquoter, _CUnquoter]
    unquoter_ids = ["PyUnquoter", "CUnquoter"]
else:
    quoters = [_PyQuoter]
    quoter_ids = ["PyQuoter"]
    unquoters = [_PyUnquoter]
    unquoter_ids = ["PyUnquoter"]


_ASCII_TEXT = st.text(alphabet=st.characters(max_codepoint=127))


@given(safe=_ASCII_TEXT, protected=_ASCII_TEXT, qs=st.booleans(), requote=st.booleans())
def test_fuzz__PyQuoter(safe: str, protected: str, qs: bool, requote: bool) -> None:  # type: ignore[misc]
    """Verify that _PyQuoter can be instantiated with any valid arguments."""
    assume(not (requote and ("%" in safe or "%" in protected)))
    assume(not (qs and (" " in safe or " " in protected)))
    _PyQuoter(safe=safe, protected=protected, qs=qs, requote=requote)


@pytest.mark.parametrize("unquoter", unquoters, ids=unquoter_ids)
@example(ignore="/", qs=False)
@example(ignore="/a", qs=False)
@example(ignore="/\u00e9", qs=True)
@given(ignore=st.text(), qs=st.booleans())
def test_fuzz_unquoter_ignore(  # type: ignore[misc]
    unquoter: type[_PyUnquoter], ignore: str, qs: bool
) -> None:
    """Verify that the unquoter rejects exactly the ignore characters that are
    not ASCII or that requoting leaves as is."""
    quoter = _PyQuoter(qs=qs)
    rejected = [ch for ch in ignore if not ch.isascii() or quoter(ch) == ch]
    if rejected:
        with pytest.raises(ValueError, match=re.escape(repr(rejected[0]))):
            unquoter(ignore=ignore, qs=qs)
    else:
        unquoter(ignore=ignore, qs=qs)


@example(text_input="0")
@given(
    text_input=st.text(
        alphabet=st.characters(max_codepoint=127, blacklist_characters="%")
    ),
)
@pytest.mark.parametrize("quoter", quoters, ids=quoter_ids)
@pytest.mark.parametrize("unquoter", unquoters, ids=unquoter_ids)
def test_quote_unquote_parameter(  # type: ignore[misc]
    quoter: type[_PyQuoter],
    unquoter: type[_PyUnquoter],
    text_input: str,
) -> None:
    quote = quoter()
    unquote = unquoter()
    text_quoted = quote(text_input)
    note(f"text_quoted={text_quoted!r}")
    text_output = unquote(text_quoted)
    assert text_input == text_output


@example(text_input="0")
@given(
    text_input=st.text(
        alphabet=st.characters(max_codepoint=127, blacklist_characters="%")
    ),
)
@pytest.mark.parametrize("quoter", quoters, ids=quoter_ids)
@pytest.mark.parametrize("unquoter", unquoters, ids=unquoter_ids)
def test_quote_unquote_parameter_requote(  # type: ignore[misc]
    quoter: type[_PyQuoter],
    unquoter: type[_PyUnquoter],
    text_input: str,
) -> None:
    quote = quoter(requote=True)
    unquote = unquoter()
    text_quoted = quote(text_input)
    note(f"text_quoted={text_quoted!r}")
    text_output = unquote(text_quoted)
    assert text_input == text_output


@example(text_input="0")
@given(
    text_input=st.text(
        alphabet=st.characters(max_codepoint=127, blacklist_characters="%")
    ),
)
@pytest.mark.parametrize("quoter", quoters, ids=quoter_ids)
@pytest.mark.parametrize("unquoter", unquoters, ids=unquoter_ids)
def test_quote_unquote_parameter_path_safe(  # type: ignore[misc]
    quoter: type[_PyQuoter],
    unquoter: type[_PyUnquoter],
    text_input: str,
) -> None:
    quote = quoter()
    unquote = unquoter(ignore="/%")
    assume("/" not in text_input)
    text_quoted = quote(text_input)
    note(f"text_quoted={text_quoted!r}")
    text_output = unquote(text_quoted)
    assert text_input == text_output


# The unquoter configurations used by yarl itself, see yarl/_quoters.py
UNQUOTER_KWARGS = [
    {},
    {"ignore": "/%"},
    {"qs": True},
    {"plus": True, "replace_invalid": True},
]

_LONG_RUN = st.integers(min_value=0, max_value=5000).map(lambda n: "a" * n)
_NO_SURROGATE_TEXT = st.text(alphabet=st.characters(codec="utf-8"))
_QUOTED = _NO_SURROGATE_TEXT.map(quote)
# Plain text, long runs, and every kind of escape the unquoter handles,
# including invalid and incomplete UTF-8 sequences.
_UNQUOTE_PIECES = st.one_of(
    st.text(),
    _LONG_RUN,
    _QUOTED,
    st.sampled_from(
        [
            "%",
            "+",
            "/",
            "=",
            "&",
            ";",
            "%2B",
            "%2b",
            "%25",
            "%2F",
            "%3D",
            "%26",
            "%e2%82",
            "%e2%82%ac",
            "%ff",
            "%C3",
            "%ed%a0%80",
            "%c0%af",  # overlong 2 byte
            "%e0%80%af",  # overlong 3 byte
            "%f0%80%80%af",  # overlong 4 byte
            "%f4%90%80%80",  # above U+10FFFF
            "%f0%9f%98",  # truncated 4 byte
            "%4",
            "%zz",
            "%%41",
        ]
    ),
)


@pytest.mark.skipif(NO_EXTENSIONS, reason="Extensions not available")
@pytest.mark.parametrize("kwargs", UNQUOTER_KWARGS)
@given(pieces=st.lists(_UNQUOTE_PIECES))
def test_c_and_py_unquoter_match(  # type: ignore[misc]
    kwargs: dict[str, Any], pieces: list[str]
) -> None:
    val = "".join(pieces)
    assert _CUnquoter(**kwargs)(val) == _PyUnquoter(**kwargs)(val)


# Characters that change how the unquoter behaves when they are in ignore,
# plus ones that do nothing special, so any configuration is covered and not
# just the ones in yarl/_quoters.py.
_CONFIG_CHARS = [
    " ",
    "+",
    "%",
    "/",
    "@",
    "=",
    "&",
    ";",
    "?",
    "#",
    "!",
    ":",
    "\t",
    "a",
    "Z",
    "0",
]
_ANY_CONFIG_PIECES = st.one_of(
    _UNQUOTE_PIECES,
    st.sampled_from(_CONFIG_CHARS),
    st.sampled_from(
        [
            "%20",
            "%40",
            "%3F",
            "%23",
            "%21",
            "%3A",
            "%3B",
            "%09",
            "%61",
            "%C3%A9",
            "%E6%97%A5",
            "%F0%9F%98%80",
        ]
    ),
)
# Letters, digits and "!" are always rejected in ignore, so leave them out;
# "+&=;" are still rejected without qs
_ANY_CONFIG_IGNORE = st.text(
    alphabet=st.sampled_from([c for c in _CONFIG_CHARS if c not in "!aZ0"]),
    max_size=4,
)
# qs together with an ignore it accepts, for tests that need a working unquoter
_ACCEPTED_QS_AND_IGNORE = st.tuples(st.booleans(), _ANY_CONFIG_IGNORE).map(
    lambda qs_and_ignore: (
        qs_and_ignore[0],
        qs_and_ignore[1]
        if qs_and_ignore[0]
        else qs_and_ignore[1].translate({ord(c): None for c in "+&=;"}),
    )
)


@pytest.mark.skipif(NO_EXTENSIONS, reason="Extensions not available")
@example(
    pieces=["a+b%20c"], qs_and_ignore=(True, " "), plus=False, replace_invalid=False
)
@example(
    pieces=["a+b%20c"], qs_and_ignore=(False, " "), plus=True, replace_invalid=True
)
@given(
    pieces=st.lists(_ANY_CONFIG_PIECES),
    qs_and_ignore=_ACCEPTED_QS_AND_IGNORE,
    plus=st.booleans(),
    replace_invalid=st.booleans(),
)
def test_c_and_py_unquoter_match_any_config(  # type: ignore[misc]
    pieces: list[str],
    qs_and_ignore: tuple[bool, str],
    plus: bool,
    replace_invalid: bool,
) -> None:
    val = "".join(pieces)
    qs, ignore = qs_and_ignore
    c_unquoter = _CUnquoter(
        ignore=ignore, qs=qs, plus=plus, replace_invalid=replace_invalid
    )
    py_unquoter = _PyUnquoter(
        ignore=ignore, qs=qs, plus=plus, replace_invalid=replace_invalid
    )
    assert c_unquoter(val) == py_unquoter(val)


@pytest.mark.parametrize("unquoter", unquoters, ids=unquoter_ids)
@given(
    pieces=st.lists(_ANY_CONFIG_PIECES),
    qs_and_ignore=_ACCEPTED_QS_AND_IGNORE,
    plus=st.booleans(),
    replace_invalid=st.booleans(),
)
def test_unquoter_output_is_never_longer(  # type: ignore[misc]
    unquoter: type[_PyUnquoter],
    pieces: list[str],
    qs_and_ignore: tuple[bool, str],
    plus: bool,
    replace_invalid: bool,
) -> None:
    # Unquoting never makes a string longer; implementations may rely on this
    # to size an output buffer to the input
    val = "".join(pieces)
    qs, ignore = qs_and_ignore
    unquote = unquoter(ignore=ignore, qs=qs, plus=plus, replace_invalid=replace_invalid)
    assert len(unquote(val)) <= len(val)


_QUOTER_CONFIG_CHARS = " %+/@:?=&;#![]~-._aZ0\t\u00e9"
_QUOTE_PIECES = st.one_of(
    st.text(),
    st.sampled_from(list(_QUOTER_CONFIG_CHARS)),
    _QUOTED,
    st.sampled_from(
        ["%41", "%4a", "%2f", "%2F", "%25", "%20", "%2B", "%zz", "%", "%e2%82"]
    ),
)


@pytest.mark.skipif(NO_EXTENSIONS, reason="Extensions not available")
@example(pieces=["%41", " %"], safe="%", protected="", qs=False, requote=True)
@example(pieces=["%41", " %"], safe="%", protected="", qs=False, requote=False)
@given(
    pieces=st.lists(_QUOTE_PIECES),
    safe=st.text(alphabet=_QUOTER_CONFIG_CHARS, max_size=4),
    protected=st.text(alphabet=_QUOTER_CONFIG_CHARS, max_size=4),
    qs=st.booleans(),
    requote=st.booleans(),
)
def test_c_and_py_quoter_match_any_config(  # type: ignore[misc]
    pieces: list[str], safe: str, protected: str, qs: bool, requote: bool
) -> None:
    val = "".join(pieces)
    try:
        py_quoter = _PyQuoter(safe=safe, protected=protected, qs=qs, requote=requote)
    except ValueError as exc:
        # Both implementations reject the same configurations
        with pytest.raises(ValueError, match=re.escape(str(exc))):
            _CQuoter(safe=safe, protected=protected, qs=qs, requote=requote)
        return
    c_quoter = _CQuoter(safe=safe, protected=protected, qs=qs, requote=requote)
    assert c_quoter(val) == py_quoter(val)


# Pieces never form an incomplete or invalid UTF-8 escape (text pieces
# exclude '%', quote() emits whole sequences, %zz is not an escape), so
# urllib.parse.unquote_plus agrees with yarl
_VALID_UNQUOTE_PIECES = st.one_of(
    st.text(alphabet=st.characters(blacklist_characters="%")),
    _LONG_RUN,
    _QUOTED,
    _NO_SURROGATE_TEXT.map(quote_plus),
    st.sampled_from(["+", "%2B", "%25", "%zz"]),
)


@pytest.mark.parametrize("unquoter", unquoters, ids=unquoter_ids)
@given(pieces=st.lists(_VALID_UNQUOTE_PIECES))
def test_unquoter_plus_matches_unquote_plus(  # type: ignore[misc]
    unquoter: type[_PyUnquoter], pieces: list[str]
) -> None:
    val = "".join(pieces)
    assert unquoter(plus=True)(val) == unquote_plus(val)


@pytest.mark.parametrize("unquoter", unquoters, ids=unquoter_ids)
@given(pieces=st.lists(_UNQUOTE_PIECES))
def test_unquoter_replace_invalid_matches_unquote_plus(  # type: ignore[misc]
    unquoter: type[_PyUnquoter], pieces: list[str]
) -> None:
    val = "".join(pieces)
    assert unquoter(plus=True, replace_invalid=True)(val) == unquote_plus(val)


_QUERY_PIECES = st.one_of(
    _UNQUOTE_PIECES,
    _NO_SURROGATE_TEXT.map(quote_plus),
    st.sampled_from(["&", "&&", "=", "%C3", "%A9", "%E9", "%ED", "%80"]),
)


@pytest.mark.parametrize("unquoter", unquoters, ids=unquoter_ids)
@given(pieces=st.lists(_QUERY_PIECES))
def test_query_to_pairs_matches_parse_qsl(  # type: ignore[misc]
    unquoter: type[_PyUnquoter], pieces: list[str]
) -> None:
    query_string = "".join(pieces)
    with pytest.MonkeyPatch.context() as monkeypatch:
        unquote = unquoter(plus=True, replace_invalid=True)
        monkeypatch.setattr("yarl._parse.UNQUOTER_PLUS", unquote)
        result = query_to_pairs(query_string)
    assert result == parse_qsl(query_string, keep_blank_values=True)


@given(
    pieces=st.lists(_QUERY_PIECES),
    encoding=st.sampled_from(["utf-8", "UTF8", "latin-1", "cp1252"]),
    max_fields=st.none() | st.integers(min_value=0, max_value=20),
)
def test_query_to_pairs_matches_parse_qsl_options(  # type: ignore[misc]
    pieces: list[str], encoding: str, max_fields: int | None
) -> None:
    query_string = "".join(pieces)
    # parse_qsl on Python 3.10 counts one field in an empty query string
    assume(query_string)
    try:
        expected = parse_qsl(
            query_string,
            keep_blank_values=True,
            encoding=encoding,
            max_num_fields=max_fields,
        )
    except ValueError:
        with pytest.raises(ValueError, match="Max number of fields exceeded"):
            query_to_pairs(query_string, max_fields=max_fields, encoding=encoding)
    else:
        assert (
            query_to_pairs(query_string, max_fields=max_fields, encoding=encoding)
            == expected
        )
