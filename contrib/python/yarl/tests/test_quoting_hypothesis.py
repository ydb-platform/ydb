"""Property-based quoting tests.

Split out from ``test_quoting.py`` so the wheel-build test run can execute the
rest of the suite without installing ``hypothesis`` (its tests are excluded
there via ``-m "not hypothesis"``). ``importorskip`` skips this module when
``hypothesis`` is not installed.
"""

from typing import TYPE_CHECKING

import pytest

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


@given(safe=st.text(), protected=st.text(), qs=st.booleans(), requote=st.booleans())
def test_fuzz__PyQuoter(safe: str, protected: str, qs: bool, requote: bool) -> None:  # type: ignore[misc]
    """Verify that _PyQuoter can be instantiated with any valid arguments."""
    _PyQuoter(safe=safe, protected=protected, qs=qs, requote=requote)


@given(ignore=st.text(), unsafe=st.text(), qs=st.booleans())
def test_fuzz__PyUnquoter(ignore: str, unsafe: str, qs: bool) -> None:  # type: ignore[misc]
    """Verify that _PyUnquoter can be instantiated with any valid arguments."""
    _PyUnquoter(ignore=ignore, unsafe=unsafe, qs=qs)


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
    unquote = unquoter(ignore="/%", unsafe="+")
    assume("+" not in text_input and "/" not in text_input)
    text_quoted = quote(text_input)
    note(f"text_quoted={text_quoted!r}")
    text_output = unquote(text_quoted)
    assert text_input == text_output
