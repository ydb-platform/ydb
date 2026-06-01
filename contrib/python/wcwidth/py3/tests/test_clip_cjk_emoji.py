"""
Tests for clip() with CJK and Emoji characters.

These ensure wide graphemes (CJK / emoji / ZWJ sequences) are clipped correctly:
- Partial columns of a wide grapheme are replaced by fillchar.
- Full grapheme included when fully inside slice.
"""

# 3rd party
import pytest

# local
from wcwidth import clip, width


@pytest.mark.parametrize("ch", [
    "中",
    "🙂",
    "👨\u200d👩\u200d👧",  # family ZWJ
    "👩\u200d👩\u200d👧"   # another ZWJ variant
])
def test_partial_and_full_wide_grapheme(ch):
    w = width(ch)
    assert w >= 1
    if w > 1:
        # partial clip of first column -> fillchar
        assert clip(ch, 0, 1) == ' '
        # full clip covering entire grapheme -> original grapheme
        assert clip(ch, 0, w) == ch
        # width of clipped full grapheme should match
        assert width(clip(ch, 0, w)) == w
    else:
        # narrow grapheme: trivial
        assert clip(ch, 0, 1) == ch


def test_mixed_cjk_emoji_sequence():
    text = 'A中🙂B'
    total_w = width(text)
    # sanity
    assert total_w >= 4
    # pick a slice that includes the middle two columns (center of string)
    # ensure clip doesn't raise and width matches requested slice
    start = 1
    end = 4
    out = clip(text, start, end)
    assert width(out) == (end - start)
