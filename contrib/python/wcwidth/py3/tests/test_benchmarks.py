"""Performance benchmarks for wcwidth module."""
# std imports
import os
import sys
import unicodedata

# 3rd party
import pytest

# local
import wcwidth

_wcwidth_module = sys.modules['wcwidth.wcwidth']


def test_wcwidth_ascii(benchmark):
    """Benchmark wcwidth() with ASCII characters."""
    benchmark(wcwidth.wcwidth, 'A')


def test_wcwidth_wide(benchmark):
    """Benchmark wcwidth() with wide characters."""
    benchmark(wcwidth.wcwidth, '中')


def test_wcwidth_emoji(benchmark):
    """Benchmark wcwidth() with emoji."""
    benchmark(wcwidth.wcwidth, '😀')


def test_wcwidth_combining(benchmark):
    """Benchmark wcwidth() with combining characters."""
    benchmark(wcwidth.wcwidth, '\u0301')


def test_wcswidth_short_ascii(benchmark):
    """Benchmark wcswidth() with short ASCII string."""
    benchmark(wcwidth.wcswidth, 'hello')


def test_wcswidth_short_mixed(benchmark):
    """Benchmark wcswidth() with short mixed-width string."""
    benchmark(wcwidth.wcswidth, 'hello世界')


def test_wcswidth_long_ascii(benchmark):
    """Benchmark wcswidth() with long ASCII string."""
    text = 'The quick brown fox jumps over the lazy dog. ' * 10
    benchmark(wcwidth.wcswidth, text)


def test_wcswidth_long_japanese(benchmark):
    """Benchmark wcswidth() with long Japanese text."""
    text = 'コンニチハ、セカイ！' * 20
    benchmark(wcwidth.wcswidth, text)


def test_wcswidth_emoji_sequence(benchmark):
    """Benchmark wcswidth() with emoji sequences."""
    text = '👨\u200d👩\u200d👧\u200d👦' * 10
    benchmark(wcwidth.wcswidth, text)


# Regional Indicator benchmarks - paired flags and unpaired RI
RI_FLAGS_PAIRED = '🇺🇸🇬🇧🇫🇷🇩🇪🇯🇵' * 100
RI_FLAGS_UNPAIRED = '🇺🇸🇬🇧🇫' * 100


def test_wcswidth_ri_flags_paired(benchmark):
    """Benchmark wcswidth() with paired regional indicator flags."""
    benchmark(wcwidth.wcswidth, RI_FLAGS_PAIRED)


def test_wcswidth_ri_flags_unpaired(benchmark):
    """Benchmark wcswidth() with mixed paired and unpaired regional indicators."""
    benchmark(wcwidth.wcswidth, RI_FLAGS_UNPAIRED)


def test_width_ri_flags_paired(benchmark):
    """Benchmark width() with paired regional indicator flags."""
    benchmark(wcwidth.width, RI_FLAGS_PAIRED)


def test_width_ri_flags_unpaired(benchmark):
    """Benchmark width() with mixed paired and unpaired regional indicators."""
    benchmark(wcwidth.width, RI_FLAGS_UNPAIRED)


# NFC vs NFD comparison - text with combining marks
DIACRITICS_COMPOSED = 'café résumé naïve ' * 100
DIACRITICS_DECOMPOSED = unicodedata.normalize('NFD', DIACRITICS_COMPOSED)


def test_wcswidth_composed(benchmark):
    """Benchmark wcswidth() with NFC-composed text."""
    benchmark(wcwidth.wcswidth, DIACRITICS_COMPOSED)


def test_wcswidth_decomposed(benchmark):
    """Benchmark wcswidth() with NFD-decomposed text."""
    benchmark(wcwidth.wcswidth, DIACRITICS_DECOMPOSED)


def test_width_composed(benchmark):
    """Benchmark width() with NFC-composed text."""
    benchmark(wcwidth.width, DIACRITICS_COMPOSED)


def test_width_decomposed(benchmark):
    """Benchmark width() with NFD-decomposed text."""
    benchmark(wcwidth.width, DIACRITICS_DECOMPOSED)


def test_width_ascii(benchmark):
    """Benchmark width() with ASCII string."""
    benchmark(wcwidth.width, 'hello world')


def test_width_with_ansi_codes(benchmark):
    """Benchmark width() with ANSI escape sequences."""
    text = '\x1b[31mred text\x1b[0m'
    benchmark(wcwidth.width, text)


def test_width_complex_ansi(benchmark):
    """Benchmark width() with complex ANSI codes."""
    text = '\x1b[38;2;255;150;100mWARN\x1b[0m: Something happened'
    benchmark(wcwidth.width, text)


def test_iter_graphemes_ascii(benchmark):
    """Benchmark iter_graphemes() with ASCII string."""
    benchmark(lambda: list(wcwidth.iter_graphemes('hello world')))


def test_iter_graphemes_emoji(benchmark):
    """Benchmark iter_graphemes() with emoji."""
    text = 'ok🇿🇼café🏴󠁧󠁢󠁷󠁬󠁳󠁿'
    benchmark(lambda: list(wcwidth.iter_graphemes(text)))


def test_iter_graphemes_combining(benchmark):
    """Benchmark iter_graphemes() with combining characters."""
    text = 'café\u0301' * 10
    benchmark(lambda: list(wcwidth.iter_graphemes(text)))


def test_grapheme_boundary_before_short(benchmark):
    """Benchmark grapheme_boundary_before() near start of short string."""
    text = 'Hello 👋🏻!'
    benchmark(wcwidth.grapheme_boundary_before, text, 8)


def test_grapheme_boundary_before_long_end(benchmark):
    """Benchmark grapheme_boundary_before() near end of long line."""
    text = 'x' * 95 + '👨\u200d👩\u200d👧!'
    benchmark(wcwidth.grapheme_boundary_before, text, 100)


def test_grapheme_boundary_before_long_mid(benchmark):
    """Benchmark grapheme_boundary_before() in middle of long line."""
    text = 'x' * 50 + '👨\u200d👩\u200d👧' + 'y' * 50
    benchmark(wcwidth.grapheme_boundary_before, text, 55)


def test_iter_graphemes_reverse_short(benchmark):
    """Benchmark iter_graphemes_reverse() with short string."""
    text = 'café\u0301 🇫🇷!'
    benchmark(lambda: list(wcwidth.iter_graphemes_reverse(text)))


def test_iter_graphemes_reverse_long(benchmark):
    """Benchmark iter_graphemes_reverse() with long string."""
    text = 'The quick brown 🦊 jumps over the lazy 🐕. ' * 5
    benchmark(lambda: list(wcwidth.iter_graphemes_reverse(text)))


def test_ljust_ascii(benchmark):
    """Benchmark ljust() with ASCII string."""
    benchmark(wcwidth.ljust, 'hello', 20)


def test_ljust_japanese(benchmark):
    """Benchmark ljust() with Japanese characters."""
    benchmark(wcwidth.ljust, 'コンニチハ', 20)


def test_rjust_ascii(benchmark):
    """Benchmark rjust() with ASCII string."""
    benchmark(wcwidth.rjust, 'hello', 20)


def test_rjust_japanese(benchmark):
    """Benchmark rjust() with Japanese characters."""
    benchmark(wcwidth.rjust, 'コンニチハ', 20)


def test_center_ascii(benchmark):
    """Benchmark center() with ASCII string."""
    benchmark(wcwidth.center, 'hello', 20)


def test_center_mixed(benchmark):
    """Benchmark center() with mixed-width string."""
    benchmark(wcwidth.center, 'café\u0301', 20)


def test_wrap_short_ascii(benchmark):
    """Benchmark wrap() with short ASCII text."""
    text = 'The quick brown fox jumps over the lazy dog'
    benchmark(wcwidth.wrap, text, 20)


def test_wrap_long_text(benchmark):
    """Benchmark wrap() with long text."""
    text = 'The quick brown fox jumps over the lazy dog. ' * 20
    benchmark(wcwidth.wrap, text, 40)


def test_wrap_japanese(benchmark):
    """Benchmark wrap() with Japanese text."""
    text = 'コンニチハ、セカイ！これは日本語のテキストです。' * 5
    benchmark(wcwidth.wrap, text, 20)


def test_wrap_with_ansi(benchmark):
    """Benchmark wrap() with ANSI escape sequences."""
    text = '\x1b[31mThe quick brown fox\x1b[0m jumps over the lazy dog'
    benchmark(wcwidth.wrap, text, 20)


def test_wrap_with_ansi_no_propagate(benchmark):
    """Benchmark wrap() with ANSI but SGR propagation disabled."""
    text = '\x1b[31mThe quick brown fox\x1b[0m jumps over the lazy dog'
    benchmark(wcwidth.wrap, text, 20, propagate_sgr=False)


def test_wrap_complex_sgr(benchmark):
    """Benchmark wrap() with complex SGR (256-color, multiple attributes)."""
    text = '\x1b[1;3;38;5;208mBold italic orange text that wraps\x1b[0m'
    benchmark(wcwidth.wrap, text, 10)


def test_wrap_hyperlink_no_id(benchmark):
    """Benchmark wrap() with OSC 8 hyperlinks without id (requires id generation)."""
    link = '\x1b]8;;https://example.com/path\x1b\\click here for details\x1b]8;;\x1b\\'
    text = f'See {link} and also {link} for more. Read {link} now. ' * 10
    benchmark(wcwidth.wrap, text, 40)


def test_wrap_hyperlink_with_id(benchmark):
    """Benchmark wrap() with OSC 8 hyperlinks with existing ids."""
    link1 = '\x1b]8;id=a;https://example.com\x1b\\click here for details\x1b]8;;\x1b\\'
    link2 = '\x1b]8;id=b;https://other.org\x1b\\visit this page now\x1b]8;;\x1b\\'
    text = f'See {link1} and also {link2} for more. Read {link1} now. ' * 10
    benchmark(wcwidth.wrap, text, 40)


def test_wrap_hyperlink_mixed(benchmark):
    """Benchmark wrap() with mixed OSC 8 hyperlinks (with and without ids)."""
    no_id = '\x1b]8;;https://example.com\x1b\\link without id here\x1b]8;;\x1b\\'
    with_id = '\x1b]8;id=x;https://other.org\x1b\\link with id here\x1b]8;;\x1b\\'
    text = f'First {no_id} then {with_id} and {no_id} again {with_id} end. ' * 10
    benchmark(wcwidth.wrap, text, 40)


def test_clip_ascii(benchmark):
    """Benchmark clip() with ASCII string."""
    benchmark(wcwidth.clip, 'hello world', 0, 5)


def test_clip_japanese(benchmark):
    """Benchmark clip() with Japanese characters."""
    benchmark(wcwidth.clip, '中文字符串', 0, 5)


def test_clip_with_ansi(benchmark):
    """Benchmark clip() with ANSI sequences."""
    text = '\x1b[31m中文字\x1b[0m'
    benchmark(wcwidth.clip, text, 0, 3)


def test_clip_with_ansi_no_propagate(benchmark):
    """Benchmark clip() with ANSI but SGR propagation disabled."""
    text = '\x1b[31m中文字\x1b[0m'
    benchmark(wcwidth.clip, text, 0, 3, propagate_sgr=False)


def test_clip_complex_sgr(benchmark):
    """Benchmark clip() with complex SGR clipping from middle."""
    text = '\x1b[1;38;5;208mHello world text\x1b[0m'
    benchmark(wcwidth.clip, text, 6, 11)


def test_propagate_sgr_multiline(benchmark):
    """Benchmark propagate_sgr() with multiple lines."""
    lines = ['\x1b[1;31mline one', 'line two', 'line three\x1b[0m']
    benchmark(wcwidth.propagate_sgr, lines)


def test_propagate_sgr_no_sequences(benchmark):
    """Benchmark propagate_sgr() fast path (no sequences)."""
    lines = ['line one', 'line two', 'line three']
    benchmark(wcwidth.propagate_sgr, lines)


def test_strip_sequences_simple(benchmark):
    """Benchmark strip_sequences() with simple ANSI codes."""
    text = '\x1b[31mred\x1b[0m'
    benchmark(wcwidth.strip_sequences, text)


def test_strip_sequences_complex(benchmark):
    """Benchmark strip_sequences() with complex ANSI codes."""
    text = '\x1b[38;2;255;150;100mWARN\x1b[0m: \x1b[1mBold\x1b[0m \x1b[4mUnderline\x1b[0m'
    benchmark(wcwidth.strip_sequences, text)


def test_iter_sequences_plain(benchmark):
    """Benchmark iter_sequences() with plain text."""
    benchmark(lambda: list(wcwidth.iter_sequences('hello world')))


def test_iter_sequences_mixed(benchmark):
    """Benchmark iter_sequences() with mixed content."""
    text = '\x1b[31mred\x1b[0m normal \x1b[32mgreen\x1b[0m'
    benchmark(lambda: list(wcwidth.iter_sequences(text)))


# Brahmic script benchmarks — text with virama conjuncts
BRAHMIC_DEVANAGARI = 'हिन्दी भाषा में लिखा गया पाठ है। क्षत्रिय स्त्री ' * 20
BRAHMIC_BENGALI = 'বাংলা ভাষায় লেখা একটি পাঠ। বাঙ্গালী ভাষা ' * 20


def test_wcswidth_brahmic_devanagari(benchmark):
    """Benchmark wcswidth() with Devanagari text containing conjuncts."""
    benchmark(wcwidth.wcswidth, BRAHMIC_DEVANAGARI)


def test_wcswidth_brahmic_bengali(benchmark):
    """Benchmark wcswidth() with Bengali text containing conjuncts."""
    benchmark(wcwidth.wcswidth, BRAHMIC_BENGALI)


def test_width_brahmic_devanagari(benchmark):
    """Benchmark width() with Devanagari text containing conjuncts."""
    benchmark(wcwidth.width, BRAHMIC_DEVANAGARI)


def test_width_brahmic_bengali(benchmark):
    """Benchmark width() with Bengali text containing conjuncts."""
    benchmark(wcwidth.width, BRAHMIC_BENGALI)


# UDHR-based benchmarks,
# Load combined text (500+ world languages)
import yatest.common as yc
UDHR_FILE = os.path.join(os.path.dirname(yc.source_path(__file__)), 'udhr_combined.txt')
UDHR_TEXT = ''
UDHR_LINES = []
UDHR_WIDTHS = []
UDHR_FILLCHAR = '█'
UDHR_SAMPLE_EVERY = 20
if os.path.exists(UDHR_FILE):
    with open(UDHR_FILE, encoding='utf-8') as f:
        UDHR_TEXT = f.read()
    _all_lines = [line.rstrip() for line in UDHR_TEXT.splitlines() if line.strip()]
    UDHR_LINES = [l for i, l in enumerate(_all_lines) if i % UDHR_SAMPLE_EVERY == 0]
    UDHR_TEXT = '\n'.join(UDHR_LINES)
    UDHR_WIDTHS = [wcwidth.width(line) for line in UDHR_LINES]

_udhr_skip = pytest.mark.skipif(
    not os.path.exists(UDHR_FILE),
    reason=f"{os.path.basename(UDHR_FILE)} is missing; run bin/update-tables.py",
)


@_udhr_skip
def test_wrap_udhr(benchmark):
    """Benchmark wrap() with multilingual UDHR text."""
    result = benchmark.pedantic(wcwidth.wrap, args=(UDHR_TEXT, 80), rounds=1, iterations=1)
    assert len(result)
    assert all(0 <= wcwidth.width(_l) <= 80 for _l in result)


@_udhr_skip
def test_width_udhr(benchmark):
    """Benchmark width() with multilingual UDHR text."""
    result = benchmark.pedantic(wcwidth.width, args=(UDHR_TEXT,), rounds=1, iterations=1)
    assert result > 0


@_udhr_skip
def test_width_udhr_lines(benchmark):
    """Benchmark width() on individual UDHR lines."""
    result = benchmark.pedantic(lambda: sum(wcwidth.width(line) for line in UDHR_LINES),
                                rounds=1, iterations=1)
    assert result > 0


@_udhr_skip
def test_width_wcswidth_consistency_udhr(benchmark):
    """Verify width() and wcswidth() agree for printable multilingual text."""
    def check():
        failures = []
        for line in UDHR_LINES:
            if not line or not line.isprintable():
                continue
            w = wcwidth.width(line)
            wcs = wcwidth.wcswidth(line)
            if w != wcs:
                failures.append((line[:60], w, wcs))
        return failures
    failures = benchmark.pedantic(check, rounds=1, iterations=1)
    assert not failures


@_udhr_skip
def test_width_fastpath_integrity_udhr(benchmark):
    """Verify width() produces identical results with and without the fast path."""
    saved = _wcwidth_module._WIDTH_FAST_PATH_MIN_LEN

    def check():
        _wcwidth_module._WIDTH_FAST_PATH_MIN_LEN = 0
        fast_total = sum(wcwidth.width(line) for line in UDHR_LINES)
        _wcwidth_module._WIDTH_FAST_PATH_MIN_LEN = 999_999
        parse_total = sum(wcwidth.width(line) for line in UDHR_LINES)
        return fast_total, parse_total

    fast_total, parse_total = benchmark.pedantic(check, rounds=1, iterations=1)
    _wcwidth_module._WIDTH_FAST_PATH_MIN_LEN = saved
    assert fast_total == parse_total


@_udhr_skip
def test_ljust_udhr_lines(benchmark):
    """Benchmark ljust() on UDHR lines."""
    benchmark.pedantic(lambda: [wcwidth.ljust(line, w + 1, UDHR_FILLCHAR)
                                for line, w in zip(UDHR_LINES, UDHR_WIDTHS)],
                       rounds=1, iterations=1)
