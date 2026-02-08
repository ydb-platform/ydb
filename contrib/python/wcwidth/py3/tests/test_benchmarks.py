"""Performance benchmarks for wcwidth module."""
# std imports
import os

# local
import wcwidth


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


# UDHR-based benchmarks,
# Load combined text (12 world languages)
import yatest.common as yc
UDHR_FILE = os.path.join(os.path.dirname(yc.source_path(__file__)), 'udhr_combined.txt')
with open(UDHR_FILE, encoding='utf-8') as f:
    UDHR_TEXT = f.read()
UDHR_LINES = UDHR_TEXT.splitlines()[:200]


def test_wrap_udhr(benchmark):
    """Benchmark wrap() with multilingual UDHR text."""
    benchmark(wcwidth.wrap, UDHR_TEXT, 80)


def test_width_udhr(benchmark):
    """Benchmark width() with multilingual UDHR text."""
    benchmark(wcwidth.width, UDHR_TEXT)


def test_wcswidth_udhr(benchmark):
    """Benchmark wcswidth() with multilingual UDHR text."""
    benchmark(wcwidth.wcswidth, UDHR_TEXT)


def test_ljust_udhr_lines(benchmark):
    """Benchmark ljust() on UDHR lines."""
    benchmark(lambda: [wcwidth.ljust(line, 100) for line in UDHR_LINES])


def test_rjust_udhr_lines(benchmark):
    """Benchmark rjust() on UDHR lines."""
    benchmark(lambda: [wcwidth.rjust(line, 100) for line in UDHR_LINES])


def test_center_udhr_lines(benchmark):
    """Benchmark center() on UDHR lines."""
    benchmark(lambda: [wcwidth.center(line, 100) for line in UDHR_LINES])
