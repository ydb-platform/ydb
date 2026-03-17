import pytest

from sacrebleu.tokenizers.tokenizer_ter import TercomTokenizer


test_cases_default = [
    ("a b c d", "a b c d"),
    ("", ""),
    ("a b c d.", "a b c d."),
    ("A B C D.", "a b c d."),
]

test_cases_no_punct = [
    ("a b c d.", "a b c d"),
    ("A ; B ) C : D.", "a b c d"),
]

test_cases_norm = [
    ("a b (c) d.", "a b ( c ) d"),
    ("Jim's car.", "Jim 's car ."),
    ("4.2", "4.2"),
]

test_cases_asian = [
    ("美众院公布对特", "美 众 院 公 布 对 特"),  # Chinese
    ("りの拳銃を持", "りの 拳 銃 を 持"),  # Japanese, first two letters are Hiragana
]


@pytest.mark.parametrize("input, expected", test_cases_default)
def test_ter_tokenizer_default(input, expected):
    tokenizer = TercomTokenizer()
    assert tokenizer(input) == expected


@pytest.mark.parametrize("input, expected", test_cases_no_punct)
def test_ter_tokenizer_default(input, expected):
    tokenizer = TercomTokenizer(no_punct=True)
    assert tokenizer(input) == expected


@pytest.mark.parametrize("input, expected", test_cases_norm)
def test_ter_tokenizer_default(input, expected):
    tokenizer = TercomTokenizer(normalized=True)
    assert tokenizer(input) == expected


@pytest.mark.parametrize("input, expected", test_cases_asian)
def test_ter_tokenizer_default(input, expected):
    tokenizer = TercomTokenizer(normalized=True, asian_support=True)
    assert tokenizer(input) == expected
