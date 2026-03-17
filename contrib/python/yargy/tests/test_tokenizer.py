
import pytest

from yargy.morph import (
    Form,
    Grams
)
from yargy.span import Span
from yargy.token import (
    Token,
    MorphToken,

    join_tokens
)
from yargy.tokenizer import (
    RUSSIAN,
    LATIN,
    INT,
    PUNCT,
    EOL,
    OTHER,

    EMAIL_RULE,

    Tokenizer,
    MorphTokenizer,
)


def test_types():
    tokenizer = Tokenizer()
    tokens = list(tokenizer('Ростов-на-Дону'))
    assert tokens == [
        Token('Ростов', Span(0, 6), RUSSIAN),
        Token('-', Span(6, 7), PUNCT),
        Token('на', Span(7, 9), RUSSIAN),
        Token('-', Span(9, 10), PUNCT),
        Token('Дону', Span(10, 14), RUSSIAN)
    ]

    tokens = list(tokenizer('vk.com'))
    assert tokens == [
        Token('vk', Span(0, 2), LATIN),
        Token('.', Span(2, 3), PUNCT),
        Token('com', Span(3, 6), LATIN)
    ]

    tokens = list(tokenizer('1 500 000$'))
    assert tokens == [
        Token('1', Span(0, 1), INT),
        Token('500', Span(2, 5), INT),
        Token('000', Span(6, 9), INT),
        Token('$', Span(9, 10), PUNCT)
    ]

    tokens = list(tokenizer('π'))
    assert tokens == [Token('π', Span(0, 1), OTHER)]


def test_check_type():
    tokenizer = Tokenizer()
    with pytest.raises(ValueError):
        tokenizer.check_type('UNK')

    tokenizer.remove_types(EOL)
    with pytest.raises(ValueError):
        tokenizer.check_type(EOL)


def test_change_rules():
    tokenizer = Tokenizer().add_rules(EMAIL_RULE)
    values = tokenizer.split('mailto:me@host.ru')
    assert values == ['mailto', ':', 'me@host.ru']

    tokenizer = Tokenizer().remove_types(EOL)
    text = """
hi,

the
"""
    values = tokenizer.split(text)
    assert values == ['hi', ',', 'the']


def test_morph():
    tokenizer = MorphTokenizer()
    tokens = list(tokenizer('dvd-диски'))
    assert tokens == [
        Token('dvd', Span(0, 3), LATIN),
        Token('-', Span(3, 4), PUNCT),
        MorphToken('диски', Span(4, 9), RUSSIAN, forms=[
            Form('диск', Grams({'NOUN', 'accs', 'inan', 'masc', 'plur'})),
            Form('диск', Grams({'NOUN', 'inan', 'masc', 'nomn', 'plur'})),
        ])
    ]


def test_join_tokens():
    tokenizer = Tokenizer()
    tokens = tokenizer('pi =        3.14')
    assert join_tokens(tokens) == 'pi = 3.14'
