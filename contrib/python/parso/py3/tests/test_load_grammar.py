import pytest
from parso.grammar import load_grammar
from parso import utils


def test_load_inexisting_grammar():
    # We support future grammars assuming future compatibility,
    # but we don't know how to parse old grammars.
    with pytest.raises(NotImplementedError):
        load_grammar(version='1.5')


def test_load_grammar_uses_older_syntax():
    load_grammar(version='4.0')


def test_load_grammar_doesnt_warn(each_version):
    load_grammar(version=each_version)


@pytest.mark.parametrize(('string', 'result'), [
    ('2', (2, 7)), ('3', (3, 6)), ('1.1', (1, 1)), ('1.1.1', (1, 1)), ('300.1.31', (300, 1))
])
def test_parse_version(string, result):
    assert utils._parse_version(string) == result


@pytest.mark.parametrize('string', ['1.', 'a', '#', '1.3.4.5'])
def test_invalid_grammar_version(string):
    with pytest.raises(ValueError):
        load_grammar(version=string)


def test_grammar_int_version():
    with pytest.raises(TypeError):
        load_grammar(version=3.8)
