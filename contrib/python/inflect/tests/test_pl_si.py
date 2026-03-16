import pytest

import inflect


@pytest.fixture(params=[False, True], ids=['classical off', 'classical on'])
def classical(request):
    return request.param


@pytest.mark.parametrize("word", ['Times', 'Jones'])
def test_pl_si(classical, word):
    p = inflect.engine()
    p.classical(all=classical)
    assert p.singular_noun(p.plural_noun(word, 2), 1) == word
