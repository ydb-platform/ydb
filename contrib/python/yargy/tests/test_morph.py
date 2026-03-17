
import pytest

from yargy.morph import (
    Grams,
    Form,

    CachedMorphAnalyzer
)


@pytest.fixture(scope='module')
def morph():
    return CachedMorphAnalyzer()


def test_morph(morph):
    forms = morph('сирота')
    assert forms == [
        Form('сирота', Grams({'ms-f', 'NOUN', 'anim', 'nomn', 'sing'}))
    ]

    grams = forms[0].grams
    assert grams.gender.bi
    assert grams.number.single
    assert not grams.case.fixed

    values = morph.normalized('стали')
    assert values == {'сталь', 'стать'}


def test_inflect(morph):
    forms = morph('Александру')
    form = forms[0]

    assert 'Name' in form.grams
    assert form.inflect() == 'александр'
    assert form.inflect({'nomn', 'plur'}) == 'александры'


def test_check_gram(morph):
    with pytest.raises(ValueError):
        morph.check_gram('verb')
