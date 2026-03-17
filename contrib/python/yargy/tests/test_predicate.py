
import pytest

from yargy import and_, or_, not_
from yargy.parser import Context
from yargy.tokenizer import MorphTokenizer
from yargy.predicates import (
    normalized,
    gram,
    custom
)


def test_predicate():
    tokenizer = MorphTokenizer()
    predicate = or_(
        normalized('московским'),
        and_(
            gram('NOUN'),
            not_(gram('femn'))
        )
    )
    context = Context(tokenizer)
    predicate = predicate.activate(context)

    tokens = tokenizer('московский зоопарк')
    values = [predicate(_) for _ in tokens]
    assert values == [True, True]

    tokens = tokenizer('московская погода')
    values = [predicate(_) for _ in tokens]
    assert values == [True, False]


def test_checks():
    tokenizer = MorphTokenizer()
    context = Context(tokenizer)
    with pytest.raises(ValueError):
        gram('UNK').activate(context)

    with pytest.raises(ValueError):
        custom(lambda _: True, types='UNK').activate(context)
