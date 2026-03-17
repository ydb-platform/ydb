
import pytest

from yargy import (
    rule,
    or_,
    forward
)


def assert_bnf(R, *bnf):
    assert list(R.normalized.as_bnf.source) == list(bnf)


def test_repeatable_optional():
    A = rule('a')
    assert_bnf(
        A.optional().repeatable(),
        "R0 -> e | 'a' R0 | 'a'",
    )
    assert_bnf(
        A.repeatable().optional(),
        "R0 -> e | 'a' R0 | 'a'",
    )
    assert_bnf(
        A.repeatable().optional().repeatable(),
        "R0 -> e | 'a' R0 | 'a'",
    )
    assert_bnf(
        A.repeatable().repeatable(),
        "R0 -> 'a' R0 | 'a'"
    )
    assert_bnf(
        A.optional().optional(),
        "R0 -> e | 'a'",
    )
    assert_bnf(
        A.repeatable(max=2).repeatable(),
        "R0 -> 'a' R0 | 'a'"
    )
    assert_bnf(
        A.repeatable().repeatable(min=1, max=2),
        "R0 -> 'a' R0 | 'a'"
    )
    assert_bnf(
        A.optional().repeatable(max=2),
        'R0 -> e | R1',
        "R1 -> 'a' 'a' | 'a'"
    )
    assert_bnf(
        A.repeatable(reverse=True).optional(),
        "R0 -> e | 'a' | 'a' R0"
    )
    assert_bnf(
        A.repeatable().repeatable(reverse=True),
        "R0 -> 'a' | 'a' R0"
    )
    assert_bnf(
        A.repeatable(reverse=True).repeatable(min=1, max=2),
        "R0 -> 'a' | 'a' R0"
    )
    assert_bnf(
        A.repeatable().repeatable(min=2, reverse=True),
        "R0 -> 'a' R0 | 'a'"
    )
    assert_bnf(
        A.repeatable(max=2, reverse=True),
        "R0 -> 'a' | 'a' 'a'"
    )


def test_or():
    assert_bnf(
        or_(rule('a'), rule('b')).named('A'),
        "A -> 'a' | 'b'"
    )


def test_flatten():
    assert_bnf(
        rule(rule('a')),
        "R0 -> 'a'"
    )


def test_activate():
    from yargy.pipelines import pipeline
    from yargy.predicates import gram
    from yargy.tokenizer import MorphTokenizer
    from yargy.parser import Context

    tokenizer = MorphTokenizer()
    context = Context(tokenizer)

    A = pipeline(['a']).named('A')
    B = A.activate(context)
    assert_bnf(
        B,
        'A -> pipeline'
    )

    A = rule(gram('NOUN')).named('A')
    B = A.activate(context)
    assert_bnf(
        B,
        "A -> gram('NOUN')"
    )


def test_bnf():
    from yargy.interpretation import fact
    from yargy.relations import gnc_relation

    F = fact('F', ['a'])
    gnc = gnc_relation()

    assert_bnf(
        rule('a').named('A').interpretation(F),
        "F -> 'a'"
    )
    assert_bnf(
        rule('a').interpretation(F.a).interpretation(F),
        'F -> F.a',
        "F.a -> 'a'"
    )
    assert_bnf(
        rule('a').match(gnc).interpretation(F.a),
        "F.a^gnc -> 'a'"
    )
    assert_bnf(
        rule('a').interpretation(F.a).repeatable(),
        'R0 -> F.a R0 | F.a',
        "F.a -> 'a'"
    )
    assert_bnf(
        rule('a').repeatable().interpretation(F.a),
        'F.a -> R1',
        "R1 -> 'a' R1 | 'a'"
    )

    A = rule('a')
    B = A.named('B')
    C = A.named('C')
    D = rule(B, C).named('D')
    assert_bnf(
        D,
        'D -> B C',
        'B -> R0',
        'C -> R0',
        "R0 -> 'a'"
    )


def test_loop():
    A = forward()
    B = A.named('A')
    A.define(B)

    assert_bnf(
        A,
        'A -> A'
    )


def test_bounded():
    A = rule('a')
    with pytest.raises(ValueError):
        A.repeatable(min=-1)
    with pytest.raises(ValueError):
        A.repeatable(min=-1)
    with pytest.raises(ValueError):
        A.repeatable(min=2, max=1)

    assert_bnf(
        A.repeatable(max=3),
        "R0 -> 'a' R1 | 'a'",
        "R1 -> 'a' 'a' | 'a'"
    )
    assert_bnf(
        A.repeatable(min=2),
        "R0 -> 'a' R1",
        "R1 -> 'a' R1 | 'a'"
    )
    assert_bnf(
        A.repeatable(min=2, max=3),
        "R0 -> 'a' R1",
        "R1 -> 'a' 'a' | 'a'"
    )
