import pytest

from yargy import (
    rule,
    Parser
)
from yargy.predicates import eq
from yargy.interpretation import (
    fact,
    attribute,

    normalized,
    inflected,
    const,
    custom
)


def test_predicate_attribute():
    F = fact('F', ['a'])
    RULE = rule(
        eq('a').interpretation(F.a)
    ).interpretation(F)
    parser = Parser(RULE)
    match = parser.match('a')
    record = match.fact
    assert record == F(a='a')
    assert record.spans == [(0, 1)]
    assert record.as_json == {'a': 'a'}


def test_merge_facts():
    F = fact('F', ['a', 'b'])
    A = rule(
        eq('a').interpretation(F.a)
    ).interpretation(F)
    B = rule(
        eq('b').interpretation(F.b)
    ).interpretation(F)
    RULE = rule(
        A, B
    ).interpretation(F)
    parser = Parser(RULE)
    match = parser.match('a b')
    record = match.fact
    assert record == F(a='a', b='b')
    assert record.spans == [(0, 1), (2, 3)]
    assert record.as_json == {'a': 'a', 'b': 'b'}


def test_rule_attribute():
    F = fact('F', ['a'])
    RULE = rule(
        'a', 'A'
    ).interpretation(
        F.a
    ).interpretation(F)
    parser = Parser(RULE)
    match = parser.match('a   A')
    record = match.fact
    assert record == F(a='a A')
    assert record.spans == [(0, 5)]
    assert record.as_json == {'a': 'a A'}


def test_insted_attributes():
    F = fact('F', ['a', 'b'])
    RULE = rule(
        eq('a').interpretation(F.a)
    ).interpretation(
        F.b
    ).interpretation(F)
    parser = Parser(RULE)
    match = parser.match('a')
    record = match.fact
    assert record == F(a=None, b='a')
    assert record.spans == [(0, 1)]
    assert record.as_json == {'b': 'a'}


def test_nested_facts():
    F = fact('F', ['a'])
    G = fact('G', ['b'])
    RULE = rule(
        eq('a').interpretation(F.a)
    ).interpretation(
        F
    ).interpretation(
        G.b
    ).interpretation(
        G
    )
    parser = Parser(RULE)
    match = parser.match('a')
    record = match.fact
    assert record == G(b=F(a='a'))
    assert record.spans == [(0, 1)]
    assert record.as_json == {'b': {'a': 'a'}}


def test_rule_custom_attribute():
    F = fact('F', ['a'])
    RULE = rule(
        '1'
    ).interpretation(
        custom(int)
    ).interpretation(
        F.a
    ).interpretation(
        F
    )
    parser = Parser(RULE)
    match = parser.match('1')
    record = match.fact
    assert record == F(a=1)
    assert record.spans == [(0, 1)]
    assert record.as_json == {'a': 1}


def test_rule_attribute_custom():
    F = fact('F', ['a'])
    RULE = rule(
        '1'
    ).interpretation(
        F.a
    ).interpretation(
        custom(int)
    )
    parser = Parser(RULE)
    match = parser.match('1')
    assert match.fact == 1


def test_rule_custom():
    RULE = rule(
        '3', '.', '14'
    ).interpretation(
        custom(float)
    )
    parser = Parser(RULE)
    match = parser.match('3.14')
    assert match.fact == 3.14


def test_rule_custom_custom():
    MAPPING = {'a': 1}
    RULE = rule(
        'A'
    ).interpretation(
        custom(str.lower).custom(MAPPING.get)
    )
    parser = Parser(RULE)
    match = parser.match('A')
    assert match.fact == 1


def test_normalized():
    RULE = rule(
        'московским'
    ).interpretation(
        normalized()
    )
    parser = Parser(RULE)
    match = parser.match('московским')
    assert match.fact == 'московский'


def test_inflected():
    RULE = rule(
        'московским'
    ).interpretation(
        inflected({'nomn', 'femn'})
    )
    parser = Parser(RULE)
    match = parser.match('московским')
    assert match.fact == 'московская'


def test_const():
    RULE = rule(
        'a'
    ).interpretation(
        const(1)
    )
    parser = Parser(RULE)
    match = parser.match('a')
    assert match.fact == 1


def test_attribute():
    F = fact('F', 'a')
    RULE = rule(
        'a'
    ).interpretation(
        F.a
    )
    parser = Parser(RULE)
    match = parser.match('a')
    assert match.fact == 'a'


def test_normalized_custom():
    MONTHS = {
        'январь': 1
    }
    RULE = rule(
        'январе'
    ).interpretation(
        normalized().custom(MONTHS.get)
    )
    parser = Parser(RULE)
    match = parser.match('январе')
    assert match.fact == 1


def test_inflected_custom_attribute():
    F = fact('F', ['a'])
    MONTHS = {
        'январь': 1
    }
    RULE = rule(
        'январе'
    ).interpretation(
        F.a.inflected({'nomn', 'sing'}).custom(MONTHS.get)
    ).interpretation(
        F
    )
    parser = Parser(RULE)
    match = parser.match('январе')
    assert match.fact == F(a=1)


def test_normalized_custom_attribute():
    F = fact('F', ['a'])
    MONTHS = {
        'январь': 1
    }
    RULE = rule(
        'январе'
    ).interpretation(
        F.a.normalized().custom(MONTHS.get)
    ).interpretation(
        F
    )
    parser = Parser(RULE)
    match = parser.match('январе')
    assert match.fact == F(a=1)


def test_inflected_custom():
    MONTHS = {
        'январь': 1
    }
    RULE = rule(
        'январе'
    ).interpretation(
        inflected({'nomn', 'sing'}).custom(MONTHS.get)
    )
    parser = Parser(RULE)
    match = parser.match('январе')
    assert match.fact == 1


def test_attribute_custom():
    F = fact('F', 'a')
    RULE = rule(
        '1'
    ).interpretation(
        F.a.custom(int)
    ).interpretation(
        F
    )
    parser = Parser(RULE)
    match = parser.match('1')
    record = match.fact
    assert record == F(a=1)
    assert record.spans == [(0, 1)]
    assert record.as_json == {'a': 1}


def test_attribute_custom_custom():
    F = fact('F', 'a')
    MAPPING = {'a': 1}
    RULE = rule(
        'A'
    ).interpretation(
        F.a.custom(str.lower).custom(MAPPING.get)
    ).interpretation(
        F
    )
    parser = Parser(RULE)
    match = parser.match('A')
    record = match.fact
    assert record == F(a=1)


def test_attribute_normalized():
    F = fact('F', 'a')
    RULE = rule(
        'январе'
    ).interpretation(
        F.a.normalized()
    ).interpretation(
        F
    )
    parser = Parser(RULE)
    match = parser.match('январе')
    record = match.fact
    assert record == F(a='январь')
    assert record.spans == [(0, 6)]
    assert record.as_json == {'a': 'январь'}


def test_attribute_const():
    F = fact('F', 'a')
    RULE = rule(
        'январь'
    ).interpretation(
        F.a.const(1)
    )
    parser = Parser(RULE)
    match = parser.match('январь')
    assert match.fact == 1


def test_attribute_inflected():
    F = fact('F', 'a')
    RULE = rule(
        'январе'
    ).interpretation(
        F.a.inflected({'nomn', 'plur'})
    ).interpretation(
        F
    )
    parser = Parser(RULE)
    match = parser.match('январе')
    record = match.fact
    assert record == F(a='январи')
    assert record.spans == [(0, 6)]
    assert record.as_json == {'a': 'январи'}


def test_repeatable():
    F = fact('F', [attribute('a').repeatable()])
    RULE = rule(
        eq('a').interpretation(F.a),
        eq('b').interpretation(F.a)
    ).interpretation(
        F
    )
    parser = Parser(RULE)
    match = parser.match('a b')
    record = match.fact
    assert record == F(a=['a', 'b'])
    assert record.spans == [(0, 1), (2, 3)]
    assert record.as_json == {'a': ['a', 'b']}


def test_type_errors():
    F = fact('F', ['a'])
    RULE = rule(
        'a',
        eq('1').interpretation(
            custom(int)
        )
    ).interpretation(
        F.a
    )
    parser = Parser(RULE)
    match = parser.match('a 1')
    with pytest.raises(TypeError):
        match.fact

    F = fact('F', ['a'])
    RULE = rule(
        'a',
        eq('1').interpretation(
            custom(int)
        )
    ).interpretation(
        custom(str)
    )
    parser = Parser(RULE)
    match = parser.match('a 1')
    with pytest.raises(TypeError):
        match.fact


def test_pipeline_key():
    from yargy.pipelines import morph_pipeline

    pipeline = morph_pipeline([
        'закрытое общество',
        'завод'
    ])

    F = fact('F', ['a'])

    RULE = pipeline.interpretation(
        F.a.normalized()
    ).interpretation(
        F
    )
    parser = Parser(RULE)
    match = parser.match('закрытом обществе')
    record = match.fact
    assert record == F(a='закрытое общество')

    RULE = pipeline.interpretation(
        normalized()
    )
    parser = Parser(RULE)
    match = parser.match('заводе')
    value = match.fact
    assert value == 'завод'
