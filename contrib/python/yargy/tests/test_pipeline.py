
from yargy import (
    Parser,
    rule,
)
from yargy.pipelines import (
    pipeline,
    caseless_pipeline,
    morph_pipeline
)


def test_pipeline():
    RULE = rule(
        pipeline(['a b c', 'b c']),
        'd'
    )
    parser = Parser(RULE)
    assert parser.match('b c d')
    assert parser.match('a b c d')

    RULE = rule(
        pipeline(['a b']).repeatable(),
        'c'
    )
    parser = Parser(RULE)
    assert parser.match('a b a b c')

    RULE = rule(
        caseless_pipeline(['A B']),
        'c'
    )
    parser = Parser(RULE)
    assert parser.match('A b c')

    RULE = morph_pipeline([
        'текст',
        'текст песни',
        'материал',
        'информационный материал',
    ])
    parser = Parser(RULE)
    matches = list(parser.findall('текстом песни музыкальной группы'))
    assert len(matches) == 1
    match = matches[0]
    assert [_.value for _ in match.tokens] == ['текстом', 'песни']

    matches = list(parser.findall('информационного материала под названием'))
    assert len(matches) == 1
    match = matches[0]
    assert [_.value for _ in match.tokens] == ['информационного', 'материала']

    RULE = morph_pipeline(['1 B.'])
    parser = Parser(RULE)
    assert parser.match('1 b .')
