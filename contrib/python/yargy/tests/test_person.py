
from yargy import Parser, rule, and_, not_
from yargy.interpretation import fact
from yargy.predicates import gram
from yargy.relations import gnc_relation
from yargy.pipelines import morph_pipeline


def test_person():
    Name = fact(
        'Name',
        ['first', 'last'],
    )
    Person = fact(
        'Person',
        ['position', 'name']
    )

    LAST = and_(
        gram('Surn'),
        not_(gram('Abbr')),
    )
    FIRST = and_(
        gram('Name'),
        not_(gram('Abbr')),
    )

    POSITION = morph_pipeline([
        'управляющий директор',
        'вице-мэр'
    ])

    gnc = gnc_relation()
    NAME = rule(
        FIRST.interpretation(
            Name.first
        ).match(gnc),
        LAST.interpretation(
            Name.last
        ).match(gnc)
    ).interpretation(
        Name
    )

    PERSON = rule(
        POSITION.interpretation(
            Person.position
        ).match(gnc),
        NAME.interpretation(
            Person.name
        )
    ).interpretation(
        Person
    )

    parser = Parser(PERSON)

    match = parser.match('управляющий директор Иван Ульянов')
    assert match

    assert match.fact == Person(
        position='управляющий директор',
        name=Name(
            first='Иван',
            last='Ульянов'
        )
    )
