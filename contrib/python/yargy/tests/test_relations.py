
from yargy import (
    Parser,
    rule,
    and_,
)
from yargy.predicates import gram
from yargy.relations import (
    main,
    gnc_relation,
    number_relation,
    gender_relation
)
from yargy.interpretation import fact


def test_name():
    Name = fact(
        'Name',
        ['first', 'last']
    )

    gnc = gnc_relation()

    FIRST = gram('Name').interpretation(
        Name.first.inflected()
    ).match(gnc)

    LAST = gram('Surn').interpretation(
        Name.last.inflected()
    ).match(gnc)

    NAME = rule(
        FIRST,
        LAST
    ).interpretation(Name)

    parser = Parser(NAME)
    match = parser.match('саше иванову')
    assert match.fact == Name(first='саша', last='иванов')

    match = parser.match('сашу иванову')
    assert match.fact == Name(first='саша', last='иванова')

    match = parser.match('сашу ивановой')
    assert not match


def test_main():
    relation = and_(
        number_relation(),
        gender_relation()
    )

    A = rule(
        gram('Surn'),
        main(gram('Name'))
    ).match(relation)

    B = gram('VERB').match(relation)

    AB = rule(A, B)

    parser = Parser(AB)
    match = parser.match('иванов иван стал')
    assert match

    match = parser.match('иванов иван стали')
    assert not match

    match = parser.match('ивановы иван стал')
    assert match
