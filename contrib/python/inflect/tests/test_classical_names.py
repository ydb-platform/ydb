import inflect


def test_ancient_1():
    p = inflect.engine()

    # DEFAULT...

    assert p.plural_noun("Sally") == "Sallys"
    assert p.plural_noun("Jones", 0) == "Joneses"

    # "person" PLURALS ACTIVATED...

    p.classical(names=True)
    assert p.plural_noun("Sally") == "Sallys"
    assert p.plural_noun("Jones", 0) == "Joneses"

    # OTHER CLASSICALS NOT ACTIVATED...

    assert p.plural_noun("wildebeest") == "wildebeests"
    assert p.plural_noun("formula") == "formulas"
    assert p.plural_noun("error", 0) == "errors"
    assert p.plural_noun("brother") == "brothers"
    assert p.plural_noun("person") == "people"
