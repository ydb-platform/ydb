import inflect


def test_ancient_1():
    p = inflect.engine()

    # DEFAULT...

    assert p.plural_noun("error", 0) == "errors"

    # "person" PLURALS ACTIVATED...

    p.classical(zero=True)
    assert p.plural_noun("error", 0) == "error"

    # OTHER CLASSICALS NOT ACTIVATED...

    assert p.plural_noun("wildebeest") == "wildebeests"
    assert p.plural_noun("formula") == "formulas"
    assert p.plural_noun("person") == "people"
    assert p.plural_noun("brother") == "brothers"
    assert p.plural_noun("Sally") == "Sallys"
