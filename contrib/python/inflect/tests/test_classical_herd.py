import inflect


def test_ancient_1():
    p = inflect.engine()

    # DEFAULT...

    assert p.plural_noun("wildebeest") == "wildebeests"

    # "person" PLURALS ACTIVATED...

    p.classical(herd=True)
    assert p.plural_noun("wildebeest") == "wildebeest"

    # OTHER CLASSICALS NOT ACTIVATED...

    assert p.plural_noun("formula") == "formulas"
    assert p.plural_noun("error", 0) == "errors"
    assert p.plural_noun("Sally") == "Sallys"
    assert p.plural_noun("brother") == "brothers"
    assert p.plural_noun("person") == "people"
