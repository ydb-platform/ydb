import inflect


class Test:
    def test_classical(self):
        p = inflect.engine()

        # DEFAULT...

        assert p.plural_noun("error", 0) == "errors", "classical 'zero' not active"
        assert (
            p.plural_noun("wildebeest") == "wildebeests"
        ), "classical 'herd' not active"
        assert p.plural_noun("Sally") == "Sallys", "classical 'names' active"
        assert p.plural_noun("brother") == "brothers", "classical others not active"
        assert p.plural_noun("person") == "people", "classical 'persons' not active"
        assert p.plural_noun("formula") == "formulas", "classical 'ancient' not active"

        # CLASSICAL PLURALS ACTIVATED...

        p.classical(all=True)
        assert p.plural_noun("error", 0) == "error", "classical 'zero' active"
        assert p.plural_noun("wildebeest") == "wildebeest", "classical 'herd' active"
        assert p.plural_noun("Sally") == "Sallys", "classical 'names' active"
        assert p.plural_noun("brother") == "brethren", "classical others active"
        assert p.plural_noun("person") == "persons", "classical 'persons' active"
        assert p.plural_noun("formula") == "formulae", "classical 'ancient' active"

        # CLASSICAL PLURALS DEACTIVATED...

        p.classical(all=False)
        assert p.plural_noun("error", 0) == "errors", "classical 'zero' not active"
        assert (
            p.plural_noun("wildebeest") == "wildebeests"
        ), "classical 'herd' not active"
        assert p.plural_noun("Sally") == "Sallies", "classical 'names' not active"
        assert p.plural_noun("brother") == "brothers", "classical others not active"
        assert p.plural_noun("person") == "people", "classical 'persons' not active"
        assert p.plural_noun("formula") == "formulas", "classical 'ancient' not active"

        # CLASSICAL PLURALS REREREACTIVATED...

        p.classical()
        assert p.plural_noun("error", 0) == "error", "classical 'zero' active"
        assert p.plural_noun("wildebeest") == "wildebeest", "classical 'herd' active"
        assert p.plural_noun("Sally") == "Sallys", "classical 'names' active"
        assert p.plural_noun("brother") == "brethren", "classical others active"
        assert p.plural_noun("person") == "persons", "classical 'persons' active"
        assert p.plural_noun("formula") == "formulae", "classical 'ancient' active"
