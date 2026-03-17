import inflect


def test_join():
    p = inflect.engine()

    # Three words...
    words = "apple banana carrot".split()

    assert p.join(words), "apple, banana == and carrot"

    assert p.join(words, final_sep="") == "apple, banana and carrot"

    assert p.join(words, final_sep="...") == "apple, banana... and carrot"

    assert p.join(words, final_sep="...", conj="") == "apple, banana... carrot"

    assert p.join(words, conj="or") == "apple, banana, or carrot"

    # Three words with semicolons...
    words = ("apple,fuji", "banana", "carrot")

    assert p.join(words) == "apple,fuji; banana; and carrot"

    assert p.join(words, final_sep="") == "apple,fuji; banana and carrot"

    assert p.join(words, final_sep="...") == "apple,fuji; banana... and carrot"

    assert p.join(words, final_sep="...", conj="") == "apple,fuji; banana... carrot"

    assert p.join(words, conj="or") == "apple,fuji; banana; or carrot"

    # Two words...
    words = ("apple", "carrot")

    assert p.join(words) == "apple and carrot"

    assert p.join(words, final_sep="") == "apple and carrot"

    assert p.join(words, final_sep="...") == "apple and carrot"

    assert p.join(words, final_sep="...", conj="") == "apple carrot"

    assert p.join(words, final_sep="...", conj="", conj_spaced=False) == "applecarrot"

    assert p.join(words, conj="or") == "apple or carrot"

    # One word...
    words = ["carrot"]

    assert p.join(words) == "carrot"

    assert p.join(words, final_sep="") == "carrot"

    assert p.join(words, final_sep="...") == "carrot"

    assert p.join(words, final_sep="...", conj="") == "carrot"

    assert p.join(words, conj="or") == "carrot"
