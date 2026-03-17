from nanoid.resources import alphabet


def test_has_no_duplicates():
    for i in range(len(alphabet)):
        assert alphabet.rindex(alphabet[i]) == i


def test_is_string():
    assert type(alphabet) == str
