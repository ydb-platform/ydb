import parse


def test_basic():
    r = parse.search("a {} c", " a b c ")
    assert r.fixed == ("b",)


def test_multiline():
    r = parse.search("age: {:d}\n", "name: Rufus\nage: 42\ncolor: red\n")
    assert r.fixed == (42,)


def test_pos():
    r = parse.search("a {} c", " a b c ", 2)
    assert r is None


def test_no_evaluate_result():
    match = parse.search(
        "age: {:d}\n", "name: Rufus\nage: 42\ncolor: red\n", evaluate_result=False
    )
    r = match.evaluate_result()
    assert r.fixed == (42,)
