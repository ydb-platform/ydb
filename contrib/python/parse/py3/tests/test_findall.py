import parse


def test_findall():
    s = "".join(
        r.fixed[0] for r in parse.findall(">{}<", "<p>some <b>bold</b> text</p>")
    )
    assert s == "some bold text"


def test_no_evaluate_result():
    s = "".join(
        m.evaluate_result().fixed[0]
        for m in parse.findall(
            ">{}<", "<p>some <b>bold</b> text</p>", evaluate_result=False
        )
    )
    assert s == "some bold text"


def test_case_sensitivity():
    l = [r.fixed[0] for r in parse.findall("x({})x", "X(hi)X")]
    assert l == ["hi"]

    l = [r.fixed[0] for r in parse.findall("x({})x", "X(hi)X", case_sensitive=True)]
    assert l == []
