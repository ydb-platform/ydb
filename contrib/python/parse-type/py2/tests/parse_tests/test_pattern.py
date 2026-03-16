import pytest

import parse


def _test_expression(format, expression):
    assert parse.Parser(format)._expression == expression


def test_braces():
    # pull a simple string out of another string
    _test_expression("{{ }}", r"\{ \}")


def test_fixed():
    # pull a simple string out of another string
    _test_expression("{}", r"(.+?)")
    _test_expression("{} {}", r"(.+?) (.+?)")


def test_named():
    # pull a named string out of another string
    _test_expression("{name}", r"(?P<name>.+?)")
    _test_expression("{name} {other}", r"(?P<name>.+?) (?P<other>.+?)")


def test_named_typed():
    # pull a named string out of another string
    _test_expression("{name:w}", r"(?P<name>\w+)")
    _test_expression("{name:w} {other:w}", r"(?P<name>\w+) (?P<other>\w+)")


def test_numbered():
    _test_expression("{0}", r"(.+?)")
    _test_expression("{0} {1}", r"(.+?) (.+?)")
    _test_expression("{0:f} {1:f}", r"([-+ ]?\d*\.\d+) ([-+ ]?\d*\.\d+)")


def test_bird():
    # skip some trailing whitespace
    _test_expression("{:>}", r" *(.+?)")


def test_format_variety():
    def _(fmt, matches):
        d = parse.extract_format(fmt, {"spam": "spam"})
        for k in matches:
            assert d.get(k) == matches[k]

    for t in "%obxegfdDwWsS":
        _(t, {"type": t})
        _("10" + t, {"type": t, "width": "10"})
    _("05d", {"type": "d", "width": "5", "zero": True})
    _("<", {"align": "<"})
    _(".<", {"align": "<", "fill": "."})
    _(">", {"align": ">"})
    _(".>", {"align": ">", "fill": "."})
    _("^", {"align": "^"})
    _(".^", {"align": "^", "fill": "."})
    _("x=d", {"type": "d", "align": "=", "fill": "x"})
    _("d", {"type": "d"})
    _("ti", {"type": "ti"})
    _("spam", {"type": "spam"})

    _(".^010d", {"type": "d", "width": "10", "align": "^", "fill": ".", "zero": True})
    _(".2f", {"type": "f", "precision": "2"})
    _("10.2f", {"type": "f", "width": "10", "precision": "2"})


def test_dot_separated_fields():
    # this should just work and provide the named value
    res = parse.parse("{hello.world}_{jojo.foo.baz}_{simple}", "a_b_c")
    assert res.named["hello.world"] == "a"
    assert res.named["jojo.foo.baz"] == "b"
    assert res.named["simple"] == "c"


def test_dict_style_fields():
    res = parse.parse("{hello[world]}_{hello[foo][baz]}_{simple}", "a_b_c")
    assert res.named["hello"]["world"] == "a"
    assert res.named["hello"]["foo"]["baz"] == "b"
    assert res.named["simple"] == "c"


def test_dot_separated_fields_name_collisions():
    # this should just work and provide the named value
    res = parse.parse("{a_.b}_{a__b}_{a._b}_{a___b}", "a_b_c_d")
    assert res.named["a_.b"] == "a"
    assert res.named["a__b"] == "b"
    assert res.named["a._b"] == "c"
    assert res.named["a___b"] == "d"


def test_invalid_groupnames_are_handled_gracefully():
    with pytest.raises(NotImplementedError):
        parse.parse("{hello['world']}", "doesn't work")
