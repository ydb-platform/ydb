from decimal import Decimal

import pytest

import parse


def assert_match(parser, text, param_name, expected):
    result = parser.parse(text)
    assert result[param_name] == expected


def assert_mismatch(parser, text, param_name):
    result = parser.parse(text)
    assert result is None


def assert_fixed_match(parser, text, expected):
    result = parser.parse(text)
    assert result.fixed == expected


def assert_fixed_mismatch(parser, text):
    result = parser.parse(text)
    assert result is None


def test_pattern_should_be_used():
    def parse_number(text):
        return int(text)

    parse_number.pattern = r"\d+"
    parse_number.name = "Number"  # For testing only.

    extra_types = {parse_number.name: parse_number}
    format = "Value is {number:Number} and..."
    parser = parse.Parser(format, extra_types)

    assert_match(parser, "Value is 42 and...", "number", 42)
    assert_match(parser, "Value is 00123 and...", "number", 123)
    assert_mismatch(parser, "Value is ALICE and...", "number")
    assert_mismatch(parser, "Value is -123 and...", "number")


def test_pattern_should_be_used2():
    def parse_yesno(text):
        return parse_yesno.mapping[text.lower()]

    parse_yesno.mapping = {
        "yes": True,
        "no": False,
        "on": True,
        "off": False,
        "true": True,
        "false": False,
    }
    parse_yesno.pattern = r"|".join(parse_yesno.mapping.keys())
    parse_yesno.name = "YesNo"  # For testing only.

    extra_types = {parse_yesno.name: parse_yesno}
    format = "Answer: {answer:YesNo}"
    parser = parse.Parser(format, extra_types)

    # -- ENSURE: Known enum values are correctly extracted.
    for value_name, value in parse_yesno.mapping.items():
        text = "Answer: %s" % value_name
        assert_match(parser, text, "answer", value)

    # -- IGNORE-CASE: In parsing, calls type converter function !!!
    assert_match(parser, "Answer: YES", "answer", True)
    assert_mismatch(parser, "Answer: __YES__", "answer")


def test_with_pattern():
    ab_vals = {"a": 1, "b": 2}

    @parse.with_pattern(r"[ab]")
    def ab(text):
        return ab_vals[text]

    parser = parse.Parser("test {result:ab}", {"ab": ab})
    assert_match(parser, "test a", "result", 1)
    assert_match(parser, "test b", "result", 2)
    assert_mismatch(parser, "test c", "result")


def test_with_pattern_and_regex_group_count():
    # -- SPECIAL-CASE: Regex-grouping is used in user-defined type
    # NOTE: Missing or wroung regex_group_counts cause problems
    #       with parsing following params.
    @parse.with_pattern(r"(meter|kilometer)", regex_group_count=1)
    def parse_unit(text):
        return text.strip()

    @parse.with_pattern(r"\d+")
    def parse_number(text):
        return int(text)

    type_converters = {"Number": parse_number, "Unit": parse_unit}
    # -- CASE: Unnamed-params (affected)
    parser = parse.Parser("test {:Unit}-{:Number}", type_converters)
    assert_fixed_match(parser, "test meter-10", ("meter", 10))
    assert_fixed_match(parser, "test kilometer-20", ("kilometer", 20))
    assert_fixed_mismatch(parser, "test liter-30")

    # -- CASE: Named-params (uncritical; should not be affected)
    # REASON: Named-params have additional, own grouping.
    parser2 = parse.Parser("test {unit:Unit}-{value:Number}", type_converters)
    assert_match(parser2, "test meter-10", "unit", "meter")
    assert_match(parser2, "test meter-10", "value", 10)
    assert_match(parser2, "test kilometer-20", "unit", "kilometer")
    assert_match(parser2, "test kilometer-20", "value", 20)
    assert_mismatch(parser2, "test liter-30", "unit")


def test_with_pattern_and_wrong_regex_group_count_raises_error():
    # -- SPECIAL-CASE:
    # Regex-grouping is used in user-defined type, but wrong value is provided.
    @parse.with_pattern(r"(meter|kilometer)", regex_group_count=1)
    def parse_unit(text):
        return text.strip()

    @parse.with_pattern(r"\d+")
    def parse_number(text):
        return int(text)

    # -- CASE: Unnamed-params (affected)
    BAD_REGEX_GROUP_COUNTS_AND_ERRORS = [
        (None, ValueError),
        (0, ValueError),
        (2, IndexError),
    ]
    for bad_regex_group_count, error_class in BAD_REGEX_GROUP_COUNTS_AND_ERRORS:
        parse_unit.regex_group_count = bad_regex_group_count  # -- OVERRIDE-HERE
        type_converters = {"Number": parse_number, "Unit": parse_unit}
        parser = parse.Parser("test {:Unit}-{:Number}", type_converters)
        with pytest.raises(error_class):
            parser.parse("test meter-10")


def test_with_pattern_and_regex_group_count_is_none():
    # -- CORNER-CASE: Increase code-coverage.
    data_values = {"a": 1, "b": 2}

    @parse.with_pattern(r"[ab]")
    def parse_data(text):
        return data_values[text]

    parse_data.regex_group_count = None  # ENFORCE: None

    # -- CASE: Unnamed-params
    parser = parse.Parser("test {:Data}", {"Data": parse_data})
    assert_fixed_match(parser, "test a", (1,))
    assert_fixed_match(parser, "test b", (2,))
    assert_fixed_mismatch(parser, "test c")

    # -- CASE: Named-params
    parser2 = parse.Parser("test {value:Data}", {"Data": parse_data})
    assert_match(parser2, "test a", "value", 1)
    assert_match(parser2, "test b", "value", 2)
    assert_mismatch(parser2, "test c", "value")


def test_case_sensitivity():
    r = parse.parse("SPAM {} SPAM", "spam spam spam")
    assert r[0] == "spam"
    assert parse.parse("SPAM {} SPAM", "spam spam spam", case_sensitive=True) is None


def test_decimal_value():
    value = Decimal("5.5")
    str_ = "test {}".format(value)
    parser = parse.Parser("test {:F}")
    assert parser.parse(str_)[0] == value


def test_width_str():
    res = parse.parse("{:.2}{:.2}", "look")
    assert res.fixed == ("lo", "ok")
    res = parse.parse("{:2}{:2}", "look")
    assert res.fixed == ("lo", "ok")
    res = parse.parse("{:4}{}", "look at that")
    assert res.fixed == ("look", " at that")


def test_width_constraints():
    res = parse.parse("{:4}", "looky")
    assert res.fixed == ("looky",)
    res = parse.parse("{:4.4}", "looky")
    assert res is None
    res = parse.parse("{:4.4}", "ook")
    assert res is None
    res = parse.parse("{:4}{:.4}", "look at that")
    assert res.fixed == ("look at ", "that")


def test_width_multi_int():
    res = parse.parse("{:02d}{:02d}", "0440")
    assert res.fixed == (4, 40)
    res = parse.parse("{:03d}{:d}", "04404")
    assert res.fixed == (44, 4)


def test_width_empty_input():
    res = parse.parse("{:.2}", "")
    assert res is None
    res = parse.parse("{:2}", "l")
    assert res is None
    res = parse.parse("{:2d}", "")
    assert res is None


def test_int_convert_stateless_base():
    parser = parse.Parser("{:d}")
    assert parser.parse("1234")[0] == 1234
    assert parser.parse("0b1011")[0] == 0b1011
