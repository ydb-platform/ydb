from precisely import assert_that, has_attrs, is_sequence

from mammoth.styles.parser.tokeniser import tokenise


def test_unknown_tokens_are_tokenised():
    assert_tokens("~", is_token("unknown", "~"))


def test_empty_string_is_tokenised_to_end_of_file_token():
    assert_tokens("")


def test_whitespace_is_tokenised():
    assert_tokens(" \t\t  ", is_token("whitespace", " \t\t  "))


def test_identifiers_are_tokenised():
    assert_tokens("Overture", is_token("identifier", "Overture"))


def test_escape_sequences_in_identifiers_are_tokenised():
    assert_tokens(r"\:", is_token("identifier", r"\:"))


def test_integers_are_tokenised():
    assert_tokens("123", is_token("integer", "123"))


def test_strings_are_tokenised():
    assert_tokens("'Tristan'", is_token("string", "'Tristan'"))


def test_escape_sequences_in_strings_are_tokenised():
    assert_tokens(r"'Tristan\''", is_token("string", r"'Tristan\''"))


def test_unterminated_strings_are_tokenised():
    assert_tokens("'Tristan", is_token("unterminated string", "'Tristan"))


def test_arrows_are_tokenised():
    assert_tokens("=>=>", is_token("symbol", "=>"), is_token("symbol", "=>"))


def test_dots_are_tokenised():
    assert_tokens(".", is_token("symbol", "."))


def test_colons_are_tokenised():
    assert_tokens("::", is_token("symbol", ":"), is_token("symbol", ":"))


def test_greater_thans_are_tokenised():
    assert_tokens(">>", is_token("symbol", ">"), is_token("symbol", ">"))


def test_equals_are_tokenised():
    assert_tokens("==", is_token("symbol", "="), is_token("symbol", "="))


def test_open_parens_are_tokenised():
    assert_tokens("((", is_token("symbol", "("), is_token("symbol", "("))


def test_close_parens_are_tokenised():
    assert_tokens("))", is_token("symbol", ")"), is_token("symbol", ")"))


def test_open_square_brackets_are_tokenised():
    assert_tokens("[[", is_token("symbol", "["), is_token("symbol", "["))


def test_close_square_brackets_are_tokenised():
    assert_tokens("]]", is_token("symbol", "]"), is_token("symbol", "]"))


def test_choices_are_tokenised():
    assert_tokens("||", is_token("symbol", "|"), is_token("symbol", "|"))


def test_bangs_are_tokenised():
    assert_tokens("!!", is_token("symbol", "!"), is_token("symbol", "!"))


def test_can_tokenise_multiple_tokens():
    assert_tokens("The Magic Position",
        is_token("identifier", "The"),
        is_token("whitespace", " "),
        is_token("identifier", "Magic"),
        is_token("whitespace", " "),
        is_token("identifier", "Position"),
    )


def assert_tokens(string, *expected):
    expected = list(expected)
    expected.append(is_token("end", ""))
    assert_that(
        tokenise(string),
        is_sequence(*expected),
    )


def is_token(token_type, value):
    return has_attrs(
        type=token_type,
        value=value,
    )
