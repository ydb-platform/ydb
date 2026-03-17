from mammoth.styles.parser.tokeniser import Token, TokenType
from mammoth.styles.parser.token_parser import decode_escape_sequences, parse_identifier, parse_string
from mammoth.styles.parser.token_iterator import TokenIterator
from ...testing import assert_equal


def test_escape_sequences_in_identifiers_are_decoded():
    assert_equal(
        ":",
        parse_identifier(TokenIterator([
            Token(0, TokenType.IDENTIFIER, r"\:"),
        ])),
    )


def test_escape_sequences_in_strings_are_decoded():
    assert_equal(
        "\n",
        parse_string(TokenIterator([
            Token(0, TokenType.STRING, r"'\n'"),
        ])),
    )


def test_line_feeds_are_decoded():
    assert_equal("\n", decode_escape_sequences(r"\n"))


def test_carriage_returns_are_decoded():
    assert_equal("\r", decode_escape_sequences(r"\r"))


def test_tabs_are_decoded():
    assert_equal("\t", decode_escape_sequences(r"\t"))


def test_backslashes_are_decoded():
    assert_equal("\\", decode_escape_sequences(r"\\"))


def test_colons_are_decoded():
    assert_equal(":", decode_escape_sequences(r"\:"))
