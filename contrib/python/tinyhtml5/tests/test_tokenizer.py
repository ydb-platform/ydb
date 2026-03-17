import json
import re
from pathlib import Path
from warnings import warn

import pytest

from tinyhtml5 import constants
from tinyhtml5.tokenizer import HTMLTokenizer


class TokenizerTestParser:
    def __init__(self, initial_state, last_start_tag):
        self._state = initial_state
        self._last_start_tag = last_start_tag

    def parse(self, stream):
        tokenizer = HTMLTokenizer(stream)
        self.output_tokens = []

        tokenizer.state = getattr(tokenizer, self._state)
        if self._last_start_tag is not None:
            tokenizer.current_token = {"type": "startTag", "name": self._last_start_tag}

        types = {
            value: key.lower() for key, value in constants.Token.__members__.items()}
        for token in tokenizer:
            getattr(self, f"process_{types[token['type']]}")(token)

        return self.output_tokens

    def process_doctype(self, token):
        self.output_tokens.append([
            "DOCTYPE", token["name"], token["publicId"],
            token["systemId"], token["correct"]])

    def process_start_tag(self, token):
        self.output_tokens.append([
            "StartTag", token["name"], token["data"], token["selfClosing"]])

    def process_empty_tag(self, token):
        if token["name"] not in constants.void_elements:
            self.output_tokens.append("ParseError")
        self.output_tokens.append(
            ["StartTag", token["name"], dict(token["data"][::-1])])

    def process_end_tag(self, token):
        self.output_tokens.append(["EndTag", token["name"], token["selfClosing"]])

    def process_comment(self, token):
        self.output_tokens.append(["Comment", token["data"]])

    def process_space_characters(self, token):
        self.output_tokens.append(["Character", token["data"]])
        self.process_space_characters = self.process_characters

    def process_characters(self, token):
        self.output_tokens.append(["Character", token["data"]])

    def process_eof(self, token):
        pass

    def process_parse_error(self, token):
        self.output_tokens.append(["ParseError", token["data"]])


def concatenate_character_tokens(tokens):
    output_tokens = []
    for token in tokens:
        last_is_character = (
            output_tokens and "ParseError" not in token + output_tokens[-1] and
            token[0] == output_tokens[-1][0] == "Character")
        if last_is_character:
            output_tokens[-1][1] += token[1]
        else:
            output_tokens.append(token)
    return output_tokens


def tokens_match(expected, received):
    """Test whether the test has passed or failed.

    If the ignoreErrorOrder flag is set we don’t test the relative
    positions of parse errors and non parse errors.

    """
    check_self_closing = any(
        token[0] == "StartTag" and len(token) == 4 or
        token[0] == "EndTag" and len(token) == 3
        for token in expected)
    if not check_self_closing:
        for token in received:
            if token[0] == "StartTag" or token[0] == "EndTag":
                token.pop()

    # Sort the tokens into two groups: non-parse errors and parse errors.
    tokens = {"expected": [[], []], "received": [[], []]}
    for token_type, token_list in zip(tokens, (expected, received)):
        for token in token_list:
            if token != "ParseError":
                tokens[token_type][0].append(token)
        tokens[token_type][0] = concatenate_character_tokens(tokens[token_type][0])
    return tokens["expected"] == tokens["received"]


def decode(input):
    """Decode \\uXXXX escapes.

    This decodes \\uXXXX escapes, possibly into non-BMP characters when
    two surrogate character escapes are adjacent to each other.

    """
    # This cannot be implemented using the unicode_escape codec because that
    # requires its input be ISO-8859-1, and we need arbitrary unicode as input.
    return re.compile(r"\\u([0-9A-Fa-f]{4})").sub(
        lambda match: chr(int(match.group(1), 16)), input)


def unescape(test):
    test["input"] = decode(test["input"])
    for token in test["output"]:
        token[1] = decode(token[1])
    return test

import yatest.common as yc
_tests = tuple(
    (f"{path.stem}-{i}", test)
    for path in (Path(yc.source_path(__file__)).parent / "tokenizer").glob("*.test")
    for i, test in enumerate(json.loads(path.read_text())["tests"]))
_xfails = (
    "domjs-4",
    "test2-0",
    "test3-280", "test3-283", "test3-284", "test3-286", "test3-287", "test3-289",
    "test3-292", "test3-293", "test3-295", "test3-296", "test3-298", "test3-310",
    "test3-718",
)


@pytest.mark.parametrize("id, test", _tests, ids=(id for id, _ in _tests))
def test_tokenizer(id, test):
    if "initialStates" not in test:
        test["initialStates"] = ["Data state"]
    if "doubleEscaped" in test:
        test = unescape(test)
    for initial_state in test.get("initialStates"):
        initial_state = re.compile(r"\W+(\w)").sub(
            lambda match: f"_{match.group(1)}", initial_state.lower())
        expected = test["output"]
        parser = TokenizerTestParser(initial_state, test.get("lastStartTag"))
        tokens = parser.parse(test["input"])
        received = [token[0] if token[0] == "ParseError" else token for token in tokens]
        error_message = (
            f"\nInitial state: {initial_state}",
            f"\nInput: {test['input']}",
            f"\nExpected: {expected!r}",
            f"\nReceived: {tokens!r}")
        match = tokens_match(expected, received)
        if id in _xfails:
            if not match:
                pytest.xfail()
        else:
            assert match, error_message
    else:
        if id in _xfails:  # pragma: no cover
            warn(f"{id} passes but is marked as xfail")
