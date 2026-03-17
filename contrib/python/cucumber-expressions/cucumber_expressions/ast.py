from __future__ import annotations

from enum import Enum


class NodeType(Enum):
    TEXT = "TEXT_NODE"
    OPTIONAL = "OPTIONAL_NODE"
    ALTERNATION = "ALTERNATION_NODE"
    ALTERNATIVE = "ALTERNATIVE_NODE"
    PARAMETER = "PARAMETER_NODE"
    EXPRESSION = "EXPRESSION_NODE"


class TokenType(Enum):
    START_OF_LINE = "START_OF_LINE"
    END_OF_LINE = "END_OF_LINE"
    WHITE_SPACE = "WHITE_SPACE"
    BEGIN_OPTIONAL = "BEGIN_OPTIONAL"
    END_OPTIONAL = "END_OPTIONAL"
    BEGIN_PARAMETER = "BEGIN_PARAMETER"
    END_PARAMETER = "END_PARAMETER"
    ALTERNATION = "ALTERNATION"
    TEXT = "TEXT"


class EscapeCharacters(Enum):
    ESCAPE_CHARACTER = "\\"


class DemarcationCharacters(Enum):
    ALTERNATION_CHARACTER = "/"
    BEGIN_PARAMETER_CHARACTER = "{"
    END_PARAMETER_CHARACTER = "}"
    BEGIN_OPTIONAL_CHARACTER = "("
    END_OPTIONAL_CHARACTER = ")"


class Node:
    def __init__(
        self,
        ast_type: NodeType,
        nodes: list[Node] | None,
        token: str | None,
        start: int,
        end: int,
    ):
        if nodes is None and token is None:
            raise Exception("Either nodes or token must be defined")
        self._ast_type = ast_type
        self._nodes = nodes
        self._token = token
        self._start = start
        self._end = end

    @property
    def ast_type(self) -> NodeType:
        return self._ast_type

    @property
    def nodes(self) -> list[Node]:
        return self._nodes

    @property
    def token(self) -> str:
        return self._token

    @property
    def start(self) -> int:
        return self._start

    @property
    def end(self) -> int:
        return self._end

    @property
    def text(self) -> str:
        return self.token or "".join([node_value.text for node_value in self.nodes])

    def to_json(self):
        json_obj = {"type": self.ast_type.value}
        if self.nodes is not None:
            json_obj["nodes"] = [node_value.to_json() for node_value in self.nodes]
        if self.token is not None:
            json_obj["token"] = self.token
        json_obj["start"] = self.start
        json_obj["end"] = self.end
        return json_obj


class Token:
    def __init__(self, ast_type: TokenType, text: str, start: int, end: int):
        self._ast_type = ast_type
        self._text = text
        self._start = start
        self._end = end

    @property
    def ast_type(self):
        return self._ast_type

    @property
    def text(self):
        return self._text

    @property
    def start(self):
        return self._start

    @property
    def end(self):
        return self._end

    @staticmethod
    def is_escape_character(char: str) -> bool:
        return char == EscapeCharacters.ESCAPE_CHARACTER.value

    @staticmethod
    def can_escape(char: str) -> bool:
        if char.isspace():
            return True

        _escape_chars = [e.value for e in EscapeCharacters]
        _demarcation_chars = [e.value for e in DemarcationCharacters]
        return any(
            char == escape_char for escape_char in _escape_chars + _demarcation_chars
        )

    @staticmethod
    def type_of(char: str) -> TokenType:
        if char.isspace():
            return TokenType.WHITE_SPACE
        possible_demarcation_match = [
            e.name for e in DemarcationCharacters if e.value == char
        ]
        if possible_demarcation_match:
            _key = possible_demarcation_match[0].replace("_CHARACTER", "")
            return TokenType(_key)
        return TokenType.TEXT

    @staticmethod
    def symbol_of(token: TokenType):
        possible_token_character_key = token.name + "_CHARACTER"
        if any(
            e.name
            for e in DemarcationCharacters
            if e.name == possible_token_character_key
        ):
            return DemarcationCharacters[possible_token_character_key].value
        return ""

    @staticmethod
    def purpose_of(token: TokenType):
        if token in [TokenType.BEGIN_OPTIONAL, TokenType.END_OPTIONAL]:
            return "optional text"
        if token in [TokenType.BEGIN_PARAMETER, TokenType.END_PARAMETER]:
            return "a parameter"
        if token == TokenType.ALTERNATION:
            return "alternation"
        return ""

    def to_json(self):
        return {
            "type": self.ast_type.value,
            "text": self.text,
            "start": self.start,
            "end": self.end,
        }
