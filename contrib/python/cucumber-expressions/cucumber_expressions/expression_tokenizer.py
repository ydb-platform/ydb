from cucumber_expressions.ast import Token, TokenType
from cucumber_expressions.errors import (
    CantEscape,
    TheEndOfLineCannotBeEscaped,
)


class CucumberExpressionTokenizer:
    def __init__(self):
        self.expression: str = ""
        self.buffer: list[str] = []
        self.escaped: int = 0
        self.buffer_start_index: int = 0

    def tokenize(self, expression: str, to_json: bool = False) -> list[Token]:
        self.expression = expression
        tokens = []
        previous_token_type = TokenType.START_OF_LINE
        treat_as_text = False

        chars = list(self.expression)

        if not chars:
            tokens.append(Token(TokenType.START_OF_LINE, "", 0, 0))

        for char in chars:
            if Token.is_escape_character(char) and not treat_as_text:
                self.escaped += 1
                treat_as_text = True
                continue

            current_token_type = self.token_type_of(char, treat_as_text)
            treat_as_text = False

            if self.should_create_new_token(previous_token_type, current_token_type):
                token = self.convert_buffer_to_token(previous_token_type)
                tokens.append(token)

            previous_token_type = current_token_type
            self.buffer.append(char)

        if self.buffer:
            token = self.convert_buffer_to_token(previous_token_type)
            tokens.append(token)

        if treat_as_text:
            raise TheEndOfLineCannotBeEscaped(expression)

        tokens.append(Token(TokenType.END_OF_LINE, "", len(chars), len(chars)))

        def convert_to_json_format(_tokens: list[Token]) -> list:
            return [
                {
                    "type": t.ast_type.value,
                    "end": t.end,
                    "start": t.start,
                    "text": t.text,
                }
                for t in _tokens
            ]

        return tokens if not to_json else convert_to_json_format(tokens)

    def convert_buffer_to_token(self, token_type: TokenType) -> Token:
        escape_tokens = 0
        if token_type == TokenType.TEXT:
            escape_tokens = self.escaped
            self.escaped = 0

        consumed_index = self.buffer_start_index + len(self.buffer) + escape_tokens
        token = Token(
            token_type,
            "".join(self.buffer),
            self.buffer_start_index,
            consumed_index,
        )

        self.buffer = []
        self.buffer_start_index = consumed_index
        return token

    def token_type_of(self, char: str, treat_as_text) -> TokenType:
        if not treat_as_text:
            return Token.type_of(char)
        if Token.can_escape(char):
            return TokenType.TEXT
        raise CantEscape(
            self.expression,
            self.buffer_start_index + len(self.buffer) + self.escaped,
        )

    @staticmethod
    def should_create_new_token(
        previous_token_type: TokenType,
        current_token_type: TokenType,
    ):
        return current_token_type != previous_token_type or (
            current_token_type not in [TokenType.WHITE_SPACE, TokenType.TEXT]
        )
