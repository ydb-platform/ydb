from collections.abc import Callable
from typing import NamedTuple

from cucumber_expressions.ast import Node, NodeType, Token, TokenType
from cucumber_expressions.errors import (
    AlternationNotAllowedInOptional,
    InvalidParameterTypeNameInNode,
    MissingEndToken,
)
from cucumber_expressions.expression_tokenizer import CucumberExpressionTokenizer


class Result(NamedTuple):
    consumed: int
    ast_node: Node | None


class Parser(NamedTuple):
    expression: str
    tokens: list[Token]
    current: int


class CucumberExpressionParser:
    # text == whitespace | ')' | '}' | .
    @staticmethod
    def parse_text(parser: Parser):
        token = parser.tokens[parser.current]
        if token.ast_type in [
            TokenType.WHITE_SPACE,
            TokenType.TEXT,
            TokenType.END_PARAMETER,
            TokenType.END_OPTIONAL,
        ]:
            return Result(
                1,
                Node(NodeType.TEXT, None, token.text, token.start, token.end),
            )
        if token.ast_type == TokenType.ALTERNATION:
            raise AlternationNotAllowedInOptional(parser.expression, token)
        # If configured correctly this will never happen
        return Result(0, None)

    # name == whitespace | .
    @staticmethod
    def parse_name(parser: Parser):
        token = parser.tokens[parser.current]
        if token.ast_type in [TokenType.WHITE_SPACE, TokenType.TEXT]:
            return Result(
                1,
                Node(NodeType.TEXT, None, token.text, token.start, token.end),
            )
        if token.ast_type in [
            TokenType.BEGIN_PARAMETER,
            TokenType.END_PARAMETER,
            TokenType.BEGIN_OPTIONAL,
            TokenType.END_OPTIONAL,
            TokenType.ALTERNATION,
        ]:
            raise InvalidParameterTypeNameInNode(parser.expression, token)
        if token.ast_type in [TokenType.START_OF_LINE, TokenType.END_OF_LINE]:
            # If configured correctly this will never happen
            return Result(0, None)
        # If configured correctly this will never happen
        return Result(0, None)

    def parse(self, expression: str) -> Node:
        # parameter := '{' + name* + '}'
        parse_parameter = self.parse_between(
            NodeType.PARAMETER,
            TokenType.BEGIN_PARAMETER,
            TokenType.END_PARAMETER,
            [self.parse_name],
        )

        # optional := '(' + option* + ')'
        # option := optional | parameter | text
        optional_sub_parsers = []
        parse_optional = self.parse_between(
            NodeType.OPTIONAL,
            TokenType.BEGIN_OPTIONAL,
            TokenType.END_OPTIONAL,
            optional_sub_parsers,
        )

        optional_sub_parsers.extend([parse_optional, parse_parameter, self.parse_text])

        # alternation := alternative* + ( '/' + alternative* )+
        def parse_alternative_separator(parser: Parser) -> Result:
            if not self.looking_at(tokens, parser.current, TokenType.ALTERNATION):
                return Result(0, None)
            token = tokens[parser.current]
            return Result(
                1,
                Node(NodeType.ALTERNATIVE, None, token.text, token.start, token.end),
            )

        alternative_parsers = [
            parse_alternative_separator,
            parse_optional,
            parse_parameter,
            self.parse_text,
        ]

        # alternation := (?<=left-boundary) + alternative* + ( '/' + alternative* )+ + (?=right-boundary)
        # left-boundary := whitespace | } | ^
        # right-boundary := whitespace | { | $
        # alternative: = optional | parameter | text
        def parse_alternation(parser: Parser) -> Result:
            previous = parser.current - 1
            if not self.looking_at_any(
                tokens,
                previous,
                [
                    TokenType.START_OF_LINE,
                    TokenType.WHITE_SPACE,
                    TokenType.END_PARAMETER,
                ],
            ):
                return Result(0, None)

            consumed, ast_nodes = self.parse_tokens_until(
                parser.expression,
                alternative_parsers,
                tokens,
                parser.current,
                [
                    TokenType.WHITE_SPACE,
                    TokenType.END_OF_LINE,
                    TokenType.BEGIN_PARAMETER,
                ],
            )
            sub_current = parser.current + consumed
            if not [
                _ast_node.ast_type
                for _ast_node in ast_nodes
                if _ast_node.ast_type == NodeType.ALTERNATIVE
            ]:
                return Result(0, None)

            start = tokens[parser.current].start
            end = tokens[sub_current].start
            # Does not consume right-hand boundary token
            return Result(
                consumed,
                Node(
                    NodeType.ALTERNATION,
                    self.split_alternatives(start, end, ast_nodes),
                    None,
                    start,
                    end,
                ),
            )

        #
        # cucumber-expression :=  ( alternation | optional | parameter | text )*
        #
        parse_cucumber_expression = self.parse_between(
            NodeType.EXPRESSION,
            TokenType.START_OF_LINE,
            TokenType.END_OF_LINE,
            [parse_alternation, parse_optional, parse_parameter, self.parse_text],
        )

        tokens = CucumberExpressionTokenizer().tokenize(expression)
        _, ast_node = parse_cucumber_expression(Parser(expression, tokens, 0))
        return ast_node

    def parse_between(
        self,
        ast_type: NodeType,
        begin_token: TokenType,
        end_token: TokenType,
        parsers: list,
    ) -> Callable[[Parser], Result | tuple[int, Node]]:
        def _parse_between(parser: Parser):
            if not self.looking_at(parser.tokens, parser.current, begin_token):
                return Result(0, None)
            sub_current = parser.current + 1
            consumed, ast = self.parse_tokens_until(
                parser.expression,
                parsers,
                parser.tokens,
                sub_current,
                [end_token, TokenType.END_OF_LINE],
            )
            sub_current += consumed

            # end_token not found
            if not self.looking_at(parser.tokens, sub_current, end_token):
                raise MissingEndToken(
                    parser.expression,
                    begin_token,
                    end_token,
                    parser.tokens[parser.current],
                )

            # consumes end_token
            start = parser.tokens[parser.current].start
            end = parser.tokens[sub_current].end
            consumed = sub_current + 1 - parser.current
            _ast = Node(ast_type, ast, None, start, end)
            return consumed, _ast

        return _parse_between

    @staticmethod
    def parse_token(
        expression,
        parsers: list,
        tokens: list[Token],
        start_at: int,
    ) -> Result:
        for parser in parsers:
            consumed, ast = parser(Parser(expression, tokens, start_at))
            if consumed:
                return Result(consumed, ast)
        # If configured correctly this will never happen
        raise Exception("No eligible parsers for " + str(tokens))

    def parse_tokens_until(
        self,
        expression,
        parsers: list,
        tokens: list[Token],
        start_at: int,
        end_tokens: list[TokenType],
    ) -> tuple[int, list[Node]]:
        current = start_at
        size = len(tokens)
        ast: list[Node] = []
        while current < size:
            if self.looking_at_any(tokens, current, end_tokens):
                break
            result = self.parse_token(expression, parsers, tokens, current)
            if result.consumed == 0:
                # If configured correctly this will never happen
                # Keep in order to avoid infinite loops
                raise Exception("No eligible parsers for " + str(tokens))
            current += result.consumed
            ast.append(result.ast_node)
        return current - start_at, ast

    def looking_at_any(
        self,
        tokens: list[Token],
        position: int,
        token_types: list[TokenType],
    ) -> bool:
        return any(
            self.looking_at(tokens, position, token_type) for token_type in token_types
        )

    @staticmethod
    def looking_at(tokens: list[Token], position: int, token_type: TokenType) -> bool:
        if position < 0:
            # If configured correctly this will never happen
            # Keep for completeness
            return token_type == TokenType.START_OF_LINE
        if position >= len(tokens):
            return token_type == TokenType.END_OF_LINE
        return tokens[position].ast_type == token_type

    def split_alternatives(
        self,
        start: int,
        end: int,
        alternation: list[Node],
    ) -> list[Node]:
        separators = []
        alternatives = []
        alternative = []
        for node in alternation:
            if NodeType.ALTERNATIVE == node.ast_type:
                separators.append(node)
                alternatives.append(alternative)
                alternative = []
            else:
                alternative.append(node)
        alternatives.append(alternative)
        return list(self.create_alternative_nodes(start, end, separators, alternatives))

    @staticmethod
    def create_alternative_nodes(
        start: int,
        end: int,
        separators: list,
        alternatives: list,
    ) -> list[Node]:
        for index, alternative in enumerate(alternatives):
            if index == 0:
                right_separator = separators[index]
                yield Node(
                    NodeType.ALTERNATIVE,
                    alternative,
                    None,
                    start,
                    right_separator.start,
                )
            elif index == len(alternatives) - 1:
                left_separator = separators[index - 1]
                yield Node(
                    NodeType.ALTERNATIVE,
                    alternative,
                    None,
                    left_separator.end,
                    end,
                )
            else:
                left_separator = separators[index - 1]
                right_separator = separators[index]
                yield Node(
                    NodeType.ALTERNATIVE,
                    alternative,
                    None,
                    left_separator.end,
                    right_separator.start,
                )
