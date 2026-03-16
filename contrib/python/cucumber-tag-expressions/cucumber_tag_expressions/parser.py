"""Parsing of boolean tag expressions.

Examples:
    >>> expression = parse("a and (b or not c)")
    >>> expression({"a", "other"})
    True
    >>> "( a and ( b or not ( c ) ) )" == str(expression)
    True
"""

from enum import Enum

from cucumber_tag_expressions.model import And, Literal, Not, Or, True_

# -----------------------------------------------------------------------------
# OPTIMIZER
# -----------------------------------------------------------------------------
# class TagExpressionOptimizer(object):
#
#     def rule_combine_and_ops(self, expression):
#         terms = getattr(expression, "terms", None)
#         if not terms:
#             return
#
#         for term in terms:
#
#     def rule_combine_or_ops(self, expression):
#         pass


# -----------------------------------------------------------------------------
# GRAMMAR DEFINITIONS:
# -----------------------------------------------------------------------------
class Associative(Enum):
    """Associativity of boolean operations.

    How operators of same precedence are grouped in the absence of parentheses.

    * LEFT: Groups `a and b and c` to `(a and b) and c`.
    * RIGHT: Groups `a or b or c` to `a or (b or c)`.
    """

    LEFT = 1
    RIGHT = 2


class TokenType(Enum):
    """Types of tag expression tokens."""

    OPERAND = 0
    OPERATOR = 1


class Token(Enum):
    """Describes tokens and their abilities for tag expression parsing."""

    # MAYBE: _order_ = "OR AND NOT OPEN_PARENTHESIS CLOSE_PARENTHESIS"
    OR = ("or", 0, Associative.LEFT, TokenType.OPERATOR)
    AND = ("and", 1, Associative.LEFT, TokenType.OPERATOR)
    NOT = ("not", 2, Associative.RIGHT, TokenType.OPERATOR)
    OPEN_PARENTHESIS = ("(", -2)  # Java, Javascript: -2; Ruby: 1
    CLOSE_PARENTHESIS = (")", -1)

    def __init__(self, keyword, precedence, assoc=None, token_type=TokenType.OPERAND):
        """Create a new token with keyword, precedence and associativity.

        Args:
            keyword (str): Keyword for the token.
            precedence (int): Precedence of the token.
            assoc (Associative | None): Associativity of the token.
            token_type (TokenType): Type of the token.

        Returns:
            None
        """
        self.keyword = keyword
        self.precedence = precedence
        self.assoc = assoc
        self.token_type = token_type

    @property
    def is_operation(self):
        """Check if this token is an operator.

        Returns:
            bool: Whether this token is an operator.
        """
        return self.token_type is TokenType.OPERATOR

    @property
    def is_binary(self):
        """Check if this token is a binary operator.

        Returns:
            bool: Whether this token is a binary operator.
        """
        return self in (Token.OR, Token.AND)

    @property
    def is_unary(self):
        """Check if this token is a unary operator.

        Returns:
            bool: Whether this token is a unary operator.
        """
        return self is Token.NOT

    def has_lower_precedence_than(self, other):
        """Checks if this token has lower precedence than another token.

        Args:
            other (Token): Token to compare with.

        Returns:
            bool: Whether this token has lower precedence than other token.
        """
        return (
            (self.assoc == Associative.LEFT) and (self.precedence <= other.precedence)
        ) or (
            (self.assoc == Associative.RIGHT) and (self.precedence < other.precedence)
        )

    def matches(self, text):
        """Check if the keyword of this token matches provided text.

        Args:
            text (str): Text to compare against.

        Returns:
            bool: Whether this token matches the provided text.
        """
        return self.keyword == text

    # def __eq__(self, other):
    #     if isinstance(other, six.string_types):
    #         # -- CONVENIENCE: token == "and"
    #         return self.matches(other)
    #     # -- OTHERWISE:
    #     return self is other


# -----------------------------------------------------------------------------
# PARSE ERRORS
# -----------------------------------------------------------------------------
class TagExpressionError(Exception):
    """Raised by parser if an invalid tag expression is detected."""


# -----------------------------------------------------------------------------
# PARSER
# -----------------------------------------------------------------------------
class TagExpressionParser:
    """Parser class to parse boolean tag expressions.

    Boolean operations:

    * and (as binary operation:  a and b)
    * or  (as binary operation:  a or b)
    * not (as unary operation:   not a)

    In addition, parenthesis can be used to group expressions, like:

        "a and (b or c)"
        "(a and not b) or (c and d)"

    Uses the `Shunting Yard algorithm`_ to parse the tag expression.

    Examples:
        Unary operations
        >>> expression = TagExpressionParser.parse("not foo")
        >>> expression({"foo"})
        False
        >>> expression({"other"})
        True

        Binary operations - And
        >>> expression = TagExpressionParser.parse("foo and bar")
        >>> expression({"foo", "bar"})
        True
        >>> expression({"foo"})
        False
        >>> expression({})
        False

        Binary operations - Or
        >>> expression = TagExpressionParser.parse("foo or bar")
        >>> expression({"foo", "bar"})
        True
        >>> expression({"foo", "other"})
        True
        >>> expression({})
        False

    .. _Shunting Yard algorithm:
        http://rosettacode.org/wiki/Parsing/Shunting-yard_algorithm
    """

    TOKEN_MAP = {token.keyword: token for token in Token.__members__.values()}

    @classmethod
    def select_token(cls, text):
        """Select the token that matches the text.

        Args:
            text (str): Text to select the matching token.

        Returns:
            Token | None: Token object or None, if not found.
        """
        return cls.TOKEN_MAP.get(text, None)

    @classmethod
    def make_operand(cls, text):
        """Creates operand object from parsed text.

        Args:
            text (str): Text to create operand from.

        Returns:
            Literal: Operand object created from text.
        """
        # -- EXTENSION-POINT: #TODO: See https://github.com/cucumber/common/issues/406 for a similar extension point.
        return Literal(text)

    @classmethod
    def tokenize(cls, text):
        """Creates a list of tokens from text.

        Args:
            text (str): Tag expression as text to parse.

        Returns:
            list[str]: List of selected tokens.

        Raises:
            TagExpressionError: If the tag expression is invalid.
                Such as an illegal escape character.
        """
        tokens = []
        escaped = False
        token = ""
        for char in text:
            if escaped:
                if char not in ["(", ")", "\\"] and not char.isspace():
                    message = (
                        'Tag expression "%s" could not be parsed because '
                        'of syntax error: Illegal escape before "%s".'
                    )
                    raise TagExpressionError(message % (text, char))
                token += char
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == "(" or char == ")" or char.isspace():
                if token:
                    tokens.append(token)
                    token = ""
                if char != " ":
                    tokens.append(char)
            else:
                token += char
        if token:
            tokens.append(token)
        return tokens

    @classmethod
    def parse(cls, text):
        """Parse a tag expression as text and return the expression tree.

        Args:
            text (str): Tag expression as text to parse.

        Returns:
            model.Expression: Parsed expression tree.

        Raises:
            TagExpressionError: If the tag expression is invalid.

        Examples:
            >>> expression = TagExpressionParser().parse("foo and bar or not baz")
            >>> expression({"foo", "bar"})
            True
        """
        # -- NOTE: Use whitespace-split to simplify tokenizing.
        #    This makes opening-/closing-parenthesis easier to parse.
        parts = cls.tokenize(text)
        if not parts:
            #  -- CASE: Empty tag expression is always true.
            return True_()

        def ensure_expected_token_type(token_type, index):
            if expected_token_type != token_type:
                message = f"Syntax error. Expected {expected_token_type.name.lower()} after {last_part}"
                message = cls._make_error_description(message, parts, index)
                raise TagExpressionError(message)

        operations = []  # TOKENS: AND, OR, NOT, OPEN_PAREN, CLOSE_PAREN
        expressions = []  # Finished expressions (And, Or, Not) and Literals
        last_part = "BEGIN"
        expected_token_type = TokenType.OPERAND

        for index, part in enumerate(parts):
            token = cls.select_token(part)
            if token is None:
                # -- CASE OPERAND: Literal or ...
                ensure_expected_token_type(TokenType.OPERAND, index)
                expressions.append(cls.make_operand(part))
                expected_token_type = TokenType.OPERATOR
            elif token.is_unary:
                ensure_expected_token_type(TokenType.OPERAND, index)
                operations.append(token)
                expected_token_type = TokenType.OPERAND
            elif token.is_operation:
                ensure_expected_token_type(TokenType.OPERATOR, index)
                while (
                    operations
                    and operations[-1].is_operation
                    and token.has_lower_precedence_than(operations[-1])
                ):
                    last_operation = operations.pop()
                    cls._push_expression(last_operation, expressions)
                operations.append(token)
                expected_token_type = TokenType.OPERAND
            elif token is Token.OPEN_PARENTHESIS:
                ensure_expected_token_type(TokenType.OPERAND, index)
                operations.append(token)
                expected_token_type = TokenType.OPERAND
            elif token is Token.CLOSE_PARENTHESIS:
                ensure_expected_token_type(TokenType.OPERATOR, index)
                while operations and operations[-1] != Token.OPEN_PARENTHESIS:
                    last_operation = operations.pop()
                    cls._push_expression(last_operation, expressions)

                if not operations:
                    # -- CASE: TOO FEW OPEN-PARENTHESIS
                    message = f"Missing '(': Too few open-parens in: {text}"
                    message = cls._make_error_description(message, parts, index)
                    raise TagExpressionError(message)
                if operations[-1] is Token.OPEN_PARENTHESIS:
                    operations.pop()
                    expected_token_type = TokenType.OPERATOR
            last_part = part  # BETTER DIAGNOSTICS: Remember last part.

        # -- PROCESS REMAINING OPERATIONS:
        while operations:
            last_operation = operations.pop()
            if last_operation is Token.OPEN_PARENTHESIS:
                # -- CASE: TOO MANY OPEN-PARENTHESIS
                message = f"Unclosed '(': Too many open-parens in: {text}"
                raise TagExpressionError(message)
            cls._push_expression(last_operation, expressions)

        # -- FINALLY: Return boolean tag expression.
        assert len(expressions) == 1  # noqa: S101
        return expressions.pop()

    @staticmethod
    def _push_expression(token, expressions):
        """Push a new boolean expression on the expression stack.

        Retrieves operands for operation from the expression stack and
        pushes the new expression onto it.

        Args:
            token (Token): Token for new expression.
            expressions (list[model.Expression]): Expression stack to use.

        Returns:
            None

        Raises:
            TagExpressionError: If the expression stack contains an unexpected token
                or too few operands for an operator.
        """

        def require_argcount(number):
            """Check if enough operands are in the expression stack.

            Args:
                number (int): Number of operands required.

            Returns:
                None

            Raises:
                TagExpressionError: If the expression stack contains an unexpected
                    token or too few operands for an operator.
            """
            # -- IMPROVED DIAGNOSTICS: When things go wrong (and where).
            if len(expressions) < number:
                message = "%s: Too few operands (expressions=%r)"
                raise TagExpressionError(message % (token.keyword, expressions))

        if token is Token.OR:
            require_argcount(2)
            term2 = expressions.pop()
            term1 = expressions.pop()
            expressions.append(Or(term1, term2))
        elif token is Token.AND:
            require_argcount(2)
            term2 = expressions.pop()
            term1 = expressions.pop()
            expressions.append(And(term1, term2))
        elif token is Token.NOT:
            require_argcount(1)
            term = expressions.pop()
            expressions.append(Not(term))
        else:
            raise TagExpressionError("Unexpected token: %r" % token)  # noqa
            # expressions.append(Literal(token))

    @staticmethod
    def _make_error_description(message, parts, error_index):
        """Construct a detailed error message for a tag expression error.

        Args:
            message (str): Error message to display.
            parts (list[str]): List of parts of the tag expression.
            error_index (int): Index of the error in the parts list.

        Returns:
            str: Detailed error message with error-position marked.
        """
        if error_index > len(parts):
            error_index = len(parts)  # noqa
        good_text_size = len(" ".join(parts[:error_index]))
        error_pos = len("Expression: ") + good_text_size + 1
        template = "Expression: {expression}\n%s^ (HERE)" % ("_" * error_pos)
        if message:  # noqa
            template = "{message}\n" + template
        expression = " ".join(parts)
        return template.format(message=message, expression=expression)


# ----------------------------------------------------------------------------
# CONVENIENCE FUNCTIONS:
# ----------------------------------------------------------------------------
def parse(text):
    """Parse a tag expression as text and return the expression tree.

    Args:
        text (str): Tag expression as text to parse.

    Returns:
        model.Expression: Parsed expression tree.

    Raises:
        TagExpressionError: If the tag expression is invalid.

    Examples:
        >>> expression = parse("foo and bar or not baz")
        >>> expression({"foo", "bar"})
        True
    """
    return TagExpressionParser.parse(text)
