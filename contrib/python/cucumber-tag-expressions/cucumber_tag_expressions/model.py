"""Model classes to evaluate parsed boolean tag expressions.

Examples:
    >>> expression = And(Literal("a"), Literal("b"))
    >>> expression({"a", "b"})
    True
    >>> expression({"a", "b"})
    True
    >>> expression({"a"})
    False
    >>> expression({})
    False

    >>> expression = Or(Literal("a"), Literal("b"))
    >>> expression({"a", "b"})
    True
    >>> expression({"a"})
    True
    >>> expression({})
    False

    >>> expression = Not(Literal("a"))
    >>> expression({"a"})
    False
    >>> expression({"other"})
    True
    >>> expression({})
    True

    >>> expression = And(Or(Literal("a"), Literal("b")), Literal("c"))
    >>> expression({"a", "c"})
    True
    >>> expression({"c", "other"})
    False
    >>> expression({})
    False
"""

import re


# -----------------------------------------------------------------------------
# TAG-EXPRESSION MODEL CLASSES:
# -----------------------------------------------------------------------------
class Expression:
    """Abstract base class for boolean expression terms of a tag expression
    (or representing a parsed tag expression (evaluation-tree)).
    """

    def evaluate(self, values):
        """Evaluate whether expression matches values.

        Args:
            values (Iterable[str]): Tag names to evaluate.

        Returns:
            bool: Whether expression evaluates with values.
        """
        raise NotImplementedError()

    def __call__(self, values):
        """Call operator to make an expression object callable.

        Args:
            values (Iterable[str]): Tag names to evaluate.

        Returns:
            bool: True if expression is true, False otherwise
        """
        return bool(self.evaluate(values))


class Literal(Expression):
    """Used as placeholder for a tag in a boolean tag expression."""

    def __init__(self, name):
        """Initialise literal with tag name.

        Args:
            name (str): Tag name to represent as a literal.
        """
        super().__init__()
        self.name = name

    def evaluate(self, values):
        truth_value = self.name in set(values)
        return bool(truth_value)

    def __str__(self):
        return re.sub(
            r"(\s)",
            r"\\\1",
            self.name.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)"),
        )

    def __repr__(self):
        return f"Literal('{self.name}')"


class And(Expression):
    """Boolean-and operation (as binary operation).

    NOTE: Class supports more than two arguments.
    """

    def __init__(self, *terms):
        """Create Boolean-AND expression.

        Args:
            terms (Iterable[Expression]): List of boolean expressions to AND.

        Returns:
            None
        """
        super().__init__()
        self.terms = terms

    def evaluate(self, values):
        values_ = set(values)
        for term in self.terms:
            truth_value = term.evaluate(values_)
            if not truth_value:
                # -- SHORTCUT: Any false makes the expression false.
                return False
        # -- OTHERWISE: All terms are true.
        return True
        # -- ALTERNATIVE:
        # return all([term.evaluate(values_) for term in self.terms])

    def __str__(self):
        if not self.terms:
            return ""  # noqa
        expression_text = " and ".join([str(term) for term in self.terms])
        return f"( {expression_text} )"

    def __repr__(self):
        return f"And({', '.join([repr(term) for term in self.terms])})"


class Or(Expression):
    """Boolean-or operation (as binary operation).

    NOTE: Class supports more than two arguments.
    """

    def __init__(self, *terms):
        """Create Boolean-OR expression.

        Args:
            terms (Iterable[Expression]): List of boolean expressions to OR.

        Returns:
            None
        """
        super().__init__()
        self.terms = terms

    def evaluate(self, values):
        values_ = set(values)
        for term in self.terms:
            truth_value = term.evaluate(values_)
            if truth_value:
                # -- SHORTCUT: Any true makes the expression true.
                return True
        # -- OTHERWISE: All terms are false.
        return False
        # -- ALTERNATIVE:
        # return any([term.evaluate(values_) for term in self.terms])

    def __str__(self):
        if not self.terms:
            return ""  # noqa
        expression_text = " or ".join([str(term) for term in self.terms])
        return f"( {expression_text} )"

    def __repr__(self):
        return f"Or({', '.join([repr(term) for term in self.terms])})"


class Not(Expression):
    """Boolean-not operation (as unary operation)."""

    def __init__(self, term):
        """Create Boolean-AND expression.

        Args:
            term (Expression): Boolean expression to negate.

        Returns:
            None
        """
        super().__init__()
        self.term = term

    def evaluate(self, values):
        values_ = set(values)
        return not self.term.evaluate(values_)

    def __str__(self):
        schema = "not ( {0} )"
        if isinstance(self.term, (And, Or)):
            # -- REASON: Binary operators have parenthesis already.
            schema = "not {0}"
        return schema.format(self.term)

    def __repr__(self):
        return f"Not({self.term!r})"


class True_(Expression):  # noqa: N801
    """Boolean expression that is always true."""

    def evaluate(self, values):
        """Evaluates to True.

        Args:
            values (Any): Required by API though not used.

        Returns:
            Literal[True]
        """
        return True

    def __str__(self):
        return ""

    def __repr__(self):
        return "True_()"
