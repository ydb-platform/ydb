"""A tiny typed SMT-LIB construction layer.

Only the operations used by the relational model live here.  Keeping this AST
small prevents Python string interpolation from becoming part of the proof.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Iterable, Iterator, Sequence


BOOL = "Bool"
INT = "Int"
SORTS = frozenset({BOOL, INT})
_SYMBOL = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class SmtError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class Term:
    sort: str
    operation: str
    arguments: tuple[Term, ...] = ()
    atom: bool | int | str | None = None

    def render(self) -> str:
        if self.operation == "symbol":
            assert isinstance(self.atom, str)
            return self.atom
        if self.operation == "bool":
            return "true" if self.atom else "false"
        if self.operation == "int":
            assert isinstance(self.atom, int)
            return str(self.atom) if self.atom >= 0 else f"(- {-self.atom})"
        if not self.arguments:
            return self.operation
        return f"({self.operation} {' '.join(argument.render() for argument in self.arguments)})"


@dataclass(frozen=True, slots=True)
class Function:
    name: str
    arguments: tuple[str, ...]
    result: str

    def __call__(self, *arguments: Term) -> Term:
        if tuple(argument.sort for argument in arguments) != self.arguments:
            raise SmtError(f"wrong argument sorts for {self.name}")
        return Term(self.result, self.name, tuple(arguments))


@dataclass(frozen=True, slots=True)
class Declaration:
    name: str
    arguments: tuple[str, ...]
    result: str
    hint: str

    def render(self) -> str:
        return (
            f"; {self.name}: {json.dumps(self.hint, ensure_ascii=True)}\n"
            f"(declare-fun {self.name} ({' '.join(self.arguments)}) {self.result})"
        )


def _check_sort(sort: str) -> str:
    if sort not in SORTS:
        raise SmtError(f"unsupported SMT sort {sort!r}")
    return sort


def _check_symbol(name: str) -> str:
    if not _SYMBOL.fullmatch(name):
        raise SmtError(f"unsafe SMT symbol {name!r}")
    return name


def symbol(name: str, sort: str) -> Term:
    return Term(_check_sort(sort), "symbol", atom=_check_symbol(name))


def bool_value(value: bool) -> Term:
    return Term(BOOL, "bool", atom=value)


def int_value(value: int) -> Term:
    return Term(INT, "int", atom=value)


TRUE = bool_value(True)
FALSE = bool_value(False)
ZERO = int_value(0)
ONE = int_value(1)


def not_(term: Term) -> Term:
    _require(term, BOOL)
    if term == TRUE:
        return FALSE
    if term == FALSE:
        return TRUE
    if term.operation == "not":
        return term.arguments[0]
    return Term(BOOL, "not", (term,))


def and_(*terms: Term) -> Term:
    flat: list[Term] = []
    for term in terms:
        _require(term, BOOL)
        if term == FALSE:
            return FALSE
        if term == TRUE:
            continue
        if term.operation == "and":
            flat.extend(term.arguments)
        else:
            flat.append(term)
    if not flat:
        return TRUE
    if len(flat) == 1:
        return flat[0]
    return Term(BOOL, "and", tuple(flat))


def or_(*terms: Term) -> Term:
    flat: list[Term] = []
    for term in terms:
        _require(term, BOOL)
        if term == TRUE:
            return TRUE
        if term == FALSE:
            continue
        if term.operation == "or":
            flat.extend(term.arguments)
        else:
            flat.append(term)
    if not flat:
        return FALSE
    if len(flat) == 1:
        return flat[0]
    return Term(BOOL, "or", tuple(flat))


def eq(left: Term, right: Term) -> Term:
    if left.sort != right.sort:
        raise SmtError(f"equality sort mismatch: {left.sort} and {right.sort}")
    if left == right:
        return TRUE
    if left.operation in {"bool", "int"} and right.operation == left.operation:
        return bool_value(left.atom == right.atom)
    return Term(BOOL, "=", (left, right))


def lt(left: Term, right: Term) -> Term:
    _require(left, INT)
    _require(right, INT)
    if left == right:
        return FALSE
    if left.operation == "int" and right.operation == "int":
        assert isinstance(left.atom, int) and isinstance(right.atom, int)
        return bool_value(left.atom < right.atom)
    return Term(BOOL, "<", (left, right))


def ite(condition: Term, when_true: Term, when_false: Term) -> Term:
    _require(condition, BOOL)
    if when_true.sort != when_false.sort:
        raise SmtError("ite branch sort mismatch")
    if condition == TRUE:
        return when_true
    if condition == FALSE:
        return when_false
    if when_true == when_false:
        return when_true
    return Term(when_true.sort, "ite", (condition, when_true, when_false))


def add(*terms: Term) -> Term:
    for term in terms:
        _require(term, INT)
    if not terms:
        return ZERO
    if len(terms) == 1:
        return terms[0]
    if all(term.operation == "int" for term in terms):
        values = []
        for term in terms:
            assert isinstance(term.atom, int)
            values.append(term.atom)
        return int_value(sum(values))
    return Term(INT, "+", tuple(terms))


def sub(left: Term, right: Term) -> Term:
    _require(left, INT)
    _require(right, INT)
    if left == right:
        return ZERO
    if right == ZERO:
        return left
    if left.operation == "int" and right.operation == "int":
        assert isinstance(left.atom, int) and isinstance(right.atom, int)
        return int_value(left.atom - right.atom)
    return Term(INT, "-", (left, right))


def mul(left: Term, right: Term) -> Term:
    _require(left, INT)
    _require(right, INT)
    if left == ZERO or right == ZERO:
        return ZERO
    if left == ONE:
        return right
    if right == ONE:
        return left
    if left.operation == "int" and right.operation == "int":
        assert isinstance(left.atom, int) and isinstance(right.atom, int)
        return int_value(left.atom * right.atom)
    return Term(INT, "*", (left, right))


def mod(term: Term, modulus: int) -> Term:
    _require(term, INT)
    if type(modulus) is not int or modulus <= 0:
        raise SmtError("modulus must be a positive integer")
    if term.operation == "int":
        assert isinstance(term.atom, int)
        return int_value(term.atom % modulus)
    return Term(INT, "mod", (term, int_value(modulus)))


def _require(term: Term, sort: str) -> None:
    if not isinstance(term, Term):
        raise SmtError(f"expected an SMT term, got {type(term).__name__}")
    if term.sort != sort:
        raise SmtError(f"expected {sort}, got {term.sort}")


class Script:
    """Ordered declarations and assertions with deterministic symbol allocation."""

    def __init__(self, timeout_ms: int | None = None) -> None:
        self.timeout_ms = timeout_ms
        self._next_symbol = 0
        self._declarations: list[Declaration] = []
        self._assertions: list[Term] = []
        self._string_codes: dict[str, int] = {}

    def fresh_constant(self, hint: str, sort: str) -> Term:
        name = f"v_{self._next_symbol}"
        self._next_symbol += 1
        checked_sort = _check_sort(sort)
        self._declarations.append(Declaration(name, (), checked_sort, hint))
        return symbol(name, checked_sort)

    def fresh_function(self, hint: str, arguments: Sequence[str], result: str) -> Function:
        name = f"f_{self._next_symbol}"
        self._next_symbol += 1
        checked_arguments = tuple(_check_sort(sort) for sort in arguments)
        checked_result = _check_sort(result)
        self._declarations.append(Declaration(name, checked_arguments, checked_result, hint))
        return Function(name, checked_arguments, checked_result)

    def assert_(self, term: Term) -> None:
        _require(term, BOOL)
        self._assertions.append(term)

    def string_atom(self, value: str) -> Term:
        """Encode a v1 string as an equality-only solver-independent atom."""

        code = self._string_codes.get(value)
        if code is None:
            code = len(self._string_codes)
            self._string_codes[value] = code
        return int_value(code)

    @property
    def string_literals(self) -> dict[int, str]:
        return {code: value for value, code in self._string_codes.items()}

    @property
    def assertions(self) -> tuple[Term, ...]:
        return tuple(self._assertions)

    def render(self, values: Iterable[Term] = ()) -> str:
        requested = tuple(values)
        lines = ["; Generated by the YDB new-RBO bounded equivalence verifier."]
        if self.timeout_ms is not None:
            lines.append(f"(set-option :timeout {self.timeout_ms})")
        lines.extend(["(set-option :produce-models true)", "(set-logic ALL)"])
        lines.extend(declaration.render() for declaration in self._declarations)
        lines.extend(f"(assert {assertion.render()})" for assertion in self._assertions)
        lines.append("(check-sat)")
        if requested:
            for term in requested:
                if term.operation != "symbol":
                    raise SmtError("only named constants may be requested")
            lines.append(f"(get-value ({' '.join(term.render() for term in requested)}))")
        return "\n".join(lines) + "\n"


SExpr = str | list["SExpr"]


def parse_sexpressions(text: str) -> list[SExpr]:
    """Parse the small S-expression subset returned by `(get-value ...)`."""

    tokens = iter(_tokens(text))

    def parse(token: str) -> SExpr:
        if token != "(":
            if token == ")":
                raise SmtError("unexpected closing parenthesis")
            return token
        result: list[SExpr] = []
        for nested in tokens:
            if nested == ")":
                return result
            result.append(parse(nested))
        raise SmtError("unterminated S-expression")

    result: list[SExpr] = []
    for token in tokens:
        if token == ")":
            raise SmtError("unexpected closing parenthesis")
        result.append(parse(token))
    return result


def atom_value(value: SExpr) -> bool | int | str:
    if isinstance(value, list):
        if len(value) == 2 and value[0] == "-" and isinstance(value[1], str) and value[1].isdigit():
            return -int(value[1])
        raise SmtError(f"unsupported model value {value!r}")
    if value == "true":
        return True
    if value == "false":
        return False
    if value.startswith('"'):
        return json.loads(value)
    try:
        return int(value)
    except ValueError:
        return value


def _tokens(text: str) -> Iterator[str]:
    index = 0
    while index < len(text):
        char = text[index]
        if char.isspace():
            index += 1
        elif char in "()":
            yield char
            index += 1
        elif char == '"':
            start = index
            index += 1
            while index < len(text):
                if text[index] != '"':
                    index += 1
                    continue
                if index + 1 < len(text) and text[index + 1] == '"':
                    index += 2
                    continue
                index += 1
                break
            else:
                raise SmtError("unterminated string literal")
            # Quoted atoms are not emitted by the v1 formula, but accepting the
            # standard doubled-quote form keeps result parsing fail-readable.
            decoded = text[start + 1 : index - 1].replace('""', '"')
            yield json.dumps(decoded)
        else:
            start = index
            while index < len(text) and not text[index].isspace() and text[index] not in "()":
                index += 1
            yield text[start:index]
