"""A tiny typed SMT-LIB construction layer.

Only the operations used by the relational model live here.  Keeping this AST
small prevents Python string interpolation from becoming part of the proof.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Callable, Iterable, Iterator, Sequence, TypeAlias

from .string_order import MAX_REPRESENTATIVES, StringOrderUniverse


BOOL = "Bool"
INT = "Int"
SORTS = frozenset({BOOL, INT})
_SYMBOL = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class SmtError(ValueError):
    pass


class _OwnerToken:
    """Identity-only marker for declarations belonging to one SMT script."""


@dataclass(frozen=True, slots=True, eq=False)
class Term:
    sort: str
    operation: str
    arguments: tuple[Term, ...] = ()
    atom: bool | int | str | _OwnerToken | None = None
    _hash: int = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        # Terms form an immutable DAG and are constructed bottom-up. Cache the
        # structural hash here so sets and routing-fact maps do not repeatedly
        # walk large shared subterms.
        object.__setattr__(
            self,
            "_hash",
            hash((self.sort, self.operation, self.arguments, self.atom)),
        )

    def __hash__(self) -> int:
        return self._hash

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Term) or other.__class__ is not self.__class__:
            return NotImplemented
        if self._hash != other._hash:
            return False

        pending = [(self, other)]
        seen: set[tuple[int, int]] = set()
        while pending:
            left, right = pending.pop()
            if left is right:
                continue
            identity = (id(left), id(right))
            if identity in seen:
                continue
            seen.add(identity)
            if (
                left.__class__ is not right.__class__
                or left.sort != right.sort
                or left.operation != right.operation
                or left.atom != right.atom
                or len(left.arguments) != len(right.arguments)
            ):
                return False
            pending.extend(zip(left.arguments, right.arguments))
        return True

    def render(self) -> str:
        if self.operation == "symbol":
            assert isinstance(self.atom, str)
            return self.atom
        if self.operation == "bool":
            return "true" if self.atom else "false"
        if self.operation == "int":
            assert isinstance(self.atom, int)
            return str(self.atom) if self.atom >= 0 else f"(- {-self.atom})"
        if self.operation in {"forall", "exists"}:
            variables = self.arguments[:-1]
            body = self.arguments[-1]
            bindings = " ".join(
                f"({variable.render()} {variable.sort})" for variable in variables
            )
            return f"({self.operation} ({bindings}) {body.render()})"
        if not self.arguments:
            return self.operation
        return f"({self.operation} {' '.join(argument.render() for argument in self.arguments)})"

    def render_shared(self) -> str:
        """Render this DAG once per quantifier scope with exact SMT ``let``s."""

        context = _RenderContext(())
        context.reserve(self)
        return _render_scope(self, context)


class _RenderContext:
    """Allocate hygienic aliases across all scopes in one rendered script."""

    def __init__(self, reserved: Iterable[str]) -> None:
        self.reserved = set(reserved)
        self.next_alias = 0

    def reserve(self, root: Term) -> None:
        pending = [root]
        seen: set[int] = set()
        while pending:
            term = pending.pop()
            identity = id(term)
            if identity in seen:
                continue
            seen.add(identity)
            self.reserved.add(term.operation)
            if term.operation == "symbol":
                assert isinstance(term.atom, str)
                self.reserved.add(term.atom)
            pending.extend(term.arguments)

    def fresh_alias(self) -> str:
        while True:
            name = f"rbo_let_{self.next_alias}"
            self.next_alias += 1
            if name not in self.reserved:
                self.reserved.add(name)
                return name


def _render_scope(root: Term, context: _RenderContext) -> str:
    """Linearize shared term identities without lifting bound expressions.

    Quantifier bodies start fresh scopes because their symbol names deliberately
    shadow global witness constants.  Within one scope, repeated compound terms
    become let aliases.  Dependency levels use nested, parallel-binding lets, so
    every alias is defined outside the aliases that refer to it.
    """

    terms: dict[int, Term] = {}
    references: dict[int, int] = {}
    discovery: list[int] = []

    pending = [root]
    while pending:
        term = pending.pop()
        identity = id(term)
        references[identity] = references.get(identity, 0) + 1
        if identity in terms:
            continue
        terms[identity] = term
        discovery.append(identity)
        if term.operation in {"forall", "exists"}:
            continue
        pending.extend(reversed(term.arguments))
    candidates = {
        identity
        for identity in discovery
        if references[identity] > 1 and terms[identity].arguments
    }
    if not candidates:
        return _render_unshared(root, context)

    levels: dict[int, int] = {}
    maximum_below: dict[int, int] = {}
    pending_levels = [(root, False)]
    while pending_levels:
        term, expanded = pending_levels.pop()
        identity = id(term)
        if identity in maximum_below:
            continue
        if not expanded:
            pending_levels.append((term, True))
            if term.operation not in {"forall", "exists"}:
                pending_levels.extend(
                    (argument, False)
                    for argument in reversed(term.arguments)
                    if id(argument) not in maximum_below
                )
            continue
        child_level = 0
        if term.operation not in {"forall", "exists"}:
            child_level = max(
                (maximum_below[id(argument)] for argument in term.arguments),
                default=0,
            )
        if identity in candidates:
            levels[identity] = child_level + 1
            child_level += 1
        maximum_below[identity] = child_level

    positions = {identity: index for index, identity in enumerate(discovery)}
    aliases = {
        identity: context.fresh_alias()
        for identity in sorted(
            (item for item in discovery if item in candidates),
            key=lambda item: (levels[item], positions[item]),
        )
    }

    body = _render_term(root, context, aliases)
    by_level: dict[int, list[int]] = {}
    for identity in discovery:
        if identity in candidates:
            by_level.setdefault(levels[identity], []).append(identity)
    for level in sorted(by_level, reverse=True):
        bindings = " ".join(
            f"({aliases[identity]} "
            f"{_render_term(terms[identity], context, aliases, identity)})"
            for identity in by_level[level]
        )
        body = f"(let ({bindings}) {body})"
    return body


def _render_unshared(term: Term, context: _RenderContext) -> str:
    """Render without aliases while giving nested quantifiers their own DAG scope."""

    return _render_term(term, context, {})


def _render_term(
    root: Term,
    context: _RenderContext,
    aliases: dict[int, str],
    defining: int | None = None,
) -> str:
    """Render one scope with an explicit stack and optional let aliases."""

    pieces: list[str] = []
    pending: list[tuple[str, Term | str, int | None]] = [
        ("term", root, defining),
    ]
    while pending:
        kind, item, bypass = pending.pop()
        if kind == "text":
            assert isinstance(item, str)
            pieces.append(item)
            continue

        assert isinstance(item, Term)
        identity = id(item)
        alias = aliases.get(identity)
        if alias is not None and identity != bypass:
            pieces.append(alias)
            continue
        if item.operation == "symbol":
            assert isinstance(item.atom, str)
            pieces.append(item.atom)
            continue
        if item.operation == "bool":
            pieces.append("true" if item.atom else "false")
            continue
        if item.operation == "int":
            assert isinstance(item.atom, int)
            pieces.append(
                str(item.atom) if item.atom >= 0 else f"(- {-item.atom})"
            )
            continue
        if item.operation in {"forall", "exists"}:
            variables = item.arguments[:-1]
            body = item.arguments[-1]
            bindings = " ".join(
                f"({variable.render()} {variable.sort})"
                for variable in variables
            )
            pieces.append(
                f"({item.operation} ({bindings}) "
                f"{_render_scope(body, context)})"
            )
            continue
        if not item.arguments:
            pieces.append(item.operation)
            continue

        pieces.append(f"({item.operation}")
        pending.append(("text", ")", None))
        for argument in reversed(item.arguments):
            pending.append(("term", argument, None))
            pending.append(("text", " ", None))
    return "".join(pieces)


@dataclass(frozen=True, slots=True)
class Function:
    name: str
    arguments: tuple[str, ...]
    result: str
    _owner: _OwnerToken | None = field(
        default=None,
        repr=False,
        compare=False,
    )

    def __call__(self, *arguments: Term) -> Term:
        if tuple(argument.sort for argument in arguments) != self.arguments:
            raise SmtError(f"wrong argument sorts for {self.name}")
        return Term(
            self.result,
            self.name,
            tuple(arguments),
            self._owner,
        )


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


@dataclass(frozen=True, slots=True)
class ProductSort:
    """One exact finite-width record sort owned by an SMT script.

    Product datatypes are intentionally narrower than a general custom-sort
    API: every field is one of the verifier's two trusted scalar sorts, and
    values can only be constructed or inspected through the generated
    constructor and selectors.
    """

    sort: str
    constructor: Function
    selectors: tuple[Function, ...]
    fields: tuple[str, ...]
    _owner: _OwnerToken = field(repr=False, compare=False)

    def pack(self, *fields: Term) -> Term:
        return self.constructor(*fields)

    def select(self, value: Term, index: int) -> Term:
        if type(index) is not int or not 0 <= index < len(self.selectors):
            raise SmtError(f"product field index {index!r} is out of range")
        _require(value, self.sort)
        return self.selectors[index](value)


@dataclass(frozen=True, slots=True)
class ProductDeclaration:
    name: str
    constructor: Function
    selectors: tuple[Function, ...]
    fields: tuple[str, ...]
    hint: str

    def render(self) -> str:
        selector_fields = " ".join(
            f"({selector.name} {sort})"
            for selector, sort in zip(self.selectors, self.fields)
        )
        return (
            f"; {self.name}: {json.dumps(self.hint, ensure_ascii=True)}\n"
            "(declare-datatypes () "
            f"(({self.name} ({self.constructor.name} {selector_fields}))))"
        )


@dataclass(frozen=True, slots=True)
class DefinitionDeclaration:
    name: str
    parameters: tuple[Term, ...]
    result: str
    body: Term
    hint: str

    def render(self) -> str:
        names = tuple(
            parameter.atom
            for parameter in self.parameters
            if isinstance(parameter.atom, str)
        )
        context = _RenderContext(names)
        context.reserve(self.body)
        bindings = " ".join(
            f"({parameter.render()} {parameter.sort})"
            for parameter in self.parameters
        )
        return (
            f"; {self.name}: {json.dumps(self.hint, ensure_ascii=True)}\n"
            f"(define-fun {self.name} ({bindings}) {self.result} "
            f"{_render_scope(self.body, context)})"
        )


DeclarationRecord: TypeAlias = (
    Declaration | ProductDeclaration | DefinitionDeclaration
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
    if type(value) is not bool:
        raise SmtError(f"Boolean literal must be bool, got {type(value).__name__}")
    return Term(BOOL, "bool", atom=value)


def int_value(value: int) -> Term:
    if type(value) is not int:
        raise SmtError(f"integer literal must be int, got {type(value).__name__}")
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


def distinct(*terms: Term) -> Term:
    """Require one finite tuple of same-sort terms to be pairwise distinct."""

    if not terms:
        return TRUE
    sort = terms[0].sort
    for term in terms:
        _require(term, sort)
    if len(terms) == 1:
        return TRUE
    if len(set(terms)) != len(terms):
        return FALSE
    if all(term.operation in {"bool", "int"} for term in terms):
        return TRUE
    return Term(BOOL, "distinct", tuple(terms))


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


def forall(variables: Sequence[Term], body: Term) -> Term:
    return _quantifier("forall", variables, body)


def exists(variables: Sequence[Term], body: Term) -> Term:
    return _quantifier("exists", variables, body)


def _quantifier(operation: str, variables: Sequence[Term], body: Term) -> Term:
    """Bind hygienic named constants, including declared witness constants.

    SMT-LIB permits a quantifier to shadow a global constant.  The verifier
    uses that deliberately: one plan's choices remain global in the witness
    direction and the same names are universally bound in the response
    direction.  Restricting binders to typed, unique symbol terms keeps that
    shadowing explicit and prevents compound expressions from becoming
    malformed declarations.
    """

    _require(body, BOOL)
    checked: list[Term] = []
    names: set[str] = set()
    for variable in variables:
        if (
            not isinstance(variable, Term)
            or variable.operation != "symbol"
            or variable.arguments
            or not isinstance(variable.atom, str)
        ):
            raise SmtError("quantifier variables must be named constants")
        name = _check_symbol(variable.atom)
        _check_sort(variable.sort)
        if name in names:
            raise SmtError(f"duplicate quantifier variable {name!r}")
        names.add(name)
        checked.append(variable)
    if not checked:
        return body
    return Term(BOOL, operation, tuple(checked) + (body,))


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


def div(term: Term, divisor: int) -> Term:
    """SMT integer division by a positive constant.

    With a positive divisor, SMT-LIB ``div`` rounds toward negative infinity,
    exactly like Python's ``//``.  Keeping the divisor concrete is sufficient
    for the trusted kernels and avoids admitting general division semantics.
    """

    _require(term, INT)
    if type(divisor) is not int or divisor <= 0:
        raise SmtError("divisor must be a positive integer")
    if divisor == 1:
        return term
    if term.operation == "int":
        assert isinstance(term.atom, int)
        return int_value(term.atom // divisor)
    return Term(INT, "div", (term, int_value(divisor)))


def div_nonnegative_by_positive(dividend: Term, divisor: Term) -> Term:
    """Floor a nonnegative integer by a positive symbolic integer.

    This deliberately narrow primitive is the only trusted-kernel entry point
    for SMT ``div`` with a symbolic divisor.  Callers must construct terms that
    satisfy the two sign preconditions for every model; known constants are
    checked here so an obviously invalid use fails closed.
    """

    _require(dividend, INT)
    _require(divisor, INT)
    if dividend.operation == "int":
        assert isinstance(dividend.atom, int)
        if dividend.atom < 0:
            raise SmtError("dividend must be a nonnegative integer")
    if divisor.operation == "int":
        assert isinstance(divisor.atom, int)
        if divisor.atom <= 0:
            raise SmtError("divisor must be a positive integer")
        if divisor.atom == 1:
            return dividend
    if dividend.operation == "int" and divisor.operation == "int":
        assert isinstance(dividend.atom, int) and isinstance(divisor.atom, int)
        return int_value(dividend.atom // divisor.atom)
    return Term(INT, "div", (dividend, divisor))


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


def structural_ids(roots: tuple[Term, ...]) -> tuple[int, ...]:
    """Intern exact term structure without recursively hashing a deep DAG."""

    by_identity: dict[int, int] = {}
    by_structure: dict[tuple[object, ...], int] = {}
    for root in roots:
        pending = [(root, False)]
        while pending:
            term, expanded = pending.pop()
            identity = id(term)
            if identity in by_identity:
                continue
            if not expanded:
                pending.append((term, True))
                pending.extend(
                    (argument, False)
                    for argument in reversed(term.arguments)
                    if id(argument) not in by_identity
                )
                continue
            structure = (
                term.sort,
                term.operation,
                term.atom,
                tuple(by_identity[id(argument)] for argument in term.arguments),
            )
            by_identity[identity] = by_structure.setdefault(
                structure,
                len(by_structure),
            )
    return tuple(by_identity[id(root)] for root in roots)


SymbolKey: TypeAlias = tuple[str, str]


def _symbol_key(term: Term) -> SymbolKey:
    if (
        term.operation != "symbol"
        or term.arguments
        or not isinstance(term.atom, str)
    ):
        raise SmtError("expected a well-formed named SMT symbol")
    return term.sort, term.atom


def _dependencies_many(
    roots: Iterable[Term],
    needles: set[SymbolKey],
) -> set[SymbolKey]:
    if not needles:
        return set()
    found: set[SymbolKey] = set()
    pending = list(roots)
    seen: set[int] = set()
    while pending:
        term = pending.pop()
        identity = id(term)
        if identity in seen:
            continue
        seen.add(identity)
        if term.operation == "symbol":
            key = _symbol_key(term)
            if key in needles:
                found.add(key)
                if found == needles:
                    break
        pending.extend(term.arguments)
    return found


def _dependencies(root: Term, needles: set[SymbolKey]) -> set[SymbolKey]:
    return _dependencies_many((root,), needles)


def _depends_on(root: Term, needles: set[SymbolKey]) -> bool:
    return bool(_dependencies(root, needles))


def _free_symbols(
    root: Term,
    owner: _OwnerToken,
) -> set[SymbolKey]:
    """Collect constants in one quantifier-free, script-owned definition."""

    result: set[SymbolKey] = set()
    pending = [root]
    seen: set[int] = set()
    while pending:
        term = pending.pop()
        identity = id(term)
        if identity in seen:
            continue
        seen.add(identity)
        if term.operation == "symbol":
            result.add(_symbol_key(term))
            continue
        if term.operation in {"forall", "exists"}:
            raise SmtError(
                "defined function body must be quantifier-free"
            )
        if isinstance(term.atom, _OwnerToken):
            if term.atom is not owner:
                raise SmtError(
                    "defined function body uses a declaration from "
                    "another SMT script"
                )
            if not term.arguments:
                raise SmtError(
                    "defined function body captures a nullary declaration"
                )
        pending.extend(term.arguments)
    return result


class Script:
    """Ordered declarations and assertions with deterministic symbol allocation."""

    def __init__(self, timeout_ms: int | None = None) -> None:
        if timeout_ms is not None and (
            type(timeout_ms) is not int or timeout_ms <= 0
        ):
            raise SmtError("script timeout must be a positive integer")
        self.timeout_ms = timeout_ms
        self._owner = _OwnerToken()
        self._next_symbol = 0
        self._declarations: list[DeclarationRecord] = []
        self._assertions: list[Term] = []
        self._ordinary_assertions: list[Term] = []
        self._global_assertions: list[Term] = []
        self._string_literals: dict[str, Term] = {}
        self._string_terms: dict[int, Term] = {}
        self._string_universe: StringOrderUniverse | None = None
        self._quantified_choices: dict[SymbolKey, tuple[Term, int]] = {}
        self._obligation_index: int | None = None

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
        return Function(
            name,
            checked_arguments,
            checked_result,
            self._owner,
        )

    def fresh_product_sort(
        self,
        hint: str,
        fields: Sequence[str],
    ) -> ProductSort:
        """Declare one constructor-only product over trusted scalar fields."""

        checked_fields = tuple(_check_sort(sort) for sort in fields)
        if not checked_fields:
            raise SmtError("product datatype must have at least one field")
        name = f"d_{self._next_symbol}"
        self._next_symbol += 1
        constructor = Function(
            f"mk_{name}",
            checked_fields,
            name,
            self._owner,
        )
        selectors = tuple(
            Function(
                f"{name}_f{index}",
                (name,),
                sort,
                self._owner,
            )
            for index, sort in enumerate(checked_fields)
        )
        product = ProductSort(
            sort=name,
            constructor=constructor,
            selectors=selectors,
            fields=checked_fields,
            _owner=self._owner,
        )
        self._declarations.append(
            ProductDeclaration(
                name=name,
                constructor=constructor,
                selectors=selectors,
                fields=checked_fields,
                hint=hint,
            )
        )
        return product

    def fresh_defined_function(
        self,
        hint: str,
        arguments: Sequence[str | ProductSort],
        result: str | ProductSort,
        body_builder: Callable[[tuple[Term, ...]], Term],
    ) -> Function:
        """Define an exact, closed function over built-in or owned row sorts."""

        checked_arguments = tuple(
            self._check_declared_sort(sort)
            for sort in arguments
        )
        checked_result = self._check_declared_sort(result)
        if not callable(body_builder):
            raise SmtError("defined function body builder must be callable")

        name = f"df_{self._next_symbol}"
        self._next_symbol += 1
        parameters = tuple(
            Term(
                sort,
                "symbol",
                atom=f"{name}_p{index}",
            )
            for index, sort in enumerate(checked_arguments)
        )
        body = body_builder(parameters)
        _require(body, checked_result)
        allowed = {_symbol_key(parameter) for parameter in parameters}
        unexpected = sorted(
            _free_symbols(body, self._owner) - allowed,
            key=lambda item: (item[1], item[0]),
        )
        if unexpected:
            rendered = ", ".join(
                f"{name}:{sort}"
                for sort, name in unexpected
            )
            raise SmtError(
                "defined function body contains free symbols outside its "
                f"parameters: {rendered}"
            )

        declaration = DefinitionDeclaration(
            name=name,
            parameters=parameters,
            result=checked_result,
            body=body,
            hint=hint,
        )
        self._declarations.append(declaration)
        return Function(
            name,
            checked_arguments,
            checked_result,
            self._owner,
        )

    def _check_declared_sort(self, sort: str | ProductSort) -> str:
        if isinstance(sort, ProductSort):
            if sort._owner is not self._owner:
                raise SmtError(
                    "product datatype belongs to another SMT script"
                )
            return sort.sort
        if sort in SORTS:
            return sort
        raise SmtError(
            f"SMT sort {sort!r} is not built-in; pass its owning "
            "ProductSort declaration"
        )

    def assert_term(self, term: Term) -> None:
        _require(term, BOOL)
        self._assertions.append(term)
        self._ordinary_assertions.append(term)

    def assert_obligation(self, term: Term) -> None:
        """Assert and mark the one predicate exact solver branches may replace."""

        if self._obligation_index is not None:
            raise SmtError("SMT script already has a marked proof obligation")
        self.assert_term(term)
        self._obligation_index = len(self._ordinary_assertions) - 1

    def assert_global(self, term: Term) -> None:
        """Assert an invariant that is independent of quantified plan choices.

        Family comparison deliberately rebinds bounded-choice symbols inside
        quantifiers.  A top-level assertion mentioning one of those symbols
        would constrain only its global valuation, not the rebound valuations.
        Keep domains and catalog invariants honest by rejecting that shape.
        """

        _require(term, BOOL)
        if _depends_on(term, set(self._quantified_choices)):
            raise SmtError(
                "global invariant depends on a quantified plan choice; "
                "that plan shape is not modeled"
            )
        self._assertions.append(term)
        self._global_assertions.append(term)

    def assert_choice_invariant(self, term: Term) -> None:
        """Assert an invariant for every legal bounded-choice valuation.

        Opaque scalar functions are deterministic globally, but their results
        can depend on relational choices rebound by family comparison.  Bind
        exactly those choices here and guard the invariant by their registered
        finite ranges.  Unrelated database symbols remain free, as required
        for a domain invariant over every bounded input instance.
        """

        _require(term, BOOL)
        dependencies = _dependencies(
            term,
            set(self._quantified_choices),
        )
        if not dependencies:
            self.assert_global(term)
            return
        choices = tuple(
            self._quantified_choices[key]
            for key in self._quantified_choices
            if key in dependencies
        )
        ranges = and_(
            *(
                and_(
                    not_(lt(choice, ZERO)),
                    lt(choice, int_value(bound)),
                )
                for choice, bound in choices
            )
        )
        guarded = forall(
            tuple(choice for choice, _ in choices),
            or_(not_(ranges), term),
        )
        self._assertions.append(guarded)
        self._global_assertions.append(guarded)

    def string_atom(self, value: str) -> Term:
        """Return one deferred rank constant for a concrete byte-string value."""

        if type(value) is not str:
            raise SmtError("string literal must be a Python string")
        try:
            value.encode("utf-8", errors="strict")
        except UnicodeEncodeError as error:
            raise SmtError("string literal is not valid Unicode/UTF-8") from error
        existing = self._string_literals.get(value)
        if existing is not None:
            return existing
        if self._string_universe is not None:
            raise SmtError("cannot register a new string literal after the order universe is sealed")
        result = self.fresh_constant(
            f"string_literal:{len(self._string_literals)}",
            INT,
        )
        self._string_literals[value] = result
        return result

    def register_string_term(self, term: Term) -> None:
        """Register one nonliteral string-generating root."""

        _require(term, INT)
        identity = id(term)
        if identity in self._string_terms:
            return
        if self._string_universe is not None:
            raise SmtError("cannot register a new string term after the order universe is sealed")
        # Retaining the value makes the identity key stable until exact
        # structural compaction at sealing.
        self._string_terms[identity] = term
        if len(self._string_terms) > MAX_REPRESENTATIVES:
            self._compact_string_terms()
            if len(self._string_terms) > MAX_REPRESENTATIVES:
                raise SmtError(
                    "string representative universe requires at least "
                    f"{len(self._string_terms)} ranks; "
                    f"limit is {MAX_REPRESENTATIVES}"
                )

    def register_quantified_choice(self, term: Term, bound: int) -> None:
        """Record one finite symbol that family comparison may quantify."""

        self.register_quantified_choices(((term, bound),))

    def register_quantified_choices(
        self,
        choices: Iterable[tuple[Term, int]],
    ) -> None:
        """Record finite symbols, auditing existing roots once for the batch."""

        pending: dict[SymbolKey, tuple[Term, int]] = {}
        for term, bound in choices:
            _require(term, INT)
            if term.operation != "symbol" or term.arguments:
                raise SmtError("quantified choice must be a named constant")
            if type(bound) is not int or bound <= 0:
                raise SmtError(
                    "quantified choice bound must be a positive integer"
                )
            key = _symbol_key(term)
            previous = self._quantified_choices.get(key, pending.get(key))
            if previous is not None:
                if previous[1] != bound:
                    raise SmtError(
                        "quantified choice has inconsistent bounds"
                    )
                continue
            pending[key] = (term, bound)
        if not pending:
            return
        keys = set(pending)
        if (
            self._string_universe is not None
            and _dependencies_many(self._string_terms.values(), keys)
        ):
            raise SmtError(
                "cannot register a quantified choice after a dependent "
                "string order universe is sealed"
            )
        if _dependencies_many(self._global_assertions, keys):
            raise SmtError(
                "global invariant depends on a quantified plan choice; "
                "that plan shape is not modeled"
            )
        self._quantified_choices.update(pending)

    def quantified_choice_bound(self, term: Term) -> int:
        """Return the immutable bound of a registered choice symbol."""

        _require(term, INT)
        if term.operation != "symbol" or term.arguments:
            raise SmtError("quantified choice must be a named constant")
        registered = self._quantified_choices.get(_symbol_key(term))
        if registered is None:
            raise SmtError("bounded choice is not registered with the SMT script")
        return registered[1]

    def quantified_choice_dependencies(
        self,
        terms: Iterable[Term],
    ) -> tuple[Term, ...]:
        """Return registered choices used by any term, in registration order."""

        keys = set(self._quantified_choices)
        roots = tuple(terms)
        if any(not isinstance(term, Term) for term in roots):
            raise SmtError("choice dependency root must be an SMT term")
        dependencies = _dependencies_many(roots, keys)
        return tuple(
            choice
            for key, (choice, _) in self._quantified_choices.items()
            if key in dependencies
        )

    def seal_string_order(self) -> None:
        """Fix literal ranks and bound every observed string term exactly once."""

        if self._string_universe is not None:
            return
        self._compact_string_terms()
        nonliteral_bound = self._string_nonliteral_bound()
        try:
            universe = StringOrderUniverse(
                self._string_literals,
                nonliteral_bound,
            )
        except ValueError as error:
            raise SmtError(f"cannot construct string order universe: {error}") from error
        self._string_universe = universe
        for value, term in self._string_literals.items():
            self.assert_global(eq(term, int_value(universe.rank(value))))
        if self._string_terms:
            upper = int_value(len(universe))
            for term in self._string_terms.values():
                self.assert_choice_invariant(
                    and_(
                        not_(lt(term, ZERO)),
                        lt(term, upper),
                    )
                )

    def _compact_string_terms(self) -> None:
        """Keep one registered root for every exact structural SMT term."""

        terms = tuple(self._string_terms.values())
        structural_terms: dict[int, Term] = {}
        for structural_id, term in zip(structural_ids(terms), terms):
            structural_terms.setdefault(structural_id, term)
        self._string_terms = {
            id(term): term
            for term in structural_terms.values()
        }

    def _string_nonliteral_bound(self) -> int:
        """Bound simultaneous string values across legal choice valuations."""

        total = 0
        choice_keys = set(self._quantified_choices)
        for term in self._string_terms.values():
            capacity = 1
            for key in _dependencies(term, choice_keys):
                bound = self._quantified_choices[key][1]
                if capacity > MAX_REPRESENTATIVES // bound:
                    capacity = MAX_REPRESENTATIVES + 1
                    break
                capacity *= bound
            total += capacity
            if total > MAX_REPRESENTATIVES:
                raise SmtError(
                    "string representative universe requires at least "
                    f"{total} ranks; limit is {MAX_REPRESENTATIVES}"
                )
        return total

    @property
    def string_literals(self) -> dict[int, str]:
        if self._string_universe is None:
            raise SmtError("string order universe is not sealed")
        return dict(enumerate(self._string_universe.representatives))

    @property
    def assertions(self) -> tuple[Term, ...]:
        return tuple(self._assertions)

    def render(
        self,
        values: Iterable[Term] = (),
        timeout_ms: int | None = None,
    ) -> str:
        self._check_render_timeout(timeout_ms)
        return self._render(values, None, timeout_ms)

    def render_branch(
        self,
        branch: Term,
        values: Iterable[Term] = (),
        timeout_ms: int | None = None,
    ) -> str:
        """Render one exact branch in place of the marked proof obligation."""

        _require(branch, BOOL)
        if self._obligation_index is None:
            raise SmtError("SMT script has no marked proof obligation")
        self._check_render_timeout(timeout_ms)
        return self._render(values, branch, timeout_ms)

    @staticmethod
    def _check_render_timeout(timeout_ms: int | None) -> None:
        if timeout_ms is not None and (
            type(timeout_ms) is not int or timeout_ms <= 0
        ):
            raise SmtError("render timeout must be a positive integer")

    def _render(
        self,
        values: Iterable[Term],
        branch: Term | None,
        timeout_ms: int | None,
    ) -> str:
        self.seal_string_order()
        requested = tuple(values)
        context = _RenderContext(
            declaration.name for declaration in self._declarations
        )
        ordinary_assertions = list(self._ordinary_assertions)
        if branch is not None:
            assert self._obligation_index is not None
            ordinary_assertions[self._obligation_index] = branch
        ordered_assertions = (
            *self._global_assertions,
            *ordinary_assertions,
        )
        for assertion in ordered_assertions:
            context.reserve(assertion)
        lines = ["; Generated by the YDB new-RBO bounded equivalence verifier."]
        effective_timeout = self.timeout_ms if timeout_ms is None else timeout_ms
        if effective_timeout is not None:
            lines.append(f"(set-option :timeout {effective_timeout})")
        lines.extend(["(set-option :produce-models true)", "(set-logic ALL)"])
        lines.extend(declaration.render() for declaration in self._declarations)
        lines.extend(
            f"(assert {_render_scope(assertion, context)})"
            for assertion in ordered_assertions
        )
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
