"""Binary64 payloads and literal variance state transitions.

Arithmetic primitives are shared deterministic UFs over unsigned IEEE bit
patterns, not real arithmetic and not aggregate-sized black boxes. Only their
payload range is constrained: no commutativity, associativity or special-value
laws are assumed. Classification and comparisons are exact bit operations.

This kernel is for an explicit all-bits floating problem mode, never passive
Double identity tokens. Its arithmetic overapproximation must use a sufficient
all-schedules equality obligation; satisfiability is abstraction ambiguity, not
a runtime counterexample. Callers own NULL skipping, empty states, visitation
and flush/merge schedules. State methods below consume non-NULL values/states.
"""

from __future__ import annotations

from dataclasses import dataclass

from . import smt
from .types import integer_bounds


_SIGN = 1 << 63
_LIMIT = 1 << 64
_INFINITY = 0x7FF0000000000000
SEMANTIC_MODE = "binary64_uf_universal_v1"


@dataclass(frozen=True, slots=True)
class Binary64:
    bits: smt.Term

    def __post_init__(self) -> None:
        if not isinstance(self.bits, smt.Term) or self.bits.sort != smt.INT:
            raise smt.SmtError("binary64 payload must have integer sort")
        if self.bits.operation == "int" and not 0 <= self.bits.atom < _LIMIT:
            raise smt.SmtError("binary64 payload must be an unsigned 64-bit pattern")


def literal_bits(bits: int) -> Binary64:
    if type(bits) is not int or not 0 <= bits < _LIMIT:
        raise smt.SmtError("binary64 literal must be an unsigned 64-bit pattern")
    return Binary64(smt.int_value(bits))


ZERO = literal_bits(0)
ONE = literal_bits(0x3FF0000000000000)


def domain(value: Binary64) -> smt.Term:
    return smt.and_(smt.not_(smt.lt(value.bits, smt.ZERO)), smt.lt(value.bits, smt.int_value(_LIMIT)))


def is_nan(value: Binary64) -> smt.Term:
    return smt.lt(smt.int_value(_INFINITY), smt.mod(value.bits, _SIGN))


def _order_key(value: Binary64) -> smt.Term:
    magnitude = smt.mod(value.bits, _SIGN)
    return smt.ite(smt.lt(value.bits, smt.int_value(_SIGN)), magnitude, smt.sub(smt.ZERO, magnitude))


def equal(left: Binary64, right: Binary64) -> smt.Term:
    """Ordinary SQL equality: NaNs differ, signed zeros compare equal."""
    return smt.and_(smt.not_(is_nan(left)), smt.not_(is_nan(right)), smt.eq(_order_key(left), _order_key(right)))


def less(left: Binary64, right: Binary64) -> smt.Term:
    return smt.and_(smt.not_(is_nan(left)), smt.not_(is_nan(right)), smt.lt(_order_key(left), _order_key(right)))


def aggregate_less(left: Binary64, right: Binary64) -> smt.Term:
    """MiniKQL AggrLess: every NaN is a peer, after every non-NaN."""
    return smt.and_(smt.not_(is_nan(left)), smt.or_(is_nan(right), less(left, right)))


@dataclass(frozen=True, slots=True)
class VarianceState:
    mean: Binary64
    count: Binary64
    m2: Binary64

    def __post_init__(self) -> None:
        if not all(isinstance(value, Binary64) for value in (self.mean, self.count, self.m2)):
            raise smt.SmtError("binary64 variance state requires three bit payloads")


State = VarianceState


def state_terms(state: State) -> tuple[smt.Term, ...]:
    if isinstance(state, VarianceState):
        return state.mean.bits, state.count.bits, state.m2.bits
    raise smt.SmtError("unknown binary64 physical state layout")


class Kernel:
    """One primitive identity registry shared by every root and side."""

    def __init__(self, script: smt.Script) -> None:
        self.script = script
        self._functions: dict[str, smt.Function] = {}
        self._bounded: set[smt.Term] = set()

    def from_bits(self, bits: smt.Term) -> Binary64:
        """Admit a payload supplied by an explicit floating-mode boundary."""
        value = Binary64(bits)
        if bits not in self._bounded:
            self.script.assert_choice_invariant(domain(value))
            self._bounded.add(bits)
        return value

    def _apply(self, operation: str, *arguments: smt.Term) -> Binary64:
        function = self._functions.get(operation)
        if function is None:
            # An arbitrary Int UF modulo 2^64 represents exactly every
            # bit-producing function. Define the normalization once, avoiding
            # a fresh quantified range assertion over each growing state DAG.
            sorts = (smt.INT,) * len(arguments)
            raw = self.script.fresh_function(f"raw_binary64:{operation}", sorts, smt.INT)
            function = self.script.fresh_defined_function(
                f"binary64:{operation}", sorts, smt.INT,
                lambda parameters: smt.mod(raw(*parameters), _LIMIT),
            )
            self._functions[operation] = function
        bits = function(*arguments)
        self._bounded.add(bits)
        return Binary64(bits)

    def from_integer(self, value: smt.Term, scalar_type: str) -> Binary64:
        if integer_bounds(scalar_type) is None:
            raise smt.SmtError("binary64 conversion requires an admitted integer type")
        return self._apply(f"from_{scalar_type}", value)

    def add(self, left: Binary64, right: Binary64) -> Binary64:
        return self._apply("add", left.bits, right.bits)

    def sub(self, left: Binary64, right: Binary64) -> Binary64:
        return self._apply("sub", left.bits, right.bits)

    def mul(self, left: Binary64, right: Binary64) -> Binary64:
        return self._apply("mul", left.bits, right.bits)

    def div(self, left: Binary64, right: Binary64) -> Binary64:
        return self._apply("div", left.bits, right.bits)

    def sqrt(self, value: Binary64) -> Binary64:
        return self._apply("sqrt", value.bits)

    @staticmethod
    def variance_init(value: Binary64) -> VarianceState:
        return VarianceState(value, ONE, ZERO)

    def variance_update(self, state: VarianceState, value: Binary64) -> VarianceState:
        # aggregate.yqls variance_traits_factory_raw; preserve every rounding.
        delta = self.sub(value, state.mean)
        count = self.add(state.count, ONE)
        return VarianceState(
            self.add(state.mean, self.div(delta, count)),
            count,
            self.add(state.m2, self.div(self.mul(self.mul(delta, delta), state.count), count)),
        )

    def variance_merge(self, incoming: VarianceState, state: VarianceState) -> VarianceState:
        # BuildVarianceUpdateComputeFinal: incoming field is operand one.
        delta = self.sub(incoming.mean, state.mean)
        count = self.add(incoming.count, state.count)
        return VarianceState(
            self.div(self.add(self.mul(incoming.mean, incoming.count), self.mul(state.mean, state.count)), count),
            count,
            self.add(self.add(incoming.m2, state.m2), self.div(
                self.mul(self.mul(self.mul(delta, delta), incoming.count), state.count), count,
            )),
        )

    def stddev_sample_finish(self, state: VarianceState) -> Binary64:
        # In particular, the one-row runtime result is non-NULL NaN, not NULL.
        return self.sqrt(self.div(state.m2, self.sub(state.count, ONE)))
