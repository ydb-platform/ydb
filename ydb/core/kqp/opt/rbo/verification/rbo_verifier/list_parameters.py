"""Exact membership-only external List inputs over the sealed String universe.

This is deliberately not a collection Value: list projection, order, length,
row sources and transformed collections remain outside the scalar grammar.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Mapping

from . import smt

LIST_TYPES = frozenset({"List<String>", "List<Optional<String>>"})
# Aggregate across all parameters, not one allowance per input. Refuse a
# partial model rather than reporting a counterexample with invented inputs.
MAX_MEMBERSHIP_CELLS = 16_384
MAX_WITNESS_BYTES = 16 << 20
MAX_DECODE_SECONDS = 2.0
MAX_PREPARE_SECONDS = 2.0


class ListWitnessError(smt.SmtError):
    """A complete realizable List witness cannot be constructed safely."""


@dataclass(slots=True)
class ListParameter:
    type: str
    has_non_null: smt.Term
    has_null: smt.Term
    member: smt.Term
    membership: smt.Function
    rank_values: tuple[smt.Term, ...] | None = None

    @classmethod
    def create(cls, script: smt.Script, name: str, parameter_type: str) -> ListParameter:
        if parameter_type not in LIST_TYPES:
            raise smt.SmtError("unsupported membership parameter type")
        has_non_null = script.fresh_constant(f"parameter:{name}:has_non_null", smt.BOOL)
        has_null = (script.fresh_constant(f"parameter:{name}:has_null", smt.BOOL)
                    if parameter_type == "List<Optional<String>>" else smt.FALSE)
        member = script.fresh_constant(f"parameter:{name}:member", smt.INT)
        script.register_string_term(member)
        membership = script.fresh_function(f"parameter:{name}:contains", (smt.INT,), smt.BOOL)
        # One registered rank suffices to realize nonempty unobserved lists.
        # H=false masks the otherwise unconstrained function to the empty set.
        script.assert_global(smt.or_(smt.not_(has_non_null), membership(member)))
        return cls(parameter_type, has_non_null, has_null, member, membership)

    def contains(self, is_null: smt.Term, value: smt.Term, ansi: bool) -> tuple[smt.Term, smt.Term]:
        matched = smt.and_(self.has_non_null, self.membership(value))
        truth = smt.and_(smt.not_(is_null), matched)
        if not ansi:
            return is_null, truth
        nonempty = smt.or_(self.has_non_null, self.has_null)
        unknown = smt.or_(smt.and_(is_null, nonempty),
                          smt.and_(smt.not_(is_null), smt.not_(matched), self.has_null))
        return unknown, truth


def prepare_witnesses(script: smt.Script, parameters: Mapping[str, ListParameter], *,
                      timeout_ms: int | None = None) -> tuple[smt.Term, ...]:
    if not parameters:
        return ()
    remaining = MAX_PREPARE_SECONDS if timeout_ms is None else min(MAX_PREPARE_SECONDS, timeout_ms / 1000)
    deadline = time.monotonic() + remaining

    def check_deadline() -> None:
        if time.monotonic() >= deadline:
            raise ListWitnessError("List parameter witness preparation deadline expired")

    check_deadline()
    # All table, scalar-input and finite-selector String roots have been
    # registered by this point. Never grow that universe while decoding.
    script.seal_string_order()
    ranks = script.string_literals
    if len(ranks) * len(parameters) > MAX_MEMBERSHIP_CELLS:
        raise ListWitnessError("List parameter witness membership-cell budget exceeded")
    if set(ranks) != set(range(len(ranks))):
        raise ListWitnessError("List parameter witness String ranks are not contiguous")
    # Bound the worst-case concrete payload before materializing the dense
    # graph; JSON escaping can use up to six bytes per character.
    encoded = 0
    for value in ranks.values():
        check_deadline()
        encoded += len(value.encode("utf-8"))
    if (6 * encoded + 4 * len(ranks)) * len(parameters) > MAX_WITNESS_BYTES:
        raise ListWitnessError("List parameter witness byte budget exceeded")
    result: list[smt.Term] = []
    for name, parameter in parameters.items():
        check_deadline()
        if parameter.rank_values is None:
            aliases: list[smt.Term] = []
            for rank in range(len(ranks)):
                check_deadline()
                # External names are unbounded source strings. Repeating one
                # in every declaration hint can bypass the dense witness byte
                # budget; the internal UF identifier is short and unique.
                alias = script.fresh_constant(f"list:{parameter.membership.name}:rank:{rank}", smt.BOOL)
                script.assert_global(smt.eq(alias, smt.and_(parameter.has_non_null,
                                                          parameter.membership(smt.int_value(rank)))))
                aliases.append(alias)
            parameter.rank_values = tuple(aliases)
        elif len(parameter.rank_values) != len(ranks):
            raise ListWitnessError("List parameter witness universe changed after sealing")
        result.extend((parameter.has_non_null, parameter.member))
        if parameter.has_null != smt.FALSE:
            result.append(parameter.has_null)
        result.extend(parameter.rank_values)
    check_deadline()
    return tuple(result)


def _value(term: smt.Term, values: Mapping[str, bool | int | str], exact_type: type):
    if term.operation in {"int", "bool"}:
        result = term.atom
    elif term.operation == "symbol" and term.atom in values:
        result = values[term.atom]
    else:
        raise ListWitnessError("solver omitted part of the complete List parameter witness")
    if type(result) is not exact_type:
        raise ListWitnessError("solver returned a malformed List parameter witness value")
    return result


def decode_lists(parameters: Mapping[str, ListParameter], values: Mapping[str, bool | int | str],
                 strings: Mapping[int, str]) -> dict[str, list[str | None]]:
    if not parameters:
        return {}
    deadline = time.monotonic() + MAX_DECODE_SECONDS
    if len(strings) * len(parameters) > MAX_MEMBERSHIP_CELLS:
        raise ListWitnessError("List parameter witness membership-cell budget exceeded")
    if set(strings) != set(range(len(strings))):
        raise ListWitnessError("List parameter witness String ranks are not contiguous")
    result: dict[str, list[str | None]] = {}
    byte_count = 0
    for name, parameter in parameters.items():
        aliases = parameter.rank_values
        if aliases is None or len(aliases) != len(strings):
            raise ListWitnessError("complete List parameter witness was not prepared")
        has_non_null = _value(parameter.has_non_null, values, bool)
        has_null = _value(parameter.has_null, values, bool)
        member = _value(parameter.member, values, int)
        if member not in strings or (has_null and parameter.type != "List<Optional<String>>"):
            raise ListWitnessError("List parameter witness violates its declared domain")
        concrete: list[str | None] = []
        present_ranks: set[int] = set()
        for rank, alias in enumerate(aliases):
            present = _value(alias, values, bool)
            if present:
                concrete.append(strings[rank])
                present_ranks.add(rank)
                byte_count += 6 * len(strings[rank].encode("utf-8")) + 4
            if byte_count > MAX_WITNESS_BYTES or time.monotonic() > deadline:
                raise ListWitnessError("List parameter witness decoding budget exceeded")
        if bool(concrete) != has_non_null or (has_non_null and member not in present_ranks):
            raise ListWitnessError("List parameter witness has inconsistent membership/nonemptiness")
        if has_null:
            concrete.append(None)
        result[name] = concrete
    return result
