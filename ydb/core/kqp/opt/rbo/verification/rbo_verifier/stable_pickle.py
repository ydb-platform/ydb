"""Equality abstraction for reviewed StablePickle scalar encodings.

MiniKQL packing is injective within each admitted base type, including NULL;
required and present Optional values share the same bytes. Two left inverses
express injectivity with linear constraints. Actual byte contents, byte order,
and cross-type collisions remain abstract, so SAT candidates need runtime replay.
Every inverse constraint covers all legal rebound relational choices.
"""
from dataclasses import dataclass
from . import smt
from .types import STABLE_PICKLE_TYPES

TYPES = STABLE_PICKLE_TYPES

@dataclass(frozen=True)
class Functions:
    encode: smt.Function
    null: smt.Function
    value: smt.Function

class Kernel:
    def __init__(self, script):
        self.script = script
        self.functions = {}

    def encode(self, scalar_type, is_null, value):
        if scalar_type not in TYPES:
            raise smt.SmtError("StablePickle requires a reviewed scalar")
        sort = smt.BOOL if scalar_type == "Bool" else smt.INT
        if is_null.sort != smt.BOOL or value.sort != sort:
            raise smt.SmtError("StablePickle source terms have inconsistent sorts")
        funcs = self.functions.get(scalar_type)
        if funcs is None:
            funcs = Functions(
                self.script.fresh_function("stable_pickle:" + scalar_type, (smt.BOOL, sort), smt.INT),
                self.script.fresh_function("stable_unpickle_null:" + scalar_type, (smt.INT,), smt.BOOL),
                self.script.fresh_function("stable_unpickle_value:" + scalar_type, (smt.INT,), sort),
            )
            self.functions[scalar_type] = funcs
        absent = smt.FALSE if scalar_type == "Bool" else smt.ZERO
        canonical = smt.ite(is_null, absent, value)
        result = funcs.encode(is_null, canonical)
        self.script.assert_choice_invariant(smt.eq(funcs.null(result), is_null))
        self.script.assert_choice_invariant(smt.eq(funcs.value(result), canonical))
        self.script.register_string_term(result)
        return result
