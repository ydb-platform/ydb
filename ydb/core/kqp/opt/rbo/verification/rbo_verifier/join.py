"""Reference bag-join kernel, independent of plans and compact encodings.

Inputs are presence guards and SQL-TRUE pair conditions (keys AND residual).
An emission names the input occurrences whose payloads it retains. Missing
sides are NULL-extended only when present in the validated output schema.
This kernel neither allocates choices nor interprets provenance certificates.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from . import smt


Side = Literal["semi", "anti", "outer"]


@dataclass(frozen=True, slots=True)
class Shape:
    pairs: bool
    left: Side | None = None
    right: Side | None = None

    def candidate_count(self, left: int, right: int) -> int:
        return (
            int(self.pairs) * left * right
            + int(self.left is not None) * left
            + int(self.right is not None) * right
        )


_SHAPES = {
    "cross": Shape(True),
    "inner": Shape(True),
    "left": Shape(True, "outer"),
    "right": Shape(True, right="outer"),
    "full": Shape(True, "outer", "outer"),
    "left_semi": Shape(False, "semi"),
    "right_semi": Shape(False, right="semi"),
    "left_anti": Shape(False, "anti"),
    "right_anti": Shape(False, right="anti"),
    "exclusion": Shape(False, "outer", "outer"),
}


def shape(kind: str) -> Shape:
    try:
        return _SHAPES[kind]
    except KeyError as error:
        raise ValueError(f"unknown join kind {kind!r}") from error


@dataclass(frozen=True, slots=True)
class Emission:
    present: smt.Term
    left: int | None
    right: int | None


def reference_rows(
    layout: Shape,
    left: tuple[smt.Term, ...],
    right: tuple[smt.Term, ...],
    conditions: tuple[tuple[smt.Term, ...], ...],
) -> tuple[Emission, ...]:
    """Emit matching pairs, then left-only rows, then right-only rows.

    M(i,j) = present(Li) AND present(Rj) AND SQL_TRUE(condition(i,j)).
    Semi retains a row iff some M holds; anti/outer iff no M holds.
    Candidate slots, including false-guarded slots, retain bag multiplicity.
    """

    if len(conditions) != len(left) or any(len(row) != len(right) for row in conditions):
        raise ValueError("join conditions do not align with input rows")
    matches = tuple(
        tuple(smt.and_(lp, rp, conditions[i][j]) for j, rp in enumerate(right))
        for i, lp in enumerate(left)
    )
    rows = (
        [Emission(matches[i][j], i, j)
         for i in range(len(left)) for j in range(len(right))]
        if layout.pairs else []
    )
    for is_left, mode, presence in ((True, layout.left, left), (False, layout.right, right)):
        if mode is None:
            continue
        for index, present in enumerate(presence):
            side_matches = matches[index] if is_left else tuple(row[index] for row in matches)
            matched = smt.or_(*side_matches)
            retained = matched if mode == "semi" else smt.not_(matched)
            rows.append(
                Emission(
                    smt.and_(present, retained),
                    index if is_left else None,
                    None if is_left else index,
                )
            )
    return tuple(rows)
