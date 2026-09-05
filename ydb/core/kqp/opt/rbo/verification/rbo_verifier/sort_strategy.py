"""Sort encoding policy over measured costs and established certificates.

No relation, SMT term, or evaluator is needed here. The encodings themselves
remain responsible for constructing the exact tie-respecting sequence language.
"""

from dataclasses import dataclass
from typing import Literal, TypeAlias


Encoding: TypeAlias = Literal["auto", "enumerated", "ordinals", "unique", "network"]


@dataclass(frozen=True, slots=True)
class Costs:
    row_pairs: int
    comparators: int
    payload_cells: int
    key_columns: int
    unique_order: bool
    enumerated: bool
    trivial: bool
    merge_pairs: int = 0


@dataclass(frozen=True, slots=True)
class Limits:
    row_pairs: int
    comparators: int
    payload_cells: int
    key_columns: int

    def require_pairs(self, costs: Costs) -> None:
        if costs.row_pairs > self.row_pairs:
            raise ValueError(
                f"sort construction requires {costs.row_pairs} candidate-row pairs, "
                f"exceeding the {self.row_pairs} pair construction audit "
                f"bound; its exact sorting network requires {costs.comparators} "
                f"comparators, {costs.payload_cells} packed payload cells, and "
                f"{costs.key_columns} order columns, with limits "
                f"{self.comparators}, {self.payload_cells}, and {self.key_columns}"
            )


@dataclass(frozen=True, slots=True)
class Plan:
    encoding: str
    unique_order: bool


def choose(costs: Costs, limits: Limits, requested: Encoding = "auto") -> Plan:
    """Automatic and forced choices obey the same certificate/budget gates."""

    if requested not in {"auto", "enumerated", "ordinals", "unique", "network"}:
        raise ValueError(f"unknown sort encoding {requested!r}")
    if requested == "auto" and costs.trivial:
        return Plan("trivial", costs.unique_order)
    network_fits = (
        costs.comparators <= limits.comparators
        and costs.payload_cells <= limits.payload_cells
        and costs.key_columns <= limits.key_columns
    )
    if requested != "auto":
        if requested == "network":
            if not network_fits:
                raise ValueError("forced sorting network exceeds its construction audit bounds")
        else:
            limits.require_pairs(costs)
            if requested == "unique" and not costs.unique_order:
                raise ValueError("unique sort requires a certified complete order key")
            if requested == "enumerated" and not costs.enumerated:
                raise ValueError("enumerated sort requires one tiny outcome family")
        return Plan(requested, costs.unique_order)
    if network_fits and max(costs.row_pairs, costs.merge_pairs) > limits.row_pairs:
        return Plan("network", costs.unique_order)
    limits.require_pairs(costs)
    if costs.unique_order:
        return Plan("unique", True)
    return Plan("enumerated" if costs.enumerated else "ordinals", False)
