from __future__ import annotations

from typing import ClassVar

from schwifty import checksum
from schwifty.domain import Component


@checksum.register("IS")
class DefaultAlgorithm(checksum.Algorithm):
    name = "default"
    accepts: ClassVar[list[Component]] = [
        Component.ACCOUNT_HOLDER_ID,
    ]

    def compute(self, components: list[str]) -> str:
        [account_holder_id] = components
        weights = [3, 2, 7, 6, 5, 4, 3, 2]
        remainder = checksum.weighted(account_holder_id, 11, weights)
        return str(remainder) if remainder == 0 else str(11 - remainder)

    def validate(self, components: list[str], expected: str) -> bool:
        [account_holder_id] = components
        return self.compute(components) == account_holder_id[8]
