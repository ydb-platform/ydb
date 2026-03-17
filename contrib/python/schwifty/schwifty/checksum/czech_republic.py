from __future__ import annotations

from typing import ClassVar

from schwifty import checksum
from schwifty.domain import Component


@checksum.register("CZ", "SK")
class DefaultAlgorithm(checksum.Algorithm):
    name = "default"
    accepts: ClassVar[list[Component]] = [
        Component.BRANCH_CODE,
        Component.ACCOUNT_CODE,
    ]

    def compute(self, components: list[str]) -> str:
        return ""

    def validate(self, components: list[str], expected: str) -> bool:
        branch_code, account_code = components
        weights = [6, 3, 7, 9, 10, 5, 8, 4, 2, 1]

        d1 = checksum.weighted(branch_code, 11, weights[4:])
        d2 = checksum.weighted(account_code, 11, weights)
        return d1 == 0 and d2 == 0
