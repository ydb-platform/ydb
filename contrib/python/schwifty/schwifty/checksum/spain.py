from __future__ import annotations

from typing import ClassVar

from schwifty import checksum
from schwifty.domain import Component


def reconcile(n: int) -> int:
    if n == 11:
        return 0
    if n == 10:
        return 1
    return n


@checksum.register("ES")
class DefaultAlgorithm(checksum.Algorithm):
    name = "default"
    accepts: ClassVar[list[Component]] = [
        Component.BANK_CODE,
        Component.BRANCH_CODE,
        Component.ACCOUNT_CODE,
    ]

    def compute(self, components: list[str]) -> str:
        bank_code, branch_code, account_code = components
        weights = [1, 2, 4, 8, 5, 10, 9, 7, 3, 6]
        d1 = reconcile(11 - checksum.weighted(bank_code + branch_code, 11, weights[2:]))
        d2 = reconcile(11 - checksum.weighted(account_code, 11, weights))
        return f"{d1}{d2}"
