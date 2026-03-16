from __future__ import annotations

from itertools import cycle
from typing import ClassVar

from schwifty import checksum
from schwifty.domain import Component


@checksum.register("EE")
class DefaultAlgorithm(checksum.Algorithm):
    name = "default"
    accepts: ClassVar[list[Component]] = [
        Component.BRANCH_CODE,
        Component.ACCOUNT_CODE,
    ]

    def compute(self, components: list[str]) -> str:
        weights = cycle([7, 3, 1])
        digit = checksum.weighted(reversed("".join(components)), 10, weights)
        digit = digit if digit == 0 else 10 - digit
        return str(digit)
