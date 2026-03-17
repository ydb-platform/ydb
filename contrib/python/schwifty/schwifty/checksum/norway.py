from __future__ import annotations

from typing import ClassVar

from schwifty import checksum
from schwifty.domain import Component
from schwifty.exceptions import InvalidAccountCode


@checksum.register("NO")
class DefaultAlgorithm(checksum.Algorithm):
    name = "default"
    accepts: ClassVar[list[Component]] = [
        Component.BANK_CODE,
        Component.ACCOUNT_CODE,
    ]

    def compute(self, components: list[str]) -> str:
        _, account_code = components
        value = account_code[2:] if account_code[:2] == "00" else "".join(components)

        weights = [5, 4, 3, 2, 7, 6, 5, 4, 3, 2]
        total: int = 0
        for n, c in zip(weights, value, strict=False):
            total += n * int(c)

        check_digit = 11 - (total % 11)
        if check_digit == 10:
            raise InvalidAccountCode("Check digit does not compute: Invalid account code.")
        return str(check_digit % 11)
