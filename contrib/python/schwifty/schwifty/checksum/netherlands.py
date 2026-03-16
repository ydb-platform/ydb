from __future__ import annotations

from typing import ClassVar

from schwifty import checksum
from schwifty.domain import Component


class DefaultAlgorithm(checksum.Algorithm):
    """This algorithm is not used anymore for Dutch IBANs and is therefore not registered anymore.

    See https://www.betaalvereniging.nl/betaalproducten-en-diensten/iban/ibannext/ for more
    information
    """

    name = "default"
    accepts: ClassVar[list[Component]] = [Component.ACCOUNT_CODE]

    def compute(self, components: list[str]) -> str:
        # There is no actual check digit as part of the BBAN.
        return ""

    def validate(self, components: list[str], expected: str) -> bool:
        [account_code] = components
        return sum(int(digit) * (10 - i) for i, digit in enumerate(account_code)) % 11 == 0
