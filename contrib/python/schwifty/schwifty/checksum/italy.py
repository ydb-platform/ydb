from __future__ import annotations

import string

from schwifty import checksum


def get_index(char: str) -> int:
    try:
        return string.digits.index(char)
    except ValueError:
        return string.ascii_uppercase.index(char.upper())


# Italy (IT)
# San Marino (SM)
@checksum.register("IT", "SM")
class DefaultAlgorithm(checksum.Algorithm):
    name = "default"

    def compute(self, components: list[str]) -> str:
        value = "".join(components)
        odds = (
            [1, 0, 5, 7, 9, 13, 15]  # noqa: RUF005
            + [17, 19, 21, 2, 4, 18, 20]
            + [11, 3, 6, 8, 12, 14, 16]
            + [10, 22, 25, 24, 23]
        )
        sum_ = 0
        for i, char in enumerate(value):
            if (i + 1) % 2 == 0:
                sum_ += get_index(char)
            else:
                sum_ += odds[get_index(char)]
        return string.ascii_uppercase[sum_ % 26]
