from __future__ import annotations

from schwifty import checksum


numerics = {
    "0": "0",
    "1": "1",
    "2": "2",
    "3": "3",
    "4": "4",
    "5": "5",
    "6": "6",
    "7": "7",
    "8": "8",
    "9": "9",
    "A": "1",
    "B": "2",
    "C": "3",
    "D": "4",
    "E": "5",
    "F": "6",
    "G": "7",
    "H": "8",
    "I": "9",
    "J": "1",
    "K": "2",
    "L": "3",
    "M": "4",
    "N": "5",
    "O": "6",
    "P": "7",
    "Q": "8",
    "R": "9",
    "S": "2",
    "T": "3",
    "U": "4",
    "V": "5",
    "W": "6",
    "X": "7",
    "Y": "8",
    "Z": "9",
}


def numerify(value: str) -> int:
    return int("".join(numerics[c] for c in value))


# France (FR)
# Monaco (MC)
@checksum.register("FR", "MC")
class DefaultAlgorithm(checksum.ISO7064_mod97_10):
    name = "default"

    def pre_process(self, components: list[str]) -> int:
        bank_code, branch_code, account_code = components
        return 89 * numerify(bank_code) + 15 * numerify(branch_code) + 3 * numerify(account_code)

    def post_process(self, r: int) -> int:
        return 97 - r
