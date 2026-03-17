"""Checksum algorithms for German Bank institutes.

A description of the algorithms can be found on the website of the Bundesbank

https://www.bundesbank.de/resource/blob/603320/16a80c739bbbae592ca575905975c2d0/mL/pruefzifferberechnungsmethoden-data.pdf
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import cycle
from typing import ClassVar

from schwifty import checksum
from schwifty.domain import Component
from schwifty.exceptions import InvalidBBANChecksum


ACCOUNT_CODE_LENGTH = 10

register = checksum.register("DE")


@dataclass
class Positions:
    start: int
    end: int
    check_digit: int


def digit_sum(number: int) -> int:
    return sum(int(d) for d in str(number))


class WeightedModulus(checksum.Algorithm):
    accepts: ClassVar[list[Component]] = [Component.ACCOUNT_CODE]
    minuend: ClassVar[int | None] = None
    modulus: ClassVar[int]
    positions: ClassVar[Positions]
    reverse: ClassVar[bool] = True
    weights: ClassVar[list[int]]

    def __init__(self) -> None:
        self.weighted_sum: int = 0
        self.remainder: int = 0

    def compute(self, components: list[str]) -> str:
        [account_code] = components
        digits = self.get_digits(self.adjust_input(account_code))
        self.remainder = self.compute_remainder(self.compute_weighted_sum(digits))
        if self.minuend is None:  # noqa: SIM108
            checksum = self.remainder
        else:
            checksum = self.minuend - self.remainder
        return str(self.reconcile(checksum))

    def adjust_input(self, account_code: str) -> str:
        return account_code

    def get_digits(self, account_code: str) -> str:
        positions = self.get_positions(account_code)
        # The positions are provided as in the specification, which starts counting at 1
        start, end = positions.start - 1, positions.end

        assert len(account_code) == ACCOUNT_CODE_LENGTH
        assert start >= 0 and start <= ACCOUNT_CODE_LENGTH  # noqa: PT018
        assert end >= start and end <= ACCOUNT_CODE_LENGTH  # noqa: PT018

        digits = account_code[start:end]
        if self.reverse:
            digits = digits[::-1]
        return digits

    def get_positions(self, account_code: str) -> Positions:
        return self.positions

    def compute_weighted_sum(self, digits: str) -> int:
        return sum(self.compute_summand(int(d), w) for d, w in zip(digits, cycle(self.weights)))

    def compute_summand(self, digit: int, weight: int) -> int:
        return digit * weight

    def compute_remainder(self, number: int) -> int:
        return number % self.modulus

    def reconcile(self, checksum: int) -> int:
        return 0 if checksum >= 10 else checksum

    def validate(self, components: list[str], expected: str) -> bool:
        account_code = self.adjust_input(components[0])
        check_digit = self.compute(components)
        positions = self.get_positions(account_code)
        return check_digit == account_code[positions.check_digit - 1]


class WeightedMod10(WeightedModulus):
    modulus: ClassVar[int] = 10
    minuend: ClassVar[int | None] = 10


class WeightedMod11(WeightedModulus):
    modulus: ClassVar[int] = 11
    minuend: ClassVar[int | None] = 11


@register
class Algorithm00(WeightedMod10):
    name: ClassVar[str] = "00"
    positions: ClassVar[Positions] = Positions(start=1, end=9, check_digit=10)
    weights: ClassVar[list[int]] = [2, 1]

    def compute_summand(self, digit: int, weight: int) -> int:
        return digit_sum(super().compute_summand(digit, weight))


@register
class Algorithm01(WeightedMod10):
    name: ClassVar[str] = "01"
    positions: ClassVar[Positions] = Positions(start=1, end=9, check_digit=10)
    weights: ClassVar[list[int]] = [3, 7, 1]


@register
class Algorithm02(WeightedMod11):
    name: ClassVar[str] = "02"
    positions: ClassVar[Positions] = Positions(start=1, end=9, check_digit=10)
    weights: ClassVar[list[int]] = [2, 3, 4, 5, 6, 7, 8, 9]

    def reconcile(self, checksum: int) -> int:
        if self.remainder == 0:
            return 0
        if self.remainder == 1:
            raise InvalidBBANChecksum(f"Invalid remaidner: {self.remainder}")
        return checksum


@register
class Algorithm03(Algorithm01):
    name = "03"
    weights: ClassVar[list[int]] = [2, 1]


@register
class Algorithm04(Algorithm02):
    name = "04"
    weights: ClassVar[list[int]] = [2, 3, 4, 5, 6, 7]


@register
class Algorithm05(Algorithm01):
    name = "05"
    weights: ClassVar[list[int]] = [7, 3, 1]


@register
class Algorithm06(WeightedMod11):
    name = "06"
    positions = Positions(start=1, end=9, check_digit=10)
    weights: ClassVar[list[int]] = [2, 3, 4, 5, 6, 7]


@register
class Algorithm07(Algorithm02):
    name = "07"
    weights: ClassVar[list[int]] = [2, 3, 4, 5, 6, 7, 8, 9, 10]


@register
class Algorithm08(Algorithm00):
    name = "08"
    min_account_code = 6000

    def compute(self, components: list[str]) -> str:
        [account_code] = components
        if int(account_code) < self.min_account_code:
            return ""
        return super().compute(components)

    def validate(self, components: list[str], expected: str) -> bool:
        [account_code] = components
        if int(account_code) < self.min_account_code:
            return True
        return super().validate(components, expected)


@register
class Algorithm09(checksum.Algorithm):
    name = "09"
    accepts: ClassVar[list[Component]] = [Component.ACCOUNT_CODE]

    def compute(self, components: list[str]) -> str:
        return ""

    def validate(self, components: list[str], expected: str) -> bool:
        return True


@register
class Algorithm10(Algorithm06):
    name = "10"
    weights: ClassVar[list[int]] = [2, 3, 4, 5, 6, 7, 8, 9, 10]


@register
class Algorithm11(Algorithm10):
    name = "11"

    def reconcile(self, checksum: int) -> int:
        if checksum == 10:
            return 9
        return checksum


@register
class Algorithm13(Algorithm00):
    name = "13"
    positions = Positions(start=2, end=7, check_digit=8)
    weights: ClassVar[list[int]] = [2, 1]


@register
class Algorithm14(Algorithm02):
    name = "14"
    positions = Positions(start=4, end=9, check_digit=10)
    weights: ClassVar[list[int]] = [2, 3, 4, 5, 6, 7]


@register
class Algorithm15(Algorithm06):
    name = "15"
    positions = Positions(start=6, end=9, check_digit=10)
    weights: ClassVar[list[int]] = [2, 3, 4, 5]


@register
class Algorithm16(Algorithm06):
    name = "16"
    positions = Positions(start=6, end=9, check_digit=10)
    weights: ClassVar[list[int]] = [2, 3, 4, 5, 6, 7]

    def validate(self, components: list[str], expected: str) -> bool:
        [account_code] = components
        check_digit = self.compute(components)
        if self.remainder == 1 and account_code[8] == account_code[9]:
            return True
        return check_digit == account_code[self.positions.check_digit - 1]


@register
class Algorithm17(WeightedMod11):
    name = "17"
    minuend: ClassVar[int | None] = 10
    positions = Positions(start=2, end=7, check_digit=8)
    reverse: ClassVar[bool] = False
    weights: ClassVar[list[int]] = [1, 2]

    def compute_weighted_sum(self, digits: str) -> int:
        return super().compute_weighted_sum(digits) - 1

    def compute_summand(self, digit: int, weight: int) -> int:
        return digit_sum(super().compute_summand(digit, weight))


@register
class Algorithm18(Algorithm01):
    name = "18"
    weights: ClassVar[list[int]] = [3, 9, 7, 1]


@register
class Algorithm19(Algorithm06):
    name = "19"
    weights: ClassVar[list[int]] = [2, 3, 4, 5, 6, 7, 8, 9, 1]


@register
class Algorithm20(Algorithm06):
    name = "20"
    weights: ClassVar[list[int]] = [2, 3, 4, 5, 6, 7, 8, 9, 3]


@register
class Algorithm21(Algorithm00):
    name = "21"
    weights: ClassVar[list[int]] = [2, 1]

    def compute_remainder(self, number: int) -> int:
        while number >= 10:
            number = digit_sum(number)
        return number


@register
class Algorithm22(Algorithm01):
    name = "22"
    weights: ClassVar[list[int]] = [3, 1]

    def compute_summand(self, digit: int, weight: int) -> int:
        return super().compute_summand(digit, weight) % 10


@register
class Algorithm23(Algorithm16):
    name = "23"
    positions = Positions(start=1, end=6, check_digit=7)
    weights: ClassVar[list[int]] = [2, 3, 4, 5, 6, 7]


@register
class Algorithm24(WeightedMod10):
    name = "24"
    minuend = None
    positions = Positions(start=1, end=9, check_digit=10)
    reverse = False
    weights: ClassVar[list[int]] = [1, 2, 3]

    def get_digits(self, account_code: str) -> str:
        digits = super().get_digits(account_code)
        if int(digits[0]) in {3, 4, 5, 6}:
            digits = digits[1:]
        elif int(digits[0]) == 9:
            digits = digits[3:]
        return digits.lstrip("0")

    def compute_summand(self, digit: int, weight: int) -> int:
        return (super().compute_summand(digit, weight) + weight) % 11


@register
class Algorithm25(WeightedMod11):
    name = "25"
    positions = Positions(start=2, end=9, check_digit=10)
    weights: ClassVar[list[int]] = [2, 3, 4, 5, 6, 7, 8, 9]

    def validate(self, components: list[str], expected) -> bool:
        result = super().validate(components, expected)
        [account_code] = components
        if self.remainder == 1 and account_code[1] not in {"8", "9"}:
            return False
        return result


@register
class Algorithm26(Algorithm06):
    name = "26"
    positions = Positions(start=1, end=7, check_digit=8)
    weights: ClassVar[list[int]] = [2, 3, 4, 5, 6, 7]

    def adjust_input(self, account_code: str) -> str:
        if account_code.startswith("00"):
            account_code = account_code[2:] + "00"
        return account_code


@register
class Algorithm28(Algorithm06):
    name = "28"
    positions = Positions(start=1, end=7, check_digit=8)
    weights: ClassVar[list[int]] = [2, 3, 4, 5, 6, 7, 8]


@register
class Algorithm32(Algorithm06):
    name = "32"
    positions = Positions(start=4, end=9, check_digit=10)
    weights: ClassVar[list[int]] = [2, 3, 4, 5, 6, 7]


@register
class Algorithm33(Algorithm06):
    name = "33"
    positions = Positions(start=5, end=9, check_digit=10)
    weights: ClassVar[list[int]] = [2, 3, 4, 5, 6]


@register
class Algorithm34(Algorithm06):
    name = "34"
    positions = Positions(start=1, end=7, check_digit=8)
    weights: ClassVar[list[int]] = [2, 4, 8, 5, 10, 9, 7]


@register
class Algorithm38(Algorithm06):
    name = "38"
    positions = Positions(start=4, end=9, check_digit=10)
    weights: ClassVar[list[int]] = [2, 4, 8, 5, 10, 9]


@register
class Algorithm60(Algorithm00):
    name = "60"
    positions = Positions(start=3, end=9, check_digit=10)


@register
class Algorithm61(Algorithm00):
    name = "61"
    positions = Positions(start=1, end=7, check_digit=8)
    weights: ClassVar[list[int]] = [2, 1]

    def get_digits(self, account_code: str) -> str:
        digits = super().get_digits(account_code)
        if account_code[8] == "8":
            digits = account_code[:7:-1] + digits
        return digits


@register
class Algorithm63(WeightedMod10):
    name = "63"
    positions = Positions(start=2, end=7, check_digit=8)
    weights: ClassVar[list[int]] = [2, 1]

    def compute_summand(self, digit: int, weight: int) -> int:
        return digit_sum(super().compute_summand(digit, weight))

    def validate(self, components: list[str], expected: str) -> bool:
        [account_code] = components
        if account_code[0] != "0":
            return False
        return super().validate(components, expected)


@register
class Algorithm68(Algorithm00):
    name = "68"
    positions = Positions(start=1, end=9, check_digit=10)
    weights: ClassVar[list[int]] = [2, 1]

    def get_digits(self, account_code: str) -> str:
        digits = super().get_digits(account_code)
        # The digits are already reversed at this point. The algorithm counts the positions from
        # right to left, so that the 7th position is at index 5, when we consider that the 1st
        # position has already been excluded because it is the check digit.
        digits = digits.rstrip("0")
        if len(digits) == 9:
            if digits[5] != "9":
                raise InvalidBBANChecksum(
                    "10 digit long acccount codes require the 7th digit to be set to 9"
                )
            digits = digits[:6]
        return digits

    def validate(self, components: list[str], expected: str) -> bool:
        [account_code] = components
        if 400_000_000 <= int(account_code) <= 499_999_999:
            return True
        if super().validate(components, expected) is False:
            # If the checksum calculation fails, the algorithm should be executed again, with the
            # 7th and 8th position of the account code removed. Since the positions are counted from
            # right to left, they translate into indices 2 and 3 counting from left.
            check_digit = self.compute([account_code[:2] + "00" + account_code[4:]])
            return check_digit == account_code[self.positions.check_digit - 1]
        return True


@register
class Algorithm76(WeightedMod11):
    name = "76"
    minuend = None
    positions = Positions(start=2, end=7, check_digit=8)
    weights: ClassVar[list[int]] = [2, 3, 4, 5, 6, 7, 8]

    def get_digits(self, account_code: str) -> str:
        digits = super().get_digits(account_code)
        return digits.rstrip("0")

    def validate(self, components: list[str], expected: str) -> bool:
        [account_code] = components
        if int(account_code[0]) not in {0, 4, 6, 7, 8, 9}:
            return False
        return super().validate(components, expected)


@register
class Algorithm88(Algorithm06):
    name = "88"
    positions = Positions(start=4, end=9, check_digit=10)
    weights: ClassVar[list[int]] = [2, 3, 4, 5, 6, 7, 8]

    def get_positions(self, account_code: str) -> Positions:
        if account_code[2] == "9":
            return Positions(start=3, end=9, check_digit=10)
        return super().get_positions(account_code)


@register
class Algorithm91(checksum.Algorithm):
    name = "91"
    accepts: ClassVar[list[Component]] = [Component.ACCOUNT_CODE]

    class Variant1(Algorithm06):
        positions = Positions(start=1, end=6, check_digit=7)
        weights: ClassVar[list[int]] = [2, 3, 4, 5, 6, 7]

    class Variant2(Variant1):
        weights: ClassVar[list[int]] = [7, 6, 5, 4, 3, 2]

    class Variant3(Variant1):
        positions = Positions(start=1, end=10, check_digit=7)
        weights: ClassVar[list[int]] = [2, 3, 4, 0, 5, 6, 7, 8, 9, 10]

    class Variant4(Variant1):
        weights: ClassVar[list[int]] = [2, 4, 8, 5, 10, 9]

    def compute(self, components: list[str]) -> str:
        return self.Variant1().compute(components)

    def validate(self, components: list[str], expected: str) -> bool:
        for algo_cls in [self.Variant1, self.Variant2, self.Variant3, self.Variant4]:
            if algo_cls().validate(components, expected):
                return True
        return False


@register
class Algorithm99(Algorithm06):
    name = "99"

    def validate(self, components: list[str], expected: str) -> bool:
        [account_code] = components
        if account_code in {"0499999999", "0396000000"}:
            return True
        return super().validate(components, expected)
