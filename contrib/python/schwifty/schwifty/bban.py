from __future__ import annotations

from dataclasses import dataclass
from random import Random
from typing import Any
from typing import cast

from rstr import Rstr

from schwifty import common
from schwifty import exceptions
from schwifty import registry
from schwifty.bic import BIC
from schwifty.checksum import algorithms
from schwifty.domain import Component


try:
    from typing import Self
except ImportError:
    from typing_extensions import Self


@dataclass
class Range:
    start: int = 0
    end: int = 0

    @property
    def length(self) -> int:
        return self.end - self.start

    @property
    def is_empty(self) -> bool:
        return self.start == 0 and self.end == 0

    def cut(self, s: str) -> str:
        return s[self.start : self.end]


def _get_bban_spec(country_code: str) -> dict[str, Any]:
    try:
        spec = registry.get("iban")
        assert isinstance(spec, dict)
        return spec[country_code]
    except KeyError as e:
        raise exceptions.InvalidCountryCode(f"Unknown country-code '{country_code}'") from e


def _get_position_range(spec: dict[str, Any], component_type: Component) -> Range:
    return Range(*spec.get("positions", {}).get(component_type, [0, 0]))


def _get_position_ranges(spec: dict[str, Any]) -> dict[Component, Range]:
    return {component: _get_position_range(spec, component) for component in Component}


def compute_national_checksum(country_code: str, components: dict[Component, str]) -> str:
    algo = algorithms.get(f"{country_code}:default")
    if algo is None:
        return ""

    return algo.compute([components[key] for key in algo.accepts])


class BBAN(common.Base):
    """The Basic Bank Account Number (BBAN).

    The format is decided by the national central bank or designated payment authority of each
    country.

    Examples:

        Most commonly :class:`.BBAN`-objects are created implicitly by the :class:`.IBAN`-class, but
        they can also be instantiated like so::

            >>> BBAN.from_components("DE", account_code="0532013000", bank_code="37040044")
            <BBAN=370400440532013000>

    Args:
        country_code (str): A two-letter ISO 3166-1 compliant country code
        value (str): The country specific BBAN value.

    .. versionadded:: 2024.01.1
    """

    def __new__(cls: type[Self], country_code: str, value: str, **kwargs: Any) -> Self:
        return super().__new__(cls, value, **kwargs)

    def __init__(self, country_code: str, value: str) -> None:
        self.country_code = country_code

    @classmethod
    def from_components(cls, country_code: str, **values: str) -> BBAN:
        """Generate a BBAN from its national components.

        The currently supported ``values`` are

            * ``bank_code``,
            * ``branch_code``
            * ``account_code``
            * ``account_id``
            * ``account_type``
            * ``account_holder_id``
            * ``currency_code``
            * ``national_checksum_digits``

        Args:
            country_code (str): The ISO 3166 alpha-2 country code.
            values: The country specific BBAN components.

        Raises:
            InvalidAccountCode: If the account code does not meet the national requirements.
        """
        spec: dict[str, Any] = _get_bban_spec(country_code)
        if "positions" not in spec:  # pragma: no cover
            raise exceptions.SchwiftyException(f"BBAN generation for {country_code} not supported")

        ranges = _get_position_ranges(spec)
        components: dict[Component, str] = {}
        for key, range_ in ranges.items():
            components[key] = common.clean(values.get(key, "")).zfill(range_.length)

        bank_code_length: int = ranges[Component.BANK_CODE].length
        branch_code_length: int = ranges[Component.BRANCH_CODE].length
        account_code_length: int = ranges[Component.ACCOUNT_CODE].length

        if len(components[Component.BANK_CODE]) == bank_code_length + branch_code_length:
            components[Component.BRANCH_CODE] = components[Component.BANK_CODE][
                bank_code_length : bank_code_length + branch_code_length
            ]
            components[Component.BANK_CODE] = components[Component.BANK_CODE][:bank_code_length]

        if len(components[Component.BANK_CODE]) > bank_code_length:
            raise exceptions.InvalidBankCode(f"Bank code exceeds maximum size {bank_code_length}")

        if len(components[Component.BRANCH_CODE]) > branch_code_length:
            raise exceptions.InvalidBranchCode(
                f"Branch code exceeds maximum size {branch_code_length}"
            )

        if len(components[Component.ACCOUNT_CODE]) > account_code_length:
            raise exceptions.InvalidAccountCode(
                f"Account code exceeds maximum size {account_code_length}"
            )

        checksum = compute_national_checksum(country_code, components)
        if checksum:
            components[Component.NATIONAL_CHECKSUM_DIGITS] = checksum

        bban = "0" * spec["bban_length"]
        for key, value in components.items():
            range_ = ranges[key]
            if range_.is_empty:
                continue
            bban = bban[: range_.start] + value + bban[range_.end :]

        return cls(country_code, bban)

    @classmethod
    def random(
        cls,
        country_code: str = "",
        random: Random | None = None,
        use_registry: bool = True,
        **values: str,
    ) -> BBAN:
        """Generate a random BBAN.

        With no further arguments a random bank from the registry will be selected as basis for the
        bank code and the BBAN structure. All other components, e.g. the account code will be
        generated with the alphabet allowed by the BBAN spec.

        If a ``country_code`` is provided the possible values will be limited to banks of the
        respective country. Additional components of the IBAN (e.g. the bank code) can be provided
        as keyword arguments to further narrow down the genreated values.

        If ``use_regsitry`` is set to ``False`` the bank information from schwifty's registry will
        be ignored and a completely random bank code will be generated.

        Args:
            country_code (str): The ISO 3166 alpha-2 country code.
            random (Random): An alternative random number generator.
            use_registry (bool): Select a random bank from the existing bank registry if available.
            values: The country specific BBAN components that should be taken as is and not be
                    generated.
        Raises:
            GenerateRandomOverflowError: If no valid random value can be gerated after multiple
                                         tries.
        """
        if random is None:
            random = Random()  # noqa: S311

        banks_by_country = cast(dict[str, Any], registry.get("country"))
        if not country_code:
            country_code = random.choice(list(banks_by_country.keys()))

        rstr = Rstr(random)
        spec = _get_bban_spec(country_code)
        bank: dict[str, Any] = {}
        if (banks := banks_by_country.get(country_code)) is not None and use_registry:
            bank = random.choice(banks)

        if "positions" not in spec:
            return cls(country_code, rstr.xeger(spec["regex"]).upper())

        ranges = _get_position_ranges(spec)
        for _ in range(100):
            bban = rstr.xeger(spec["regex"]).upper()
            components: dict[Component, str] = {}
            for key, range_ in ranges.items():
                if (value := values.get(key)) is not None:
                    components[key] = value
                else:
                    components[key] = bank.get(key) or spec.get(
                        f"default_{key.value}", range_.cut(bban)
                    )

            bank_code = components[Component.BANK_CODE]
            bank_code_length = ranges[Component.BANK_CODE].length
            branch_code_length = ranges[Component.BRANCH_CODE].length

            if len(bank_code) >= bank_code_length + branch_code_length:
                start = bank_code_length
                end = start + branch_code_length
                components[Component.BRANCH_CODE] = bank_code[start:end]

            for key, value in components.items():
                components[key] = value[: ranges[key].length]

            try:
                return cls.from_components(
                    country_code, **{key.value: value for key, value in components.items()}
                )
            except exceptions.SchwiftyException:
                pass
        else:
            raise exceptions.GenerateRandomOverflowError

    def validate_national_checksum(self) -> bool:
        """bool: Validate the national checksum digits.

        Raises:
            InvalidBBANChecksum: If the country specific BBAN checksum is invalid.
        """
        bank = self.bank or {}
        algo_name = bank.get("checksum_algo", "default")
        algo = algorithms.get(f"{self.country_code}:{algo_name}")
        if algo is None:
            return True
        components = [self._get_component(component) for component in algo.accepts]
        if not algo.validate(components, self.national_checksum_digits):
            raise exceptions.InvalidBBANChecksum("Invalid national checksum")
        return False

    def _get_component(self, component_type: Component) -> str:
        position = _get_position_range(self.spec, component_type)
        return self._get_slice(position.start, position.end)

    @property
    def spec(self) -> dict[str, Any]:
        """dict: The country specific BBAN specification."""
        return _get_bban_spec(self.country_code)

    @property
    def bic(self) -> BIC | None:
        """BIC or None: The BIC associated to the BBAN's bank-code.

        If the bank code is not available in schwifty's registry ``None`` is returned.
        """
        lookup_by = self.spec.get("bic_lookup_components", [Component.BANK_CODE])
        key = "".join(self._get_component(component) for component in lookup_by)
        try:
            return BIC.from_bank_code(self.country_code, key)
        except exceptions.SchwiftyException:
            return None

    @property
    def national_checksum_digits(self) -> str:
        """str: National checksum digits if available."""
        return self._get_component(Component.NATIONAL_CHECKSUM_DIGITS)

    @property
    def bank_code(self) -> str:
        """str: The country specific bank-code."""
        return self._get_component(Component.BANK_CODE)

    @property
    def branch_code(self) -> str:
        """str: The branch-code of the bank if available."""
        return self._get_component(Component.BRANCH_CODE)

    @property
    def account_code(self) -> str:
        """str: The domestic account-code"""
        return self._get_component(Component.ACCOUNT_CODE)

    @property
    def account_id(self) -> str:
        """str: Holder specific account identification.

        This is currently only available for Brazil.
        """
        return self._get_component(Component.ACCOUNT_ID)

    @property
    def account_type(self) -> str:
        """str: Account type specifier.

        This value is only available for Seychelles, Brazil and Bulgaria.
        """
        return self._get_component(Component.ACCOUNT_TYPE)

    @property
    def account_holder_id(self) -> str:
        """str: Account holder's national identification.

        This value is only available for Iceland.
        """
        return self._get_component(Component.ACCOUNT_HOLDER_ID)

    @property
    def currency_code(self) -> str:
        """str: The account's currency code.

        This value is only available for Mauretania, Seychelles and Guatemala.
        """
        return self._get_component(Component.CURRENCY_CODE)

    @property
    def bank(self) -> dict | None:
        """dict | None: The information of bank related to this BBANs bank code."""
        bank_registry = registry.get("bank_code")
        assert isinstance(bank_registry, dict)

        lookup_by = self.spec.get("bic_lookup_components", [Component.BANK_CODE])
        key = "".join(self._get_component(component) for component in lookup_by)

        bank_entry = bank_registry.get((self.country_code, key))
        if not bank_entry:
            return None
        return bank_entry and bank_entry[0]

    @property
    def bank_name(self) -> str | None:
        """str or None: The name of the bank associated with the IBAN bank code.

        Examples:
            >>> IBAN("DE89370400440532013000").bank_name
            'Commerzbank'
        """
        return None if self.bank is None else self.bank["name"]

    @property
    def bank_short_name(self) -> str | None:
        """str or None: The name of the bank associated with the IBAN bank code.

        Examples:
            >>> IBAN("DE89370400440532013000").bank_short_name
            'Commerzbank Köln'
        """
        return None if self.bank is None else self.bank["short_name"]


registry.build_index("bank", "country", key="country_code", accumulate=True)
