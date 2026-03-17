from __future__ import annotations

import re
from random import Random
from typing import Any
from typing import TYPE_CHECKING

from pycountry import countries  # type: ignore
from pycountry.db import Data  # type: ignore

from schwifty import common
from schwifty import exceptions
from schwifty import registry
from schwifty.bban import BBAN
from schwifty.bic import BIC
from schwifty.checksum import ISO7064_mod97_10
from schwifty.checksum import numerify


if TYPE_CHECKING:
    from pydantic import GetCoreSchemaHandler
    from pydantic import GetJsonSchemaHandler
    from pydantic import ValidatorFunctionWrapHandler
    from pydantic.json_schema import JsonSchemaValue
    from pydantic_core.core_schema import CoreSchema
    from pydantic_core.core_schema import NoInfoWrapValidatorFunction


_spec_to_re: dict[str, str] = {"n": r"\d", "a": r"[A-Z]", "c": r"[A-Za-z0-9]", "e": r" "}


class IBAN(common.Base):
    """The IBAN object.

    Examples:

        You create a new IBAN object by supplying an IBAN code in text form. The IBAN
        is validated behind the scenes and you can then access all relevant components
        as properties::

            >>> iban = IBAN('DE89 3704 0044 0532 0130 00')
            >>> iban.account_code
            '0532013000'
            >>> iban.bank_code
            '37040044'
            >>> iban.country_code
            'DE'
            >>> iban.checksum_digits
            '89'
            >>> iban.bban
            <BBAN=370400440532013000>


    Args:
        iban (str): The IBAN code.
        allow_invalid (bool): If set to `True` IBAN validation is skipped on instantiation.
        validate_bban (bool): If set to `True` also check the country specific checksum of the BBAN.

    Raises:
        InvalidStructure: If the IBAN contains invalid characters or the BBAN does not match the
                          country specific format.
        InvalidChecksumDigits: If the IBAN's checksum is invalid.
        InvalidBBANChecksum: If the country specific BBAN checksum is invalid and `validate_bban`
                             was set to `True`.
        InvalidLength: If the length does not match the country specific specification.

    .. versionchanged:: 2021.05.1
        Added the `validate_bban` parameter that controls if the country specific checksum within
        the BBAN is also validated.
    .. versionchanged:: 2023.10.0
        The :class:`.IBAN` is now a subclass of :class:`str` and supports all its methods.
    .. versionchanged:: 2024.01.1
        Added the :attr:`.bban`-attribute that provideds all country specific account and bank
        information.
    """

    def __init__(self, iban: str, allow_invalid: bool = False, validate_bban: bool = False) -> None:
        super().__init__()
        self.bban = BBAN(self.country_code, self._get_slice(start=4))
        if not allow_invalid:
            self.validate(validate_bban)

    @classmethod
    def from_bban(
        cls,
        country_code: str,
        bban: str | BBAN,
        allow_invalid: bool = False,
        validate_bban: bool = False,
    ) -> IBAN:
        """Create an IBAN from a given BBAN.

        This will automatically calculate the IBAN checksum digits.

        Args:
            country_code (str): The ISO 3166 alpha-2 country code.
            bban (str or BBAN): The national Basic Bank Account Number.
            allow_invalid (bool): If set to `True` IBAN validation is skipped on instantiation.
            validate_bban (bool): If set to `True` also check the country specific checksum.

        .. versionadded:: 2024.01.2
        """
        checksum_algo = ISO7064_mod97_10()
        return cls(
            country_code + checksum_algo.compute([bban, country_code]) + bban,
            allow_invalid=allow_invalid,
            validate_bban=validate_bban,
        )

    @classmethod
    def generate(
        cls,
        country_code: str,
        bank_code: str,
        account_code: str,
        branch_code: str = "",
        **kwargs: Any,
    ) -> IBAN:
        """Generate an IBAN from it's components.

        If the bank-code and/or account-number have less digits than required by their
        country specific representation, the respective component is padded with zeros.

        Examples:

            To generate an IBAN do the following::

                >>> bank_code = '37040044'
                >>> account_code = '532013000'
                >>> iban = IBAN.generate('DE', bank_code, account_code)
                >>> iban.formatted
                'DE89 3704 0044 0532 0130 00'

        Args:
            country_code (str): The ISO 3166 alpha-2 country code.
            bank_code (str): The country specific bank-code.
            account_code (str): The customer specific account-code.
            kwargs (str): Additional country specific fields.

        Raises:
            InvalidAccountCode: If the account code does not meet the national requirements.

        .. versionchanged:: 2020.08.3
            Added the `branch_code` parameter to allow the branch code (or sort code) to be
            specified independently.

        .. versionchanged:: 2021.05.2
            Added support for generating the country specific checksum of the BBAN for Belgian
            banks.
        """
        return cls.from_bban(
            country_code,
            BBAN.from_components(
                country_code,
                bank_code=bank_code,
                branch_code=branch_code,
                account_code=account_code,
                **kwargs,
            ),
        )

    @classmethod
    def random(
        cls,
        country_code: str = "",
        random: Random | None = None,
        use_registry: bool = True,
        **values: str,
    ) -> IBAN:
        """Generate a random IBAN.

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
        bban = BBAN.random(country_code, random=random, use_registry=use_registry, **values)
        return cls.from_bban(bban.country_code, bban)

    def validate(self, validate_bban: bool = False) -> bool:
        """Validate the structural integrity of this IBAN.

        This function will verify the country specific format as well as the Luhn checksum in the
        3rd and 4th position of the IBAN. For some countries (currently Belgium, Germany and Italy)
        it will also verify the correctness of the country specific checksum within the BBAN if the
        `validate_bban` parameter is set to `True`. For German banks it will pick the appropriate
        algorithm based on the bank code and verify that the account code has the correct checksum.

        Note:
            You have to use the `allow_invalid` paramter when constructing the :class:`IBAN`-object
            to circumvent the implicit validation.

        Raises:
            InvalidStructure: If the IBAN contains invalid characters or the BBAN does not match the
                              country specific format.
            InvalidChecksumDigits: If the IBAN's checksum is invalid.
            InvalidBBANChecksum: If the country specific BBAN checksum is invalid.
            InvalidLength: If the length does not match the country specific specification.

        .. versionchanged:: 2021.05.1
            Added the `validate_bban` parameter that controls if the country specific checksum
            within the BBAN is also validated.
        """
        self._validate_characters()
        self._validate_length()
        self._validate_format()
        self._validate_iban_checksum()
        if validate_bban:
            self.bban.validate_national_checksum()
        return True

    def _validate_characters(self) -> None:
        if not re.match(r"[A-Z]{2}\d{2}[A-Z]*", self):
            raise exceptions.InvalidStructure(f"Invalid characters in IBAN {self!s}")

    def _validate_length(self) -> None:
        if self.spec["iban_length"] != len(self):
            raise exceptions.InvalidLength("Invalid IBAN length")

    def _validate_format(self) -> None:
        if not self.spec["regex"].match(self.bban):
            raise exceptions.InvalidStructure(
                f"Invalid BBAN structure: '{self.bban}' doesn't match '{self.spec['bban_spec']}'"
            )

    def _validate_iban_checksum(self) -> None:
        checksum_algo = ISO7064_mod97_10()
        if self.numeric % 97 != 1 or not checksum_algo.validate(
            [self.bban, self.country_code], self.checksum_digits
        ):
            raise exceptions.InvalidChecksumDigits("Invalid checksum digits")

    @property
    def is_valid(self) -> bool:
        """bool: Indicate if this is a valid IBAN.

        Note:
            You have to use the `allow_invalid` paramter when constructing the :class:`IBAN`-object
            to circumvent the implicit validation.

        Examples:
            >>> IBAN("AB1234567890", allow_invalid=True).is_valid
            False

        .. versionadded:: 2020.08.1
        """
        try:
            return self.validate()
        except exceptions.SchwiftyException:
            return False

    @property
    def numeric(self) -> int:
        """int: A numeric represenation of the IBAN."""
        return numerify(self.bban + self[:4])

    @property
    def formatted(self) -> str:
        """str: The IBAN formatted in blocks of 4 digits."""
        return " ".join(self[i : i + 4] for i in range(0, len(self), 4))

    @property
    def spec(self) -> dict[str, Any]:
        """dict: The country specific IBAN specification."""
        try:
            spec = registry.get("iban")
            assert isinstance(spec, dict)
            return spec[self.country_code]
        except KeyError as e:
            raise exceptions.InvalidCountryCode(
                f"Unknown country-code '{self.country_code}'"
            ) from e

    @property
    def bic(self) -> BIC | None:
        """BIC or None: The BIC associated to the IBAN's bank-code.

        If the bank code is not available in schwifty's registry ``None`` is returned.

        .. versionchanged:: 2020.08.1
            Returns ``None`` if no appropriate :class:`BIC` can be constructed.
        """
        return self.bban.bic

    @property
    def country(self) -> Data | None:
        """Country: The country this IBAN is registered in."""
        return countries.get(alpha_2=self.country_code)

    @property
    def in_sepa_zone(self) -> bool:
        """bool: Is the country in the Single Euro Payments Area (SEPA) zone.

        .. versionadded:: 2024.01.1
        """
        return self.spec["in_sepa_zone"]

    @property
    def country_code(self) -> str:
        """str: ISO 3166 alpha-2 country code."""
        return self._get_slice(start=0, end=2)

    @property
    def checksum_digits(self) -> str:
        """str: Two digit checksum of the BBAN."""
        return self._get_slice(start=2, end=4)

    @property
    def national_checksum_digits(self) -> str:
        """str: National checksum digits.

        .. versionadded:: 2024.01.1
        """
        return self.bban.national_checksum_digits

    @property
    def bank_code(self) -> str:
        """str: The country specific bank-code."""
        return self.bban.bank_code

    @property
    def branch_code(self) -> str:
        """str: The branch-code of the bank if available."""
        return self.bban.branch_code

    @property
    def account_code(self) -> str:
        """str: The domestic account-code"""
        return self.bban.account_code

    @property
    def account_id(self) -> str:
        """str: Holder specific account identification.

        This is currently only available for Brazil.
        """
        return self.bban.account_id

    @property
    def account_type(self) -> str:
        """str: Account type specifier.

        This value is only available for Seychelles, Brazil and Bulgaria.
        """
        return self.bban.account_type

    @property
    def account_holder_id(self) -> str:
        """str: Account holder's national identification.

        This value is only available for Iceland.
        """
        return self.bban.account_holder_id

    @property
    def currency_code(self) -> str:
        """str: The account's currency code.

        This value is only available for Mauretania, Seychelles and Guatemala.
        """
        return self.bban.currency_code

    @property
    def bank(self) -> dict | None:
        """dict or None: The information of the bank related to the bank code as part of the BBAN"""
        return self.bban.bank

    @property
    def bank_name(self) -> str | None:
        """str or None: The name of the bank associated with the IBAN bank code.

        Examples:
            >>> IBAN("DE89370400440532013000").bank_name
            'Commerzbank'

        .. versionadded:: 2022.04.2
        """
        return self.bban.bank_name

    @property
    def bank_short_name(self) -> str | None:
        """str or None: The name of the bank associated with the IBAN bank code.

        Examples:
            >>> IBAN("DE89370400440532013000").bank_short_name
            'Commerzbank Köln'

        .. versionadded:: 2022.04.2
        """
        return self.bban.bank_short_name

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source: type[Any], handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        from pydantic_core import core_schema

        return core_schema.lax_or_strict_schema(
            cls._get_pydantic_schema(allow_invalid=True),
            cls._get_pydantic_schema(allow_invalid=False),
            strict=True,
        )

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: CoreSchema, handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        json_schema = handler(core_schema)
        json_schema = handler.resolve_ref_schema(json_schema)
        json_schema["title"] = "IBAN"
        return json_schema

    @classmethod
    def _get_pydantic_schema(
        cls,
        *,
        allow_invalid: bool = False,
    ) -> CoreSchema:
        from pydantic_core import core_schema

        return core_schema.no_info_wrap_validator_function(
            cls._get_pydantic_validate_func(allow_invalid=allow_invalid),
            core_schema.union_schema(
                [
                    core_schema.is_instance_schema(IBAN),
                    core_schema.no_info_plain_validator_function(IBAN),
                    core_schema.str_schema(max_length=34),
                ]
            ),
            serialization=core_schema.to_string_ser_schema(
                when_used="json-unless-none",
            ),
        )

    @classmethod
    def _get_pydantic_validate_func(
        cls,
        *,
        allow_invalid: bool = False,
    ) -> NoInfoWrapValidatorFunction:
        def validate(value: Any, handler: ValidatorFunctionWrapHandler) -> Any:
            from pydantic_core import PydanticCustomError

            try:
                iban = cls(value, allow_invalid=allow_invalid)
            except (exceptions.SchwiftyException, TypeError) as err:
                raise PydanticCustomError("iban_format", "{err}", {"err": str(err)}) from err
            return handler(iban)

        return validate


def add_bban_regex(country: str, spec: dict) -> dict:
    if "regex" not in spec:
        spec["regex"] = re.compile(convert_bban_spec_to_regex(spec["bban_spec"]))
    return spec


def convert_bban_spec_to_regex(spec: str) -> str:
    spec_re = rf"(\d+)(!)?([{''.join(_spec_to_re.keys())}])"

    def convert(match: re.Match) -> str:
        quantifier = ("{{{}}}" if match.group(2) else "{{1,{}}}").format(match.group(1))
        return _spec_to_re[match.group(3)] + quantifier

    return rf"^{re.sub(spec_re, convert, spec)}$"


registry.manipulate("iban", add_bban_regex)
