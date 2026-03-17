from __future__ import annotations

import re
import warnings
from operator import itemgetter
from typing import Any
from typing import TYPE_CHECKING

from pycountry import countries  # type: ignore
from pycountry.db import Data  # type: ignore

from schwifty import common
from schwifty import exceptions
from schwifty import registry


if TYPE_CHECKING:
    from pydantic import GetCoreSchemaHandler
    from pydantic import GetJsonSchemaHandler
    from pydantic import ValidatorFunctionWrapHandler
    from pydantic.json_schema import JsonSchemaValue
    from pydantic_core import CoreSchema

_bic_iso9362_re = re.compile(r"[A-Z0-9]{4}[A-Z]{2}[A-Z0-9]{2}(?:[A-Z0-9]{3})?")
_bic_swift_re = re.compile(r"[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}(?:[A-Z0-9]{3})?")


class BIC(common.Base):
    """The BIC object.

    When creating an instance of a BIC the provided value is checked for correctnes, unless the
    ``allow_invalid`` parameter is set to ``True``.

    The validation is done in the context of ISO 9362:2022, which allows numbers in the business
    party prefix (the first 4 chracters). If strict SWIFT compliance is required the
    ``enforce_swift_compliance`` parameter can be set to ``True``, in which case numbers are
    disallowed as described in `revision 2023`_ of the SWIFT BIC policy.

    Examples:

        You can either create a new BIC object by providing a code as text::

            >>> bic = BIC('GENODEM1GLS')
            >>> bic.country_code
            'DE'
            >>> bic.location_code
            'M1'
            >>> bic.bank_code
            'GENO'

        or by using the :meth:`from_bank_code` classmethod::

            >>> bic = BIC.from_bank_code('DE', '43060967')
            >>> bic.formatted
            'GENO DE M1 GLS'

    Args:
        bic (str): The BIC value.
        allow_invalid (bool): If set to ``True`` validation is skipped on instantiation.
        enforce_swift_commpliance (bool): If set to ``True`` the stricter SWIFT BIC policy is
                                          applied.

    Raises:
        InvalidLength: If the BIC's length is not 8 or 11 characters long.
        InvalidStructure: If the BIC contains unexpected characters.
        InvalidCountryCode: If the BIC's country code is unknown.

    .. versionchanged:: 2023.10.0
        The :class:`.BIC` is now a subclass of :class:`str` and supports all its methods.

    .. versionchanged:: 2023.11.0
        The validation of the :class:`.BIC` structure is now allowing numbers in the business party
        prefix, conforming to ISO 9362:2022. If strict SWIFT compliance should be ensured, the
        ``enforce_swift_compliance`` parameter can be set to ``True``, which will then fall back to
        the previous validation method.

    .. _revision 2023: https://www2.swift.com/knowledgecentre/rest/v1/publications/bic_policy/3.0/bic_policy.pdf
    """

    def __init__(
        self,
        bic: str,
        allow_invalid: bool = False,
        enforce_swift_compliance: bool = False,
    ) -> None:
        super().__init__()
        if not allow_invalid:
            self.validate(enforce_swift_compliance)

    @classmethod
    def candidates_from_bank_code(cls, country_code: str, bank_code: str) -> list[BIC]:
        """Create a list of potential BIC objects from country-code and domestic bank-code.

        Examples:
            >>> bic_codes = BIC.candidates_from_bank_code("FR", "30004")
            >>> bic_codes # doctest: +NORMALIZE_WHITESPACE
            [<BIC=BNPAFRPPIFN>, <BIC=BNPAFRPPPAA>, <BIC=BNPAFRPPMED>, \
             <BIC=BNPAFRPPCRN>, <BIC=BNPAFRPP>, <BIC=BNPAFRPPPAE>, <BIC=BNPAFRPPPBQ>,
             <BIC=BNPAFRPPNFE>, <BIC=BNPAFRPPPGN>, <BIC=BNPAFRPPXXX>, <BIC=BNPAFRPPBOR>,
             <BIC=BNPAFRPPCRM>, <BIC=BNPAFRPPPVD>, <BIC=BNPAFRPPPTX>, <BIC=BNPAFRPPPAC>,
             <BIC=BNPAFRPPPLZ>, <BIC=BNPAFRPP039>, <BIC=BNPAFRPPENG>, <BIC=BNPAFRPPNEU>,
             <BIC=BNPAFRPPORE>, <BIC=BNPAFRPPPEE>, <BIC=BNPAFRPPPXV>, <BIC=BNPAFRPPIFO>]

            >>> BIC.candidates_from_bank_code("DE", "20070024")
            [<BIC=DEUTDEDBHAM>]

            >>> BIC.candidates_from_bank_code("DE", "01010101")
            Traceback (most recent call last):
            ...
            InvalidBankCode: Unknown bank code '01010101' for country 'DE'


        Args:
            country_code (str): ISO 3166 alpha2 country-code.
            bank_code (str): Country specific bank-code.

        Returns:
            list[BIC]: a list of BIC objects generated from the given country code and bank code.

        Raises:
            InvalidBankCode: If the given bank code wasn't found in the registry

        Note:
            This currently only works for selected countries. Amongst them

            * Andorra
            * Austria
            * Belgium
            * Bosnia and Herzegovina
            * Bulgaria
            * Costa Rica
            * Croatia
            * Czech Republic
            * Cyprus
            * Denmark
            * Estonia
            * Finland
            * France
            * Germany
            * Greece
            * Hungary
            * Ireland
            * Iceland
            * Italy
            * Israel
            * Kazakhstan
            * Latvia
            * Liechtenstein
            * Lithuania
            * Luxembourg
            * Moldova
            * Monaco
            * Netherlands
            * Norway
            * Poland
            * Portugal
            * Romania
            * Saudi Arabia
            * Serbia
            * Slovakia
            * Slovenia
            * South Africa
            * Spain
            * Sweden
            * Switzerland
            * Turkiye
            * Ukraine
            * United Arab Emirates
            * United Kingdom
        """
        try:
            index = registry.get("bank_code")
            assert isinstance(index, dict)
            banks = sorted(
                index[(country_code, bank_code)], key=itemgetter("primary"), reverse=True
            )
            return [cls(entry["bic"]) for entry in banks if entry["bic"]]
        except KeyError as e:
            raise exceptions.InvalidBankCode(
                f"Unknown bank code {bank_code!r} for country {country_code!r}"
            ) from e

    @classmethod
    def from_bank_code(cls, country_code: str, bank_code: str) -> BIC:
        """Create a new BIC object from country-code and domestic bank-code.

        Examples:
            >>> bic = BIC.from_bank_code("DE", "20070000")
            >>> bic.country_code
            'DE'
            >>> bic.bank_code
            'DEUT'
            >>> bic.location_code
            'HH'

            >>> BIC.from_bank_code("DE", "01010101")
            Traceback (most recent call last):
            ...
            InvalidBankCode: Unknown bank code '01010101' for country 'DE'


        Args:
            country_code (str): ISO 3166 alpha2 country-code.
            bank_code (str): Country specific bank-code.

        Returns:
            BIC: a BIC object generated from the given country code and bank code.

        Raises:
            InvalidBankCode: If the given bank code wasn't found in the registry

        Note:
            This currently only works for selected countries. Amongst them

            * Austria
            * Belgium
            * Bulgaria
            * Croatia
            * Czech Republic
            * Finland
            * France
            * Germany
            * Great Britan
            * Hungary
            * Ireland
            * Latvia
            * Lithuania
            * Netherlands
            * Poland
            * Romania
            * Saudi Arabia
            * Slovakia
            * Slovenia
            * Spain
            * Sweden
            * Switzerland
        """
        try:
            candidates = cls.candidates_from_bank_code(country_code, bank_code)
            if len(candidates) > 1:
                # If we have multiple candidates, we try to pick the
                # one with no branch code which is the most generic one.
                generic_codes = [c for c in candidates if not c.branch_code]
                if generic_codes:
                    return sorted(generic_codes)[-1]

                # If we don't have one, we try to pick the one with
                # 'XXX' as a branch code
                generic_codes = [c for c in candidates if c.branch_code == "XXX"]
                if generic_codes:
                    return sorted(generic_codes)[-1]
            return candidates[0]
        except (KeyError, IndexError) as e:
            raise exceptions.InvalidBankCode(
                f"Unknown bank code {bank_code!r} for country {country_code!r}"
            ) from e

    def validate(self, enforce_swift_compliance: bool = False) -> bool:
        """Validate the structural integrity of this BIC.

        This function will verify the correct length, structure and the existence of the country
        code.

        Args:
            enforce_swift_commpliance (bool): If set to ``True`` the stricter SWIFT BIC policy is
                                              applied, which disallows numbers in the business
                                              prefix.

        Note:
            You have to use the `allow_invalid` paramter when constructing the :class:`BIC`-object
            to circumvent the implicit validation.

        Raises:
            InvalidLength: If the BIC's length is not 8 or 11 characters long.
            InvalidStructure: If the BIC contains unexpected characters.
            InvalidCountryCode: If the BIC's country code is unknown.
        """
        self._validate_length()
        self._validate_structure(enforce_swift_compliance)
        self._validate_country_code()
        return True

    def _validate_length(self) -> None:
        if len(self) not in (8, 11):
            raise exceptions.InvalidLength(f"Invalid length '{len(self)}'")

    def _validate_structure(self, enforce_swift_compliance: bool = False) -> None:
        regex = _bic_swift_re if enforce_swift_compliance else _bic_iso9362_re
        if not regex.match(str(self)):
            raise exceptions.InvalidStructure(f"Invalid structure '{self!s}'")

    def _validate_country_code(self) -> None:
        if self.country is None:
            raise exceptions.InvalidCountryCode(f"Invalid country code '{self.country_code}'")

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source: type[Any], handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        from pydantic_core import core_schema

        return core_schema.no_info_wrap_validator_function(
            cls._pydantic_validate,
            core_schema.union_schema(
                [
                    core_schema.is_instance_schema(BIC),
                    core_schema.no_info_plain_validator_function(BIC),
                    core_schema.str_schema(
                        pattern=r"[A-Z0-9]{4}[A-Z]{2}[A-Z0-9]{2}(:?[A-Z0-9]{3})?",
                        min_length=8,
                        max_length=11,
                    ),
                ]
            ),
            serialization=core_schema.to_string_ser_schema(
                when_used="json-unless-none",
            ),
        )

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: CoreSchema, handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        json_schema = handler(core_schema)
        json_schema = handler.resolve_ref_schema(json_schema)
        json_schema["title"] = "BIC"
        return json_schema

    @classmethod
    def _pydantic_validate(cls, value: Any, handler: ValidatorFunctionWrapHandler) -> Any:
        from pydantic_core import PydanticCustomError

        try:
            bic = cls(value)
        except exceptions.SchwiftyException as err:
            raise PydanticCustomError("bic_format", str(err)) from err
        return handler(bic)

    @property
    def is_valid(self) -> bool:
        """bool: Indicate if this is a valid BIC.

        Note:
            You have to use the `allow_invalid` paramter when constructing the :class:`BIC`-object
            to circumvent the implicit validation.

        Examples:
            >>> BIC("FOOBARBAZ", allow_invalid=True).is_valid
            False

        .. versionadded:: 2020.08.1
        """
        try:
            return self.validate()
        except exceptions.SchwiftyException:
            return False

    @property
    def formatted(self) -> str:
        """str: The BIC separated in the blocks bank-, country- and location-code.

        Examples:
            >>> BIC("MARKDEF1100").formatted
            'MARK DE F1 100'
        """
        formatted = " ".join([self.bank_code, self.country_code, self.location_code])
        if self.branch_code:
            formatted += " " + self.branch_code
        return formatted

    def _lookup_values(self, key: str) -> list:
        spec = registry.get("bic")
        assert isinstance(spec, dict)
        entries = spec.get(str(self), [])
        return sorted({entry[key] for entry in entries})

    @property
    def domestic_bank_codes(self) -> list[str]:
        """List[str]: The country specific bank-codes associated with the BIC.

        Examples:
            >>> BIC("MARKDEF1100").domestic_bank_codes
            ['10000000']

        .. versionadded:: 2020.01.0
        """
        return self._lookup_values("bank_code")

    @property
    def bank_names(self) -> list[str]:
        """List[str]: The name of the banks associated with the BIC.

        Examples:
            >>> BIC("MARKDEF1100").bank_names
            ['Bundesbank']

        .. versionadded:: 2020.01.0
        """
        return self._lookup_values("name")

    @property
    def bank_short_names(self) -> list[str]:
        """List[str]: The short name of the banks associated with the BIC.

        Examples:
            >>> BIC("MARKDEF1100").bank_short_names
            ['BBk Berlin']

        .. versionadded:: 2020.01.0
        """
        return self._lookup_values("short_name")

    @property
    def country_bank_code(self) -> str | None:
        """str or None: The country specific bank-code associated with the BIC.

        .. deprecated:: 2020.01.0
           Use :meth:`domestic_bank_codes` instead.
        """
        warnings.warn("Use `BIC.domestic_bank_codes` instead", DeprecationWarning, stacklevel=2)
        codes = self.domestic_bank_codes
        return codes[0] if codes else None

    @property
    def bank_name(self) -> str | None:
        """str or None: The name of the bank associated with the BIC.

        .. deprecated:: 2020.01.0
           Use :meth:`bank_names` instead.
        """
        warnings.warn("Use `BIC.bank_names` instead", DeprecationWarning, stacklevel=2)
        names = self.bank_names
        return names[0] if names else None

    @property
    def bank_short_name(self) -> str | None:
        """str or None: The short name of the bank associated with the BIC.

        .. deprecated:: 2020.01.0
           Use :meth:`bank_short_names` instead.
        """
        warnings.warn("Use `BIC.bank_short_names` instead", DeprecationWarning, stacklevel=2)
        names = self.bank_short_names
        return names[0] if names else None

    @property
    def exists(self) -> bool:
        """bool: Indicates if the BIC is available in Schwifty's registry."""
        spec = registry.get("bic")
        assert isinstance(spec, dict)
        return bool(spec.get(str(self)))

    @property
    def type(self) -> str:
        """Indicates the type of BIC.

        This can be one of 'testing', 'passive', 'reverse billing' or 'default'

        Examples:
            >>> BIC("MARKDEF1100").type
            'passive'

        Returns:
            str: The BIC type.
        """
        if self.location_code[1] == "0":
            return "testing"
        elif self.location_code[1] == "1":
            return "passive"
        elif self.location_code[1] == "2":
            return "reverse billing"
        else:
            return "default"

    @property
    def country(self) -> Data | None:
        """Country: The country this BIC is registered in."""
        return countries.get(alpha_2=self.country_code)

    @property
    def bank_code(self) -> str:
        """str: The bank-code part of the BIC."""
        return self._get_slice(start=0, end=4)

    @property
    def country_code(self) -> str:
        """str: The ISO 3166 alpha2 country-code."""
        return self._get_slice(start=4, end=6)

    @property
    def location_code(self) -> str:
        """str: The location code of the BIC."""
        return self._get_slice(start=6, end=8)

    @property
    def branch_code(self) -> str:
        """str: The branch-code part of the BIC (if available)"""
        return self._get_slice(start=8, end=11)


registry.build_index("bank", index_name="bic", key="bic", accumulate=True)
registry.build_index(
    "bank",
    index_name="bank_code",
    key=("country_code", "bank_code"),
    accumulate=True,
)
