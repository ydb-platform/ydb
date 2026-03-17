from __future__ import annotations

import copy

import pytest
from pycountry import countries  # type: ignore

from schwifty import BIC
from schwifty import exceptions


def test_bic() -> None:
    bic = BIC("GENODEM1GLS")
    assert bic.formatted == "GENO DE M1 GLS"
    assert bic.validate()


def test_bic_allow_invalid() -> None:
    bic = BIC("GENODXM1GLS", allow_invalid=True)
    assert bic
    assert bic.country_code == "DX"
    with pytest.raises(exceptions.InvalidCountryCode):
        bic.validate()


def test_bic_no_branch_code() -> None:
    bic = BIC("GENODEM1")
    assert bic.branch_code == ""
    assert bic.formatted == "GENO DE M1"


def test_bic_properties() -> None:
    bic = BIC("GENODEM1GLS")
    assert bic.length == len(bic) == 11
    assert bic.bank_code == "GENO"
    assert bic.country_code == "DE"
    assert bic.location_code == "M1"
    assert bic.branch_code == "GLS"
    assert bic.domestic_bank_codes == ["43060967", "43060988"]
    assert bic.bank_names == [
        "GLS Gemeinschaftsbank",
        "GLS Gemeinschaftsbank (GAA)",
    ]
    assert bic.bank_short_names == [
        "GLS Bank in Bochum (GAA)",
        "GLS Gemeinschaftsbk Bochum",
    ]
    assert bic.country == countries.get(alpha_2="DE")
    with pytest.warns(DeprecationWarning, match="Use `BIC.bank_names` instead"):
        assert bic.bank_name == "GLS Gemeinschaftsbank"
    with pytest.warns(DeprecationWarning, match="Use `BIC.bank_short_names` instead"):
        assert bic.bank_short_name == "GLS Bank in Bochum (GAA)"
    with pytest.warns(DeprecationWarning, match="Use `BIC.domestic_bank_codes` instead"):
        assert bic.country_bank_code == "43060967"
    assert bic.exists
    assert bic.type == "passive"


def test_unknown_bic_properties() -> None:
    bic = BIC("ABNAJPJTXXX")
    assert bic.length == len(bic) == 11
    assert bic.bank_code == "ABNA"
    assert bic.country_code == "JP"
    assert bic.location_code == "JT"
    assert bic.branch_code == "XXX"
    assert bic.country_bank_code is None
    assert bic.domestic_bank_codes == []
    assert bic.bank_name is None
    assert bic.bank_names == []
    assert bic.bank_short_name is None
    assert bic.bank_short_names == []
    assert not bic.exists
    assert bic.type == "default"


@pytest.mark.parametrize(
    ("code", "type"),
    [
        ("GENODEM0GLS", "testing"),
        ("GENODEM1GLS", "passive"),
        ("GENODEM2GLS", "reverse billing"),
        ("GENODEMMGLS", "default"),
        ("1234DEWWXXX", "default"),
    ],
)
def test_bic_type(code: str, type: str) -> None:  # noqa: A002
    bic = BIC(code)
    assert bic.type == type


def test_enforce_swift_compliance() -> None:
    with pytest.raises(exceptions.InvalidStructure):
        BIC("1234DEWWXXX", enforce_swift_compliance=True)


@pytest.mark.parametrize(
    ("code", "exc"),
    [
        ("AAAA", exceptions.InvalidLength),
        ("AAAADEM1GLSX", exceptions.InvalidLength),
        ("GENOD1M1GLS", exceptions.InvalidStructure),
        ("GENOXXM1GLS", exceptions.InvalidCountryCode),
    ],
)
def test_invalid_bic(code: str, exc: type[Exception]) -> None:
    with pytest.raises(exc):
        BIC(code)


@pytest.mark.parametrize(
    ("country", "bank_code", "bic"),
    [
        ("AT", "36274", "RZTIAT22274"),
        ("BE", "002", "GEBABEBB"),
        ("CH", "00777", "KBSZCH22XXX"),
        ("CH", "08390", "ABSOCH22XXX"),
        ("CZ", "0600", "AGBACZPP"),
        ("DE", "43060967", "GENODEM1GLS"),
        ("ES", "0209", "BSABESBB"),
        ("FI", "101", "NDEAFIHH"),
        ("FR", "30004", "BNPAFRPP"),
        ("FR", "30066", "CMCIFRPPXXX"),
        ("FR", "17469", "SOCBPFTXXXX"),
        ("FR", "10096", "CMCIFRPP"),
        ("FR", "18719", "BFCOYTYTXXX"),
        ("FR", "30077", "SMCTFR2A"),
        ("FR", "13489", "NORDFRPP"),
        ("HU", "107", "CIBHHUHB"),
        ("HR", "2485003", "CROAHR2X"),
        ("IT", "01015", "SARDIT31"),  # generated
        ("IT", "01030", "PASCITMM"),  # curated
        ("LV", "RIKO", "RIKOLV2XXXX"),
        ("MC", "30003", "SOGEMCM1"),
        ("NL", "ADYB", "ADYBNL2A"),
        ("PL", "10100055", "NBPLPLPWXXX"),
        ("PL", "10900004", "WBKPPLPPXXX"),
        ("RO", "BPOS", "BPOSROBU"),
        ("SE", "500", "ESSESESS"),
        ("SI", "01050", "BSLJSI2XFNB"),
        ("SK", "0900", "GIBASKBX"),
    ],
)
def test_bic_from_bank_code(country: str, bank_code: str, bic: str) -> None:
    assert BIC.from_bank_code(country, bank_code).compact == bic


def test_bic_from_unknown_bank_code() -> None:
    with pytest.raises(exceptions.InvalidBankCode):
        BIC.from_bank_code("PO", "12345678")


@pytest.mark.parametrize(
    ("country", "bank_code", "bic_codes"),
    [
        ("AT", "36274", ["RZTIAT22274"]),
        ("BE", "002", ["GEBABEBB"]),
        ("CH", "00100", ["SNBZCHZZXXX"]),
        ("CZ", "0600", ["AGBACZPP"]),
        ("ES", "0209", ["BSABESBB"]),
        ("FI", "101", ["NDEAFIHH"]),
        (
            "FR",
            "30004",
            [
                "BNPAFRPPIFN",
                "BNPAFRPPPAA",
                "BNPAFRPPMED",
                "BNPAFRPPCRN",
                "BNPAFRPP",
                "BNPAFRPPPAE",
                "BNPAFRPPPBQ",
                "BNPAFRPPNFE",
                "BNPAFRPPPGN",
                "BNPAFRPPXXX",
                "BNPAFRPPBOR",
                "BNPAFRPPCRM",
                "BNPAFRPPPVD",
                "BNPAFRPPPTX",
                "BNPAFRPPPAC",
                "BNPAFRPPPLZ",
                "BNPAFRPP039",
                "BNPAFRPPENG",
                "BNPAFRPPNEU",
                "BNPAFRPPORE",
                "BNPAFRPPPEE",
                "BNPAFRPPPXV",
                "BNPAFRPPIFO",
            ],
        ),
        ("DE", "43060967", ["GENODEM1GLS"]),
        ("DK", "0040", ["NDEADKKK"]),
        ("HU", "107", ["CIBHHUHB"]),
        ("HR", "2485003", ["CROAHR2X"]),
        ("LV", "RIKO", ["RIKOLV2XXXX"]),
        ("ME", "907", ["CBCGMEPG"]),
        ("NL", "ADYB", ["ADYBNL2A"]),
        ("PL", "10100055", ["NBPLPLPWXXX"]),
        ("RO", "BPOS", ["BPOSROBU"]),
        ("SE", "500", ["ESSESESS"]),
        ("SI", "01050", ["BSLJSI2XFNB"]),
        ("SK", "0900", ["GIBASKBX"]),
    ],
)
def test_bic_candidates_from_bank_code(country: str, bank_code: str, bic_codes: list[str]) -> None:
    for bic in BIC.candidates_from_bank_code(country, bank_code):
        assert bic.compact in bic_codes


def test_bic_candidates_from_unknown_bank_code() -> None:
    with pytest.raises(exceptions.InvalidBankCode):
        BIC.candidates_from_bank_code("PO", "12345678")


def test_bic_is_from_primary_bank_code() -> None:
    bic = BIC.from_bank_code("DE", "20070024")
    assert bic.compact == "DEUTDEDBHAM"


def test_magic_methods() -> None:
    bic = BIC("GENODEM1GLS")
    assert bic == "GENODEM1GLS"
    assert bic == BIC("GENODEM1GLS")
    assert bic != BIC("GENODEMMXXX")
    assert bic != 12345
    assert bic < "GENODEM1GLT"

    assert str(bic) == "GENODEM1GLS"
    assert hash(bic) == hash("GENODEM1GLS")
    assert repr(bic) == "<BIC=GENODEM1GLS>"


def test_pydantic_protocol() -> None:
    from pydantic import BaseModel
    from pydantic import ValidationError

    class Model(BaseModel):
        bic: BIC

    model = Model(bic="GENODEM1GLS")  # type: ignore[arg-type]
    assert isinstance(model.bic, BIC)
    assert model.model_dump() == {"bic": model.bic}

    with pytest.raises(ValidationError) as err:
        Model(bic="GENODXM1GLS")  # type: ignore[arg-type]
    assert len(err.value.errors()) == 1
    error = err.value.errors()[0]
    assert error["type"] == "bic_format"
    assert error["msg"] == "Invalid country code 'DX'"
    assert error["input"] == "GENODXM1GLS"

    dumped = model.model_dump_json()
    assert dumped == '{"bic":"GENODEM1GLS"}'

    loaded = Model.model_validate_json(dumped)
    assert loaded == model


def test_deepcopy() -> None:
    bic = BIC("GENODEM1GLS")

    bic_deepcopy = copy.deepcopy(bic)
    assert id(bic) != id(bic_deepcopy)
    assert bic == bic_deepcopy

    bic_copy = copy.copy(bic)
    assert id(bic) != id(bic_copy)
    assert bic == bic_copy
