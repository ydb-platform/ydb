import copy
from typing import Annotated
from typing import Any

import pytest
from pycountry import countries  # type: ignore

from schwifty import IBAN
from schwifty.exceptions import SchwiftyException
from schwifty.iban import convert_bban_spec_to_regex


valid = [
    "AL47 2121 1009 0000 0002 3569 8741",  # Albania
    "AD12 0001 2030 2003 5910 0100",  # Andorra
    "AT61 1904 3002 3457 3201",  # Austria
    "AZ21 NABZ 0000 0000 1370 1000 1944",  # Republic of Azerbaijan
    "BH67 BMAG 0000 1299 1234 56",  # Bahrain (Kingdom of)
    "BE68 5390 0754 7034",  # Belgium
    "BA39 1290 0794 0102 8494",  # Bosnia and Herzegovina
    "BR97 0036 0305 0000 1000 9795 493P 1",  # Brazil
    "BR18 0000 0000 1414 5512 3924 100C 2",  # Brazil
    "BG80 BNBG 9661 1020 3456 78",  # Bulgaria
    "CR05 0152 0200 1026 2840 66",  # Costa Rica
    "HR12 1001 0051 8630 0016 0",  # Croatia
    "CY17 0020 0128 0000 0012 0052 7600",  # Cyprus
    "CZ65 0800 0000 1920 0014 5399",  # Czech Republic
    "CZ94 5500 0000 0010 1103 8930",  # Czech Republic
    "DK50 0040 0440 1162 43",  # Greenland
    "FO62 6460 0001 6316 34",  # Faroer
    "GL89 6471 0001 0002 06",  # Denmark
    "DO28 BAGR 0000 0001 2124 5361 1324",  # Dominican Republic
    "EE38 2200 2210 2014 5685",  # Estonia
    "FI21 1234 5600 0007 85",  # Finland
    "FR14 2004 1010 0505 0001 3M02 606",  # France
    "GE29 NB00 0000 0101 9049 17",  # Georgia
    "DE89 3704 0044 0532 0130 00",  # Germany
    "GI75 NWBK 0000 0000 7099 453",  # Gibraltar
    "GR16 0110 1250 0000 0001 2300 695",  # Greece
    "GT82 TRAJ 0102 0000 0012 1002 9690",  # Guatemala
    "HU42 1177 3016 1111 1018 0000 0000",  # Hungary
    "IS14 0159 2600 7654 5510 7303 39",  # Iceland
    "IE29 AIBK 9311 5212 3456 78",  # Ireland
    "IL62 0108 0000 0009 9999 999",  # Israel
    "IT60 X054 2811 1010 0000 0123 456",  # Italy
    "JO94 CBJO 0010 0000 0000 0131 0003 02",  # Jordan
    "KZ86 125K ZT50 0410 0100",  # Kazakhstan
    "XK05 1212 0123 4567 8906",  # Republic of Kosovo
    "KW81 CBKU 0000 0000 0000 1234 5601 01",  # Kuwait
    "LV80 BANK 0000 4351 9500 1",  # Latvia
    "LB62 0999 0000 0001 0019 0122 9114",  # Lebanon
    "LI21 0881 0000 2324 013A A",  # Liechtenstein (Principality of)
    "LT12 1000 0111 0100 1000",  # Lithuania
    "LU28 0019 4006 4475 0000",  # Luxembourg
    "MK07 2501 2000 0058 984",  # Macedonia, Former Yugoslav Republic of
    "MT84 MALT 0110 0001 2345 MTLC AST0 01S",  # Malta
    "MR13 0002 0001 0100 0012 3456 753",  # Mauritania
    "MU17 BOMM 0101 1010 3030 0200 000M UR",  # Mauritius
    "MD24 AG00 0225 1000 1310 4168",  # Moldova
    "MC58 1122 2000 0101 2345 6789 030",  # Monaco
    "ME25 5050 0001 2345 6789 51",  # Montenegro
    "NL91 ABNA 0417 1643 00",  # The Netherlands
    "NO93 8601 1117 947",  # Norway
    "PK36 SCBL 0000 0011 2345 6702",  # Pakistan
    "PS92 PALS 0000 0000 0400 1234 5670 2",  # Palestine, State of
    "PL61 1090 1014 0000 0712 1981 2874",  # Poland
    "PT50 0002 0123 1234 5678 9015 4",  # Portugal
    "QA58 DOHB 0000 1234 5678 90AB CDEF G",  # Qatar
    "RO49 AAAA 1B31 0075 9384 0000",  # Romania
    "LC55 HEMM 0001 0001 0012 0012 0002 3015",  # Saint Lucia
    "SM86 U032 2509 8000 0000 0270 100",  # San Marino
    "ST68 0001 0001 0051 8453 1011 2",  # Sao Tome And Principe
    "SA03 8000 0000 6080 1016 7519",  # Saudi Arabia
    "RS35 2600 0560 1001 6113 79",  # Serbia
    "SN08 SN01 0015 2000 0485 0000 3035",  # Senegal
    "SC18 SSCB 1101 0000 0000 0000 1497 USD",  # Seychelles
    "SK31 1200 0000 1987 4263 7541",  # Slovak Republic
    "SI56 1910 0000 0123 438",  # Slovenia
    "ES91 2100 0418 4502 0005 1332",  # Spain
    "SE45 5000 0000 0583 9825 7466",  # Sweden
    "CH93 0076 2011 6238 5295 7",  # Switzerland
    "TL38 0080 0123 4567 8910 157",  # Timor-Leste
    "TN59 1000 6035 1835 9847 8831",  # Tunisia
    "TR33 0006 1005 1978 6457 8413 26",  # Turkey
    "UA21 3996 2200 0002 6007 2335 6600 1",  # Ukraine
    "AE07 0331 2345 6789 0123 456",  # United Arab Emirates
    "GB29 NWBK 6016 1331 9268 19",  # United Kingdom
    "VG96 VPVG 0000 0123 4567 8901",  # Virgin Islands, British
    "BY13 NBRB 3600 9000 0000 2Z00 AB00",  # Republic of Belarus
    "SV62 CENR 0000 0000 0000 0070 0025",  # El Salvador
    "FO62 6460 0001 6316 34",  # Faroe Islands
    "GL89 6471 0001 0002 06",  # Grenland
    "IQ98 NBIQ 8501 2345 6789 012",  # Iraq
]


invalid = [
    "DE89 3704 0044 0532 0130",  # Too short
    "DE89 3704 0044 0532 0130 0000",  # Too long
    "GB96 BARC 2020 1530 0934 591",  # Too long
    "XX89 3704 0044 0532 0130 00",  # Wrong country-code
    "DE99 3704 0044 0532 0130 00",  # Wrong check digits
    "DEAA 3704 0044 0532 0130 00",  # Wrong format (check digits)
    "GB2L ABBY 0901 2857 2017 07",  # Wrong format (check digits)
    "DE89 AA04 0044 0532 0130 00",  # Wrong format (country specific)
    "GB12 BARC 2020 1530 093A 59",  # Wrong account format (country specific)
    "GB01 BARC 2071 4583 6083 87",  # Wrong checksum digits
    "GB00 HLFX 1101 6111 4553 65",  # Wrong checksum digits
    "GB94 BARC 2020 1530 0934 59",  # Wrong checksum digits
]

experimental = [
    "DZ58 0002 1000 0111 3000 0005 70",  # Algeria
    "AO06 0044 0000 6729 5030 1010 2",  # Angola
    "BJ66 BJ06 1010 0100 1443 9000 0769",  # Benin
    "BF42 BF08 4010 1300 4635 7400 0390",  # Burkina Faso
    "CM21 1000 2000 3002 7797 6315 008",  # Cameroon
    "CV64 0005 0000 0020 1082 1514 4",  # Cape Verde
    "CF42 2000 1000 0101 2006 9700 160",  # Central African Republic
    "TD89 6000 2000 0102 7109 1600 153",  # Chad
    "KM46 0000 5000 0100 1090 4400 137",  # Comoros
    "CG39 3001 1000 1010 1345 1300 019",  # Congo
    "GQ70 5000 2001 0037 1522 8190 196",  # Equatorial Guinea
    "GA21 4002 1010 0320 0189 0020 126",  # Gabon
    "GW04 GW14 3001 0181 8006 3760 1",  # Guinea-Bissau
    "HN54 PISA 0000 0000 0000 0012 3124",  # Honduras
    "IR71 0570 0299 7160 1460 6410 01",  # Iran
    "CI93 CI00 8011 1301 1342 9120 0589",  # Ivory Coast
    "MG46 0000 5030 0712 8942 1016 045",  # Madagascar
    "ML13 ML01 6012 0102 6001 0066 8497",  # Mali
    "MA64 0115 1900 0001 2050 0053 4921",  # Morocco
    "MZ59 0003 0108 0016 3671 0237 1",  # Mozambique
    "NE58 NE03 8010 0100 1303 0500 0268",  # Niger
    "SN08 SN01 0015 2000 0485 0000 3035",  # Senegal
    "TG53 TG00 9060 4310 3465 0040 0070",  # Togo
]


sepa_countries = {
    "AD",
    "AT",
    "BE",
    "BG",
    "CH",
    "CY",
    "CZ",
    "DE",
    "DK",
    "EE",
    "ES",
    "FI",
    "FR",
    "GB",
    "GI",
    "GR",
    "HR",
    "HU",
    "IE",
    "IS",
    "IT",
    "LI",
    "LT",
    "LU",
    "LV",
    "MC",
    "MT",
    "NL",
    "NO",
    "PL",
    "PT",
    "RO",
    "SE",
    "SK",
    "SI",
    "SM",
    "VA",
}


@pytest.mark.parametrize("value", valid)
def test_parse_iban(value: str) -> None:
    iban = IBAN(value, validate_bban=True)
    assert iban.formatted == value
    assert iban.country == countries.get(alpha_2=iban.country_code)
    assert iban.in_sepa_zone is (iban.country_code in sepa_countries)


@pytest.mark.parametrize("value", experimental)
def test_experimental_iban(value: str) -> None:
    iban = IBAN(value, validate_bban=True)
    assert iban.formatted == value


@pytest.mark.parametrize("value", invalid)
def test_parse_iban_allow_invalid(value: str) -> None:
    iban = IBAN(value, allow_invalid=True)
    with pytest.raises(SchwiftyException):
        iban.validate()


@pytest.mark.parametrize("value", invalid)
def test_invalid_iban(value: str) -> None:
    with pytest.raises(SchwiftyException):
        IBAN(value)


def test_iban_properties_de() -> None:
    iban = IBAN("DE42430609677000534100")
    assert iban.is_valid is True
    assert iban.bank_code == "43060967"
    assert iban.branch_code == ""
    assert iban.account_code == "7000534100"
    assert iban.account_id == ""
    assert iban.account_type == ""
    assert iban.country_code == "DE"
    assert iban.currency_code == ""
    assert iban.account_holder_id == ""
    assert iban.national_checksum_digits == ""
    assert iban.bic == "GENODEM1GLS"
    assert iban.formatted == "DE42 4306 0967 7000 5341 00"
    assert iban.length == len(iban) == 22
    assert iban.country == countries.get(alpha_2="DE")
    assert iban.bank_name == "GLS Gemeinschaftsbank"
    assert iban.bank_short_name == "GLS Gemeinschaftsbk Bochum"
    assert iban.in_sepa_zone is True


def test_iban_properties_it() -> None:
    iban = IBAN("IT60 X054 2811 1010 0000 0123 456")
    assert iban.bank_code == "05428"
    assert iban.branch_code == "11101"
    assert iban.account_code == "000000123456"
    assert iban.national_checksum_digits == "X"
    assert iban.country == countries.get(alpha_2="IT")
    assert iban.bic == "BLOPIT22"
    assert iban.bank_name == "Unione Di Banche Italiane SpA"
    assert iban.in_sepa_zone is True


def test_iban_properties_is() -> None:
    iban = IBAN("IS14 0159 2600 7654 5510 7303 39")
    assert iban.account_holder_id == "5510730339"
    assert iban.account_code == "007654"
    assert iban.account_type == "26"
    assert iban.branch_code == "59"
    assert iban.bank_code == "01"


def test_iban_properties_pl() -> None:
    iban = IBAN("PL66114010100000123400005678")
    assert iban.is_valid
    assert iban.bank_code == "11401010"
    assert iban.branch_code == ""
    assert iban.account_code == "0000123400005678"
    assert iban.account_id == ""
    assert iban.account_type == ""
    assert iban.country_code == "PL"
    assert iban.currency_code == ""
    assert iban.account_holder_id == ""
    assert iban.national_checksum_digits == "0"
    assert iban.bic == "BREXPLPWWA1"
    assert iban.formatted == "PL66 1140 1010 0000 1234 0000 5678"
    assert iban.length == len(iban) == 28
    assert iban.country == countries.get(alpha_2="PL")
    assert iban.bank_name == "mBank Spółka Akcyjna"
    assert iban.bank_short_name == "mBank Spółka Akcyjna"
    assert iban.in_sepa_zone


@pytest.mark.parametrize(
    ("components", "compact"),
    [
        (("BE", "050", "123"), "BE66050000012343"),
        (("BE", "050", "123456"), "BE45050012345689"),
        (("BE", "539", "0075470"), "BE68539007547034"),
        (("BE", "050", "177"), "BE54050000017797"),
        (("DE", "43060967", "7000534100"), "DE42430609677000534100"),
        (("DE", "51230800", "2622196545"), "DE61512308002622196545"),
        (("DE", "20690500", "9027378"), "DE37206905000009027378"),
        (("DK", "0040", "0440116243"), "DK5000400440116243"),
        (("FR", "2004101005", "0500013M026"), "FR1420041010050500013M02606"),
        (("GB", "NWBK", "31926819", "601613"), "GB29NWBK60161331926819"),
        (("GB", "NWBK", "31926819"), "GB66NWBK00000031926819"),
        (("GB", "NWBK601613", "31926819"), "GB29NWBK60161331926819"),
        (("IT", "0538703601", "000000198036"), "IT18T0538703601000000198036"),
        (("IT", "0538703601", "000000198060"), "IT57V0538703601000000198060"),
        (("IT", "0538703601", "000000198072"), "IT40Z0538703601000000198072"),
        (("IT", "0538742530", "000000802006"), "IT29P0538742530000000802006"),
        (("IT", "0306940101", "100100003599"), "IT94I0306940101100100003599"),
        (("IT", "0335901600", "100000131525"), "IT63M0335901600100000131525"),
        (("IT", "03359", "100000131525", "01600"), "IT63M0335901600100000131525"),
        (("IT", "39189", "CHTEE9UATVVO", "13896"), "IT12D3918913896CHTEE9UATVVO"),
        (("IT", "49076", "YB4EQZ0PL4GT", "75919"), "IT70Y4907675919YB4EQZ0PL4GT"),
        (("IT", "05428", "00ABCD12ZE34", "01600"), "IT21Q054280160000ABCD12ZE34"),
        (("IT", "05428", "000000123456", "11101"), "IT60X0542811101000000123456"),
        (("IT", "55354", "AUKNAEXQVZOG", "87408"), "IT77D5535487408AUKNAEXQVZOG"),
        (("IT", "31582", "YMZJIILHBUN0", "80362"), "IT49B3158280362YMZJIILHBUN0"),
        (("IT", "39814", "LSLLPTLPK716", "69135"), "IT37W3981469135LSLLPTLPK716"),
        (("IT", "86388", "3VVRNLLMMN9N", "85779"), "IT69U86388857793VVRNLLMMN9N"),
        (("IT", "43482", "DNNDKPHAGTIB", "07900"), "IT22K4348207900DNNDKPHAGTIB"),
        (("IT", "70000", "Mq8gyacBzEqP", "30810"), "IT39M7000030810MQ8GYACBZEQP"),
        (("IT", "76494", "2Sbpqelox4wG", "16460"), "IT87A76494164602SBPQELOX4WG"),
        (("PL", "11401010", "0000123400005678"), "PL66114010100000123400005678"),
        (("PL", "11401010", "123400005678"), "PL66114010100000123400005678"),
        (("PL", "11401010", "12340000"), "PL04114010100000000012340000"),
        (("PL", "11401010", "1234"), "PL89114010100000000000001234"),
    ],
)
def test_generate_iban(components: tuple[str, ...], compact: str) -> None:
    iban = IBAN.generate(*components)
    iban.validate(validate_bban=True)
    assert iban.compact == compact


@pytest.mark.parametrize(
    "components",
    [
        ("DE", "012345678", "7000123456"),
        ("DE", "51230800", "01234567891"),
        ("GB", "NWBK", "31926819", "1234567"),
        ("PL", "11401010", "ABCD"),
        ("PL", "11401010", "10000123400005678"),
    ],
)
def test_generate_iban_invalid(components: tuple[str, ...]) -> None:
    with pytest.raises(SchwiftyException):
        IBAN.generate(*components)


def test_magic_methods() -> None:
    iban = IBAN("DE42430609677000534100")
    assert iban == "DE42430609677000534100"
    assert iban == IBAN("DE42430609677000534100")
    assert iban != IBAN("ES9121000418450200051332")
    assert iban < IBAN("ES9121000418450200051332")

    assert str(iban) == "DE42430609677000534100"
    assert hash(iban) == hash("DE42430609677000534100")
    assert repr(iban) == "<IBAN=DE42430609677000534100>"


@pytest.mark.parametrize(
    ("iban", "compact"),
    [
        ("AD1200012030200359100100", "BACAADADXXX"),
        ("AE070331234567890123456", "BOMLAEADXXX"),
        ("AT483200000012345864", "RLNWATWWXXX"),
        ("AT930100000000123145", "BUNDATWWXXX"),
        ("BA393385804800211234", "UNCRBA22XXX"),
        ("BE71096123456769", "GKCCBEBB"),
        ("BG18RZBB91550123456789", "RZBBBGSF"),
        ("CH5604835012345678009", "CRESCHZZ80A"),
        ("CR23015108410026012345", "BNCRCRSJXXX"),
        ("CY21002001950000357001234567", "BCYPCY2N"),
        ("CZ5508000000001234567899", "GIBACZPX"),
        ("DE37206905000009027378", "GENODEF1S11"),
        # ("DK9520000123456789", "NDEADKKK"),
        ("EE471000001020145685", "EEUHEE2X"),
        ("ES7921000813610123456789", "CAIXESBB"),
        ("FI1410093000123458", "NDEAFIHH"),
        ("FR7630006000011234567890189", "AGRIFRPPXXX"),
        ("GB33BUKB20201555555555", "BUKBGB22"),
        ("GR9608100010000001234567890", "BOFAGR2XXXX"),
        ("HR1723600001101234565", "ZABAHR2X"),
        ("HU42117730161111101800000000", "OTPVHUHB"),
        ("IE64IRCE92050112345678", "IRCEIE2DXXX"),
        ("IT60X0542811101000000123456", "BLOPIT22"),
        # ("IL170108000000012612345", "LUMIILIT"),
        ("IS480114007083000000000000", "NBIIISRE"),
        ("KZ244350000012344567", "SHBKKZKA"),
        ("LT601010012345678901", "LIABLT2XXXX"),
        ("LU120010001234567891", "BCEELULL"),
        ("LV97HABA0012345678910", "HABALV22XXX"),
        # ("MD21EX000000000001234567", "EXMMMD22"),
        ("NL02ABNA0123456789", "ABNANL2A"),
        ("NO8330001234567", "SPSONO22"),
        ("PL50860000020000000000093122", "POLUPLPRXXX"),
        ("PL66114010100000123400005678", "BREXPLPWWA1"),
        ("PT50002700000001234567833", "BPIPPTPLXXX"),
        ("RO66BACX0000001234567890", "BACXROBU"),
        ("RS35105008123123123173", "AIKBRS22XXX"),
        ("SE7280000810340009783242", "SWEDSESS"),
        ("SI56192001234567892", "SZKBSI2XXXX"),
        ("SK8975000000000012345671", "CEKOSKBX"),
        ("LI21088100002324013AA", "BLFLLI2XXXX"),
        # ("UA903052992990004149123456789", "PBANUA2X"),
    ],
)
def test_bic_from_iban(iban: str, compact: str) -> None:
    bic = IBAN(iban).bic
    assert bic is not None
    assert bic.compact == compact


def test_unknown_bic_from_iban() -> None:
    assert IBAN("SI72000001234567892").bic is None


def test_unknown_bank_name_from_iban() -> None:
    assert IBAN("SI72000001234567892").bank_name is None


def test_unknown_bank_name_short_from_iban() -> None:
    assert IBAN("SI72000001234567892").bank_short_name is None


def test_random_iban() -> None:
    for _ in range(100):
        iban = IBAN.random()
        assert isinstance(iban, IBAN)


def test_random_special_cases() -> None:
    iban = IBAN.random(country_code="MU")
    assert iban.endswith("000MUR")

    iban = IBAN.random(country_code="SC")
    assert iban.endswith("SCR")

    iban = IBAN.random(country_code="KM")
    assert iban.is_valid


def test_pydantic_protocol() -> None:
    from pydantic import BaseModel
    from pydantic import ValidationError

    class Model(BaseModel):
        iban: IBAN

    model = Model(iban=IBAN("GL89 6471 0001 0002 06"))
    assert model.model_dump() == {"iban": model.iban}

    model = Model(iban="GL89 6471 0001 0002 06")  # type: ignore[arg-type]
    assert isinstance(model.iban, IBAN)
    assert model.model_dump() == {"iban": model.iban}

    with pytest.raises(ValidationError) as err:
        Model(iban="GB00 HLFX 1101 6111 4553 65")  # type: ignore[arg-type]
    assert len(err.value.errors()) == 1
    error = err.value.errors()[0]
    assert error["type"] == "iban_format"
    assert error["msg"] == "Invalid checksum digits"
    assert error["input"] == "GB00 HLFX 1101 6111 4553 65"

    json_schema = model.model_json_schema()
    assert json_schema["properties"]["iban"] == {"maxLength": 34, "title": "IBAN", "type": "string"}

    dumped = model.model_dump_json()
    assert dumped == '{"iban":"GL8964710001000206"}'

    loaded = Model.model_validate_json(dumped)
    assert loaded == model


def test_pydantic_invalid_iban() -> None:
    from pydantic import BaseModel
    from pydantic import Field

    class Model(BaseModel):
        iban: Annotated[IBAN, Field(strict=False)]

    model = Model(iban="GB00 HLFX 1101 6111 4553 65")  # type: ignore[arg-type]
    assert isinstance(model.iban, IBAN)
    assert model.iban.is_valid is False


def test_pydantic_union_type() -> None:
    from pydantic import BaseModel

    class Model(BaseModel):
        iban_or_dict: IBAN | dict[str, Any]

    model = Model(iban_or_dict="GL89 6471 0001 0002 06")  # type: ignore[arg-type]
    assert isinstance(model.iban_or_dict, IBAN)
    assert model.iban_or_dict.is_valid is True

    model = Model(iban_or_dict={"foo": 1})
    assert isinstance(model.iban_or_dict, dict)
    assert model.iban_or_dict["foo"] == 1


@pytest.mark.parametrize(
    ("spec", "regex"),
    [
        ("5!n", r"^\d{5}$"),
        ("4!a", r"^[A-Z]{4}$"),
        ("10!c", r"^[A-Za-z0-9]{10}$"),
        ("5!e", r"^ {5}$"),
        ("3n", r"^\d{1,3}$"),
        ("5!n3!a", r"^\d{5}[A-Z]{3}$"),
    ],
)
def test_convert_bban_spec_to_regex(spec: str, regex: str) -> None:
    assert convert_bban_spec_to_regex(spec) == regex


def test_copy() -> None:
    iban = IBAN("BE68 5390 0754 7034")

    iban_deepcopy = copy.deepcopy(iban)
    assert id(iban) != id(iban_deepcopy)
    assert iban == iban_deepcopy

    iban_copy = copy.copy(iban)
    assert id(iban) != id(iban_copy)
    assert iban == iban_copy
