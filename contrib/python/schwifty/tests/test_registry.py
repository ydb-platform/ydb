from schwifty import BIC
from schwifty import registry
from schwifty.checksum.poland import DefaultAlgorithm


def test_validate_bics():
    for bic in (bank["bic"] for bank in registry.get("bank") if bank["bic"]):
        BIC(bic, allow_invalid=False)


def test_validate_cz_encoding():
    assert "Komerční banka, a.s.", "Československá obchodní banka, a. s." in [
        bank["name"] for bank in registry.get("bank") if bank["country_code"] == "CZ"
    ]


def test_valid_national_checksum_pl():
    bank_by_country = registry.get("country")
    algo = DefaultAlgorithm()
    for bank in bank_by_country["PL"]:
        bank_code = bank["bank_code"]
        check_digit = bank_code[7]
        branch_code = bank_code[3:7]
        bank_code = bank_code[:3]

        assert algo.compute([bank_code, branch_code]) == check_digit


def test_bank_code_matches_spec():
    bank_by_country = registry.get("country")
    specs = registry.get("iban")

    for country_code, banks in bank_by_country.items():
        spec = specs[country_code]
        start, end = spec["bban_length"], 0
        for component in spec.get("bic_lookup_components", ["bank_code"]):
            a, b = spec["positions"][component]
            start = min(start, a)
            end = max(end, b)
        length = end - start
        for bank in banks:
            bank_code = bank["bank_code"]
            if not bank_code:
                continue
            assert len(bank_code) == length
