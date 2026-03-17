import pytest

from schwifty.checksum import algorithms


@pytest.mark.parametrize(
    ("account_code", "algorithm_name"),
    [
        ("0009290701", "DE:00"),
        ("0539290858", "DE:00"),
        ("0001501824", "DE:00"),
        ("0001501832", "DE:00"),
        ("0094012341", "DE:06"),
        ("5073321010", "DE:06"),
        ("0012345008", "DE:10"),
        ("0087654008", "DE:10"),
        ("0446786040", "DE:17"),
        ("0240334000", "DE:19"),
        ("0200520016", "DE:19"),
        ("0000138301", "DE:24"),
        ("1306118605", "DE:24"),
        ("3307118608", "DE:24"),
        ("9307118603", "DE:24"),
        ("0521382181", "DE:25"),
        ("0520309001", "DE:26"),
        ("1111118111", "DE:26"),
        ("0005501024", "DE:26"),
        ("0009141405", "DE:32"),
        ("1709107983", "DE:32"),
        ("0122116979", "DE:32"),
        ("0121114867", "DE:32"),
        ("9030101192", "DE:32"),
        ("9245500460", "DE:32"),
        ("9913000700", "DE:34"),
        ("9914001000", "DE:34"),
        ("0000191919", "DE:38"),
        ("0001100660", "DE:38"),
        ("2063099200", "DE:61"),
        ("0260760481", "DE:61"),
        ("0123456600", "DE:63"),
        ("8889654328", "DE:68"),
        ("0987654324", "DE:68"),
        ("0987654328", "DE:68"),
        ("0006543200", "DE:76"),
        ("9012345600", "DE:76"),
        ("7876543100", "DE:76"),
        ("0002525259", "DE:88"),
        ("0001000500", "DE:88"),
        ("0090013000", "DE:88"),
        ("0092525253", "DE:88"),
        ("0099913003", "DE:88"),
        ("2974118000", "DE:91"),
        ("5281741000", "DE:91"),
        ("9952810000", "DE:91"),
        ("2974117000", "DE:91"),
        ("5281770000", "DE:91"),
        ("9952812000", "DE:91"),
        ("8840019000", "DE:91"),
        ("8840050000", "DE:91"),
        ("8840087000", "DE:91"),
        ("8840045000", "DE:91"),
        ("8840012000", "DE:91"),
        ("8840055000", "DE:91"),
        ("8840080000", "DE:91"),
        ("0068007003", "DE:99"),
        ("0847321750", "DE:99"),
        ("0396000000", "DE:99"),
        ("0499999999", "DE:99"),
    ],
)
def test_german_checksum_success(account_code: str, algorithm_name: str) -> None:
    assert algorithms[algorithm_name].validate([account_code], "") is True


@pytest.mark.parametrize(
    ("account_code", "algorithm_name"),
    [
        ("8840017000", "DE:91"),
        ("8840023000", "DE:91"),
        ("8840041000", "DE:91"),
        ("8840014000", "DE:91"),
        ("8840026000", "DE:91"),
        ("8840011000", "DE:91"),
        ("8840025000", "DE:91"),
        ("8840062000", "DE:91"),
        ("8840010000", "DE:91"),
        ("8840057000", "DE:91"),
    ],
)
def test_german_checksum_failure(account_code: str, algorithm_name: str) -> None:
    assert algorithms[algorithm_name].validate([account_code], "") is False


def test_belgium_checksum() -> None:
    assert algorithms["BE:default"].validate(["539", "0075470"], "34") is True


def test_belgium_checksum_failure() -> None:
    assert algorithms["BE:default"].validate(["050", "0001234"], "56") is False


def test_belgium_checksum_checksum_edge_case() -> None:
    assert algorithms["BE:default"].validate(["050", "0000177"], "97") is True


def test_norway_checksum_checksum_edge_case() -> None:
    assert algorithms["NO:default"].validate(["6042", "143964"], "0") is True
