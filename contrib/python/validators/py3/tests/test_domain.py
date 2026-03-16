"""Test Domain."""

# external
import pytest

# local
from validators import ValidationError, domain


@pytest.mark.parametrize(
    ("value", "rfc_1034", "rfc_2782"),
    [
        ("example.com", False, False),
        ("exa_mple.com", False, True),
        ("xn----gtbspbbmkef.xn--p1ai", False, False),
        ("underscore_subdomain.example.com", False, True),
        ("something.versicherung", False, False),
        ("someThing.versicherung.", True, False),
        ("11.com", False, False),
        ("3.cn.", True, False),
        ("_example.com", False, True),
        ("example_.com", False, True),
        ("_exa_mple_.com", False, True),
        ("a.cn", False, False),
        ("sub1.sub2.sample.co.uk", False, False),
        ("somerandomexample.xn--fiqs8s", False, False),
        ("kräuter.com.", True, False),
        ("über.com", False, False),
    ],
)
def test_returns_true_on_valid_domain(value: str, rfc_1034: bool, rfc_2782: bool):
    """Test returns true on valid domain."""
    assert domain(value, rfc_1034=rfc_1034, rfc_2782=rfc_2782)


@pytest.mark.parametrize(
    ("value", "consider_tld", "rfc_1034", "rfc_2782"),
    [
        ("example.com", True, False, False),
        ("exa_mple.com", True, False, True),
        ("xn----gtbspbbmkef.xn--p1ai", True, False, False),
        ("underscore_subdomain.example.com", True, False, True),
        ("someThing.versicherung.", True, True, False),
        ("11.com", True, False, False),
        ("3.cn.", True, True, False),
        ("_example.com", True, False, True),
        ("example_.com", True, False, True),
        ("somerandomexample.xn--fiqs8s", True, False, False),
        ("somerandomexample.onion", True, False, False),
    ],
)
def test_returns_true_on_valid_top_level_domain(
    value: str, consider_tld: bool, rfc_1034: bool, rfc_2782: bool
):
    """Test returns true on valid top level domain."""
    assert domain(value, consider_tld=consider_tld, rfc_1034=rfc_1034, rfc_2782=rfc_2782)


@pytest.mark.parametrize(
    ("value", "rfc_1034", "rfc_2782"),
    [
        ("example.com/.", True, False),
        ("example.com:4444", False, False),
        ("example.-com", False, False),
        ("example.", False, False),
        ("-example.com", False, False),
        ("example-.com.", True, False),
        ("_example.com", False, False),
        ("_example._com", False, False),
        ("example_.com", False, False),
        ("example", False, False),
        ("example.com!", True, False),
        ("example?.com", True, False),
        ("__exa__mple__.com", False, True),
        ("a......b.com", False, False),
        ("a.123", False, False),
        ("123.123", False, False),
        ("123.123.123.", True, False),
        ("123.123.123.123", False, False),
    ],
)
def test_returns_failed_validation_on_invalid_domain(value: str, rfc_1034: bool, rfc_2782: bool):
    """Test returns failed validation on invalid domain."""
    assert isinstance(domain(value, rfc_1034=rfc_1034, rfc_2782=rfc_2782), ValidationError)


@pytest.mark.parametrize(
    ("value", "consider_tld", "rfc_1034", "rfc_2782"),
    [
        ("example.266", True, False, False),
        ("exa_mple.org_", True, False, True),
        ("xn----gtbspbbmkef.xn-p1ai", True, False, False),
        ("underscore_subdomain.example.flat", True, False, True),
        ("someThing.versicherung.reddit.", True, True, False),
        ("11.twitter", True, False, False),
        ("3.cnx.", True, True, False),
        ("_example.#13", True, False, True),
        ("example_.fo-ul", True, False, True),
        ("somerandomexample.xn-n-fiqs8s", True, False, False),
    ],
)
def test_returns_failed_validation_invalid_top_level_domain(
    value: str, consider_tld: bool, rfc_1034: bool, rfc_2782: bool
):
    """Test returns failed validation invalid top level domain."""
    assert isinstance(
        domain(value, consider_tld=consider_tld, rfc_1034=rfc_1034, rfc_2782=rfc_2782),
        ValidationError,
    )
