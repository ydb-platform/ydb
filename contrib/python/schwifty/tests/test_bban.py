import pytest

from schwifty.bban import BBAN


@pytest.mark.parametrize("country_code", ["DE", "ES", "GB", "FR", "PL"])
def test_random(country_code: str) -> None:
    n = 100
    bbans = {BBAN.random(country_code) for _ in range(n)}
    assert len(bbans) == n

    for bban in bbans:
        assert bban.bank is not None
        assert bban.country_code == country_code

    assert any(
        bban.bank is None
        for bban in (BBAN.random(country_code, use_registry=False) for _ in range(n))
    )
