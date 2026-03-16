import pytest  # type: ignore

from ua_parser import Domain, PartialResult

re2 = pytest.importorskip("ua_parser.re2")


def test_empty(capfd: pytest.CaptureFixture[str]) -> None:
    r = re2.Resolver(([], [], []))
    assert r("", Domain.ALL) == PartialResult(Domain.ALL, None, None, None, "")
    out, err = capfd.readouterr()
    assert out == ""
    assert err == ""
