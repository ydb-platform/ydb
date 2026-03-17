from __future__ import annotations

import pytest

from rapidfuzz import utils_cpp, utils_py
from tests.distance.common import JaroWinkler


def test_hash_special_case():
    assert pytest.approx(JaroWinkler.similarity([0, -1], [0, -2])) == 0.666666


def test_large_prefix_weight():
    assert pytest.approx(JaroWinkler.similarity("milyarder", "milyarderlik", prefix_weight=0.5)) == 1.0
    assert pytest.approx(JaroWinkler.similarity("milyarder", "milyarderlik", prefix_weight=1.0)) == 1.0


def test_invalid_prefix_weight():
    with pytest.raises(ValueError, match="prefix_weight has to be in the range 0.0 - 1.0"):
        JaroWinkler.similarity("milyarder", "milyarderlik", prefix_weight=-0.1)

    with pytest.raises(ValueError, match="prefix_weight has to be in the range 0.0 - 1.0"):
        JaroWinkler.similarity("milyarder", "milyarderlik", prefix_weight=1.1)


def test_edge_case_lengths():
    """
    these are largely found by fuzz tests and implemented here as regression tests
    """
    assert pytest.approx(JaroWinkler.similarity("", "")) == 1.0
    assert pytest.approx(JaroWinkler.similarity("0", "0")) == 1
    assert pytest.approx(JaroWinkler.similarity("00", "00")) == 1
    assert pytest.approx(JaroWinkler.similarity("0", "00")) == 0.85

    assert pytest.approx(JaroWinkler.similarity("0" * 65, "0" * 65)) == 1
    assert pytest.approx(JaroWinkler.similarity("0" * 64, "0" * 65)) == 0.996923
    assert pytest.approx(JaroWinkler.similarity("0" * 63, "0" * 65)) == 0.993846

    s1 = "000000001"
    s2 = "0000010"
    assert pytest.approx(JaroWinkler.similarity(s1, s2)) == 0.926984

    s1 = "10000000000000000000000000000000000000000000000000000000000000020"
    s2 = "00000000000000000000000000000000000000000000000000000000000000000"
    assert pytest.approx(JaroWinkler.similarity(s1, s2)) == 0.979487

    s1 = "00000000000000100000000000000000000000010000000000000000000000000"
    s2 = "0000000000000000000000000000000000000000000000000000000000000000000000000000001"
    assert pytest.approx(JaroWinkler.similarity(s2, s1)) == 0.95334

    s1 = "00000000000000000000000000000000000000000000000000000000000000000"
    s2 = (
        "010000000000000000000000000000000000000000000000000000000000000000"
        "00000000000000000000000000000000000000000000000000000000000000"
    )
    assert pytest.approx(JaroWinkler.similarity(s2, s1)) == 0.852344


def testCaseInsensitive():
    assert (
        pytest.approx(JaroWinkler.similarity("new york mets", "new YORK mets", processor=utils_cpp.default_process))
        == 1.0
    )
    assert (
        pytest.approx(JaroWinkler.similarity("new york mets", "new YORK mets", processor=utils_py.default_process))
        == 1.0
    )
