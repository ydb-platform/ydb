import pytest  # type: ignore

from ua_parser.utils import fa_simplifier


@pytest.mark.parametrize(
    ("from_", "to"),
    [
        (r"\d", "[0-9]"),
        (r"[\d]", "[0-9]"),
        (r"[\d\.]", r"[0-9\.]"),
    ],
)
def test_classes(from_, to):
    assert fa_simplifier(from_) == to
