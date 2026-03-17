"""Test UUIDs."""

# standard
from typing import Union
from uuid import UUID, uuid4

# external
import pytest

# local
from validators import ValidationError, uuid


@pytest.mark.parametrize(
    ("value",),
    [
        (uuid4(),),
        ("2bc1c94f-0deb-43e9-92a1-4775189ec9f8",),
        (uuid4(),),
        ("888256d7c49341f19fa33f29d3f820d7",),
    ],
)
def test_returns_true_on_valid_uuid(value: Union[str, UUID]):
    """Test returns true on valid uuid."""
    assert uuid(value)


@pytest.mark.parametrize(
    ("value",),
    [
        ("2bc1c94f-deb-43e9-92a1-4775189ec9f8",),
        ("2bc1c94f-0deb-43e9-92a1-4775189ec9f",),
        ("gbc1c94f-0deb-43e9-92a1-4775189ec9f8",),
        ("2bc1c94f 0deb-43e9-92a1-4775189ec9f8",),
    ],
)
def test_returns_failed_validation_on_invalid_uuid(value: Union[str, UUID]):
    """Test returns failed validation on invalid uuid."""
    assert isinstance(uuid(value), ValidationError)
