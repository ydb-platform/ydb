"""Test validation Failure."""

# local
from validators import between

failed_obj_repr = "ValidationError(func=between"


class TestValidationError:
    """Test validation Failure."""

    def setup_method(self):
        """Setup Method."""
        self.is_in_between = between(3, min_val=4, max_val=5)

    def test_boolean_coerce(self):
        """Test Boolean."""
        assert not bool(self.is_in_between)
        assert not self.is_in_between

    def test_repr(self):
        """Test Repr."""
        assert failed_obj_repr in repr(self.is_in_between)

    def test_string(self):
        """Test Repr."""
        assert failed_obj_repr in str(self.is_in_between)

    def test_arguments_as_properties(self):
        """Test argument properties."""
        assert self.is_in_between.__dict__["value"] == 3
        assert self.is_in_between.__dict__["min_val"] == 4
        assert self.is_in_between.__dict__["max_val"] == 5
