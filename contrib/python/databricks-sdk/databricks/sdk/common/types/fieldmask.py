class FieldMask(object):
    """Class for FieldMask message type."""

    # This is based on the base implementation from protobuf.
    # https://pigweed.googlesource.com/third_party/github/protocolbuffers/protobuf/+/HEAD/python/google/protobuf/internal/field_mask.py
    # The original implementation only works with proto generated classes.
    # Since our classes are not generated from proto files, we need to implement it manually.

    def __init__(self, field_mask=None):
        """Initializes the FieldMask."""
        if field_mask:
            self.paths = field_mask

    def ToJsonString(self) -> str:
        """Converts FieldMask to string."""
        return ",".join(self.paths)

    def FromJsonString(self, value: str) -> None:
        """Converts string to FieldMask."""
        if not isinstance(value, str):
            raise ValueError("FieldMask JSON value not a string: {!r}".format(value))
        if value:
            self.paths = value.split(",")
        else:
            self.paths = []

    def __eq__(self, other) -> bool:
        """Check equality based on paths."""
        if not isinstance(other, FieldMask):
            return False
        return self.paths == other.paths

    def __hash__(self) -> int:
        """Hash based on paths tuple."""
        return hash(tuple(self.paths))

    def __repr__(self) -> str:
        """String representation for debugging."""
        return f"FieldMask(paths={self.paths})"
