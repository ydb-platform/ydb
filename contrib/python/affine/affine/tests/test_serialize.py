"""Test unpacking and repacking affine matrices."""

from affine import Affine


def test_issue79():
    """An affine matrix can be created from an unpacked matrix."""
    Affine(*Affine.identity())
