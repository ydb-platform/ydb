"""Test general pseudo-class cases."""
from __future__ import unicode_literals
from .. import util


class TestPseudoClass(util.TestCase):
    """Test pseudo-classes."""

    def test_pseudo_class_not_implemented(self):
        """Test pseudo-class that is not implemented."""

        self.assert_raises(':not-implemented', NotImplementedError)

    def test_unrecognized_pseudo(self):
        """Test unrecognized pseudo class."""

        self.assert_raises(':before', NotImplementedError)
