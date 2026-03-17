"""Test general pseudo-class cases."""
from .. import util
from soupsieve import SelectorSyntaxError


class TestPseudoClass(util.TestCase):
    """Test pseudo-classes."""

    def test_pseudo_class_not_implemented(self):
        """Test pseudo-class that is not implemented."""

        self.assert_raises(':not-implemented', SelectorSyntaxError)

    def test_unrecognized_pseudo(self):
        """Test unrecognized pseudo class."""

        self.assert_raises(':before', SelectorSyntaxError)
