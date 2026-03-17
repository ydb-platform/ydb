"""Test custom selector aliases."""
from .. import util
import soupsieve as sv


class TestCustomSelectors(util.TestCase):
    """Test custom selector aliases."""

    MARKUP = """
    <body>
    <h1 id="1">Header 1</h1>
    <h2 id="2">Header 2</h2>
    <p id="3"></p>
    <p id="4"><span>child</span></p>
    </body>
    """

    def test_custom_selectors(self):
        """Test custom selectors."""

        custom_selectors = {
            ":--headers": "h1, h2, h3, h4, h5, h6",
            ":--parent": ":has(> *|*)"
        }

        self.assert_selector(
            self.MARKUP,
            ':--headers',
            ['1', '2'],
            custom=custom_selectors,
            flags=util.HTML
        )

        self.assert_selector(
            self.MARKUP,
            ':--headers:nth-child(2)',
            ['2'],
            custom=custom_selectors,
            flags=util.HTML
        )

        self.assert_selector(
            self.MARKUP,
            'p:--parent',
            ['4'],
            custom=custom_selectors,
            flags=util.HTML
        )

    def test_custom_escapes(self):
        """Test custom selectors with escapes."""

        custom_selectors = {
            r":--Header\s": "h1, h2, h3, h4, h5, h6"
        }

        self.assert_selector(
            self.MARKUP,
            r':--\HeaderS',
            ['1', '2'],
            custom=custom_selectors,
            flags=util.HTML
        )

    def test_custom_selectors_exotic(self):
        """Test custom selectors."""

        custom_selectors = {":--": "h1, h2, h3, h4, h5, h6"}

        self.assert_selector(
            self.MARKUP,
            ':--',
            ['1', '2'],
            custom=custom_selectors,
            flags=util.HTML
        )

    def test_custom_dependency(self):
        """Test custom selector dependency on other custom selectors."""

        custom_selectors = {
            ":--parent": ":has(> *|*)",
            ":--parent-paragraph": "p:--parent"
        }

        self.assert_selector(
            self.MARKUP,
            ':--parent-paragraph',
            ['4'],
            custom=custom_selectors,
            flags=util.HTML
        )

    def test_custom_dependency_out_of_order(self):
        """Test custom selector out of order dependency."""

        custom_selectors = {
            ":--parent-paragraph": "p:--parent",
            ":--parent": ":has(> *|*)"
        }

        self.assert_selector(
            self.MARKUP,
            ':--parent-paragraph',
            ['4'],
            custom=custom_selectors,
            flags=util.HTML
        )

    def test_custom_dependency_recursion(self):
        """Test that we fail on dependency recursion."""

        custom_selectors = {
            ":--parent-paragraph": "p:--parent",
            ":--parent": "p:--parent-paragraph"
        }

        self.assert_raises(':--parent', sv.SelectorSyntaxError, custom=custom_selectors)

    def test_bad_custom(self):
        """Test that a bad custom raises a syntax error."""

        custom_selectors = {
            ":--parent": ":has(> *|*)",
            ":--parent-paragraph": "p:--parent"
        }

        self.assert_raises(':--wrong', sv.SelectorSyntaxError, custom=custom_selectors)

    def test_bad_custom_syntax(self):
        """Test that a custom selector with bad syntax in its name fails."""

        self.assert_raises('div', sv.SelectorSyntaxError, custom={":--parent.": ":has(> *|*)"})

    def test_pseudo_class_collision(self):
        """Test that a custom selector cannot match an already existing pseudo-class name."""

        self.assert_raises('div', sv.SelectorSyntaxError, custom={":hover": ":has(> *|*)"})

    def test_custom_collision(self):
        """Test that a custom selector cannot match an already existing custom name."""

        self.assert_raises('div', KeyError, custom={":--parent": ":has(> *|*)", ":--PARENT": ":has(> *|*)"})
