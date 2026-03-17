"""Test `nth` child selectors."""
from .. import util
import soupsieve as sv
from soupsieve import SelectorSyntaxError


class TestNthChild(util.TestCase):
    """Test `nth` child selectors."""

    def test_nth_child(self):
        """Test `nth` child."""

        markup = """
        <body>
        <p id="0"></p>
        <p id="1"></p>
        <span id="2"></span>
        <span id="3"></span>
        <span id="4"></span>
        <span id="5"></span>
        <span id="6"></span>
        <p id="7"></p>
        <p id="8"></p>
        <p id="9"></p>
        <p id="10"></p>
        <span id="11"></span>
        </body>
        """

        self.assert_selector(
            markup,
            "p:nth-child(-2)",
            [],
            flags=util.HTML
        )

        self.assert_selector(
            markup,
            "p:nth-child(2)",
            ['1'],
            flags=util.HTML
        )

        self.assert_selector(
            markup,
            "p:NTH-CHILD(2)",
            ['1'],
            flags=util.HTML
        )

        self.assert_selector(
            markup,
            r"p:NT\H-CH\ILD(2)",
            ['1'],
            flags=util.HTML
        )

    def test_nth_child_odd(self):
        """Test `nth` child odd."""

        markup = """
        <body>
        <p id="0"></p>
        <p id="1"></p>
        <span id="2"></span>
        <span id="3"></span>
        <span id="4"></span>
        <span id="5"></span>
        <span id="6"></span>
        <p id="7"></p>
        <p id="8"></p>
        <p id="9"></p>
        <p id="10"></p>
        <span id="11"></span>
        </body>
        """

        self.assert_selector(
            markup,
            "p:nth-child(odd)",
            ['0', '8', '10'],
            flags=util.HTML
        )

        self.assert_selector(
            markup,
            "p:nth-child(ODD)",
            ['0', '8', '10'],
            flags=util.HTML
        )

    def test_nth_child_even(self):
        """Test `nth` child even."""

        markup = """
        <body>
        <p id="0"></p>
        <p id="1"></p>
        <span id="2"></span>
        <span id="3"></span>
        <span id="4"></span>
        <span id="5"></span>
        <span id="6"></span>
        <p id="7"></p>
        <p id="8"></p>
        <p id="9"></p>
        <p id="10"></p>
        <span id="11"></span>
        </body>
        """

        self.assert_selector(
            markup,
            "p:nth-child(even)",
            ['1', '7', '9'],
            flags=util.HTML
        )

        self.assert_selector(
            markup,
            "p:nth-child(EVEN)",
            ['1', '7', '9'],
            flags=util.HTML
        )

    def test_nth_child_complex(self):
        """Test `nth` child complex."""

        markup = """
        <body>
        <p id="0"></p>
        <p id="1"></p>
        <span id="2"></span>
        <span id="3"></span>
        <span id="4"></span>
        <span id="5"></span>
        <span id="6"></span>
        <p id="7"></p>
        <p id="8"></p>
        <p id="9"></p>
        <p id="10"></p>
        <span id="11"></span>
        </body>
        """

        self.assert_selector(
            markup,
            "p:nth-child(2n-5)",
            ['0', '8', '10'],
            flags=util.HTML
        )

        self.assert_selector(
            markup,
            "p:nth-child(2N-5)",
            ['0', '8', '10'],
            flags=util.HTML
        )

        self.assert_selector(
            markup,
            "p:nth-child(-2n+20)",
            ['1', '7', '9'],
            flags=util.HTML
        )

        self.assert_selector(
            markup,
            "p:nth-child(50n-20)",
            [],
            flags=util.HTML
        )

        self.assert_selector(
            markup,
            "p:nth-child(-2n-2)",
            [],
            flags=util.HTML
        )

        self.assert_selector(
            markup,
            "p:nth-child(9n - 1)",
            ['7'],
            flags=util.HTML
        )

        self.assert_selector(
            markup,
            "p:nth-child(2n + 1)",
            ['0', '8', '10'],
            flags=util.HTML
        )

        self.assert_selector(
            markup,
            "p:nth-child(-n+3)",
            ['0', '1'],
            flags=util.HTML
        )

        self.assert_selector(
            markup,
            "span:nth-child(-n+3)",
            ['2'],
            flags=util.HTML
        )

        self.assert_selector(
            markup,
            "body *:nth-child(-n+3)",
            ['0', '1', '2'],
            flags=util.HTML
        )

    def test_nth_child_no_parent(self):
        """Test `nth` child with no parent."""

        markup = """
        <body>
        <p id="0"></p>
        <p id="1"></p>
        <span id="2"></span>
        <span id="3"></span>
        <span id="4"></span>
        <span id="5"></span>
        <span id="6"></span>
        <p id="7"></p>
        <p id="8"></p>
        <p id="9"></p>
        <p id="10"></p>
        <span id="11"></span>
        </body>
        """

        for parser in util.available_parsers('html.parser', 'lxml', 'html5lib'):
            # Paragraph is the root. There is no document.
            markup = """<p id="1">text</p>"""
            soup = self.soup(markup, parser)
            fragment = soup.p.extract()
            self.assertTrue(sv.match("p:nth-child(1)", fragment, flags=sv.DEBUG))

    def test_nth_child_with_bad_parameters(self):
        """Test that pseudo class fails with bad parameters (basically it doesn't match)."""

        self.assert_raises(':nth-child(a)', SelectorSyntaxError)
