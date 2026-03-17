"""Test empty selectors."""
from .. import util


class TestEmpty(util.TestCase):
    """Test empty selectors."""

    def test_empty(self):
        """Test empty."""

        markup = """
        <body>
        <div id="div">
        <p id="0" class="somewordshere">Some text <span id="1"> in a paragraph</span>.</p>
        <a id="2" href="http://google.com">Link</a>
        <span id="3" class="herewords">Direct child</span>
        <pre id="pre" class="wordshere">
        <span id="4"> <!-- comment --> </span>
        <span id="5"> </span>
        <span id="6"></span>
        <span id="7"><span id="8"></span></span>
        </pre>
        </div>
        </body>
        """

        self.assert_selector(
            markup,
            "body :empty",
            ["4", "5", "6", "8"],
            flags=util.HTML
        )
