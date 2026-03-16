"""Test visited selectors."""
from .. import util


class TestVisited(util.TestCase):
    """Test visited selectors."""

    def test_visited(self):
        """Test visited."""

        markup = """
        <div>
        <p>Some text <span id="1" class="foo:bar:foobar"> in a paragraph</span>.
        <a id="2" class="bar" href="http://google.com">Link</a>
        <a id="3">Placeholder text.</a>
        </p>
        </div>
        """

        self.assert_selector(
            markup,
            "a:visited",
            [],
            flags=util.HTML
        )
