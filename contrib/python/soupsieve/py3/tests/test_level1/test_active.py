"""Test active selectors."""
from .. import util


class TestActive(util.TestCase):
    """Test active selectors."""

    def test_active(self):
        """Test active."""

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
            "a:active",
            [],
            flags=util.HTML
        )
