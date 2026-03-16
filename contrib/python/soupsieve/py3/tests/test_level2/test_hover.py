"""Test hover selector."""
from .. import util


class TestHover(util.TestCase):
    """Test hover selector."""

    def test_hover(self):
        """Test hover."""

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
            "a:hover",
            [],
            flags=util.HTML
        )
