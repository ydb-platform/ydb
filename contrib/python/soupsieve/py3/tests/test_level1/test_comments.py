"""Test comments."""
from .. import util


class TestComments(util.TestCase):
    """Test comments."""

    MARKUP = """
    <div>
    <p id="0">Some text <span id="1"> in a paragraph</span>.</p>
    <a id="2" href="http://google.com">Link</a>
    <span id="3">Direct child</span>
    <pre>
    <span id="4">Child 1</span>
    <span id="5">Child 2</span>
    <span id="6">Child 3</span>
    </pre>
    </div>
    """

    def test_comments(self):
        """Test comments."""

        self.assert_selector(
            self.MARKUP,
            """
            /* Start comment */
            div
            /* This still works as new lines and whitespace count as descendant combiner.
               This comment won't be seen. */
            span#\\33
            /* End comment */
            """,
            ['3'],
            flags=util.HTML
        )

    def test_comments_in_pseudo_classes(self):
        """Test comments in pseudo-classes."""

        self.assert_selector(
            self.MARKUP,
            """
            span:not(
                /* Comments should basically work like they do in real CSS. */
                span#\\33 /* Don't select id 3 */
            )
            """,
            ['1', '4', '5', '6'],
            flags=util.HTML
        )
