"""Test paused selectors."""
from .. import util


class TestPaused(util.TestCase):
    """Test paused selectors."""

    MARKUP = """
    <!DOCTYPE html>
    <html>
    <body>

    <video id="vid" width="320" height="240" controls>
      <source src="movie.mp4" type="video/mp4">
      <source src="movie.ogg" type="video/ogg">
      Your browser does not support the video tag.
    </video>

    </body>
    </html>
    """

    def test_paused(self):
        """Test paused (matches nothing)."""

        # Not actually sure how this is used, but it won't match anything anyways
        self.assert_selector(
            self.MARKUP,
            "video:paused",
            [],
            flags=util.HTML
        )

    def test_not_paused(self):
        """Test not paused."""

        self.assert_selector(
            self.MARKUP,
            "video:not(:paused)",
            ["vid"],
            flags=util.HTML
        )
