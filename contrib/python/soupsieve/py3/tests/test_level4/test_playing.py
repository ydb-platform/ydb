"""Test playing selectors."""
from .. import util


class TestPlaying(util.TestCase):
    """Test playing selectors."""

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

    def test_playing(self):
        """Test playing (matches nothing)."""

        # Not actually sure how this is used, but it won't match anything anyways
        self.assert_selector(
            self.MARKUP,
            "video:playing",
            [],
            flags=util.HTML
        )

    def test_not_playing(self):
        """Test not playing."""

        self.assert_selector(
            self.MARKUP,
            "video:not(:playing)",
            ["vid"],
            flags=util.HTML
        )
