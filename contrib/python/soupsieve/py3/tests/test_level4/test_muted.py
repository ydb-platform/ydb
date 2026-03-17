"""Test muted selectors."""
from .. import util


class TestPaused(util.TestCase):
    """Test paused selectors."""

    MARKUP = """
    <!DOCTYPE html>
    <html>
    <body>

    <video id="vid1" width="320" height="240" controls muted>
      <source src="movie.mp4" type="video/mp4">
      <source src="movie.ogg" type="video/ogg">
      Your browser does not support the video tag.
    </video>

    <video id="vid2" width="320" height="240" controls>
      <source src="movie.mp4" type="video/mp4">
      <source src="movie.ogg" type="video/ogg">
      Your browser does not support the video tag.
    </video>

    </body>
    </html>
    """

    def test_muted(self):
        """Test muted."""

        self.assert_selector(
            self.MARKUP,
            "video:muted",
            ['vid1'],
            flags=util.HTML
        )

    def test_not_muted(self):
        """Test not muted."""

        self.assert_selector(
            self.MARKUP,
            "video:not(:muted)",
            ["vid2"],
            flags=util.HTML
        )
