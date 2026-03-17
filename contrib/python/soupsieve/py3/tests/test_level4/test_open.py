"""Test open selectors."""
from .. import util


class TestOpen(util.TestCase):
    """Test open selectors."""

    MARKUP = """
    <!DOCTYPE html>
    <html>
    <body>

    <details id="1">
    <summary>This is closed.</summary<
    <p>A closed details element.</p>
    </details>

    <details id="2" open>
    <summary>This is open.</summary<
    <p>An open details element.</p>
    </details>

    <dialog id="3" open>
      <p>Greetings, one and all!</p>
      <form method="dialog">
        <button>OK</button>
      </form>
    </dialog>

    <dialog id="4">
      <p>Goodbye, one and all!</p>
      <form method="dialog">
        <button>OK</button>
      </form>
    </dialog>

    </body>
    </html>
    """

    def test_open(self):
        """Test open."""

        self.assert_selector(
            self.MARKUP,
            ":open",
            ['2', '3'],
            flags=util.HTML
        )

    def test_targted_open(self):
        """Test targeted open."""

        self.assert_selector(
            self.MARKUP,
            "details:open",
            ['2'],
            flags=util.HTML
        )

        self.assert_selector(
            self.MARKUP,
            "dialog:open",
            ['3'],
            flags=util.HTML
        )

    def test_not_open(self):
        """Test not open."""

        self.assert_selector(
            self.MARKUP,
            ":is(dialog, details):not(:open)",
            ["1", "4"],
            flags=util.HTML
        )
