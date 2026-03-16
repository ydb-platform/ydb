"""Test checked selectors."""
from .. import util


class TestChecked(util.TestCase):
    """Test checked selectors."""

    def test_checked(self):
        """Test checked."""

        markup = """
        <body>
        <div>
          <input type="radio" name="my-input" id="yes" checked>
          <label for="yes">Yes</label>

          <input type="radio" name="my-input" id="no">
          <label for="no">No</label>
        </div>

        <div>
          <input type="checkbox" name="my-checkbox" id="opt-in" checked>
          <label for="opt-in">Check me!</label>
        </div>

        <select name="my-select" id="fruit">
          <option id="1" value="opt1">Apples</option>
          <option id="2" value="opt2" selected>Grapes</option>
          <option id="3" value="opt3">Pears</option>
        </select>
        </body>
        """

        self.assert_selector(
            markup,
            ":checked",
            ['yes', 'opt-in', '2'],
            flags=util.HTML
        )
