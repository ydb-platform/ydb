"""Test focus selector."""
from .. import util


class TestFocus(util.TestCase):
    """Test focus selector."""

    MARKUP = """
    <form action="#">
      <fieldset id='a' disabled>
        <legend>
          Simple fieldset <input type="radio" id="1" checked>
          <fieldset id='b' disabled>
            <legend>Simple fieldset <input type="radio" id="2" checked></legend>
            <input type="radio" id="3" checked>
            <label for="radio">radio</label>
          </fieldset>
        </legend>
        <fieldset id='c' disabled>
          <legend>Simple fieldset <input type="radio" id="4" checked></legend>
          <input type="radio" id="5" checked>
          <label for="radio">radio</label>
        </fieldset>
        <input type="radio" id="6" checked>
        <label for="radio">radio</label>
      </fieldset>
      <optgroup id="opt-enable">
        <option id="7" disabled>option</option>
      </optgroup>
      <optgroup id="8" disabled>
        <option id="9">option</option>
      </optgroup>
      <a href="" id="link">text</a>
    </form>
    """

    def test_focus(self):
        """Test focus."""

        self.assert_selector(
            self.MARKUP,
            "input:focus",
            [],
            flags=util.HTML
        )

    def test_not_focus(self):
        """Test not focus."""

        self.assert_selector(
            self.MARKUP,
            "input:not(:focus)",
            ["1", "2", "3", "4", "5", "6"],
            flags=util.HTML
        )
