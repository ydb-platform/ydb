"""Test indeterminate selectors."""
from __future__ import unicode_literals
from .. import util


class TestIndeterminate(util.TestCase):
    """Test indeterminate selectors."""

    def test_indeterminate(self):
        """Test indeterminate."""

        markup = """
        <input type="radio" name="" id="radio-no-name1">
        <label>No name 1</label>
        <input type="radio" name="" id="radio-no-name2" checked>
        <label>no name 2</label>
        <div>
          <input type="checkbox" id="checkbox" indeterminate>
          <label for="checkbox">This label starts out lime.</label>
        </div>
        <div>
          <input type="radio" name="test" id="radio1">
          <label for="radio1">This label starts out lime.</label>
          <form>
            <input type="radio" name="test" id="radio2">
            <label for="radio2">This label starts out lime.</label>

            <input type="radio" name="test" id="radio3" checked>
            <label for="radio3">This label starts out lime.</label>

            <input type="radio" name="other" id="radio4">
            <label for="radio4">This label starts out lime.</label>

            <input type="radio" name="other" id="radio5">
            <label for="radio5">This label starts out lime.</label>
          </form>
          <input type="radio" name="test" id="radio6">
          <label for="radio6">This label starts out lime.</label>
        </div>
        """

        self.assert_selector(
            markup,
            ":indeterminate",
            ['checkbox', 'radio1', 'radio6', 'radio4', 'radio5', 'radio-no-name1'],
            flags=util.HTML
        )

    def test_iframe(self):
        """Test indeterminate when `iframe` is involved."""

        markup = """
        <form>
            <input type="radio" name="test" id="radio1">
            <label for="radio1">This label starts out lime.</label>

            <iframe>
            <html>
            <body>
            <input type="radio" name="test" id="radio2" checked>
            <label for="radio2">This label starts out lime.</label>

            <input type="radio" name="other" id="radio3">
            <label for="radio3">This label starts out lime.</label>
            </body>
            </html>
            </iframe></form>"""

        self.assert_selector(
            markup,
            ":indeterminate",
            ['radio1', 'radio3'],
            flags=util.PYHTML
        )
