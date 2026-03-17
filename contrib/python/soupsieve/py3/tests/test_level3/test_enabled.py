"""Test enabled selectors."""
from .. import util


class TestEnabled(util.TestCase):
    """Test enabled selectors."""

    MARKUP = """
    <body>
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
    </body>
    """

    MAKRUP_NESTED = """
    <body>
    <form action="#">
      <fieldset id='a' disabled>
        <legend>
          Simple fieldset <input type="radio" id="1" checked>
          <fieldset id='b'>
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
    </body>
    """

    def test_enable_html5(self):
        """
        Test enable in the HTML5 parser.

        `:any-link`
        Form elements that have `disabled`.
        Form elements that are children of a disabled `fieldset`, but not it's `legend`.
        """

        self.assert_selector(
            self.MARKUP,
            ":enabled",
            ['1', '2', 'opt-enable'],
            flags=util.HTML5
        )

    def test_enable_lxml(self):
        """
        Test enable in the `lxml` HTML parser.

        `:any-link`
        Form elements that have `disabled`.
        Form elements that are children of a disabled `fieldset`, but not it's `legend`.
        """

        self.assert_selector(
            self.MARKUP,
            ":enabled",
            ['1', 'opt-enable'],
            flags=util.LXML_HTML
        )

    def test_enable_python(self):
        """
        Test enable in the built-in HTML parser.

        `:any-link`
        Form elements that have `disabled`.
        Form elements that are children of a disabled `fieldset`, but not it's `legend`.
        """

        self.assert_selector(
            self.MARKUP,
            ":enabled",
            ['1', '2', 'opt-enable'],
            flags=util.PYHTML
        )

    def test_enable_with_nested_disabled_form_html5(self):
        """Test enable in the HTML5 parser."""

        self.assert_selector(
            self.MAKRUP_NESTED,
            ":enabled",
            ['1', '2', 'opt-enable', 'b', '3'],
            flags=util.HTML5
        )

    def test_enable_with_nested_disabled_form_lxml(self):
        """Test enable in the `lxml` HTML parser."""

        self.assert_selector(
            self.MAKRUP_NESTED,
            ":enabled",
            ['1', 'opt-enable'],
            flags=util.LXML_HTML
        )

    def test_enable_with_nested_disabled_form_python(self):
        """Test enable in the built-in HTML parser."""

        self.assert_selector(
            self.MAKRUP_NESTED,
            ":enabled",
            ['1', '2', 'opt-enable', 'b', '3'],
            flags=util.PYHTML
        )
