"""Test disabled selectors."""
from .. import util


class TestDisabled(util.TestCase):
    """Test disabled selectors."""

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
      <optgroup>
        <option id="7" disabled>option</option>
      </optgroup>
      <optgroup id="8" disabled>
        <option id="9">option</option>
      </optgroup>
    </form>
    </body>
    """

    MARKUP_NESTED = """
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
      <optgroup>
        <option id="7" disabled>option</option>
      </optgroup>
      <optgroup id="8" disabled>
        <option id="9">option</option>
      </optgroup>
    </form>
    </body>
    """

    def test_disabled_html5(self):
        """
        Test disabled for HTML5 parser.

        Form elements that have `disabled`.
        `option` that is child of disabled `optgroup`.
        Form elements that are children of a disabled `fieldset`, but not it's `legend`.
        """

        self.assert_selector(
            self.MARKUP,
            ":disabled",
            ['3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c'],
            flags=util.HTML5
        )

    def test_disabled_lxml(self):
        """
        Test disabled for `lxml` HTML parser.

        Form elements that have `disabled`.
        `option` that is child of disabled `optgroup`.
        Form elements that are children of a disabled `fieldset`, but not it's `legend`.
        """

        self.assert_selector(
            self.MARKUP,
            ":disabled",
            ['2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c'],
            flags=util.LXML_HTML
        )

    def test_disabled_python(self):
        """
        Test disabled for the built-in HTML parser.

        Form elements that have `disabled`.
        `option` that is child of disabled `optgroup`.
        Form elements that are children of a disabled `fieldset`, but not it's `legend`.
        """

        self.assert_selector(
            self.MARKUP,
            ":disabled",
            ['3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c'],
            flags=util.PYHTML
        )

    def test_disabled_with_nested_disabled_form_html5(self):
        """Test disabled (with nested disabled forms) in the built-in HTML parser."""

        self.assert_selector(
            self.MARKUP_NESTED,
            ":disabled",
            ['4', '5', '6', '7', '8', '9', 'a', 'c'],
            flags=util.HTML5
        )

    def test_disabled_with_nested_disabled_form_lxml(self):
        """Test disabled (with nested disabled forms) in the `lxml` HTML parser."""

        self.assert_selector(
            self.MARKUP_NESTED,
            ":disabled",
            ['2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c'],
            flags=util.LXML_HTML
        )

    def test_disabled_with_nested_disabled_form_python(self):
        """Test disabled (with nested disabled forms) the built-in HTML parser."""

        self.assert_selector(
            self.MARKUP_NESTED,
            ":disabled",
            ['4', '5', '6', '7', '8', '9', 'a', 'c'],
            flags=util.PYHTML
        )

    def test_disabled_with_nested_optgroup(self):
        """Test `:disabled` only selects `option` elements whose closest `optgroup` parent is disabled."""

        self.assert_selector(
            """
            <optgroup id="0" disabled>
               <option id="1"></option>
               <optgroup id="3">
                   <option id="4"></option>
               </optgroup>
            </optgroup>
            """,
            ":disabled",
            ['0', '1'],
            flags=util.HTML
        )
