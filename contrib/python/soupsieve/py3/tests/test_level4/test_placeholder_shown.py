"""Test placeholder shown selectors."""
from .. import util


class TestPlaceholderShown(util.TestCase):
    """Test placeholder shown selectors."""

    def test_placeholder_shown(self):
        """Test placeholder shown."""

        markup = """
        <!-- These have a placeholder. -->
        <input id="0" placeholder="This is some text">
        <textarea id="1" placeholder="This is some text"></textarea>

        <!-- These do not have a placeholder. -->
        <input id="2" placeholder="">
        <input id="3">

        <!-- All types that should register has having a placeholder. -->
        <input id="4" type="email" placeholder="This is some text">
        <input id="5" type="number" placeholder="This is some text">
        <input id="6" type="password" placeholder="This is some text">
        <input id="7" type="search" placeholder="This is some text">
        <input id="8" type="tel" placeholder="This is some text">
        <input id="9" type="text" placeholder="This is some text">
        <input id="10" type="url" placeholder="This is some text">
        <input id="11" type="" placeholder="This is some text">
        <input id="12" type placeholder="This is some text">

        <!-- Types that should not register has having a placeholder. -->
        <input id="13" type="button" placeholder="This is some text">
        <input id="14" type="checkbox" placeholder="This is some text">
        <input id="15" type="color" placeholder="This is some text">
        <input id="16" type="date" placeholder="This is some text">
        <input id="17" type="datetime-local" placeholder="This is some text">
        <input id="18" type="file" placeholder="This is some text">
        <input id="19" type="hidden" placeholder="This is some text">
        <input id="20" type="image" placeholder="This is some text">
        <input id="21" type="month" placeholder="This is some text">
        <input id="22" type="radio" placeholder="This is some text">
        <input id="23" type="range" placeholder="This is some text">
        <input id="24" type="reset" placeholder="This is some text">
        <input id="25" type="submit" placeholder="This is some text">
        <input id="26" type="time" placeholder="This is some text">
        <input id="27" type="week" placeholder="This is some text">

        <!-- Value will not override this instance as value is empty. -->
        <input id="28" type placeholder="This is some text" value="">

        <!-- Value will override this input -->
        <input id="29" type placeholder="This is some text" value="Actual value">

        <!-- Text area content overrides the placehold-->
        <textarea id="30" placeholder="This is some text">Value</textarea>
        <textarea id="31" placeholder="This is some text">


        </textarea>

        <!-- Text area is still considered empty with a single new line (does not include carriage return). -->
        <textarea id="32" placeholder="This is some text">
        </textarea>
        """

        self.assert_selector(
            markup,
            ":placeholder-shown",
            ['0', '1', '4', '5', '6', '7', '8', '9', '10', '11', '12', '28', '32'],
            flags=util.HTML
        )
