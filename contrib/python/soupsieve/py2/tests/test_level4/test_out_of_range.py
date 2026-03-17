"""Test out of range selectors."""
from __future__ import unicode_literals
from .. import util


class TestOutOfRange(util.TestCase):
    """Test out of range selectors."""

    def test_out_of_range_number(self):
        """Test in range number."""

        markup = """
        <!-- These should not match -->
        <input id="0" type="number" min="0" max="10" value="5">
        <input id="1" type="number" min="-1" max="10" value="5">
        <input id="2" type="number" min="2.2" max="8.8" value="5.2">
        <input id="3" type="number" min="2.2" value="5.2">
        <input id="4" type="number" max="8.8" value="5.2">
        <input id="5" type="number" min="2.2" value="2.2">
        <input id="6" type="number" max="8.8" value="8.8">
        <input id="7" type="number" max="8.8">
        <input id="8" type="number" max="8.8" value="invalid">

        <!-- These should match -->
        <input id="9" type="number" min="0" max="10" value="-1">
        <input id="10" type="number" min="0" max="10" value="10.1">
        <input id="11" type="number" max="0" min="10" value="11">

        <!-- These cannot match -->
        <input id="12" type="number" value="10">
        <input id="13" type="number" min="invalid" value="10">
        """

        self.assert_selector(
            markup,
            ":out-of-range",
            ['9', '10', '11'],
            flags=util.HTML
        )

    def test_out_of_range_range(self):
        """Test in range range."""

        markup = """
        <!-- These should not match -->
        <input id="0" type="range" min="0" max="10" value="5">
        <input id="1" type="range" min="-1" max="10" value="5">
        <input id="2" type="range" min="2.2" max="8.8" value="5.2">
        <input id="3" type="range" min="2.2" value="5.2">
        <input id="4" type="range" max="8.8" value="5.2">
        <input id="5" type="range" min="2.2" value="2.2">
        <input id="6" type="range" max="8.8" value="8.8">
        <input id="7" type="range" max="8.8">
        <input id="8" type="range" max="8.8" value="invalid">

        <!-- These should match -->
        <input id="9" type="range" min="0" max="10" value="-1">
        <input id="10" type="range" min="0" max="10" value="10.1">

        <!-- These cannot match -->
        <input id="11" type="range" value="10">
        <input id="12" type="range" min="invalid" value="10">
        """

        self.assert_selector(
            markup,
            ":out-of-range",
            ['9', '10'],
            flags=util.HTML
        )

    def test_out_of_range_month(self):
        """Test in range month."""

        markup = """
        <!-- These should not match -->
        <input id="0" type="month" min="1980-02" max="2004-08" value="1999-05">
        <input id="1" type="month" min="1980-02" max="2004-08" value="1980-02">
        <input id="2" type="month" min="1980-02" max="2004-08" value="2004-08">
        <input id="3" type="month" min="1980-02" value="1999-05">
        <input id="4" type="month" max="2004-08" value="1999-05">
        <input id="5" type="month" min="1980-02" max="2004-08" value="1999-13">
        <input id="6" type="month" min="1980-02" max="2004-08">

        <!-- These should match -->
        <input id="7" type="month" min="1980-02" max="2004-08" value="1979-02">
        <input id="8" type="month" min="1980-02" max="2004-08" value="1980-01">
        <input id="9" type="month" min="1980-02" max="2004-08" value="2005-08">
        <input id="10" type="month" min="1980-02" max="2004-08" value="2004-09">

        <!-- These cannot match -->
        <input id="11" type="month" value="1999-05">
        <input id="12" type="month" min="invalid" value="1999-05">
        """

        self.assert_selector(
            markup,
            ":out-of-range",
            ['7', '8', '9', '10'],
            flags=util.HTML
        )

    def test_out_of_range_week(self):
        """Test in range week."""

        markup = """
        <!-- These should not match -->
        <input id="0" type="week" min="1980-W53" max="2004-W20" value="1999-W05">
        <input id="1" type="week" min="1980-W53" max="2004-W20" value="1980-W53">
        <input id="2" type="week" min="1980-W53" max="2004-W20" value="2004-W20">
        <input id="3" type="week" min="1980-W53" value="1999-W05">
        <input id="4" type="week" max="2004-W20" value="1999-W05">
        <input id="5" type="week" min="1980-W53" max="2004-W20" value="2005-W53">
        <input id="6" type="week" min="1980-W53" max="2004-W20" value="2005-w52">
        <input id="7" type="week" min="1980-W53" max="2004-W20">

        <!-- These should match -->
        <input id="8" type="week" min="1980-W53" max="2004-W20" value="1979-W53">
        <input id="9" type="week" min="1980-W53" max="2004-W20" value="1980-W52">
        <input id="10" type="week" min="1980-W53" max="2004-W20" value="2005-W20">
        <input id="11" type="week" min="1980-W53" max="2004-W20" value="2004-W21">

        <!-- These cannot match -->
        <input id="12" type="week" value="1999-W05">
        <input id="13" type="week" min="invalid" value="1999-W05">
        """

        self.assert_selector(
            markup,
            ":out-of-range",
            ['8', '9', '10', '11'],
            flags=util.HTML
        )

    def test_out_of_range_date(self):
        """Test in range date."""

        markup = """
        <!-- These should not match -->
        <input id="0" type="date" min="1980-02-20" max="2004-08-14" value="1999-05-16">
        <input id="1" type="date" min="1980-02-20" max="2004-08-14" value="1980-02-20">
        <input id="2" type="date" min="1980-02-20" max="2004-08-14" value="2004-08-14">
        <input id="3" type="date" min="1980-02-20" value="1999-05-16">
        <input id="4" type="date" max="2004-08-14" value="1999-05-16">
        <input id="5" type="date" min="1980-02-20" max="2004-08-14" value="1999-13-16">
        <input id="6" type="date" min="1980-02-20" max="2004-08-14">

        <!-- These should match -->
        <input id="7" type="date" min="1980-02-20" max="2004-08-14" value="1979-02-20">
        <input id="8" type="date" min="1980-02-20" max="2004-08-14" value="1980-01-20">
        <input id="9" type="date" min="1980-02-20" max="2004-08-14" value="1980-02-19">
        <input id="10" type="date" min="1980-02-20" max="2004-08-14" value="2005-08-14">
        <input id="11" type="date" min="1980-02-20" max="2004-08-14" value="2004-09-14">
        <input id="12" type="date" min="1980-02-20" max="2004-08-14" value="2004-09-15">

        <!-- These cannot match -->
        <input id="13" type="date" value="1999-05-16">
        <input id="14" type="date" min="invalid" value="1999-05-16">
        """

        self.assert_selector(
            markup,
            ":out-of-range",
            ['7', '8', '9', '10', '11', '12'],
            flags=util.HTML
        )

    def test_out_of_range_date_time(self):
        """Test in range date time."""

        markup = """
        <!-- These should not match -->
        <input id="0" type="datetime-local" min="1980-02-20T01:30" max="2004-08-14T18:45" value="1999-05-16T20:20">
        <input id="1" type="datetime-local" min="1980-02-20T01:30" max="2004-08-14T18:45" value="1980-02-20T01:30">
        <input id="2" type="datetime-local" min="1980-02-20T01:30" max="2004-08-14T18:45" value="2004-08-14T18:45">
        <input id="3" type="datetime-local" min="1980-02-20T01:30" value="1999-05-16T20:20">
        <input id="4" type="datetime-local" max="2004-08-14T18:45" value="1999-05-16T20:20">
        <input id="5" type="datetime-local" min="1980-02-20T01:30" max="2004-08-14T18:45" value="1999-05-16T24:20">
        <input id="6" type="datetime-local" min="1980-02-20T01:30" max="2004-08-14T18:45">

        <!-- These should match -->
        <input id="7" type="datetime-local" min="1980-02-20T01:30" max="2004-08-14T18:45" value="1979-02-20T01:30">
        <input id="8" type="datetime-local" min="1980-02-20T01:30" max="2004-08-14T18:45" value="1980-01-20T01:30">
        <input id="9" type="datetime-local" min="1980-02-20T01:30" max="2004-08-14T18:45" value="1980-02-19T01:30">
        <input id="10" type="datetime-local" min="1980-02-20T01:30" max="2004-08-14T18:45" value="1980-02-19T00:30">
        <input id="11" type="datetime-local" min="1980-02-20T01:30" max="2004-08-14T18:45" value="1980-02-19T01:29">
        <input id="12" type="datetime-local" min="1980-02-20T01:30" max="2004-08-14T18:45" value="2005-08-14T18:45">
        <input id="13" type="datetime-local" min="1980-02-20T01:30" max="2004-08-14T18:45" value="2004-09-14T18:45">
        <input id="14" type="datetime-local" min="1980-02-20T01:30" max="2004-08-14T18:45" value="2004-08-15T18:45">
        <input id="15" type="datetime-local" min="1980-02-20T01:30" max="2004-08-14T18:45" value="2004-08-14T19:45">
        <input id="16" type="datetime-local" min="1980-02-20T01:30" max="2004-08-14T18:45" value="2004-08-14T18:46">

        <!-- These cannot match -->
        <input id="17" type="datetime-local" value="1999-05-16T20:20">
        <input id="18" type="datetime-local" min="invalid" value="1999-05-16T20:20">
        """

        self.assert_selector(
            markup,
            ":out-of-range",
            ['7', '8', '9', '10', '11', '12', '13', '14', '15', '16'],
            flags=util.HTML
        )

    def test_out_of_range_time(self):
        """Test in range time."""

        markup = """
        <!-- These should not match -->
        <input id="0" type="time" min="01:30" max="18:45" value="10:20">
        <input id="1" type="time" max="01:30" min="18:45" value="20:20">
        <input id="2" type="time" min="01:30" max="18:45" value="01:30">
        <input id="3" type="time" min="01:30" max="18:45" value="18:45">
        <input id="4" type="time" min="01:30" value="10:20">
        <input id="5" type="time" max="18:45" value="10:20">
        <input id="6" type="time" min="01:30" max="18:45" value="24:20">
        <input id="7" type="time" min="01:30" max="18:45">

        <!-- These should match -->
        <input id="8" type="time" min="01:30" max="18:45" value="00:30">
        <input id="9" type="time" min="01:30" max="18:45" value="01:29">
        <input id="10" type="time" min="01:30" max="18:45" value="19:45">
        <input id="11" type="time" min="01:30" max="18:45" value="18:46">
        <input id="12" type="time" max="01:30" min="18:45" value="02:30">
        <input id="13" type="time" max="01:30" min="18:45" value="17:45">
        <input id="14" type="time" max="01:30" min="18:45" value="18:44">

        <!-- These cannot match -->
        <input id="15" type="time" value="10:20">
        <input id="16" type="time" min="invalid" value="10:20">
        """

        self.assert_selector(
            markup,
            ":out-of-range",
            ['8', '9', '10', '11', '12', '13', '14'],
            flags=util.HTML
        )
