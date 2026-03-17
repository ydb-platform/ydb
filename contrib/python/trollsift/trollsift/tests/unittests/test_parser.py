"""Basic unit tests for the parser module."""

import unittest
import datetime as dt
import pytest

from trollsift.parser import get_convert_dict, extract_values
from trollsift.parser import _convert
from trollsift.parser import parse, globify, validate, is_one2one, compose, Parser


class TestParser(unittest.TestCase):
    def setUp(self):
        self.fmt = "/somedir/{directory}/hrpt_{platform:4s}{platnum:2s}" + "_{time:%Y%m%d_%H%M}_{orbit:05d}.l1b"
        self.string = "/somedir/otherdir/hrpt_noaa16_20140210_1004_69022.l1b"
        self.string2 = "/somedir/otherdir/hrpt_noaa16_20140210_1004_00022.l1b"
        self.string3 = "/somedir/otherdir/hrpt_noaa16_20140210_1004_69022"
        self.string4 = "/somedir/otherdir/hrpt_noaa16_20140210_1004_69022"

    def test_parser_keys(self):
        parser = Parser(self.fmt)
        keys = {"directory", "platform", "platnum", "time", "orbit"}
        self.assertTrue(keys.issubset(parser.keys()) and keys.issuperset(parser.keys()))

    def test_get_convert_dict(self):
        # Run
        result = get_convert_dict(self.fmt)
        # Assert
        self.assertDictEqual(
            result,
            {
                "directory": "",
                "platform": "4s",
                "platnum": "2s",
                "time": "%Y%m%d_%H%M",
                "orbit": "05d",
            },
        )

    def test_extract_values(self):
        fmt = "/somedir/{directory}/hrpt_{platform:4s}{platnum:2s}_{time:%Y%m%d_%H%M}_{orbit:d}.l1b"
        result = extract_values(fmt, self.string)
        self.assertDictEqual(
            result,
            {
                "directory": "otherdir",
                "platform": "noaa",
                "platnum": "16",
                "time": "20140210_1004",
                "orbit": "69022",
            },
        )

    def test_extract_values_end(self):
        fmt = "/somedir/{directory}/hrpt_{platform:4s}{platnum:2s}_{time:%Y%m%d_%H%M}_{orbit:d}"
        result = extract_values(fmt, self.string3)
        self.assertDictEqual(
            result,
            {
                "directory": "otherdir",
                "platform": "noaa",
                "platnum": "16",
                "time": "20140210_1004",
                "orbit": "69022",
            },
        )

    def test_extract_values_beginning(self):
        fmt = "{directory}/hrpt_{platform:4s}{platnum:2s}_{time:%Y%m%d_%H%M}_{orbit:d}"
        result = extract_values(fmt, self.string4)
        self.assertDictEqual(
            result,
            {
                "directory": "/somedir/otherdir",
                "platform": "noaa",
                "platnum": "16",
                "time": "20140210_1004",
                "orbit": "69022",
            },
        )

    def test_extract_values_s4spair(self):
        fmt = "{directory}/hrpt_{platform:4s}{platnum:s}_{time:%Y%m%d_%H%M}_{orbit:d}"
        result = extract_values(fmt, self.string4)
        self.assertDictEqual(
            result,
            {
                "directory": "/somedir/otherdir",
                "platform": "noaa",
                "platnum": "16",
                "time": "20140210_1004",
                "orbit": "69022",
            },
        )

    def test_extract_values_ss2pair(self):
        fmt = "{directory}/hrpt_{platform:s}{platnum:2s}_{time:%Y%m%d_%H%M}_{orbit:d}"
        result = extract_values(fmt, self.string4)
        self.assertDictEqual(
            result,
            {
                "directory": "/somedir/otherdir",
                "platform": "noaa",
                "platnum": "16",
                "time": "20140210_1004",
                "orbit": "69022",
            },
        )

    def test_extract_values_ss2pair_end(self):
        fmt = "{directory}/hrpt_{platform:s}{platnum:2s}"
        result = extract_values(fmt, "/somedir/otherdir/hrpt_noaa16")
        self.assertDictEqual(
            result,
            {"directory": "/somedir/otherdir", "platform": "noaa", "platnum": "16"},
        )

    def test_extract_values_sdatetimepair_end(self):
        fmt = "{directory}/hrpt_{platform:s}{date:%Y%m%d}"
        result = extract_values(fmt, "/somedir/otherdir/hrpt_noaa20140212")
        self.assertDictEqual(
            result,
            {"directory": "/somedir/otherdir", "platform": "noaa", "date": "20140212"},
        )

    def test_extract_values_everything(self):
        fmt = "{everything}"
        result = extract_values(fmt, self.string)
        self.assertDictEqual(
            result,
            {"everything": "/somedir/otherdir/hrpt_noaa16_20140210_1004_69022.l1b"},
        )

    def test_extract_values_padding2(self):
        fmt = "/somedir/{directory}/hrpt_{platform:4s}{platnum:2s}_{time:%Y%m%d_%H%M}_{orbit:0>5d}.l1b"
        # parsedef = ['/somedir/', {'directory': None}, '/hrpt_',
        #             {'platform': '4s'}, {'platnum': '2s'},
        #             '_', {'time': '%Y%m%d_%H%M'}, '_',
        #             {'orbit': '0>5d'}, '.l1b']
        result = extract_values(fmt, self.string2)
        # Assert
        self.assertDictEqual(
            result,
            {
                "directory": "otherdir",
                "platform": "noaa",
                "platnum": "16",
                "time": "20140210_1004",
                "orbit": "00022",
            },
        )

    def test_extract_values_fails(self):
        fmt = "/somedir/{directory}/hrpt_{platform:4s}{platnum:2s}_{time:%Y%m%d_%H%M}_{orbit:4d}.l1b"
        self.assertRaises(ValueError, extract_values, fmt, self.string)

    def test_extract_values_full_match(self):
        """Test that a string must completely match."""
        fmt = "{orbit:05d}"
        val = extract_values(fmt, "12345")
        self.assertEqual(val, {"orbit": "12345"})
        self.assertRaises(ValueError, extract_values, fmt, "12345abc")
        val = extract_values(fmt, "12345abc", full_match=False)
        self.assertEqual(val, {"orbit": "12345"})

    def test_convert_digits(self):
        self.assertEqual(_convert("d", "69022"), 69022)
        self.assertRaises(ValueError, _convert, "d", "69dsf")
        self.assertEqual(_convert("d", "00022"), 22)
        self.assertEqual(_convert("4d", "69022"), 69022)
        self.assertEqual(_convert("_>10d", "_____69022"), 69022)
        self.assertEqual(_convert("%Y%m%d_%H%M", "20140210_1004"), dt.datetime(2014, 2, 10, 10, 4))

    def test_parse(self):
        # Run
        result = parse(self.fmt, "/somedir/avhrr/2014/hrpt_noaa19_20140212_1412_12345.l1b")
        # Assert
        self.assertDictEqual(
            result,
            {
                "directory": "avhrr/2014",
                "platform": "noaa",
                "platnum": "19",
                "time": dt.datetime(2014, 2, 12, 14, 12),
                "orbit": 12345,
            },
        )

    def test_parse_string_padding_syntax_with_and_without_s(self):
        """Test that, in string padding syntax, '' is equivalent to 's'.

        From <https://docs.python.org/3.4/library/string.html#format-specification-mini-language>:
            * Type 's': String format. This is the default type for strings and may be omitted.
            * Type None: The same as 's'.
        """
        result = parse("{foo}/{bar:_<8}", "baz/qux_____")
        expected_result = parse("{foo}/{bar:_<8s}", "baz/qux_____")
        self.assertEqual(expected_result["foo"], "baz")
        self.assertEqual(expected_result["bar"], "qux")
        self.assertEqual(result, expected_result)

    def test_parse_wildcards(self):
        # Run
        result = parse(
            "hrpt_{platform}{platnum:2s}_{time:%Y%m%d_%H%M}_{orbit:05d}{ext}",
            "hrpt_noaa19_20140212_1412_12345.l1b",
        )
        # Assert
        self.assertDictEqual(
            result,
            {
                "platform": "noaa",
                "platnum": "19",
                "time": dt.datetime(2014, 2, 12, 14, 12),
                "orbit": 12345,
                "ext": ".l1b",
            },
        )

    def test_parse_align(self):
        filepattern = (
            "H-000-{hrit_format:4s}__-{platform_name:4s}________-"
            "{channel_name:_<9s}-{segment:_<9s}-{start_time:%Y%m%d%H%M}-__"
        )
        result = parse(filepattern, "H-000-MSG3__-MSG3________-IR_039___-000007___-201506051700-__")
        self.assertDictEqual(
            result,
            {
                "channel_name": "IR_039",
                "hrit_format": "MSG3",
                "platform_name": "MSG3",
                "segment": "000007",
                "start_time": dt.datetime(2015, 6, 5, 17, 0),
            },
        )

    def test_parse_digits(self):
        """Test when a digit field is shorter than the format spec."""
        result = parse(
            "hrpt_{platform}{platnum:2s}_{time:%Y%m%d_%H%M}_{orbit:05d}{ext}",
            "hrpt_noaa19_20140212_1412_02345.l1b",
        )
        self.assertDictEqual(
            result,
            {
                "platform": "noaa",
                "platnum": "19",
                "time": dt.datetime(2014, 2, 12, 14, 12),
                "orbit": 2345,
                "ext": ".l1b",
            },
        )
        result = parse(
            "hrpt_{platform}{platnum:2s}_{time:%Y%m%d_%H%M}_{orbit:5d}{ext}",
            "hrpt_noaa19_20140212_1412_ 2345.l1b",
        )
        self.assertDictEqual(
            result,
            {
                "platform": "noaa",
                "platnum": "19",
                "time": dt.datetime(2014, 2, 12, 14, 12),
                "orbit": 2345,
                "ext": ".l1b",
            },
        )
        result = parse(
            "hrpt_{platform}{platnum:2s}_{time:%Y%m%d_%H%M}_{orbit:_>5d}{ext}",
            "hrpt_noaa19_20140212_1412___345.l1b",
        )
        self.assertDictEqual(
            result,
            {
                "platform": "noaa",
                "platnum": "19",
                "time": dt.datetime(2014, 2, 12, 14, 12),
                "orbit": 345,
                "ext": ".l1b",
            },
        )

    def test_parse_bad_pattern(self):
        """Test when a digit field is shorter than the format spec."""
        self.assertRaises(
            ValueError,
            parse,
            "hrpt_{platform}{platnum:-=2s}_{time:%Y%m%d_%H%M}_{orbit:05d}{ext}",
            "hrpt_noaa19_20140212_1412_02345.l1b",
        )

    def test_globify_simple(self):
        # Run
        result = globify("{a}_{b}.end", {"a": "a", "b": "b"})
        # Assert
        self.assertEqual(result, "a_b.end")

    def test_globify_empty(self):
        # Run
        result = globify("{a}_{b:4d}.end", {})
        # Assert
        self.assertEqual(result, "*_????.end")

    def test_globify_noarg(self):
        # Run
        result = globify("{a}_{b:4d}.end")
        # Assert
        self.assertEqual(result, "*_????.end")

    def test_globify_known_lengths(self):
        # Run
        result = globify(
            "{directory}/{platform:4s}{satnum:2d}/{orbit:05d}",
            {"directory": "otherdir", "platform": "noaa"},
        )
        # Assert
        self.assertEqual(result, "otherdir/noaa??/?????")

    def test_globify_unknown_lengths(self):
        # Run
        result = globify(
            "hrpt_{platform_and_num}_" + "{date}_{time}_{orbit}.l1b",
            {"platform_and_num": "noaa16"},
        )
        # Assert
        self.assertEqual(result, "hrpt_noaa16_*_*_*.l1b")

    def test_globify_datetime(self):
        # Run
        result = globify(
            "hrpt_{platform}{satnum}_" + "{time:%Y%m%d_%H%M}_{orbit}.l1b",
            {"platform": "noaa", "time": dt.datetime(2014, 2, 10, 12, 12)},
        )
        # Assert
        self.assertEqual(result, "hrpt_noaa*_20140210_1212_*.l1b")

    def test_globify_partial_datetime(self):
        # Run
        result = globify(
            "hrpt_{platform:4s}{satnum:2d}_" + "{time:%Y%m%d_%H%M}_{orbit}.l1b",
            {"platform": "noaa", "time": (dt.datetime(2014, 2, 10, 12, 12), "Ymd")},
        )
        # Assert
        self.assertEqual(result, "hrpt_noaa??_20140210_????_*.l1b")

    def test_globify_datetime_nosub(self):
        # Run
        result = globify(
            "hrpt_{platform:4s}{satnum:2d}_" + "{time:%Y%m%d_%H%M}_{orbit}.l1b",
            {"platform": "noaa"},
        )

        # Assert
        self.assertEqual(result, "hrpt_noaa??_????????_????_*.l1b")

    def test_validate(self):
        # These cases are True
        self.assertTrue(validate(self.fmt, "/somedir/avhrr/2014/hrpt_noaa19_20140212_1412_12345.l1b"))
        self.assertTrue(validate(self.fmt, "/somedir/avhrr/2014/hrpt_noaa01_19790530_0705_00000.l1b"))
        self.assertTrue(validate(self.fmt, "/somedir/funny-char$dir/hrpt_noaa19_20140212_1412_12345.l1b"))
        self.assertTrue(validate(self.fmt, "/somedir//hrpt_noaa19_20140212_1412_12345.l1b"))
        # These cases are False
        self.assertFalse(validate(self.fmt, "/somedir/bla/bla/hrpt_noaa19_20140212_1412_1A345.l1b"))
        self.assertFalse(validate(self.fmt, "/somedir/bla/bla/hrpt_noaa19_2014021_1412_00000.l1b"))
        self.assertFalse(validate(self.fmt, "/somedir/bla/bla/hrpt_noaa19_20140212__412_00000.l1b"))
        self.assertFalse(validate(self.fmt, "/somedir/bla/bla/hrpt_noaa19_20140212__1412_00000.l1b"))
        self.assertFalse(validate(self.fmt, "/somedir/bla/bla/hrpt_noaa19_20140212_1412_00000.l1"))
        self.assertFalse(validate(self.fmt, "/somedir/bla/bla/hrpt_noaa19_20140212_1412_00000"))
        self.assertFalse(validate(self.fmt, "{}/somedir/bla/bla/hrpt_noaa19_20140212_1412_00000.l1b"))

    def test_is_one2one(self):
        # These cases are True
        self.assertTrue(is_one2one("/somedir/{directory}/somedata_{platform:4s}_{time:%Y%d%m-%H%M}_{orbit:5d}.l1b"))
        # These cases are False
        self.assertFalse(is_one2one("/somedir/{directory}/somedata_{platform:4s}_{time:%Y%d%m-%H%M}_{orbit:d}.l1b"))

    def test_greediness(self):
        """Test that the minimum match is parsed out.

        See GH #18.
        """
        from trollsift import parse

        template = "{band_type}_{polarization_extracted}_{unit}_{s1_fname}"
        fname = "Amplitude_VH_db_S1A_IW_GRDH_1SDV_20160528T171628_20160528T171653_011462_011752_0EED.tif"
        res_dict = parse(template, fname)
        exp = {
            "band_type": "Amplitude",
            "polarization_extracted": "VH",
            "unit": "db",
            "s1_fname": "S1A_IW_GRDH_1SDV_20160528T171628_20160528T171653_011462_011752_0EED.tif",
        }
        self.assertEqual(exp, res_dict)

        template = "{band_type:s}_{polarization_extracted}_{unit}_{s1_fname}"
        res_dict = parse(template, fname)
        self.assertEqual(exp, res_dict)


class TestCompose:
    """Test routines related to `compose` methods."""

    @pytest.mark.parametrize("allow_partial", [False, True])
    def test_compose(self, allow_partial):
        """Test the compose method's custom conversion options."""
        key_vals = {"a": "this Is A-Test b_test c test"}

        new_str = compose("{a!c}", key_vals, allow_partial=allow_partial)
        assert new_str == "This is a-test b_test c test"
        new_str = compose("{a!h}", key_vals, allow_partial=allow_partial)
        assert new_str == "thisisatestbtestctest"
        new_str = compose("{a!H}", key_vals, allow_partial=allow_partial)
        assert new_str == "THISISATESTBTESTCTEST"
        new_str = compose("{a!l}", key_vals, allow_partial=allow_partial)
        assert new_str == "this is a-test b_test c test"
        new_str = compose("{a!R}", key_vals, allow_partial=allow_partial)
        assert new_str == "thisIsATestbtestctest"
        new_str = compose("{a!t}", key_vals, allow_partial=allow_partial)
        assert new_str == "This Is A-Test B_Test C Test"
        new_str = compose("{a!u}", key_vals, allow_partial=allow_partial)
        assert new_str == "THIS IS A-TEST B_TEST C TEST"
        # builtin repr
        new_str = compose("{a!r}", key_vals, allow_partial=allow_partial)
        assert new_str == "'this Is A-Test b_test c test'"
        # no formatting
        new_str = compose("{a}", key_vals, allow_partial=allow_partial)
        assert new_str == "this Is A-Test b_test c test"
        # bad formatter
        with pytest.raises(ValueError):
            new_str = compose("{a!X}", key_vals, allow_partial=allow_partial)
        assert new_str == "this Is A-Test b_test c test"

    def test_default_compose_is_strict(self):
        """Make sure the default compose call does not accept partial composition."""
        fmt = "{foo}_{bar}.qux"
        with pytest.raises(KeyError):
            _ = compose(fmt, {"foo": "foo"})

    def test_partial_compose_simple(self):
        """Test partial compose with a simple use case."""
        fmt = "{variant:s}/{platform_name}_{start_time:%Y%m%d_%H%M}_{product}.{format}"
        composed = compose(
            fmt=fmt,
            keyvals={"platform_name": "foo", "format": "bar"},
            allow_partial=True,
        )
        assert composed == "{variant:s}/foo_{start_time:%Y%m%d_%H%M}_{product}.bar"

    def test_partial_compose_with_similarly_named_params(self):
        """Test that partial compose handles well vars with common substrings in name."""
        original_fmt = "{foo}{afooo}{fooo}.{bar}/{baz:%Y}/{baz:%Y%m%d_%H}/{baz:%Y}/{bar:d}"
        composed = compose(fmt=original_fmt, keyvals={"afooo": "qux"}, allow_partial=True)
        assert composed == "{foo}qux{fooo}.{bar}/{baz:%Y}/{baz:%Y%m%d_%H}/{baz:%Y}/{bar:d}"

    def test_partial_compose_repeated_vars_with_different_formatting(self):
        """Test partial compose with a fmt with repeated vars with different formatting."""
        fmt = "/foo/{start_time:%Y%m}/bar/{baz}_{start_time:%Y%m%d_%H%M}.{format}"
        composed = compose(fmt=fmt, keyvals={"format": "qux"}, allow_partial=True)
        assert composed == "/foo/{start_time:%Y%m}/bar/{baz}_{start_time:%Y%m%d_%H%M}.qux"

    @pytest.mark.parametrize(
        "original_fmt",
        ["{}_{}", "{foo}{afooo}{fooo}.{bar}/{baz:%Y}/{baz:%Y%m%d_%H}/{baz:%Y}/{bar:d}"],
    )
    def test_partial_compose_is_identity_with_empty_keyvals(self, original_fmt):
        """Test that partial compose leaves the input untouched if no keyvals at all."""
        assert compose(fmt=original_fmt, keyvals={}, allow_partial=True) == original_fmt

    def test_that_some_invalid_fmt_can_confuse_partial_compose(self):
        """Test that a fmt with a weird char can confuse partial compose."""
        fmt = "{foo?}_{bar}_{foo}.qux"
        with pytest.raises(ValueError):
            _ = compose(fmt=fmt, keyvals={}, allow_partial=True)


class TestParserFixedPoint:
    """Test parsing of fixed point numbers."""

    @pytest.mark.parametrize("allow_partial_compose", [False, True])
    @pytest.mark.parametrize(
        ("fmt", "string", "expected"),
        [
            # Naive
            ("{foo:f}", "12.34", 12.34),
            # Including width and precision
            ("{foo:5.2f}", "12.34", 12.34),
            ("{foo:5.2f}", "-1.23", -1.23),
            ("{foo:5.2f}", "12.34", 12.34),
            ("{foo:5.2f}", "123.45", 123.45),
            # Whitespace padded
            ("{foo:5.2f}", " 1.23", 1.23),
            ("{foo:5.2f}", " 12.34", 12.34),
            # Zero padded
            ("{foo:05.2f}", "01.23", 1.23),
            ("{foo:05.2f}", "012.34", 12.34),
            # Only precision, no width
            ("{foo:.2f}", "12.34", 12.34),
            # Only width, no precision
            ("{foo:16f}", "            1.12", 1.12),
            # No digits before decimal point
            ("{foo:3.2f}", ".12", 0.12),
            ("{foo:4.2f}", "-.12", -0.12),
            ("{foo:4.2f}", " .12", 0.12),
            ("{foo:4.2f}", "  .12", 0.12),
            ("{foo:16f}", "             .12", 0.12),
            # Exponential format
            ("{foo:7.2e}", "-1.23e4", -1.23e4),
        ],
    )
    def test_match(self, allow_partial_compose, fmt, string, expected):
        """Test cases expected to be matched."""
        # Test parsed value
        parsed = parse(fmt, string)
        assert parsed["foo"] == expected

        # Test round trip
        composed = compose(fmt, {"foo": expected}, allow_partial=allow_partial_compose)
        parsed = parse(fmt, composed)
        assert parsed["foo"] == expected

    @pytest.mark.parametrize(
        ("fmt", "string"),
        [
            # Decimals incorrect
            ("{foo:5.2f}", "12345"),
            ("{foo:5.2f}", "1234."),
            ("{foo:5.2f}", "1.234"),
            ("{foo:5.2f}", "123.4"),
            ("{foo:.2f}", "12.345"),
            # Decimals correct, but width too short
            ("{foo:5.2f}", "1.23"),
            ("{foo:5.2f}", ".23"),
            ("{foo:10.2e}", "1.23e4"),
            # Invalid
            ("{foo:5.2f}", "12_34"),
            ("{foo:5.2f}", "aBcD"),
        ],
    )
    def test_no_match(self, fmt, string):
        """Test cases expected to not be matched."""
        with pytest.raises(ValueError):
            parse(fmt, string)


@pytest.mark.parametrize(
    ("fmt", "string", "expected"),
    [
        # Decimal
        ("{foo:d}", "123", 123),
        # Hex with small letter
        ("{foo:x}", "7b", 123),
        # Hex with big letter
        ("{foo:X}", "7B", 123),
        # Fixed length hex
        ("{foo:03x}", "07b", 123),
        ("{foo:3x}", " 7b", 123),
        ("{foo:3X}", " 7B", 123),
        # Octal
        ("{foo:o}", "173", 123),
        # Free size with octal
        ("{bar:s}{foo:o}", "something173", 123),
        # Fixed length with octal
        ("{foo:_>4o}", "_173", 123),
        ("{foo:4o}", " 173", 123),
        # Binary
        ("{foo:b}", "1111011", 123),
        # Fixed length with binary
        ("{foo:8b}", " 1111011", 123),
        ("{foo:_>8b}", "_1111011", 123),
    ],
)
def test_parse_integers(fmt, string, expected):
    assert parse(fmt, string)["foo"] == expected
