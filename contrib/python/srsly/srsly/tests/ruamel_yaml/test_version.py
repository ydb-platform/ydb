# coding: utf-8

import pytest  # NOQA

from .roundtrip import dedent, round_trip, round_trip_load


def load(s, version=None):
    import srsly.ruamel_yaml  # NOQA

    return srsly.ruamel_yaml.round_trip_load(dedent(s), version)


class TestVersions:
    def test_explicit_1_2(self):
        r = load(
            """\
        %YAML 1.2
        ---
        - 12:34:56
        - 012
        - 012345678
        - 0o12
        - on
        - off
        - yes
        - no
        - true
        """
        )
        assert r[0] == "12:34:56"
        assert r[1] == 12
        assert r[2] == 12345678
        assert r[3] == 10
        assert r[4] == "on"
        assert r[5] == "off"
        assert r[6] == "yes"
        assert r[7] == "no"
        assert r[8] is True

    def test_explicit_1_1(self):
        r = load(
            """\
        %YAML 1.1
        ---
        - 12:34:56
        - 012
        - 012345678
        - 0o12
        - on
        - off
        - yes
        - no
        - true
        """
        )
        assert r[0] == 45296
        assert r[1] == 10
        assert r[2] == "012345678"
        assert r[3] == "0o12"
        assert r[4] is True
        assert r[5] is False
        assert r[6] is True
        assert r[7] is False
        assert r[8] is True

    def test_implicit_1_2(self):
        r = load(
            """\
        - 12:34:56
        - 12:34:56.78
        - 012
        - 012345678
        - 0o12
        - on
        - off
        - yes
        - no
        - true
        """
        )
        assert r[0] == "12:34:56"
        assert r[1] == "12:34:56.78"
        assert r[2] == 12
        assert r[3] == 12345678
        assert r[4] == 10
        assert r[5] == "on"
        assert r[6] == "off"
        assert r[7] == "yes"
        assert r[8] == "no"
        assert r[9] is True

    def test_load_version_1_1(self):
        inp = """\
        - 12:34:56
        - 12:34:56.78
        - 012
        - 012345678
        - 0o12
        - on
        - off
        - yes
        - no
        - true
        """
        r = load(inp, version="1.1")
        assert r[0] == 45296
        assert r[1] == 45296.78
        assert r[2] == 10
        assert r[3] == "012345678"
        assert r[4] == "0o12"
        assert r[5] is True
        assert r[6] is False
        assert r[7] is True
        assert r[8] is False
        assert r[9] is True


class TestIssue62:
    # bitbucket issue 62, issue_62
    def test_00(self):
        import srsly.ruamel_yaml  # NOQA

        s = dedent(
            """\
        {}# Outside flow collection:
        - ::vector
        - ": - ()"
        - Up, up, and away!
        - -123
        - http://example.com/foo#bar
        # Inside flow collection:
        - [::vector, ": - ()", "Down, down and away!", -456, http://example.com/foo#bar]
        """
        )
        with pytest.raises(srsly.ruamel_yaml.parser.ParserError):
            round_trip(s.format("%YAML 1.1\n---\n"), preserve_quotes=True)
        round_trip(s.format(""), preserve_quotes=True)

    def test_00_single_comment(self):
        import srsly.ruamel_yaml  # NOQA

        s = dedent(
            """\
        {}# Outside flow collection:
        - ::vector
        - ": - ()"
        - Up, up, and away!
        - -123
        - http://example.com/foo#bar
        - [::vector, ": - ()", "Down, down and away!", -456, http://example.com/foo#bar]
        """
        )
        with pytest.raises(srsly.ruamel_yaml.parser.ParserError):
            round_trip(s.format("%YAML 1.1\n---\n"), preserve_quotes=True)
        round_trip(s.format(""), preserve_quotes=True)
        # round_trip(s.format('%YAML 1.2\n---\n'), preserve_quotes=True, version=(1, 2))

    def test_01(self):
        import srsly.ruamel_yaml  # NOQA

        s = dedent(
            """\
        {}[random plain value that contains a ? character]
        """
        )
        with pytest.raises(srsly.ruamel_yaml.parser.ParserError):
            round_trip(s.format("%YAML 1.1\n---\n"), preserve_quotes=True)
        round_trip(s.format(""), preserve_quotes=True)
        # note the flow seq on the --- line!
        round_trip(s.format("%YAML 1.2\n--- "), preserve_quotes=True, version="1.2")

    def test_so_45681626(self):
        # was not properly parsing
        round_trip_load('{"in":{},"out":{}}')
