from __future__ import print_function

"""
various test cases for YAML files
"""

import sys
import pytest  # NOQA
import platform

from .roundtrip import round_trip, dedent, round_trip_load, round_trip_dump  # NOQA


class TestYAML:
    def test_backslash(self):
        round_trip(
            """
        handlers:
          static_files: applications/\\1/static/\\2
        """
        )

    def test_omap_out(self):
        # ordereddict mapped to !!omap
        from srsly.ruamel_yaml.compat import ordereddict
        import srsly.ruamel_yaml  # NOQA

        x = ordereddict([("a", 1), ("b", 2)])
        res = srsly.ruamel_yaml.dump(x, default_flow_style=False)
        assert res == dedent(
            """
        !!omap
        - a: 1
        - b: 2
        """
        )

    def test_omap_roundtrip(self):
        round_trip(
            """
        !!omap
        - a: 1
        - b: 2
        - c: 3
        - d: 4
        """
        )

    @pytest.mark.skipif(sys.version_info < (2, 7), reason="collections not available")
    def test_dump_collections_ordereddict(self):
        from collections import OrderedDict
        import srsly.ruamel_yaml  # NOQA

        # OrderedDict mapped to !!omap
        x = OrderedDict([("a", 1), ("b", 2)])
        res = srsly.ruamel_yaml.dump(
            x, Dumper=srsly.ruamel_yaml.RoundTripDumper, default_flow_style=False
        )
        assert res == dedent(
            """
        !!omap
        - a: 1
        - b: 2
        """
        )

    @pytest.mark.skipif(
        sys.version_info >= (3, 0) or platform.python_implementation() != "CPython",
        reason="srsly.ruamel_yaml not available",
    )
    def test_dump_ruamel_ordereddict(self):
        from srsly.ruamel_yaml.compat import ordereddict
        import srsly.ruamel_yaml  # NOQA

        # OrderedDict mapped to !!omap
        x = ordereddict([("a", 1), ("b", 2)])
        res = srsly.ruamel_yaml.dump(
            x, Dumper=srsly.ruamel_yaml.RoundTripDumper, default_flow_style=False
        )
        assert res == dedent(
            """
        !!omap
        - a: 1
        - b: 2
        """
        )

    def test_CommentedSet(self):
        from srsly.ruamel_yaml.constructor import CommentedSet

        s = CommentedSet(["a", "b", "c"])
        s.remove("b")
        s.add("d")
        assert s == CommentedSet(["a", "c", "d"])
        s.add("e")
        s.add("f")
        s.remove("e")
        assert s == CommentedSet(["a", "c", "d", "f"])

    def test_set_out(self):
        # preferable would be the shorter format without the ': null'
        import srsly.ruamel_yaml  # NOQA

        x = set(["a", "b", "c"])
        res = srsly.ruamel_yaml.dump(x, default_flow_style=False)
        assert res == dedent(
            """
        !!set
        a: null
        b: null
        c: null
        """
        )

    # ordering is not preserved in a set
    def test_set_compact(self):
        # this format is read and also should be written by default
        round_trip(
            """
        !!set
        ? a
        ? b
        ? c
        """
        )

    def test_blank_line_after_comment(self):
        round_trip(
            """
        # Comment with spaces after it.


        a: 1
        """
        )

    def test_blank_line_between_seq_items(self):
        round_trip(
            """
        # Seq with empty lines in between items.
        b:
        - bar


        - baz
        """
        )

    @pytest.mark.skipif(
        platform.python_implementation() == "Jython",
        reason="Jython throws RepresenterError",
    )
    def test_blank_line_after_literal_chip(self):
        s = """
        c:
        - |
          This item
          has a blank line
          following it.

        - |
          To visually separate it from this item.

          This item contains a blank line.


        """
        d = round_trip_load(dedent(s))
        print(d)
        round_trip(s)
        assert d["c"][0].split("it.")[1] == "\n"
        assert d["c"][1].split("line.")[1] == "\n"

    @pytest.mark.skipif(
        platform.python_implementation() == "Jython",
        reason="Jython throws RepresenterError",
    )
    def test_blank_line_after_literal_keep(self):
        """ have to insert an eof marker in YAML to test this"""
        s = """
        c:
        - |+
          This item
          has a blank line
          following it.

        - |+
          To visually separate it from this item.

          This item contains a blank line.


        ...
        """
        d = round_trip_load(dedent(s))
        print(d)
        round_trip(s)
        assert d["c"][0].split("it.")[1] == "\n\n"
        assert d["c"][1].split("line.")[1] == "\n\n\n"

    @pytest.mark.skipif(
        platform.python_implementation() == "Jython",
        reason="Jython throws RepresenterError",
    )
    def test_blank_line_after_literal_strip(self):
        s = """
        c:
        - |-
          This item
          has a blank line
          following it.

        - |-
          To visually separate it from this item.

          This item contains a blank line.


        """
        d = round_trip_load(dedent(s))
        print(d)
        round_trip(s)
        assert d["c"][0].split("it.")[1] == ""
        assert d["c"][1].split("line.")[1] == ""

    def test_load_all_perserve_quotes(self):
        import srsly.ruamel_yaml  # NOQA

        s = dedent(
            """\
        a: 'hello'
        ---
        b: "goodbye"
        """
        )
        data = []
        for x in srsly.ruamel_yaml.round_trip_load_all(s, preserve_quotes=True):
            data.append(x)
        out = srsly.ruamel_yaml.dump_all(data, Dumper=srsly.ruamel_yaml.RoundTripDumper)
        print(type(data[0]["a"]), data[0]["a"])
        # out = srsly.ruamel_yaml.round_trip_dump_all(data)
        print(out)
        assert out == s
