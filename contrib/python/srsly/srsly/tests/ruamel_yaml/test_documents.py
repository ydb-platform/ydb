# coding: utf-8

import pytest  # NOQA

from .roundtrip import round_trip, round_trip_load_all


class TestDocument:
    def test_single_doc_begin_end(self):
        inp = """\
        ---
        - a
        - b
        ...
        """
        round_trip(inp, explicit_start=True, explicit_end=True)

    def test_multi_doc_begin_end(self):
        from srsly.ruamel_yaml import dump_all, RoundTripDumper

        inp = """\
        ---
        - a
        ...
        ---
        - b
        ...
        """
        docs = list(round_trip_load_all(inp))
        assert docs == [["a"], ["b"]]
        out = dump_all(
            docs, Dumper=RoundTripDumper, explicit_start=True, explicit_end=True
        )
        assert out == "---\n- a\n...\n---\n- b\n...\n"

    def test_multi_doc_no_start(self):
        inp = """\
        - a
        ...
        ---
        - b
        ...
        """
        docs = list(round_trip_load_all(inp))
        assert docs == [["a"], ["b"]]

    def test_multi_doc_no_end(self):
        inp = """\
        - a
        ---
        - b
        """
        docs = list(round_trip_load_all(inp))
        assert docs == [["a"], ["b"]]

    def test_multi_doc_ends_only(self):
        # this is ok in 1.2
        inp = """\
        - a
        ...
        - b
        ...
        """
        docs = list(round_trip_load_all(inp, version=(1, 2)))
        assert docs == [["a"], ["b"]]

    def test_multi_doc_ends_only_1_1(self):
        from srsly.ruamel_yaml import parser

        # this is not ok in 1.1
        with pytest.raises(parser.ParserError):
            inp = """\
            - a
            ...
            - b
            ...
            """
            docs = list(round_trip_load_all(inp, version=(1, 1)))
            assert docs == [["a"], ["b"]]  # not True, but not reached
