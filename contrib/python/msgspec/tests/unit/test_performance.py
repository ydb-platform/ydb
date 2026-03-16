import textwrap

import msgspec

from .utils import temp_module


def test_process_large_recursive_union():
    """
    A recursive schema processing perf test from
    https://github.com/pydantic/pydantic/issues/8499

    This test is mostly to ensure that processing deeply recursive schemas with
    unions succeeds.
    """

    def gen_code():
        yield "from __future__ import annotations"
        yield "from msgspec import Struct"
        yield "from typing import Union"

        for i in range(50):
            yield textwrap.dedent(
                f"""
                class Node{i}(Struct, tag='node{i}'):
                    data: Union[Node, None]
                """
            )
        yield "Node = Union["
        for i in range(50):
            yield f"    Node{i},"
        yield "]"

    code = "\n".join(gen_code())

    with temp_module(code) as mod:
        dec = msgspec.json.Decoder(mod.Node)

    msg = b"""
    {
        "type": "node25",
        "data": {
            "type": "node13",
            "data": null
        }
    }
    """

    sol = mod.Node25(mod.Node13(None))

    assert dec.decode(msg) == sol
