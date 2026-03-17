from __future__ import annotations

import abc
import contextlib
import re
import textwrap
from typing import List, Union

from annet import tracing
from annet.tracing import tracing_connector
from annet.vendors import tabparser

from .exceptions import InvalidValueFromGenerator


NONE_SEARCHER = re.compile(r"\bNone\b")


class DefaultBlockIfCondition:
    pass


ParamsList = tabparser.JuniperList


class GenStringable(abc.ABC):
    @abc.abstractmethod
    def gen_str(self) -> str:
        pass


def _filter_str(value: Union[str, int, float, tabparser.JuniperList, ParamsList, GenStringable]):
    if isinstance(
        value,
        (
            str,
            int,
            float,
            tabparser.JuniperList,
            ParamsList,
        ),
    ):
        return str(value)

    if hasattr(value, "gen_str") and callable(value.gen_str):
        return value.gen_str()

    raise InvalidValueFromGenerator("Invalid yield type: %s(%s)" % (type(value).__name__, value))


def _split_and_strip(text):
    if "\n" in text:
        rows = textwrap.dedent(text).strip().split("\n")
    else:
        rows = [text]
    return rows


# =====
class BaseGenerator:
    TYPE: str
    TAGS: List[str] = []

    def supports_device(self, device) -> bool:  # pylint: disable=unused-argument
        return True

    @classmethod
    def get_aliases(cls) -> set[str]:
        return {cls.__name__, *cls.TAGS}


class TreeGenerator(BaseGenerator):
    def __init__(self, indent="  "):
        self._indents = []
        self._rows = []
        self._block_path = []
        self._indent = indent

    @tracing.contextmanager(min_duration="0.1")
    @contextlib.contextmanager
    def block(self, *tokens, indent=None):
        span = tracing_connector.get().get_current_span()
        if span:
            span.set_attribute("tokens", " ".join(map(str, tokens)))

        indent = self._indent if indent is None else indent
        block = " ".join(map(_filter_str, tokens))
        self._block_path.append(block)
        self._append_text(block)
        self._indents.append(indent)
        yield
        self._indents.pop(-1)
        self._block_path.pop(-1)

    @contextlib.contextmanager
    def block_if(self, *tokens, condition=DefaultBlockIfCondition):
        if condition is DefaultBlockIfCondition:
            condition = None not in tokens and "" not in tokens
        if condition:
            with self.block(*tokens):
                yield
                return
        yield

    @contextlib.contextmanager
    def multiblock(self, *blocks):
        if blocks:
            blk = blocks[0]
            tokens = blk if isinstance(blk, (list, tuple)) else [blk]
            with self.block(*tokens):
                with self.multiblock(*blocks[1:]):
                    yield
                    return
        yield

    @contextlib.contextmanager
    def multiblock_if(self, *blocks, condition=DefaultBlockIfCondition):
        if condition is DefaultBlockIfCondition:
            condition = None not in blocks
            if condition:
                if blocks:
                    blk = blocks[0]
                    tokens = blk if isinstance(blk, (list, tuple)) else [blk]
                    with self.block(*tokens):
                        with self.multiblock(*blocks[1:]):
                            yield
                            return
        yield

    # ===
    def _append_text(self, text):
        self._append_text_cb(text)

    def _append_text_cb(self, text, row_cb=None):
        for row in _split_and_strip(text):
            if row_cb:
                row = row_cb(row)
            self._rows.append("".join(self._indents) + row)


class TextGenerator(TreeGenerator):
    def __add__(self, line):
        self._append_text(line)
        return self

    def __iter__(self):
        yield from self._rows
