"""
Template strings support in queries.
"""

# Copyright (C) 2025 The Psycopg Team

from __future__ import annotations

from typing import Any

from . import abc
from . import errors as e
from . import sql
from ._enums import PyFormat
from ._compat import Interpolation, Template
from ._transformer import Transformer

# Formats supported by template strings
FMT_AUTO = PyFormat.AUTO.value
FMT_TEXT = PyFormat.TEXT.value
FMT_BINARY = PyFormat.BINARY.value
FMT_IDENT = "i"
FMT_LITERAL = "l"
FMT_SQL = "q"


class TemplateProcessor:
    def __init__(self, template: Template, *, tx: abc.Transformer, server_params: bool):
        self.template = template
        self._tx = tx
        self._server_params = server_params

        self.query = b""
        self.formats: list[PyFormat] = []
        self.params: list[Any] = []

        self._chunks: list[bytes] = []

    def process(self) -> None:
        self._process_template(self.template)
        self.query = b"".join(self._chunks)

    def _check_template_format(self, item: Interpolation, want_fmt: str) -> None:
        if item.format_spec == want_fmt:
            return
        fmt = f":{item.format_spec}" if item.format_spec else ""
        cls = type(item.value)
        msg = f"{cls.__module__}.{cls.__qualname__} require format ':{want_fmt}'"
        raise e.ProgrammingError(f"{msg}; got '{{{item.expression}{fmt}}}'")

    def _process_template(self, t: Template) -> None:
        for item in t:
            if isinstance(item, str):
                self._chunks.append(item.encode(self._tx.encoding))
                continue

            assert isinstance(item, Interpolation)
            if item.conversion:
                raise TypeError(
                    "conversions not supported in query; got"
                    f" '{{{item.expression}!{item.conversion}}}'"
                )

            if isinstance(item.value, Template):
                self._check_template_format(item, FMT_SQL)
                self._process_template(item.value)

            elif isinstance(item.value, sql.Composable):
                self._process_composable(item)

            elif (fmt := item.format_spec or FMT_AUTO) == FMT_IDENT:
                if not isinstance(item.value, str):
                    raise e.ProgrammingError(
                        "identifier values must be strings; got"
                        f" {type(item.value).__qualname__}"
                        f" in {{{item.expression}:{fmt}}}"
                    )
                self._chunks.append(sql.Identifier(item.value).as_bytes(self._tx))

            elif fmt == FMT_LITERAL:
                self._chunks.append(sql.Literal(item.value).as_bytes(self._tx))

            elif fmt == FMT_SQL:
                # It must have been processed already
                raise e.ProgrammingError(
                    "sql values must be sql.Composed, sql.SQL, or Template;"
                    f" got {type(item.value).__qualname__}"
                    f" in {{{item.expression}:{fmt}}}"
                )

            else:
                if self._server_params:
                    self._process_server_variable(item, fmt)
                else:
                    self._process_client_variable(item, fmt)

    def _process_server_variable(self, item: Interpolation, fmt: str) -> None:
        try:
            pyfmt = PyFormat(fmt)
        except ValueError:
            raise e.ProgrammingError(
                f"format '{fmt}' not supported in query;"
                f" got '{{{item.expression}:{fmt}}}'"
            )

        self.formats.append(pyfmt)
        self.params.append(item.value)
        self._chunks.append(b"$%d" % len(self.params))

    def _process_client_variable(self, item: Interpolation, fmt: str) -> None:
        try:
            PyFormat(fmt)
        except ValueError:
            raise e.ProgrammingError(
                f"format '{fmt}' not supported in query;"
                f" got '{{{item.expression}:{fmt}}}'"
            )

        param = sql.Literal(item.value).as_bytes(self._tx)
        self._chunks.append(param)
        self.params.append(param)

    def _process_composable(self, item: Interpolation) -> None:
        if isinstance(item.value, sql.Identifier):
            self._check_template_format(item, FMT_IDENT)
            self._chunks.append(item.value.as_bytes(self._tx))
            return

        elif isinstance(item.value, sql.Literal):
            self._check_template_format(item, FMT_LITERAL)
            self._chunks.append(item.value.as_bytes(self._tx))
            return

        elif isinstance(item.value, (sql.SQL, sql.Composed)):
            self._check_template_format(item, FMT_SQL)
            self._chunks.append(item.value.as_bytes(self._tx))
            return

        else:
            raise e.ProgrammingError(
                f"{type(item.value).__qualname__} not supported in string templates"
            )


def as_string(t: Template, context: abc.AdaptContext | None = None) -> str:
    """Convert a template string to a string.

    This function is exposed as part of psycopg.sql.as_string().
    """
    tx = Transformer(context)
    tp = TemplateProcessor(t, tx=tx, server_params=False)
    tp.process()
    return tp.query.decode(tx.encoding)


def as_bytes(t: Template, context: abc.AdaptContext | None = None) -> bytes:
    """Convert a template string to a bytes string.

    This function is exposed as part of psycopg.sql.as_bytes().
    """
    tx = Transformer(context)
    tp = TemplateProcessor(t, tx=tx, server_params=False)
    tp.process()
    return tp.query
