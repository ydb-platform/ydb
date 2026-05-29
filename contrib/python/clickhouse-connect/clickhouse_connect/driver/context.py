import logging
import re
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

_empty_map = {}


class BaseQueryContext:
    def __init__(
        self,
        settings: dict[str, Any] | None = None,
        query_formats: dict[str, str] | None = None,
        column_formats: dict[str, str | dict[str, str]] | None = None,
        encoding: str | None = None,
        use_extended_dtypes: bool = False,
        use_numpy: bool = False,
        transport_settings: dict[str, str] | None = None,
    ):
        self.settings = settings or {}
        if query_formats is None:
            self.type_formats = _empty_map
        else:
            self.type_formats = {re.compile(type_name.replace("*", ".*"), re.IGNORECASE): fmt for type_name, fmt in query_formats.items()}
        if column_formats is None:
            self.col_simple_formats = _empty_map
            self.col_type_formats = _empty_map
        else:
            self.col_simple_formats = {col_name: fmt for col_name, fmt in column_formats.items() if isinstance(fmt, str)}
            self.col_type_formats = {}
            for col_name, fmt in column_formats.items():
                if not isinstance(fmt, str):
                    self.col_type_formats[col_name] = {
                        re.compile(type_name.replace("*", ".*"), re.IGNORECASE): fmt for type_name, fmt in fmt.items()
                    }
        self.query_formats = query_formats or {}
        self.column_formats = column_formats or {}
        self.transport_settings = transport_settings
        self.column_name = None
        self.encoding = encoding
        self.use_numpy = use_numpy
        self.use_extended_dtypes = use_extended_dtypes
        self._active_col_fmt = None
        self._active_col_type_fmts = _empty_map
        self.column_renamer: Callable[[str], str] | None = None

    def start_column(self, name: str):
        self.column_name = name
        self._active_col_fmt = self.col_simple_formats.get(name)
        self._active_col_type_fmts = self.col_type_formats.get(name, _empty_map)

    def active_fmt(self, ch_type):
        if self._active_col_fmt:
            return self._active_col_fmt
        for type_pattern, fmt in self._active_col_type_fmts.items():
            if type_pattern.match(ch_type):
                return fmt
        for type_pattern, fmt in self.type_formats.items():
            if type_pattern.match(ch_type):
                return fmt
        return None
