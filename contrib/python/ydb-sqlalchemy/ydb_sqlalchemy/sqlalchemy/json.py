from typing import Tuple, Union

from sqlalchemy import types as sqltypes


class YqlJSON(sqltypes.JSON):
    class YqlJSONPathType(sqltypes.JSON.JSONPathType):
        def _format_value(self, value: Tuple[Union[str, int]]) -> str:
            path = "/"
            for elem in value:
                path += f"/{elem}"
            return path

        def bind_processor(self, dialect):
            super_proc = self.string_bind_processor(dialect)

            def process(value: Tuple[Union[str, int]]):
                value = self._format_value(value)
                if super_proc:
                    value = super_proc(value)
                return value

            return process

        def literal_processor(self, dialect):
            super_proc = self.string_literal_processor(dialect)

            def process(value: Tuple[Union[str, int]]):
                value = self._format_value(value)
                if super_proc:
                    value = super_proc(value)
                return value

            return process
