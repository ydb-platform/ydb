"""
This module defines a converter that uses :py:mod:`pydantic.v1` models
to deserialize and serialize values.
"""

from typing import Any

from uplink.converters.interfaces import Converter


def _encode_pydantic_v1(obj: Any) -> Any:
    from pydantic.v1.json import pydantic_encoder

    # json atoms
    if isinstance(obj, str | int | float | bool) or obj is None:
        return obj

    # json containers
    if isinstance(obj, dict):
        return {_encode_pydantic_v1(k): _encode_pydantic_v1(v) for k, v in obj.items()}
    if isinstance(obj, list | tuple):
        return [_encode_pydantic_v1(i) for i in obj]

    # pydantic v1 types
    return _encode_pydantic_v1(pydantic_encoder(obj))


class _PydanticV1RequestBody(Converter):
    def __init__(self, model):
        self._model = model

    def convert(self, value):
        if isinstance(value, self._model):
            return _encode_pydantic_v1(value)
        return _encode_pydantic_v1(self._model.parse_obj(value))


class _PydanticV1ResponseBody(Converter):
    def __init__(self, model):
        self._model = model

    def convert(self, response):
        try:
            data = response.json()
        except AttributeError:
            data = response

        return self._model.parse_obj(data)
