"""
This module defines a converter that uses :py:mod:`pydantic` models
to deserialize and serialize values.
"""

from typing import TYPE_CHECKING, Any

from uplink.converters.interfaces import Converter

if TYPE_CHECKING:
    # To allow using string anotations, as we wanna lazy-import
    import pydantic


def _encode_pydantic_v2(model: "pydantic.BaseModel") -> dict[str, Any]:
    return model.model_dump(mode="json")


class _PydanticV2RequestBody(Converter):
    def __init__(self, model):
        self._model = model

    def convert(self, value):
        if isinstance(value, self._model):
            return _encode_pydantic_v2(value)
        return _encode_pydantic_v2(self._model.model_validate(value))


class _PydanticV2ResponseBody(Converter):
    def __init__(self, model):
        self._model = model

    def convert(self, response):
        try:
            data = response.json()
        except AttributeError:
            data = response

        return self._model.parse_obj(data)
