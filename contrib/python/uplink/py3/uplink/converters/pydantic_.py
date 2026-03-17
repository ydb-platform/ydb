from uplink.converters import register_default_converter_factory
from uplink.converters.interfaces import Factory
from uplink.utils import is_subclass

from .pydantic_v1 import _PydanticV1RequestBody, _PydanticV1ResponseBody
from .pydantic_v2 import _PydanticV2RequestBody, _PydanticV2ResponseBody


class PydanticConverter(Factory):
    """
    A converter that serializes and deserializes values using
    `pydantic.v1` and `pydantic` models.

    To deserialize JSON responses into Python objects with this
    converter, define a `pydantic.v1.BaseModel` or `pydantic.BaseModel` subclass and set
    it as the return annotation of a consumer method:

    ```python
    @returns.json()
    @get("/users")
    def get_users(self, username) -> List[UserModel]:
        '''Fetch multiple users'''
    ```

    !!! note
        This converter is an optional feature and requires the
        `pydantic` package. For example, here's how to
        install this feature using pip:

        ```bash
        $ pip install uplink[pydantic]
        ```
    """

    try:
        import pydantic
        import pydantic.v1 as pydantic_v1
    except ImportError:  # pragma: no cover
        pydantic = None
        pydantic_v1 = None

    def __init__(self):
        """
        Validates if :py:mod:`pydantic` is installed
        """
        if (self.pydantic or self.pydantic_v1) is None:
            raise ImportError("No module named 'pydantic'")

    def _get_model(self, type_):
        if is_subclass(type_, (self.pydantic_v1.BaseModel, self.pydantic.BaseModel)):
            return type_
        raise ValueError(
            "Expected pydantic.BaseModel or pydantic.v1.BaseModel subclass or instance"
        )

    def _make_converter(self, converter, type_):
        try:
            model = self._get_model(type_)
        except ValueError:
            return None

        return converter(model)

    def create_request_body_converter(self, type_, *args, **kwargs):
        if is_subclass(type_, self.pydantic.BaseModel):
            return self._make_converter(_PydanticV2RequestBody, type_)
        return self._make_converter(_PydanticV1RequestBody, type_)

    def create_response_body_converter(self, type_, *args, **kwargs):
        if is_subclass(type_, self.pydantic.BaseModel):
            return self._make_converter(_PydanticV2ResponseBody, type_)
        return self._make_converter(_PydanticV1ResponseBody, type_)

    @classmethod
    def register_if_necessary(cls, register_func):
        if cls.pydantic is not None:
            register_func(cls)


PydanticConverter.register_if_necessary(register_default_converter_factory)
