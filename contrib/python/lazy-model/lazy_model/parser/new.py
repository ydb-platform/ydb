from typing import TYPE_CHECKING, Any, Optional, Set, Dict

import pydantic_core
from pydantic import BaseModel, PrivateAttr, ValidationError, TypeAdapter
from pydantic._internal import _fields

from lazy_model.nao import NAO

ROOT_KEY = "__root__"
_object_setattr = object.__setattr__


class LazyModel(BaseModel):
    _store: Dict[str, Any] = PrivateAttr(default_factory=dict)
    _lazily_parsed: bool = PrivateAttr(default=False)

    @classmethod
    def lazy_parse(
        cls,
        data: Dict[str, Any],
        fields: Optional[Set[str]] = None,
    ):
        fields_values = {}
        field_alias_map = {}
        if fields is None:
            fields = set()
        for name, field in cls.model_fields.items():
            fields_values[name] = NAO
            alias = field.alias or name
            if alias in fields:
                field_alias_map[alias] = name
        field_set = set(fields_values.keys())
        m = cls.model_construct(field_set, **fields_values)
        m._store = data

        for alias in fields:
            m._set_attr(field_alias_map[alias], data[alias])
        return m

    def _parse_value(self, name, value):
        field_type = self.__class__.model_fields.get(name).annotation
        try:
            value = TypeAdapter(field_type).validate_python(value)
        except ValidationError as e:
            if (
                value is None
                and self.__class__.model_fields.get(name).required is False
            ):
                value = None
            else:
                raise e
        return value

    def parse_store(self):
        for name in self.__class__.model_fields:
            self.__getattribute__(name)

    def _set_attr(self, name: str, value: Any) -> None:
        """
        Stolen from Pydantic.
        :param name:
        :param value:
        :return:

        TODO rework
        """
        if name in self.__class_vars__:
            raise AttributeError(
                f"{name!r} is a ClassVar of `{self.__class__.__name__}` and cannot be set on an instance. "  # noqa: E501
                f"If you want to set a value on the class, use `{self.__class__.__name__}.{name} = value`."  # noqa: E501
            )
        elif not _fields.is_valid_field_name(name):
            if (
                self.__pydantic_private__ is None
                or name not in self.__private_attributes__
            ):
                _object_setattr(self, name, value)
            else:
                attribute = self.__private_attributes__[name]
                if hasattr(attribute, "__set__"):
                    attribute.__set__(self, value)  # type: ignore
                else:
                    self.__pydantic_private__[name] = value
            return
        elif self.model_config.get("frozen", None):
            error: pydantic_core.InitErrorDetails = {
                "type": "frozen_instance",
                "loc": (name,),
                "input": value,
            }
            raise pydantic_core.ValidationError.from_exception_data(
                self.__class__.__name__, [error]
            )

        attr = getattr(self.__class__, name, None)
        if isinstance(attr, property):
            attr.__set__(self, value)
        self.__pydantic_validator__.validate_assignment(self, name, value)

    if not TYPE_CHECKING:

        def __getattribute__(self, item):
            # If __class__ is accessed, return it directly to avoid recursion
            if item == "__class__":
                return super().__getattribute__(item)

            # If called on the class itself,
            # delegate to super's __getattribute__
            if type(self) is type:  # Check if self is a class
                return super(type, self).__getattribute__(item)

            # For instances, use the object's __getattribute__
            # to prevent recursion
            res = object.__getattribute__(self, item)
            if res is NAO:
                field_info = self.__class__.model_fields.get(item)
                alias = field_info.alias or item
                value = self._store.get(alias, NAO)
                if value is NAO:
                    value = field_info.get_default()
                else:
                    value = self._parse_value(item, value)
                self._set_attr(item, value)
                res = super(LazyModel, self).__getattribute__(item)
            return res
