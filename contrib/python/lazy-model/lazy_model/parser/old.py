from typing import Any, Optional, Set, Dict

from pydantic import BaseModel, PrivateAttr, parse_obj_as, ValidationError

from pydantic.error_wrappers import ErrorWrapper

from lazy_model.nao import NAO

ROOT_KEY = "__root__"
object_setattr = object.__setattr__


class LazyModel(BaseModel):
    _store: Dict[str, Any] = PrivateAttr(default_factory=dict)
    _lazily_parsed: bool = PrivateAttr(default=False)

    @classmethod
    def lazy_parse(
        cls,
        data: Dict[str, Any],
        fields: Optional[Set[str]] = None,
        **new_kwargs,
    ):
        m = cls.__new__(cls, **new_kwargs)  # type: ignore
        if fields is None:
            fields = set()
        fields_values: Dict[str, Any] = {}
        field_alias_map: Dict[str, str] = {}
        for name, field in cls.__fields__.items():
            fields_values[name] = NAO
            if field.alias in fields:
                field_alias_map[field.alias] = name
        object_setattr(m, "__dict__", fields_values)
        _fields_set = set(fields_values.keys())
        object_setattr(m, "__fields_set__", _fields_set)
        m._init_private_attributes()

        m._store = data
        m._lazily_parsed = True

        for alias in fields:
            m._set_attr(field_alias_map[alias], data[alias])
        return m

    def _set_attr(self, name, value):
        """
        Stolen from Pydantic.

        # TODO rework
        """
        field_type = self.__fields__.get(name).annotation
        try:
            value = parse_obj_as(field_type, value)
        except ValidationError as e:
            if value is None and self.__fields__.get(name).required is False:
                value = None
            else:
                raise e

        new_values = {**self.__dict__, name: value}

        for validator in self.__pre_root_validators__:
            try:
                new_values = validator(self.__class__, new_values)
            except (ValueError, TypeError, AssertionError) as exc:
                raise ValidationError(
                    [ErrorWrapper(exc, loc=ROOT_KEY)], self.__class__
                )

        known_field = self.__fields__.get(name, None)
        if known_field:
            if not known_field.field_info.allow_mutation:
                raise TypeError(
                    f'"{known_field.name}" has allow_mutation set '
                    f"to False and cannot be assigned"
                )
            dict_without_original_value = {
                k: v for k, v in self.__dict__.items() if k != name
            }
            value, error_ = known_field.validate(
                value,
                dict_without_original_value,
                loc=name,
                cls=self.__class__,
            )
            if error_:
                raise ValidationError([error_], self.__class__)
            else:
                new_values[name] = value

        errors = []
        for skip_on_failure, validator in self.__post_root_validators__:
            if skip_on_failure and errors:
                continue
            try:
                new_values = validator(self.__class__, new_values)
            except (ValueError, TypeError, AssertionError) as exc:
                errors.append(ErrorWrapper(exc, loc=ROOT_KEY))
        if errors:
            raise ValidationError(errors, self.__class__)

        object_setattr(self, "__dict__", new_values)

        self.__fields_set__.add(name)

    def parse_store(self):
        for name in self.__fields__:
            self.__getattribute__(name)

    def __getattribute__(self, item):
        # If __class__ is accessed, return it directly to avoid recursion
        if item == "__class__":
            return super().__getattribute__(item)

        # If called on the class itself, delegate to super's __getattribute__
        if type(self) is type:  # Check if self is a class
            return super(type, self).__getattribute__(item)

        # For instances, use the object's __getattribute__ to prevent recursion
        res = object.__getattribute__(self, item)
        if res is NAO:
            field_info = self.__fields__.get(item)
            value = self._store.get(field_info.alias, NAO)
            if value is NAO:
                value = field_info.get_default()
            self._set_attr(item, value)
            res = super(LazyModel, self).__getattribute__(item)
        return res
