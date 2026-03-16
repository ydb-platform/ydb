from typing import (
    Any,
    Sequence,
    Type,
)

import bson
import odmantic
import pydantic
from odmantic import Model
from odmantic.field import (
    FieldProxy,
    ODMEmbedded,
    ODMEmbeddedGeneric,
    ODMField,
    ODMReference,
)
from starlette_admin.contrib.odmantic.exceptions import NotSupportedAnnotation
from starlette_admin.converters import StandardModelConverter, converts
from starlette_admin.exceptions import (
    NotSupportedAnnotation as BaseNotSupportedAnnotation,
)
from starlette_admin.fields import (
    BaseField,
    CollectionField,
    DateTimeField,
    DecimalField,
    EmailField,
    HasOne,
    IntegerField,
    ListField,
    StringField,
    URLField,
)
from starlette_admin.helpers import slugify_class_name


class BaseODMModelConverter(StandardModelConverter):
    def get_type(self, model: Model, value: Any) -> Any:
        if isinstance(value, str) and hasattr(model, value):
            return model.__odm_fields__[value]
        raise ValueError(f"Can't find attribute with key {value}")

    def convert_fields_list(
        self, *, fields: Sequence[Any], model: Type[Model], **kwargs: Any
    ) -> Sequence[BaseField]:
        fields = [str(+v) if isinstance(v, FieldProxy) else v for v in fields]
        try:
            return super().convert_fields_list(fields=fields, model=model, **kwargs)
        except BaseNotSupportedAnnotation as e:
            raise NotSupportedAnnotation(*e.args) from e


class ModelConverter(BaseODMModelConverter):
    @converts(ODMField)
    def conv_odm_field(self, *args: Any, type: ODMField, **kwargs: Any) -> BaseField:
        kwargs.update(
            {
                "type": type.pydantic_field.annotation,
                "required": type.is_required_in_doc() and not type.primary_field,
            }
        )
        return self.convert(*args, **kwargs)

    @converts(ODMEmbedded)
    def conv_odm_embedded(
        self, *args: Any, type: ODMEmbedded, **kwargs: Any
    ) -> BaseField:
        standard_type_common = self._standard_type_common(*args, **kwargs)
        sub_fields = []
        for name, sub_field in type.model.__odm_fields__.items():
            kwargs.update({"name": name, "type": sub_field})
            sub_fields.append(self.convert(*args, **kwargs))
        return CollectionField(**standard_type_common, fields=sub_fields)

    @converts(ODMEmbeddedGeneric)
    def conv_odm_embedded_generic(
        self, *args: Any, type: ODMEmbeddedGeneric, **kwargs: Any
    ) -> BaseField:
        sub_fields = []
        standard_type_common = self._standard_type_common(*args, **kwargs)
        for name, sub_field in type.model.__odm_fields__.items():
            kwargs.update({"name": name, "type": sub_field})
            sub_fields.append(self.convert(*args, **kwargs))
        return ListField(
            required=kwargs.get("required", True),
            field=CollectionField(**standard_type_common, fields=sub_fields),
        )

    @converts(ODMReference)
    def conv_odm_reference(
        self, *args: Any, type: ODMReference, **kwargs: Any
    ) -> BaseField:
        return HasOne(
            **self._standard_type_common(*args, **kwargs),
            identity=slugify_class_name(type.model.__name__),
        )

    @converts(bson.ObjectId, bson.Regex, bson.Binary, pydantic.NameEmail)
    def conv_bson_string(self, *args: Any, **kwargs: Any) -> BaseField:
        return StringField(**self._standard_type_common(**kwargs))

    @converts(bson.Int64)
    def conv_bson_int64(self, *args: Any, **kwargs: Any) -> BaseField:
        return IntegerField(**self._standard_type_common(**kwargs))

    @converts(bson.Decimal128)
    def conv_bson_decimal(self, *args: Any, **kwargs: Any) -> BaseField:
        return DecimalField(**self._standard_type_common(**kwargs))

    @converts(odmantic.bson._datetime)
    def conv_bson_datetime(self, *args: Any, **kwargs: Any) -> BaseField:
        return DateTimeField(**self._standard_type_common(**kwargs))

    @converts(pydantic.EmailStr)
    def conv_pydantic_email(self, *args: Any, **kwargs: Any) -> BaseField:
        return EmailField(**self._standard_type_common(**kwargs))

    @converts(pydantic.AnyUrl)
    def conv_pydantic_url(self, *args: Any, **kwargs: Any) -> BaseField:
        return URLField(**self._standard_type_common(**kwargs))
