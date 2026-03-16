import uuid
from typing import Any, Dict, Sequence, Type, Union, get_args, get_origin

from beanie import BackLink, Link, PydanticObjectId
from beanie.odm.fields import ExpressionField
from pydantic import (  # type: ignore[attr-defined]
    AnyUrl,
    AwareDatetime,
    BaseModel,
    EmailStr,
    FutureDate,
    FutureDatetime,
    NaiveDatetime,
    PastDate,
    PastDatetime,
    SecretStr,
)
from starlette_admin.contrib.beanie.helpers import (
    is_link_type,
    is_list_of_links_type,
    isvalid_field,
    resolve_expression_field_name,
)
from starlette_admin.converters import StandardModelConverter, converts
from starlette_admin.fields import (
    BaseField,
    CollectionField,
    DateTimeField,
    EmailField,
    HasMany,
    HasOne,
    PasswordField,
    StringField,
    URLField,
)
from starlette_admin.helpers import slugify_class_name


class BeanieModelConverter(StandardModelConverter):

    @converts(PydanticObjectId)
    def conv_pydantic_object_id(self, *args: Any, **kwargs: Any) -> BaseField:
        return StringField(
            **self._standard_type_common(*args, **kwargs), label=kwargs.get("name")
        )

    @converts(uuid.UUID)
    def conv_uuid(self, *args: Any, **kwargs: Any) -> BaseField:
        return StringField(
            **self._standard_type_common(*args, **kwargs), label=kwargs.get("name")
        )

    @converts(BackLink)
    def conv_back_link(self, *args: Any, **kwargs: Any) -> BaseField:
        return StringField(
            **self._standard_type_common(*args, **kwargs), label=kwargs.get("name")
        )

    @converts(SecretStr)
    def conv_secret_str(self, *args: Any, **kwargs: Any) -> BaseField:
        return PasswordField(
            **self._standard_type_common(*args, **kwargs), label=kwargs.get("name")
        )

    @converts(EmailStr)
    def conv_email_str(self, *args: Any, **kwargs: Any) -> BaseField:
        return EmailField(
            **self._standard_type_common(*args, **kwargs), label=kwargs.get("name")
        )

    @converts(AnyUrl)
    def conv_any_url(self, *args: Any, **kwargs: Any) -> BaseField:
        return URLField(
            **self._standard_type_common(*args, **kwargs), label=kwargs.get("name")
        )

    @converts(
        AwareDatetime, NaiveDatetime, FutureDatetime, PastDatetime, PastDate, FutureDate
    )
    def conv_aware_datetime(self, *args: Any, **kwargs: Any) -> BaseField:
        return DateTimeField(
            **self._standard_type_common(*args, **kwargs), label=kwargs.get("name")
        )

    @converts(Link)
    def conv_link(self, *args: Any, **kwargs: Any) -> BaseField:
        link_type = kwargs.get("type")
        # get the model type from the Link field
        link_model_type = get_args(link_type)[0]

        # check if this is a list of links
        if get_origin(link_type) is list:
            link_model_type = get_args(link_model_type)[0]
            return HasMany(
                **self._standard_type_common(*args, **kwargs),
                label=kwargs.get("name"),
                identity=slugify_class_name(link_model_type.__name__),
            )

        return HasOne(
            **self._standard_type_common(*args, **kwargs),
            label=kwargs.get("name"),
            identity=slugify_class_name(link_model_type.__name__),
        )

    @converts(BaseModel)
    def conv_base_model(
        self,
        name: str,
        required: bool,
        *args: Any,
        **kwargs: Any,
    ) -> BaseField:
        model_type: Union[Type[BaseModel], None] = kwargs.get("type")
        assert model_type is not None

        _fields = []
        for subfield_name, subfield_field in model_type.model_fields.items():  # type: ignore[attr-defined]
            kwargs["type"] = subfield_field.annotation
            kwargs["name"] = subfield_name
            kwargs["required"] = subfield_field.is_required()
            _fields.append(self.convert(*args, **kwargs))
        return CollectionField(name=name, fields=_fields, required=required)

    def get_type_beanie(self, model_fields: Any, value: Any) -> Any:
        field_type = model_fields[value].annotation
        while get_origin(field_type) is Union:
            field_type = get_args(field_type)[0]

        return field_type

    def convert_fields_list(
        self, *, fields: Sequence[Any], model: Type[Any], **kwargs: Dict[str, Any]
    ) -> Sequence[BaseField]:
        converted_fields = []
        for value in fields:
            if isinstance(value, BaseField):
                converted_fields.append(value)
            else:
                field = (
                    resolve_expression_field_name(value)
                    if isinstance(value, ExpressionField)
                    else value
                )
                if not isvalid_field(model, field):
                    raise ValueError(f"Invalid field: {field}")
                field_type = self.get_type_beanie(model.model_fields, value)
                if is_link_type(field_type) or is_list_of_links_type(field_type):
                    converted_fields.append(
                        self.conv_link(
                            name=field,
                            type=field_type,
                            required=model.model_fields[value].is_required(),
                        )
                    )
                else:
                    converted_fields.append(
                        self.convert(
                            name=field,
                            type=field_type,
                            required=model.model_fields[value].is_required(),
                            model=model,
                        )
                    )
        return converted_fields
