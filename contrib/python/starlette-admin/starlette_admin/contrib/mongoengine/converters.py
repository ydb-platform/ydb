from typing import Any, Callable, Dict, Sequence, Type

import mongoengine.fields as me
import starlette_admin.contrib.mongoengine.fields as internal_fields
import starlette_admin.fields as sa
from starlette_admin.contrib.mongoengine.exceptions import NotSupportedField
from starlette_admin.converters import BaseModelConverter, converts
from starlette_admin.helpers import slugify_class_name


class BaseMongoEngineModelConverter(BaseModelConverter):
    def get_converter(self, field: me.BaseField) -> Callable[..., sa.BaseField]:
        if field.__class__ in self.converters:
            return self.converters[field.__class__]
        for cls, converter in self.converters.items():
            if isinstance(field, cls):
                return converter
        raise NotSupportedField(
            f"Field {field.__class__.__name__} can not be converted automatically. Find the appropriate field "
            "manually or provide your custom converter"
        )

    def convert(self, *args: Any, **kwargs: Any) -> sa.BaseField:
        return self.get_converter(kwargs.get("field"))(*args, **kwargs)

    def convert_fields_list(
        self,
        *,
        fields: Sequence[Any],
        model: Type[me.Document],
        **kwargs: Any,
    ) -> Sequence[sa.BaseField]:
        converted_fields = []
        for value in fields:
            if isinstance(value, sa.BaseField):
                converted_fields.append(value)
            else:
                if isinstance(value, me.BaseField):
                    field = value
                elif isinstance(value, str) and hasattr(model, value):
                    field = getattr(model, value)
                else:
                    raise ValueError(f"Can't find field with key {value}")
                converted_fields.append(self.convert(field=field))
        return converted_fields


class ModelConverter(BaseMongoEngineModelConverter):
    @classmethod
    def _field_common(cls, *, field: me.BaseField, **kwargs: Any) -> Dict[str, Any]:
        return {
            "name": field.name,
            "help_text": getattr(field, "help_text", None),
            "required": field.required,
        }

    @classmethod
    def _numeric_field_common(
        cls, *, field: me.BaseField, **kwargs: Any
    ) -> Dict[str, Any]:
        return {
            "min": getattr(field, "min_value", None),
            "max": getattr(field, "max_value", None),
        }

    @converts(me.StringField, me.ObjectIdField, me.UUIDField)
    def conv_string_field(self, *args: Any, **kwargs: Any) -> sa.BaseField:
        return sa.StringField(**self._field_common(*args, **kwargs))

    @converts(me.IntField, me.LongField)
    def conv_int_field(self, *args: Any, **kwargs: Any) -> sa.BaseField:
        return sa.IntegerField(
            **self._field_common(*args, **kwargs),
            **self._numeric_field_common(*args, **kwargs),
        )

    @converts(me.FloatField)
    def conv_float_field(self, *args: Any, **kwargs: Any) -> sa.BaseField:
        return sa.FloatField(**self._field_common(*args, **kwargs))

    @converts(me.DecimalField, me.Decimal128Field)
    def conv_decimal_field(self, *args: Any, **kwargs: Any) -> sa.BaseField:
        return sa.DecimalField(
            **self._field_common(*args, **kwargs),
            **self._numeric_field_common(*args, **kwargs),
        )

    @converts(me.BooleanField)
    def conv_boolean_field(self, *args: Any, **kwargs: Any) -> sa.BaseField:
        return sa.BooleanField(**self._field_common(*args, **kwargs))

    @converts(me.DateTimeField, me.ComplexDateTimeField)
    def conv_datetime_field(self, *args: Any, **kwargs: Any) -> sa.BaseField:
        return sa.DateTimeField(**self._field_common(*args, **kwargs))

    @converts(me.DateField)
    def conv_date_field(self, *args: Any, **kwargs: Any) -> sa.BaseField:
        return sa.DateField(**self._field_common(*args, **kwargs))

    @converts(me.EmailField)
    def conv_email_field(self, *args: Any, **kwargs: Any) -> sa.BaseField:
        return sa.EmailField(**self._field_common(*args, **kwargs))

    @converts(me.URLField)
    def conv_url_field(self, *args: Any, **kwargs: Any) -> sa.BaseField:
        return sa.URLField(**self._field_common(*args, **kwargs))

    @converts(me.MapField, me.DictField)
    def conv_map_field(self, *args: Any, **kwargs: Any) -> sa.BaseField:
        return sa.JSONField(**self._field_common(*args, **kwargs))

    @converts(me.FileField)
    def conv_file_field(self, *args: Any, **kwargs: Any) -> sa.BaseField:
        return internal_fields.FileField(**self._field_common(*args, **kwargs))

    @converts(me.ImageField)
    def conv_image_field(self, *args: Any, **kwargs: Any) -> sa.BaseField:
        return internal_fields.ImageField(**self._field_common(*args, **kwargs))

    @converts(me.EnumField)
    def conv_enum_field(self, *args: Any, **kwargs: Any) -> sa.BaseField:
        field = kwargs["field"]
        return sa.EnumField(**self._field_common(*args, **kwargs), enum=field._enum_cls)

    @converts(me.ReferenceField)
    def conv_reference_field(self, *args: Any, **kwargs: Any) -> sa.BaseField:
        field = kwargs["field"]
        dtype = field.document_type_obj
        identity = slugify_class_name(
            dtype if isinstance(dtype, str) else dtype.__name__
        )
        return sa.HasOne(**self._field_common(*args, **kwargs), identity=identity)

    @converts(me.EmbeddedDocumentField)
    def conv_embedded_document_field(self, *args: Any, **kwargs: Any) -> sa.BaseField:
        field = kwargs["field"]
        document_type_obj: me.EmbeddedDocument = field.document_type
        _fields = []
        for _field in document_type_obj._fields_ordered:
            kwargs["field"] = getattr(document_type_obj, _field)
            _fields.append(self.convert(*args, **kwargs))
        return sa.CollectionField(field.name, _fields, field.required)

    @converts(me.ListField, me.SortedListField)
    def conv_list_field(self, *args: Any, **kwargs: Any) -> sa.BaseField:
        field = kwargs["field"]
        if field.field is None:
            raise ValueError(f'ListField "{field.name}" must have field specified')
        if isinstance(
            field.field,
            (me.ReferenceField, me.CachedReferenceField, me.LazyReferenceField),
        ):
            """To Many reference"""
            dtype = field.field.document_type_obj
            identity = slugify_class_name(
                dtype if isinstance(dtype, str) else dtype.__name__
            )
            return sa.HasMany(**self._field_common(*args, **kwargs), identity=identity)
        field.field.name = field.name
        kwargs["field"] = field.field
        if isinstance(field.field, (me.DictField, me.MapField)):
            return self.convert(*args, **kwargs)
        if isinstance(field.field, me.EnumField):
            admin_field = self.convert(*args, **kwargs)
            admin_field.multiple = True  # type: ignore [attr-defined]
            return admin_field
        return sa.ListField(self.convert(*args, **kwargs), required=field.required)
