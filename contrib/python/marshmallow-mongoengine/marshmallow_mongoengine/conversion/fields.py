import inspect
import functools

import mongoengine as me

from marshmallow_mongoengine import fields as ma_fields
from marshmallow_mongoengine.conversion import params
from marshmallow_mongoengine.exceptions import ModelConversionError


class MetaFieldBuilder(object):
    """
    Convert a given Mongoengine Field to an equivalent Marshmallow Field
    """

    BASE_AVAILABLE_PARAMS = (
        params.DescriptionParam,
        params.AllowNoneParam,
        params.ChoiceParam,
        params.RequiredParam,
    )
    AVAILABLE_PARAMS = ()
    MARSHMALLOW_FIELD_CLS = None

    def __init__(self, field):
        self.mongoengine_field = field
        self.params = [
            paramCls(field)
            for paramCls in self.BASE_AVAILABLE_PARAMS + self.AVAILABLE_PARAMS
        ]

    def build_marshmallow_field(self, **kwargs):
        """
        :return: The Marshmallow Field instanciated and configured
        """
        field_kwargs = None
        for param in self.params:
            field_kwargs = param.apply(field_kwargs)
        field_kwargs.update(kwargs)
        return self.marshmallow_field_cls(**field_kwargs)

    def _get_marshmallow_field_cls(self):
        """
        Return the marshmallow Field class, overload this method to
        generate more dynamic field class
        """
        return self.MARSHMALLOW_FIELD_CLS

    @property
    def marshmallow_field_cls(self):
        return self._get_marshmallow_field_cls()


class ListBuilder(MetaFieldBuilder):
    AVAILABLE_PARAMS = (params.LengthParam,)
    MARSHMALLOW_FIELD_CLS = ma_fields.List

    def _get_marshmallow_field_cls(self):
        sub_field = get_field_builder_for_data_type(self.mongoengine_field.field)
        return functools.partial(
            self.MARSHMALLOW_FIELD_CLS, sub_field.build_marshmallow_field()
        )


class ReferenceBuilder(MetaFieldBuilder):
    AVAILABLE_PARAMS = ()
    MARSHMALLOW_FIELD_CLS = ma_fields.Reference

    def _get_marshmallow_field_cls(self):
        return functools.partial(
            self.MARSHMALLOW_FIELD_CLS, self.mongoengine_field.document_type
        )


class GenericReferenceBuilder(MetaFieldBuilder):
    BASE_AVAILABLE_PARAMS = tuple(
        [
            p
            for p in MetaFieldBuilder.BASE_AVAILABLE_PARAMS
            if p is not params.ChoiceParam
        ]
    )
    AVAILABLE_PARAMS = ()
    MARSHMALLOW_FIELD_CLS = ma_fields.GenericReference

    def build_marshmallow_field(self, **kwargs):
        # Special handle for the choice field given it represent the
        # reference's document class
        kwargs["choices"] = getattr(self.mongoengine_field, "choices", None)
        return super(GenericReferenceBuilder, self).build_marshmallow_field(**kwargs)


class EmbeddedDocumentBuilder(MetaFieldBuilder):
    AVAILABLE_PARAMS = ()
    MARSHMALLOW_FIELD_CLS = ma_fields.Nested
    BASE_NESTED_SCHEMA_CLS = None

    def _get_marshmallow_field_cls(self):
        # Recursive build of marshmallow schema
        from marshmallow_mongoengine.schema import ModelSchema

        base_nested_schema_cls = self.BASE_NESTED_SCHEMA_CLS or ModelSchema

        class NestedSchema(base_nested_schema_cls):
            class Meta:
                model = self.mongoengine_field.document_type

        return functools.partial(self.MARSHMALLOW_FIELD_CLS, NestedSchema)


class MapBuilder(MetaFieldBuilder):
    AVAILABLE_PARAMS = ()
    MARSHMALLOW_FIELD_CLS = ma_fields.Map

    def _get_marshmallow_field_cls(self):
        # Recursive build of marshmallow schema
        from marshmallow_mongoengine.convert import convert_field

        return functools.partial(
            self.MARSHMALLOW_FIELD_CLS, convert_field(self.mongoengine_field.field)
        )


def get_field_builder_for_data_type(field_me):
    field_me_types = inspect.getmro(type(field_me))
    for field_me_type in field_me_types:
        if field_me_type in FIELD_MAPPING:
            field_ma_cls = FIELD_MAPPING[field_me_type]
            break
    else:
        raise ModelConversionError("Could not find field of type {0}.".format(field_me))
    return field_ma_cls(field_me)


FIELD_MAPPING = {}


def register_field_builder(mongo_field_cls, builder):
    """
    Register a :class MetaFieldBuilder: to a given Mongoengine Field
    :param mongo_field_cls: Mongoengine Field
    :param build: field_builder to register
    """
    FIELD_MAPPING[mongo_field_cls] = builder


def register_field(mongo_field_cls, marshmallow_field_cls, available_params=()):
    """
    Bind a marshmallow field to it corresponding mongoengine field
    :param mongo_field_cls: Mongoengine Field
    :param marshmallow_field_cls: Marshmallow Field
    :param available_params: List of :class marshmallow_mongoengine.conversion.params.MetaParam:
        instances to import the mongoengine field config to marshmallow
    """

    class Builder(MetaFieldBuilder):
        AVAILABLE_PARAMS = available_params
        MARSHMALLOW_FIELD_CLS = marshmallow_field_cls

    register_field_builder(mongo_field_cls, Builder)


register_field(me.fields.BinaryField, ma_fields.Raw)
register_field(me.fields.BooleanField, ma_fields.Boolean)
register_field(me.fields.ComplexDateTimeField, ma_fields.DateTime)
register_field(me.fields.DateField, ma_fields.Date)
register_field(me.fields.DateTimeField, ma_fields.DateTime)
register_field(
    me.fields.DecimalField,
    ma_fields.Decimal,
    available_params=(params.SizeParam, params.PrecisionParam),
)
register_field(me.fields.DictField, ma_fields.Raw)
register_field(me.fields.DynamicField, ma_fields.Raw)
register_field(
    me.fields.EmailField, ma_fields.Email, available_params=(params.LengthParam,)
)
register_field(
    me.fields.FloatField, ma_fields.Float, available_params=(params.SizeParam,)
)
register_field(
    me.fields.GenericEmbeddedDocumentField, ma_fields.GenericEmbeddedDocument
)
register_field_builder(me.fields.GenericReferenceField, GenericReferenceBuilder)
register_field_builder(me.fields.ReferenceField, ReferenceBuilder)
# LazyReferenceField and GenericLazyReference need mongoengine >= 0.15.0
if hasattr(me.fields, "LazyReferenceField"):
    register_field_builder(me.fields.LazyReferenceField, ReferenceBuilder)
if hasattr(me.fields, "GenericLazyReferenceField"):
    register_field_builder(me.fields.GenericLazyReferenceField, GenericReferenceBuilder)
# FilesField and ImageField can't be simply displayed...
register_field(me.fields.FileField, ma_fields.Skip)
register_field(me.fields.ImageField, ma_fields.Skip)
register_field(
    me.fields.IntField, ma_fields.Integer, available_params=(params.SizeParam,)
)
register_field(
    me.fields.LongField, ma_fields.Integer, available_params=(params.SizeParam,)
)
register_field(me.fields.ObjectIdField, ma_fields.ObjectId)
register_field(me.fields.UUIDField, ma_fields.UUID)
register_field(me.fields.PointField, ma_fields.Point)
register_field(me.fields.LineStringField, ma_fields.LineString)
register_field(
    me.fields.SequenceField, ma_fields.Integer, available_params=(params.SizeParam,)
)  # TODO: handle value_decorator
register_field(
    me.fields.StringField,
    ma_fields.String,
    available_params=(params.LengthParam, params.RegexParam),
)
register_field(
    me.fields.URLField, ma_fields.URL, available_params=(params.LengthParam,)
)
register_field_builder(me.fields.EmbeddedDocumentField, EmbeddedDocumentBuilder)
register_field_builder(me.fields.ListField, ListBuilder)
register_field_builder(me.fields.MapField, MapBuilder)
register_field_builder(me.fields.SortedListField, ListBuilder)
# TODO: finish fields...
# me.fields.GeoPointField: ma_fields.GeoPoint,
# me.fields.PolygonField: ma_fields.Polygon,
# me.fields.MultiPointField: ma_fields.MultiPoint,
# me.fields.MultiLineStringField: ma_fields.MultiLineString,
# me.fields.MultiPolygonField: ma_fields.MultiPolygon,
