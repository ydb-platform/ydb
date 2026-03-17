# -*- coding: utf-8 -*-
from marshmallow_mongoengine.conversion import fields


def _is_field(value):
    return isinstance(value, type) and issubclass(value, fields.Field)


class ModelConverter(object):
    """Class that converts a mongoengine Document into a dictionary of
    corresponding marshmallow `Fields <marshmallow.fields.Field>`.
    """

    def fields_for_model(self, model, fields_kwargs=None, fields=None):
        result = {}
        fields_kwargs = fields_kwargs or {}
        for field_name, field_me in model._fields.items():
            if fields and field_name not in fields:
                continue
            if field_name in fields_kwargs:
                field_ma_cls = self.convert_field(field_me, **fields_kwargs[field_name])
            else:
                field_ma_cls = self.convert_field(field_me)
            if field_ma_cls:
                result[field_name] = field_ma_cls
        return result

    def convert_field(self, field_me, instance=True, **kwargs):
        field_builder = fields.get_field_builder_for_data_type(field_me)
        if not instance:
            return field_builder.marshmallow_field_cls
        return field_builder.build_marshmallow_field(**kwargs)

    def field_for(self, model, property_name, **kwargs):
        field_me = getattr(model, property_name)
        field_builder = fields.get_field_builder_for_data_type(field_me)
        return field_builder.build_marshmallow_field(**kwargs)


default_converter = ModelConverter()


fields_for_model = default_converter.fields_for_model
"""Generate a dict of field_name: `marshmallow.fields.Field` pairs for the
given model.

:param model: The Mongoengine Document model
:return: dict of field_name: Field instance pairs
"""


convert_field = default_converter.convert_field
"""Convert a Mongoengine `Field` to a field instance or class.

:param Property field_me: Mongoengine Field Property.
:param fields_kwargs: Dict of per-field kwargs to pass at field creation.
:param bool instance: If `True`, return  `Field` instance, computing
    relevant kwargs from the given property. If `False`, return
    the `Field` class.
:param kwargs: Additional keyword arguments to pass to the field constructor.
:return: A `marshmallow.fields.Field` class or instance.
"""


field_for = default_converter.field_for
"""Convert a property for a mapped Mongoengine Document class to a
   marshmallow `Field`.
Example: ::

    date_created = field_for(Author, 'date_created', dump_only=True)
    author = field_for(Book, 'author')

:param type ,: A Mongoengine Document mapped class.
:param str property_name: The name of the property to convert.
:param kwargs: Extra keyword arguments to pass to `property2field`
:return: A `marshmallow.fields.Field` class or instance.
"""
