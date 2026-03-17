# -*- coding: utf-8 -*-
import marshmallow as ma


def dict2schema(dct, schema_class=ma.Schema):
    """Generate a `marshmallow.Schema` class given a dictionary of
    `Fields <marshmallow.fields.Field>`.
    """
    if hasattr(schema_class, "from_dict"):  # marshmallow 3
        return schema_class.from_dict(dct)
    attrs = dct.copy()

    class Meta(object):
        strict = True

    attrs["Meta"] = Meta
    return type(str(""), (schema_class,), attrs)
