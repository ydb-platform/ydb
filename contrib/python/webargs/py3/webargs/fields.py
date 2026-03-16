# -*- coding: utf-8 -*-
"""Field classes.

Includes all fields from `marshmallow.fields` in addition to a custom
`Nested` field and `DelimitedList`.

All fields can optionally take a special `location` keyword argument, which
tells webargs where to parse the request argument from.

.. code-block:: python

    args = {
        "active": fields.Bool(location='query'),
        "content_type": fields.Str(data_key="Content-Type", location="headers"),
    }

Note: `data_key` replaced `load_from` in marshmallow 3.
When using marshmallow 2, use `load_from`.
"""
import marshmallow as ma

# Expose all fields from marshmallow.fields.
from marshmallow.fields import *  # noqa: F40
from webargs.compat import MARSHMALLOW_VERSION_INFO
from webargs.dict2schema import dict2schema

__all__ = ["DelimitedList"] + ma.fields.__all__


class Nested(ma.fields.Nested):
    """Same as `marshmallow.fields.Nested`, except can be passed a dictionary as
    the first argument, which will be converted to a `marshmallow.Schema`.

    .. note::

        The schema class here will always be `marshmallow.Schema`, regardless
        of whether a custom schema class is set on the parser. Pass an explicit schema
        class if necessary.
    """

    def __init__(self, nested, *args, **kwargs):
        if isinstance(nested, dict):
            nested = dict2schema(nested)
        super(Nested, self).__init__(nested, *args, **kwargs)


class DelimitedList(ma.fields.List):
    """Same as `marshmallow.fields.List`, except can load from either a list or
    a delimited string (e.g. "foo,bar,baz").

    :param Field cls_or_instance: A field class or instance.
    :param str delimiter: Delimiter between values.
    :param bool as_string: Dump values to string.
    """

    delimiter = ","

    def __init__(self, cls_or_instance, delimiter=None, as_string=False, **kwargs):
        self.delimiter = delimiter or self.delimiter
        self.as_string = as_string
        super(DelimitedList, self).__init__(cls_or_instance, **kwargs)

    def _serialize(self, value, attr, obj):
        ret = super(DelimitedList, self)._serialize(value, attr, obj)
        if self.as_string:
            return self.delimiter.join(format(each) for each in ret)
        return ret

    def _deserialize(self, value, attr, data, **kwargs):
        try:
            ret = (
                value
                if ma.utils.is_iterable_but_not_string(value)
                else value.split(self.delimiter)
            )
        except AttributeError:
            if MARSHMALLOW_VERSION_INFO[0] < 3:
                self.fail("invalid")
            else:
                raise self.make_error("invalid")
        return super(DelimitedList, self)._deserialize(ret, attr, data, **kwargs)
