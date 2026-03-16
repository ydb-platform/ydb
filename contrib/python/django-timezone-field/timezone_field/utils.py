from django.db.models.query_utils import DeferredAttribute


class AutoDeserializedAttribute(DeferredAttribute):
    """
    Use as the descriptor_class for a Django custom field.
    Allows setting the field to a serialized (typically string) value,
    and immediately reflecting that as the deserialized `to_python` value.

    (This requires that the field's `to_python` returns the same thing
    whether called with a serialized or deserialized value.)
    """

    # (Adapted from django.db.models.fields.subclassing.Creator,
    # which was included in Django 1.8 and earlier.)

    def __set__(self, instance, value):
        value = self.field.to_python(value)
        instance.__dict__[self.field.attname] = value
