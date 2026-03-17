from mongoengine.base import BaseField

__all__ = "WtfBaseField"


class WtfBaseField(BaseField):
    """
    Extension wrapper class for mongoengine BaseField.

    This enables flask-mongoengine  wtf to extend the
    number of field parameters, and settings on behalf
    of document model form generator for WTForm.

    @param validators:  wtf model form field validators.
    @param filters:     wtf model form field filters.
    """

    def __init__(self, validators=None, filters=None, **kwargs):

        self.validators = self._ensure_callable_or_list(validators, "validators")
        self.filters = self._ensure_callable_or_list(filters, "filters")

        BaseField.__init__(self, **kwargs)

    def _ensure_callable_or_list(self, field, msg_flag):
        """
        Ensure the value submitted via field is either
        a callable object to convert to list or it is
        in fact a valid list value.

        """
        if field is not None:
            if callable(field):
                field = [field]
            else:
                msg = "Argument '%s' must be a list value" % msg_flag
                if not isinstance(field, list):
                    raise TypeError(msg)

        return field
