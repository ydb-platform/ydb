from django.core.exceptions import ObjectDoesNotExist
from django.db.models.fields import NOT_PROVIDED
from django.db.models.manager import Manager

from . import widgets
from .exceptions import FieldError


class Field:
    """
    ``Field`` represents a mapping between an ``instance`` field and a representation of
    the field's data.

    :param attribute: A string of either an instance attribute or callable of
        the instance.

    :param column_name: An optional column name for the column that represents
        this field in the export.

    :param widget: Defines a widget that will be used to represent this
        field's data in the export, or transform the value during import.

    :param readonly: A Boolean which defines if this field will be ignored
        during import.

    :param default: This value will be returned by
        :meth:`~import_export.fields.Field.clean` if this field's widget returned
        a value defined in :attr:`~import_export.fields.empty_values`.

    :param saves_null_values: Controls whether null values are saved on the instance.
      This can be used if the widget returns null, but there is a default instance
      value which should not be overwritten.

    :param dehydrate_method: You can provide a `dehydrate_method` as a string to use
        instead of the default `dehydrate_{field_name}` syntax, or you can provide
        a callable that will be executed with the instance as its argument.

    :param m2m_add: changes save of this field to add the values, if they do not exist,
        to a ManyToMany field instead of setting all values.  Only useful if field is
        a ManyToMany field.
    """

    empty_values = [None, ""]

    def __init__(
        self,
        attribute=None,
        column_name=None,
        widget=None,
        default=NOT_PROVIDED,
        readonly=False,
        saves_null_values=True,
        dehydrate_method=None,
        m2m_add=False,
    ):
        self.attribute = attribute
        self.default = default
        self.column_name = column_name
        if not widget:
            widget = widgets.Widget()
        self.widget = widget
        self.readonly = readonly
        self.saves_null_values = saves_null_values
        self.dehydrate_method = dehydrate_method
        self.m2m_add = m2m_add

    def __repr__(self):
        """
        Displays the module, class and name of the field.
        """
        path = f"{self.__class__.__module__}.{self.__class__.__name__}"
        if self.column_name is not None:
            return f"<{path}: {self.column_name}>"
        return "<%s>" % path

    def clean(self, row, **kwargs):
        """
        Translates the value stored in the imported datasource to an
        appropriate Python object and returns it.
        """
        try:
            value = row[self.column_name]
        except KeyError:
            raise KeyError(
                "Column '%s' not found in dataset. Available "
                "columns are: %s" % (self.column_name, list(row))
            )

        value = self.widget.clean(value, row=row, **kwargs)

        if value in self.empty_values and self.default != NOT_PROVIDED:
            if callable(self.default):
                return self.default()
            return self.default

        return value

    def get_value(self, instance):
        """
        Returns the value of the instance's attribute.
        """
        if self.attribute is None:
            return None

        attrs = self.attribute.split("__")
        value = instance

        for attr in attrs:
            try:
                if isinstance(value, dict):
                    value = value[attr]
                else:
                    value = getattr(value, attr, None)
            except (ValueError, ObjectDoesNotExist, KeyError):
                # needs to have a primary key value before a many-to-many
                # relationship can be used.
                return None
            if value is None:
                return None

        # RelatedManager and ManyRelatedManager classes are callable in
        # Django >= 1.7 but we don't want to call them
        if callable(value) and not isinstance(value, Manager):
            value = value()
        return value

    def save(self, instance, row, is_m2m=False, **kwargs):
        """
        If this field is not declared readonly, the instance's attribute will
        be set to the value returned by :meth:`~import_export.fields.Field.clean`.
        """
        if not self.readonly:
            attrs = self.attribute.split("__")
            for attr in attrs[:-1]:
                instance = getattr(instance, attr, None)
            cleaned = self.clean(row, **kwargs)
            if cleaned is not None or self.saves_null_values:
                if not is_m2m:
                    setattr(instance, attrs[-1], cleaned)
                elif self.m2m_add:
                    getattr(instance, attrs[-1]).add(*cleaned)
                else:
                    getattr(instance, attrs[-1]).set(cleaned)

    def export(self, instance, **kwargs):
        """
        Returns value from the provided instance converted to export
        representation.
        """
        value = self.get_value(instance)
        return self.widget.render(value, **kwargs)

    def get_dehydrate_method(self, field_name=None):
        """
        Returns method name to be used for dehydration of the field.
        Defaults to `dehydrate_{field_name}`
        """
        DEFAULT_DEHYDRATE_METHOD_PREFIX = "dehydrate_"

        if not self.dehydrate_method and not field_name:
            raise FieldError("Both dehydrate_method and field_name are not supplied.")

        return self.dehydrate_method or DEFAULT_DEHYDRATE_METHOD_PREFIX + field_name
