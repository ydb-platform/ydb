from django.utils.safestring import mark_safe

from django_tables2.utils import Accessor, AttributeDict, computed_values

from .base import Column, library


@library.register
class CheckBoxColumn(Column):
    """
    A subclass of `.Column` that renders as a checkbox form input.

    This column allows a user to *select* a set of rows. The selection
    information can then be used to apply some operation (e.g. "delete") onto
    the set of objects that correspond to the selected rows.

    The value that is extracted from the :term:`table data` for this column is
    used as the value for the checkbox, i.e. ``<input type="checkbox"
    value="..." />``

    This class implements some sensible defaults:

    - HTML input's ``name`` attribute is the :term:`column name` (can override
      via *attrs* argument).
    - ``orderable`` defaults to `False`.

    Arguments:
        attrs (dict): In addition to *attrs* keys supported by `~.Column`, the
            following are available:

             - ``input``     -- ``<input>`` elements in both ``<td>`` and ``<th>``.
             - ``th__input`` -- Replaces ``input`` attrs in header cells.
             - ``td__input`` -- Replaces ``input`` attrs in body cells.

        checked (`~.Accessor`, bool, callable): Allow rendering the checkbox as
            checked. If it resolves to a truthy value, the checkbox will be
            rendered as checked.

    .. note::

        You might expect that you could select multiple checkboxes in the
        rendered table and then *do something* with that. This functionality
        is not implemented. If you want something to actually happen, you will
        need to implement that yourself.
    """

    def __init__(self, attrs=None, checked=None, **extra):
        self.checked = checked
        kwargs = {"orderable": False, "attrs": attrs}
        kwargs.update(extra)
        super().__init__(**kwargs)

    @property
    def header(self):
        default = {"type": "checkbox"}
        general = self.attrs.get("input")
        specific = self.attrs.get("th__input")
        attrs = AttributeDict(default, **(specific or general or {}))
        return mark_safe(f"<input {attrs.as_html()} />")

    def render(self, value, bound_column, record):
        default = {"type": "checkbox", "name": bound_column.name, "value": value}
        if self.is_checked(value, record):
            default.update({"checked": "checked"})

        general = self.attrs.get("input")
        specific = self.attrs.get("td__input")

        attrs = dict(default, **(specific or general or {}))
        attrs = computed_values(attrs, kwargs={"record": record, "value": value})
        return mark_safe(f"<input {AttributeDict(attrs).as_html()} />")

    def is_checked(self, value, record):
        """Determine if the checkbox should be checked."""
        if self.checked is None:
            return False
        if self.checked is True:
            return True

        if callable(self.checked):
            return bool(self.checked(value, record))

        checked = Accessor(self.checked)
        if checked in record:
            return bool(record[checked])
        return False
