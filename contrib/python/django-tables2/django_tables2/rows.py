from django.core.exceptions import FieldDoesNotExist
from django.db import models

from .columns.linkcolumn import BaseLinkColumn
from .columns.manytomanycolumn import ManyToManyColumn
from .utils import A, AttributeDict, call_with_appropriate, computed_values


class CellAccessor:
    """Access cell contents on a row object (see `BoundRow`)."""

    def __init__(self, row):
        self.row = row

    def __getitem__(self, key):
        return self.row.get_cell(key)

    def __getattr__(self, name):
        return self.row.get_cell(name)


class BoundRow:
    """
    Represents a *specific* row in a table.

    `.BoundRow` objects are a container that make it easy to access the
    final 'rendered' values for cells in a row. You can simply iterate over a
    `.BoundRow` object and it will take care to return values rendered
    using the correct method (e.g. :ref:`table.render_FOO`)

    To access the rendered value of each cell in a row, just iterate over it::

        >>> import django_tables2 as tables
        >>> class SimpleTable(tables.Table):
        ...     a = tables.Column()
        ...     b = tables.CheckBoxColumn(attrs={'name': 'my_chkbox'})
        ...
        >>> table = SimpleTable([{'a': 1, 'b': 2}])
        >>> row = table.rows[0]  # we only have one row, so let's use it
        >>> for cell in row:
        ...     print(cell)
        ...
        1
        <input type="checkbox" name="my_chkbox" value="2" />

    Alternatively you can use row.cells[0] to retrieve a specific cell::

        >>> row.cells[0]
        1
        >>> row.cells[1]
        '<input type="checkbox" name="my_chkbox" value="2" />'
        >>> row.cells[2]
        ...
        IndexError: list index out of range

    Finally you can also use the column names to retrieve a specific cell::

        >>> row.cells.a
        1
        >>> row.cells.b
        '<input type="checkbox" name="my_chkbox" value="2" />'
        >>> row.cells.c
        ...
        KeyError: "Column with name 'c' does not exist; choices are: ['a', 'b']"

    If you have the column name in a variable, you can also treat the `cells`
    property like a `dict`::

        >>> key = 'a'
        >>> row.cells[key]
        1

    Arguments:
        table: The `.Table` in which this row exists.
        record: a single record from the :term:`table data` that is used to
            populate the row. A record could be a `~django.db.Model` object, a
            `dict`, or something else.

    """

    def __init__(self, record, table):
        self._record = record
        self._table = table

        self.row_counter = next(table._counter)

        # support accessing cells from a template: {{ row.cells.column_name }}
        self.cells = CellAccessor(self)

    @property
    def table(self):
        """The `.Table` this row is part of."""
        return self._table

    def get_even_odd_css_class(self):
        """
        Return css class, alternating for odd and even records.

        Return:
            string: `even` for even records, `odd` otherwise.
        """
        return "odd" if self.row_counter % 2 else "even"

    @property
    def attrs(self):
        """Return the attributes for a certain row."""
        cssClass = self.get_even_odd_css_class()

        row_attrs = computed_values(
            self._table.row_attrs, kwargs=dict(table=self._table, record=self._record)
        )

        if "class" in row_attrs and row_attrs["class"]:
            row_attrs["class"] += " " + cssClass
        else:
            row_attrs["class"] = cssClass

        return AttributeDict(row_attrs)

    @property
    def record(self):
        """The data record from the data source which is used to populate this row with data."""
        return self._record

    def __iter__(self):
        """
        Iterate over the rendered values for cells in the row.

        Under the hood this method just makes a call to
        `.BoundRow.__getitem__` for each cell.
        """
        for column, value in self.items():
            # this uses __getitem__, using the name (rather than the accessor)
            # is correct – it's what __getitem__ expects.
            yield value

    def _get_and_render_with(self, bound_column, render_func, default):
        value = None
        accessor = A(bound_column.accessor)
        column = bound_column.column

        # We need to take special care here to allow get_FOO_display()
        # methods on a model to be used if available. See issue #30.
        penultimate, remainder = accessor.penultimate(self.record)

        # If the penultimate is a model and the remainder is a field
        # using choices, use get_FOO_display().
        if isinstance(penultimate, models.Model):
            try:
                field = accessor.get_field(self.record)
                display_fn = getattr(penultimate, f"get_{remainder}_display", None)
                if getattr(field, "choices", ()) and display_fn:
                    value = display_fn()
                    remainder = None
            except FieldDoesNotExist:
                pass

        # Fall back to just using the original accessor
        if remainder:
            try:
                value = accessor.resolve(self.record)
            except Exception:
                # we need to account for non-field based columns (issue #257)
                if isinstance(column, BaseLinkColumn) and column.text is not None:
                    return render_func(bound_column)

        is_manytomanycolumn = isinstance(column, ManyToManyColumn)
        if value in column.empty_values or (is_manytomanycolumn and not value.exists()):
            return default

        return render_func(bound_column, value)

    def _optional_cell_arguments(self, bound_column, value):
        """Arguments that will optionally be passed while rendering cells."""
        return {
            "value": value,
            "record": self.record,
            "column": bound_column.column,
            "bound_column": bound_column,
            "bound_row": self,
            "table": self._table,
        }

    def get_cell(self, name):
        """Return the final rendered html for a cell in the row, given the name of a column."""
        bound_column = self.table.columns[name]

        return self._get_and_render_with(
            bound_column, render_func=self._call_render, default=bound_column.default
        )

    def _call_render(self, bound_column, value=None):
        """Call the column's render method with appropriate kwargs."""
        render_kwargs = self._optional_cell_arguments(bound_column, value)
        content = call_with_appropriate(bound_column.render, render_kwargs)

        return bound_column.link(content, **render_kwargs) if bound_column.link else content

    def get_cell_value(self, name):
        """Return the final rendered value (excluding any html) for a cell in the row, given the name of a column."""
        return self._get_and_render_with(
            self.table.columns[name], render_func=self._call_value, default=None
        )

    def _call_value(self, bound_column, value=None):
        """Call the column's value method with appropriate kwargs."""
        return call_with_appropriate(
            bound_column.value, self._optional_cell_arguments(bound_column, value)
        )

    def __contains__(self, item):
        """Check by both row object and column name."""
        return item in (self.table.columns if isinstance(item, str) else self)

    def items(self):
        """
        Return an iterator yielding ``(bound_column, cell)`` pairs.

        *cell* is ``row[name]`` -- the rendered unicode value that should be
        ``rendered within ``<td>``.
        """
        for column in self.table.columns:
            # column gets some attributes relevant only relevant in this iteration,
            # used to allow passing the value/record to a callable Column.attrs /
            # Table.attrs item.
            column.current_value = self.get_cell(column.name)
            column.current_record = self.record
            yield (column, column.current_value)


class BoundPinnedRow(BoundRow):
    """A *pinned* row in a table."""

    @property
    def attrs(self):
        """
        Return the attributes for a certain pinned row.

        Add CSS classes `pinned-row` and `odd` or `even` to `class` attribute.

        Return:
            AttributeDict: Attributes for pinned rows.
        """
        row_attrs = computed_values(self._table.pinned_row_attrs, kwargs={"record": self._record})
        css_class = " ".join(
            [self.get_even_odd_css_class(), "pinned-row", row_attrs.get("class", "")]
        )
        row_attrs["class"] = css_class
        return AttributeDict(row_attrs)


class BoundRows:
    """
    Container for spawning `.BoundRow` objects.

    Arguments:
        data: iterable of records
        table: the `~.Table` in which the rows exist
        pinned_data: dictionary with iterable of records for top and/or
         bottom pinned rows.

    Example:
        >>> pinned_data = {
        ...    'top': iterable,      # or None value
        ...    'bottom': iterable,   # or None value
        ... }

    This is used for `~.Table.rows`.
    """

    def __init__(self, data, table, pinned_data=None):
        self.data = data
        self.table = table
        self.pinned_data = pinned_data or {}

    def generator_pinned_row(self, data):
        """
        Top and bottom pinned rows generator.

        Arguments:
            data: Iterable data for all records for top or bottom pinned rows.

        Yields:
            BoundPinnedRow: Top or bottom `BoundPinnedRow` object for single pinned record.
        """
        if data is not None:
            if hasattr(data, "__iter__") is False:
                raise ValueError("The data for pinned rows must be iterable")

            for pinned_record in data:
                yield BoundPinnedRow(pinned_record, table=self.table)

    def __iter__(self):
        # Top pinned rows
        yield from self.generator_pinned_row(self.pinned_data.get("top"))

        for record in self.data:
            yield BoundRow(record, table=self.table)

        # Bottom pinned rows
        yield from self.generator_pinned_row(self.pinned_data.get("bottom"))

    def __len__(self):
        length = len(self.data)
        pinned_top = self.pinned_data.get("top")
        pinned_bottom = self.pinned_data.get("bottom")
        length += 0 if pinned_top is None else len(pinned_top)
        length += 0 if pinned_bottom is None else len(pinned_bottom)
        return length

    def __getitem__(self, key):
        """Return a new `~.BoundRows` instance for a slice, a single `~.BoundRow` instance for an index."""
        if isinstance(key, slice):
            return BoundRows(data=self.data[key], table=self.table, pinned_data=self.pinned_data)
        else:
            return BoundRow(record=self.data[key], table=self.table)
