import copy
from collections import OrderedDict
from itertools import count

from django.conf import settings
from django.core.paginator import Paginator
from django.db import models
from django.template.loader import get_template
from django.utils.encoding import force_str

from .columns import BoundColumns, Column, library
from .config import RequestConfig
from .data import TableData
from .rows import BoundRows
from .utils import Accessor, AttributeDict, OrderBy, OrderByTuple, Sequence


class DeclarativeColumnsMetaclass(type):
    """
    Metaclass that converts `.Column` objects defined on a class to the
    dictionary `.Table.base_columns`, taking into account parent class
    `base_columns` as well.
    """

    def __new__(mcs, name, bases, attrs):
        attrs["_meta"] = opts = TableOptions(attrs.get("Meta", None), name)

        # extract declared columns
        columns, remainder = [], {}
        for attr_name, attr in attrs.items():
            if isinstance(attr, Column):
                attr._explicit = True
                columns.append((attr_name, attr))
            else:
                remainder[attr_name] = attr
        attrs = remainder

        columns.sort(key=lambda x: x[1].creation_counter)

        # If this class is subclassing other tables, add their fields as
        # well. Note that we loop over the bases in *reverse* - this is
        # necessary to preserve the correct order of columns.
        parent_columns = []
        for base in reversed(bases):
            if hasattr(base, "base_columns"):
                parent_columns = list(base.base_columns.items()) + parent_columns

        # Start with the parent columns
        base_columns = OrderedDict(parent_columns)

        # Possibly add some generated columns based on a model
        if opts.model:
            extra = OrderedDict()

            # honor Table.Meta.fields, fallback to model._meta.fields
            if opts.fields is not None:
                # Each item in opts.fields is the name of a model field or a normal attribute on the model
                for field_name in opts.fields:
                    extra[field_name] = library.column_for_field(
                        field=Accessor(field_name).get_field(opts.model),
                        accessor=field_name,
                        linkify=opts.linkify.get(field_name),
                    )
            else:
                for field in opts.model._meta.fields:
                    extra[field.name] = library.column_for_field(
                        field, linkify=opts.linkify.get(field.name), accessor=field.name
                    )

            # update base_columns with extra columns
            for key, column in extra.items():
                # skip current column because the parent was explicitly defined,
                # and the current column is not.
                if key in base_columns and base_columns[key]._explicit is True:
                    continue
                base_columns[key] = column

        # Explicit columns override both parent and generated columns
        base_columns.update(OrderedDict(columns))

        # Apply any explicit exclude setting
        for exclusion in opts.exclude:
            if exclusion in base_columns:
                base_columns.pop(exclusion)

        # Remove any columns from our remainder, else columns from our parent class will remain
        for attr_name in remainder:
            if attr_name in base_columns:
                base_columns.pop(attr_name)

        # Set localize on columns
        for column_name in base_columns.keys():
            localize_column = None
            if column_name in opts.localize:
                localize_column = True
            # unlocalize gets higher precedence
            if column_name in opts.unlocalize:
                localize_column = False

            if localize_column is not None:
                base_columns[column_name].localize = localize_column

        attrs["base_columns"] = base_columns
        return super().__new__(mcs, name, bases, attrs)


class TableOptions:
    """
    Extracts and exposes options for a `.Table` from a `.Table.Meta`
    when the table is defined. See `.Table` for documentation on the impact of
    variables in this class.

    Arguments:
        options (`.Table.Meta`): options for a table from `.Table.Meta`
    """

    def __init__(self, options, class_name):
        super().__init__()
        self._check_types(options, class_name)

        DJANGO_TABLES2_TEMPLATE = getattr(
            settings, "DJANGO_TABLES2_TEMPLATE", "django_tables2/table.html"
        )
        DJANGO_TABLES2_TABLE_ATTRS = getattr(settings, "DJANGO_TABLES2_TABLE_ATTRS", {})

        self.attrs = getattr(options, "attrs", DJANGO_TABLES2_TABLE_ATTRS)
        self.row_attrs = getattr(options, "row_attrs", {})
        self.pinned_row_attrs = getattr(options, "pinned_row_attrs", {})
        self.default = getattr(options, "default", "—")
        self.empty_text = getattr(options, "empty_text", None)
        self.fields = getattr(options, "fields", None)
        linkify = getattr(options, "linkify", [])
        if not isinstance(linkify, dict):
            linkify = dict.fromkeys(linkify, True)
        self.linkify = linkify

        self.exclude = getattr(options, "exclude", ())
        order_by = getattr(options, "order_by", None)
        if isinstance(order_by, str):
            order_by = (order_by,)
        self.order_by = OrderByTuple(order_by) if order_by is not None else None
        self.order_by_field = getattr(options, "order_by_field", "sort")
        self.page_field = getattr(options, "page_field", "page")
        self.per_page = getattr(options, "per_page", 25)
        self.per_page_field = getattr(options, "per_page_field", "per_page")
        self.prefix = getattr(options, "prefix", "")
        self.show_header = getattr(options, "show_header", True)
        self.show_footer = getattr(options, "show_footer", True)
        self.sequence = getattr(options, "sequence", ())
        self.orderable = getattr(options, "orderable", True)
        self.model = getattr(options, "model", None)
        self.template_name = getattr(options, "template_name", DJANGO_TABLES2_TEMPLATE)
        self.localize = getattr(options, "localize", ())
        self.unlocalize = getattr(options, "unlocalize", ())

    def _check_types(self, options, class_name):
        """Check class Meta attributes to prevent common mistakes."""
        if options is None:
            return

        checks = {
            (bool,): ["show_header", "show_footer", "orderable"],
            (int,): ["per_page"],
            (tuple, list, set): ["fields", "sequence", "exclude", "localize", "unlocalize"],
            (tuple, list, set, dict): ["linkify"],
            str: ["template_name", "prefix", "order_by_field", "page_field", "per_page_field"],
            (dict,): ["attrs", "row_attrs", "pinned_row_attrs"],
            (tuple, list, str): ["order_by"],
            (type(models.Model),): ["model"],
        }

        for types, keys in checks.items():
            for key in keys:
                value = getattr(options, key, None)
                if value is not None and not isinstance(value, types):
                    expression = f"{class_name}.{key} = {value.__repr__()}"

                    allowed = ", ".join([t.__name__ for t in types])
                    raise TypeError(
                        f"{expression} (type {type(value).__name__}), but type must be one of ({allowed})"
                    )


class Table(metaclass=DeclarativeColumnsMetaclass):
    """
    A representation of a table.

    Arguments:
        data (QuerySet, list of dicts): The data to display.
            This is a required variable, a `TypeError` will be raised if it's not passed.

        order_by: (tuple or str): The default ordering tuple or comma separated str.
            A hyphen `-` can be used to prefix a column name to indicate
            *descending* order, for example: `("name", "-age")` or `name,-age`.

        orderable (bool): Enable/disable column ordering on this table

        empty_text (str): Empty text to render when the table has no data.
            (default `.Table.Meta.empty_text`)

        exclude (iterable or str): The names of columns that should not be
            included in the table.

        attrs (dict): HTML attributes to add to the ``<table>`` tag.
            When accessing the attribute, the value is always returned as an
            `.AttributeDict` to allow easily conversion to HTML.

        row_attrs (dict): Add custom html attributes to the table rows.
            Allows custom HTML attributes to be specified which will be added
            to the ``<tr>`` tag of the rendered table.

        pinned_row_attrs (dict): Same as row_attrs but for pinned rows.

        sequence (iterable): The sequence/order of columns the columns (from
            left to right).

            Items in the sequence must be :term:`column names <column name>`, or
            `"..."` (string containing three periods). `'...'` can be used as a
            catch-all for columns that are not specified.

        prefix (str): A prefix for query string fields.
            To avoid name-clashes when  using multiple tables on single page.

        order_by_field (str): If not `None`, defines the name of the *order by*
            query string field in the URL.

        page_field (str): If not `None`, defines the name of the *current page*
            query string field.

        per_page_field (str): If not `None`, defines the name of the *per page*
            query string field.

        template_name (str): The template to render when using ``{% render_table %}``
            (defaults to DJANGO_TABLES2_TEMPLATE, which is ``"django_tables2/table.html"``
            by default).

        default (str): Text to render in empty cells (determined by
            `.Column.empty_values`, default `.Table.Meta.default`)

        request: Django's request to avoid using `RequestConfig`

        show_header (bool): If `False`, the table will not have a header
            (`<thead>`), defaults to `True`

        show_footer (bool): If `False`, the table footer will not be rendered,
            even if some columns have a footer, defaults to `True`.

        extra_columns (str, `.Column`): list of `(name, column)`-tuples containing
            extra columns to add to the instance. If `column` is `None`, the column
            with `name` will be removed from the table.
    """

    def __init__(
        self,
        data=None,
        order_by=None,
        orderable=None,
        empty_text=None,
        exclude=None,
        attrs=None,
        row_attrs=None,
        pinned_row_attrs=None,
        sequence=None,
        prefix=None,
        order_by_field=None,
        page_field=None,
        per_page_field=None,
        template_name=None,
        default=None,
        request=None,
        show_header=None,
        show_footer=True,
        extra_columns=None,
    ):
        super().__init__()

        # note that although data is a keyword argument, it used to be positional
        # so it is assumed to be the first argument to this method.
        if data is None:
            raise TypeError(f"Argument data to {type(self).__name__} is required")

        self.exclude = exclude or self._meta.exclude
        self.sequence = sequence
        self.data = TableData.from_data(data=data)
        self.data.set_table(self)

        if default is None:
            default = self._meta.default
        self.default = default

        # Pinned rows #406
        self.pinned_row_attrs = AttributeDict(pinned_row_attrs or self._meta.pinned_row_attrs)
        self.pinned_data = {
            "top": self.get_top_pinned_data(),
            "bottom": self.get_bottom_pinned_data(),
        }

        self.rows = BoundRows(data=self.data, table=self, pinned_data=self.pinned_data)
        self.attrs = AttributeDict(attrs if attrs is not None else self._meta.attrs)

        for tag in ["thead", "tbody", "tfoot"]:
            # Add these attrs even if they haven't been passed so we can safely refer to them in the templates
            self.attrs[tag] = AttributeDict(self.attrs.get(tag, {}))

        self.row_attrs = AttributeDict(row_attrs or self._meta.row_attrs)
        self.empty_text = empty_text if empty_text is not None else self._meta.empty_text
        self.orderable = orderable
        self.prefix = prefix
        self.order_by_field = order_by_field
        self.page_field = page_field
        self.per_page_field = per_page_field
        self.show_header = show_header
        self.show_footer = show_footer

        # Make a copy so that modifying this will not touch the class
        # definition. Note that this is different from forms, where the
        # copy is made available in a ``fields`` attribute.
        base_columns = copy.deepcopy(type(self).base_columns)

        if extra_columns is not None:
            for name, column in extra_columns:
                if column is None and name in base_columns:
                    del base_columns[name]
                else:
                    base_columns[name] = column

        # Keep fully expanded ``sequence`` at _sequence so it's easily accessible
        # during render. The priority is as follows:
        # 1. sequence passed in as an argument
        # 2. sequence declared in ``Meta``
        # 3. sequence defaults to '...'
        if sequence is not None:
            sequence = sequence
        elif self._meta.sequence:
            sequence = self._meta.sequence
        else:
            if self._meta.fields is not None:
                sequence = tuple(self._meta.fields) + ("...",)
            else:
                sequence = ("...",)

        sequence = Sequence(sequence)
        self._sequence = sequence.expand(base_columns.keys())

        # reorder columns based on sequence.
        base_columns = OrderedDict((x, base_columns[x]) for x in sequence if x in base_columns)
        self.columns = BoundColumns(self, base_columns)
        # `None` value for order_by means no order is specified. This means we
        # `shouldn't touch our data's ordering in any way. *However*
        # `table.order_by = None` means "remove any ordering from the data"
        # (it's equivalent to `table.order_by = ()`).
        if order_by is None and self._meta.order_by is not None:
            order_by = self._meta.order_by
        if order_by is None:
            self._order_by = None
            # If possible inspect the ordering on the data we were given and
            # update the table to reflect that.
            order_by = self.data.ordering
            if order_by is not None:
                self.order_by = order_by
        else:
            self.order_by = order_by
        self.template_name = template_name
        # If a request is passed, configure for request
        if request:
            RequestConfig(request).configure(self)

        self._counter = count()

    def get_top_pinned_data(self):
        """
        Return data for top pinned rows containing data for each row.

        Iterable type like: QuerySet, list of dicts, list of objects.
        Having a non-zero number of pinned rows will not result in an empty result set message being rendered,
        even if there are no regular data rows

        Returns:
            `None` (default) no pinned rows at the top, iterable, data for pinned rows at the top.

        Note:
            To show pinned row this method should be overridden.

        Example:
            >>> class TableWithTopPinnedRows(Table):
            ...     def get_top_pinned_data(self):
            ...         return [{
            ...             "column_a" : "some value",
            ...             "column_c" : "other value",
            ...         }]
        """
        return None

    def get_bottom_pinned_data(self):
        """
        Return data for bottom pinned rows containing data for each row.

        Iterable type like: QuerySet, list of dicts, list of objects.
        Having a non-zero number of pinned rows will not result in an empty result set message being rendered,
        even if there are no regular data rows

        Returns:
            `None` (default) no pinned rows at the bottom, iterable, data for pinned rows at the bottom.

        Note:
            To show pinned row this method should be overridden.

        Example:
            >>> class TableWithBottomPinnedRows(Table):
            ...     def get_bottom_pinned_data(self):
            ...         return [{
            ...             "column_a" : "some value",
            ...             "column_c" : "other value",
            ...         }]
        """
        return None

    def before_render(self, request):
        """
        Perform an action just before rendering the template.

        Can be used to hide a column.

        Arguments:
            request: contains the `WGSIRequest` instance, containing a `user` attribute if
                `.django.contrib.auth.middleware.AuthenticationMiddleware` is added to
                your `MIDDLEWARE_CLASSES`.

        Example::

            class Table(tables.Table):
                name = tables.Column(orderable=False)
                country = tables.Column(orderable=False)

                def before_render(self, request):
                    if request.user.has_perm('foo.delete_bar'):
                        self.columns.hide('country')
                    else:
                        self.columns.show('country')
        """
        return

    def as_html(self, request):
        """Render the table to an HTML table, adding `request` to the context."""
        # reset counter for new rendering
        self._counter = count()
        template = get_template(self.template_name)

        context = {"table": self, "request": request}

        self.before_render(request)
        return template.render(context)

    def as_values(self, exclude_columns=None):
        """
        Return a row iterator of the data which would be shown in the table where the first row is the table headers.

        Arguments:
            exclude_columns (iterable): columns to exclude in the data iterator.

        This can be used to output the table data as CSV, excel, for example using the `~.export.ExportMixin`.

        If a column is defined using a :ref:`table.render_FOO`, the returned value from that method is used.
        If you want to differentiate between the rendered cell and a value, use a `value_Foo`-method::

            class Table(tables.Table):
                name = tables.Column()

                def render_name(self, value):
                    return format_html('<span class="name">{}</span>', value)

                def value_name(self, value):
                    return value

        will have a value wrapped in `<span>` in the rendered HTML, and just returns
        the value when `as_values()` is called.

        Note that any invisible columns will be part of the row iterator.
        """
        if exclude_columns is None:
            exclude_columns = ()

        columns = [
            column
            for column in self.columns.iterall()
            if not (column.column.exclude_from_export or column.name in exclude_columns)
        ]

        yield [force_str(column.header, strings_only=True) for column in columns]

        for row in self.rows:
            yield [
                force_str(row.get_cell_value(column.name), strings_only=True) for column in columns
            ]

    def has_footer(self):
        """Return True if any of the columns define a ``_footer`` attribute or a ``render_footer()`` method."""
        return self.show_footer and any(column.has_footer() for column in self.columns)

    @property
    def show_header(self):
        return self._show_header if self._show_header is not None else self._meta.show_header

    @show_header.setter
    def show_header(self, value):
        self._show_header = value

    @property
    def order_by(self):
        return self._order_by

    @order_by.setter
    def order_by(self, value):
        """
        Order the rows of the table based on columns.

        Arguments:
            value: iterable or comma separated string of order by aliases.
        """
        # collapse empty values to ()
        order_by = () if not value else value
        # accept string
        order_by = order_by.split(",") if isinstance(order_by, str) else order_by
        valid = []

        # everything's been converted to a iterable, accept iterable!
        for alias in order_by:
            name = OrderBy(alias).bare
            if name in self.columns and self.columns[name].orderable:
                valid.append(alias)
        self._order_by = OrderByTuple(valid)
        self.data.order_by(self._order_by)

    @property
    def order_by_field(self):
        return (
            self._order_by_field if self._order_by_field is not None else self._meta.order_by_field
        )

    @order_by_field.setter
    def order_by_field(self, value):
        self._order_by_field = value

    @property
    def page_field(self):
        return self._page_field if self._page_field is not None else self._meta.page_field

    @page_field.setter
    def page_field(self, value):
        self._page_field = value

    def paginate(self, paginator_class=Paginator, per_page=None, page=1, *args, **kwargs):
        """
        Paginate the table using a paginator and creates a `page` property containing information for the current page.

        Arguments:
            paginator_class (`~django.core.paginator.Paginator`): A paginator class to
                paginate the results.

            per_page (int): Number of records to display on each page.
            page (int): Page to display.
            *args: passed on to the paginator.
            **kwargs: passed on to the paginator.

        Pagination exceptions (`~django.core.paginator.EmptyPage` and `~django.core.paginator.PageNotAnInteger`)
        may be raised from this method and should be handled by the caller.
        """
        per_page = per_page or self._meta.per_page
        self.paginator = paginator_class(self.rows, per_page, *args, **kwargs)
        self.page = self.paginator.page(page)

        return self

    @property
    def per_page_field(self):
        return (
            self._per_page_field if self._per_page_field is not None else self._meta.per_page_field
        )

    @per_page_field.setter
    def per_page_field(self, value):
        self._per_page_field = value

    @property
    def prefix(self):
        return self._prefix if self._prefix is not None else self._meta.prefix

    @prefix.setter
    def prefix(self, value):
        self._prefix = value

    @property
    def prefixed_order_by_field(self):
        return f"{self.prefix}{self.order_by_field}"

    @property
    def prefixed_page_field(self):
        return f"{self.prefix}{self.page_field}"

    @property
    def prefixed_per_page_field(self):
        return f"{self.prefix}{self.per_page_field}"

    @property
    def sequence(self):
        return self._sequence

    @sequence.setter
    def sequence(self, value):
        if value:
            value = Sequence(value)
            value.expand(self.base_columns.keys())
        self._sequence = value

    @property
    def orderable(self):
        if self._orderable is not None:
            return self._orderable
        else:
            return self._meta.orderable

    @orderable.setter
    def orderable(self, value):
        self._orderable = value

    @property
    def template_name(self):
        if self._template is not None:
            return self._template
        else:
            return self._meta.template_name

    @template_name.setter
    def template_name(self, value):
        self._template = value

    @property
    def paginated_rows(self):
        """Return the rows for the current page if the table is paginated, else all rows."""
        if hasattr(self, "page"):
            return self.page.object_list
        return self.rows

    def get_column_class_names(self, classes_set, bound_column):
        """
        Return a set of HTML class names for cells (both ``td`` and ``th``) of a **bound column** in this table.

        By default this returns the column class names defined in the table's attributes.
        This method can be overridden to change the default behavior, for example to simply `return classes_set`.

        Arguments:
            classes_set(set of string): a set of class names to be added
              to the cell, retrieved from the column's attributes. In the case
              of a header cell (th), this also includes ordering classes.
              To set the classes for a column, see `.Column`.
              To configure ordering classes, see :ref:`ordering-class-name`

            bound_column(`.BoundColumn`): the bound column the class names are
              determined for. Useful for accessing `bound_column.name`.

        Returns:
            A set of class names to be added to cells of this column

        If you want to add the column names to the list of classes for a column,
        override this method in your custom table::

            class MyTable(tables.Table):
                ...

                def get_column_class_names(self, classes_set, bound_column):
                    classes_set = super().get_column_class_names(classes_set, bound_column)
                    classes_set.add(bound_column.name)

                    return classes_set
        """
        return classes_set


def table_factory(model, table=Table, fields=None, exclude=None, localize=None):
    """
    Return Table class for given `model`, equivalent to defining a custom table class.


    Example::
        class MyTable(tables.Table):
            class Meta:
                model = model

    Arguments:
        model (`~django.db.models.Model`): Model associated with the new table
        table (`.Table`): Base Table class used to create the new one
        fields (list of str): Fields displayed in tables
        exclude (list of str): Fields exclude in tables
        localize (list of str): Fields to localize
    """
    attrs = {"model": model}
    if fields is not None:
        attrs["fields"] = fields
    if exclude is not None:
        attrs["exclude"] = exclude
    if localize is not None:
        attrs["localize"] = localize
    # If parent form class already has an inner Meta, the Meta we're
    # creating needs to inherit from the parent's inner meta.
    parent = (table.Meta, object) if hasattr(table, "Meta") else (object,)
    Meta = type("Meta", parent, attrs)

    # Give this new table class a reasonable name.
    class_name = model.__name__ + "AutogeneratedTable"
    # Class attributes for the new table class.
    table_class_attrs = {"Meta": Meta}
    return type(table)(class_name, (table,), table_class_attrs)
