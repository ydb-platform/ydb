from collections import OrderedDict
from itertools import islice

from django.core.exceptions import ImproperlyConfigured
from django.urls import reverse
from django.utils.html import format_html
from django.utils.safestring import SafeData
from django.utils.text import capfirst

from ..utils import (
    Accessor,
    AttributeDict,
    OrderBy,
    OrderByTuple,
    call_with_appropriate,
    computed_values,
)


class Library:
    """A collection of columns."""

    def __init__(self):
        self.columns = []

    def register(self, column):
        if not hasattr(column, "from_field"):
            raise ImproperlyConfigured(f"{column.__class__.__name__} is not a subclass of Column")
        self.columns.append(column)
        return column

    def column_for_field(self, field, **kwargs):
        """
        Return a column object suitable for model field.

        Returns:
            `.Column` object or `None`
        """
        if field is None:
            return self.columns[0](**kwargs)

        # Iterate in reverse order as columns are registered in order
        # of least to most specialised (i.e. Column is registered
        # first). This also allows user-registered columns to be
        # favoured.
        for candidate in reversed(self.columns):
            if hasattr(field, "get_related_field"):
                verbose_name = field.get_related_field().verbose_name
            else:
                verbose_name = getattr(field, "verbose_name", field.name)
            kwargs["verbose_name"] = capfirst(verbose_name)
            column = candidate.from_field(field, **kwargs)
            if column is None:
                continue
            return column


# The library is a mechanism for announcing what columns are available. Its
# current use is to allow the table metaclass to ask columns if they're a
# suitable match for a model field, and if so to return an approach instance.
library = Library()


class LinkTransform:
    viewname = None
    accessor = None
    attrs = None

    def __init__(self, url=None, accessor=None, attrs=None, reverse_args=None):
        """
        Object used to generate attributes for the `<a>`-tag to wrap the cell content in.

        Arguments:
            url (callable): If supplied, the result of this callable will be used as ``href`` attribute.
            accessor (Accessor): if supplied, the accessor will be used to decide on which object
                ``get_absolute_url()`` is called.
            attrs (dict): Customize attributes for the ``<a>`` tag.
                Values of the dict can be either static text or a
                callable. The callable can optionally declare any subset
                of the following keyword arguments: value, record, column,
                bound_column, bound_row, table. These arguments will then
                be passed automatically.
            reverse_args (dict, tuple): Arguments to ``django.urls.reverse()``. If dict, the arguments
                are assumed to be keyword arguments to ``reverse()``, if tuple, a ``(viewname, args)``
                or ``(viewname, kwargs)``
        """
        self.url = url
        self.attrs = attrs
        self.accessor = accessor

        if isinstance(reverse_args, (list, tuple)):
            viewname, args = reverse_args
            reverse_args = {"viewname": viewname}
            reverse_args["kwargs" if isinstance(args, dict) else "args"] = args

        self.reverse_args = reverse_args or {}

    def compose_url(self, **kwargs):
        if self.url and callable(self.url):
            return call_with_appropriate(self.url, kwargs)

        bound_column = kwargs.get("bound_column", None)
        record = kwargs["record"]

        if self.reverse_args.get("viewname", None) is not None:
            return self.call_reverse(record=record)

        if bound_column is None and self.accessor is None:
            accessor = Accessor("")
        else:
            accessor = Accessor(self.accessor if self.accessor is not None else bound_column.name)
        context = accessor.resolve(record)
        if not hasattr(context, "get_absolute_url"):
            if hasattr(record, "get_absolute_url"):
                context = record
            else:
                raise TypeError(
                    f"for linkify=True, '{context}' must have a method get_absolute_url"
                )
        return context.get_absolute_url()

    def call_reverse(self, record):
        """Prepare the arguments to reverse() for this record and calls reverse()."""

        def resolve_if_accessor(val):
            return val.resolve(record) if isinstance(val, Accessor) else val

        params = self.reverse_args.copy()

        params["viewname"] = resolve_if_accessor(params["viewname"])
        if params.get("urlconf", None):
            params["urlconf"] = resolve_if_accessor(params["urlconf"])
        if params.get("args", None):
            params["args"] = [resolve_if_accessor(a) for a in params["args"]]
        if params.get("kwargs", None):
            params["kwargs"] = {
                key: resolve_if_accessor(val) for key, val in params["kwargs"].items()
            }
        if params.get("current_app", None):
            params["current_app"] = resolve_if_accessor(params["current_app"])

        return reverse(**params)

    def get_attrs(self, **kwargs):
        attrs = AttributeDict(computed_values(self.attrs or {}, kwargs=kwargs))
        attrs["href"] = self.compose_url(**kwargs)

        return attrs

    def __call__(self, content, **kwargs):
        attrs = self.get_attrs(**kwargs)
        if attrs["href"] is None:
            return content

        return format_html("<a {}>{}</a>", attrs.as_html(), content)


@library.register
class Column:
    """
    Represents a single column of a table.

    `.Column` objects control the way a column (including the cells that fall
    within it) are rendered.

    Arguments:
        attrs (dict): HTML attributes for elements that make up the column.
            This API is extended by subclasses to allow arbitrary HTML
            attributes to be added to the output.

            By default `.Column` supports:

             - ``th`` -- ``table/thead/tr/th`` elements
             - ``td`` -- ``table/tbody/tr/td`` elements
             - ``cell`` -- fallback if ``th`` or ``td`` is not defined
             - ``a`` -- To control the attributes for the ``a`` tag if the cell
               is wrapped in a link.
        accessor (str or `~.Accessor`): An accessor that describes how to
            extract values for this column from the :term:`table data`.
        default (str or callable): The default value for the column. This can be
            a value or a callable object [1]_. If an object in the data provides
            `None` for a column, the default will be used instead.

            The default value may affect ordering, depending on the type of data
            the table is using. The only case where ordering is not affected is
            when a `.QuerySet` is used as the table data (since sorting is
            performed by the database).
        empty_values (iterable): list of values considered as a missing value,
            for which the column will render the default value. Defaults to
            `(None, '')`
        exclude_from_export (bool): If `True`, this column will not be added to
            the data iterator returned from as_values().
        footer (str, callable): Defines the footer of this column. If a callable
            is passed, it can take optional keyword arguments `column`,
            `bound_column` and `table`.
        order_by (str, tuple or `.Accessor`): Allows one or more accessors to be
            used for ordering rather than *accessor*.
        orderable (bool): If `False`, this column will not be allowed to
            influence row ordering/sorting.
        verbose_name (str): A human readable version of the column name.
        visible (bool): If `True`, this column will be rendered.
            Columns with `visible=False` will not be rendered, but will be included
            in ``.Table.as_values()`` and thus also in :ref:`export`.
        localize: If the cells in this column will be localized by the
            `localize` filter:

              - If `True`, force localization
              - If `False`, values are not localized
              - If `None` (default), localization depends on the ``USE_L10N`` setting.
        linkify (bool, str, callable, dict, tuple): Controls if cell content will be wrapped in an
            ``a`` tag. The different ways to define the ``href`` attribute:

             - If `True`, the ``record.get_absolute_url()`` or the related model's
               `get_absolute_url()` is used.
             - If a callable is passed, the returned value is used, if it's not ``None``.
               The callable can optionally accept any argument valid for :ref:`table.render_foo`-methods,
               for example `record` or `value`.
             - If a `dict` is passed, it's passed on to ``~django.urls.reverse``.
             - If a `tuple` is passed, it must be either a (viewname, args) or (viewname, kwargs)
               tuple, which is also passed to ``~django.urls.reverse``.

    Examples, assuming this model::

        class Blog(models.Model):
            title = models.CharField(max_length=100)
            body = model.TextField()
            user = model.ForeignKey(get_user_model(), on_delete=models.CASCADE)

    Using the ``linkify`` argument to control the linkification. These columns will all display
    the value returned from `str(record.user)`::

        # If the column is named 'user', the column will use record.user.get_absolute_url()
        user = tables.Column(linkify=True)

        # We can also do that explicitly:
        user = tables.Column(linkify=lambda record: record.user.get_absolute_url())

        # or, if no get_absolute_url is defined, or a custom link is required, we have a couple
        # of ways to define what is passed to reverse()
        user = tables.Column(linkify={"viewname": "user_detail", "args": [tables.A("user__pk")]})
        user = tables.Column(linkify=("user_detail", [tables.A("user__pk")])) # (viewname, args)
        user = tables.Column(linkify=("user_detail", {"pk": tables.A("user__pk")})) # (viewname, kwargs)

        initial_sort_descending (bool): If `True`, a column will sort in descending order
            on "first click" after table has been rendered. If `False`, column will follow
            default behavior, and sort ascending on "first click". Defaults to `False`.

    .. [1] The provided callable object must not expect to receive any arguments.
    """

    # Tracks each time a Column instance is created. Used to retain order.
    creation_counter = 0
    empty_values = (None, "")

    # by default, contents are not wrapped in an <a>-tag.
    link = None

    # Explicit is set to True if the column is defined as an attribute of a
    # class, used to give explicit columns precedence.
    _explicit = False

    def __init__(
        self,
        verbose_name=None,
        accessor=None,
        default=None,
        visible=True,
        orderable=None,
        attrs=None,
        order_by=None,
        empty_values=None,
        localize=None,
        footer=None,
        exclude_from_export=False,
        linkify=False,
        initial_sort_descending=False,
    ):
        if not (accessor is None or isinstance(accessor, str) or callable(accessor)):
            raise TypeError(f"accessor must be a string or callable, not {type(accessor).__name__}")
        if callable(accessor) and default is not None:
            raise TypeError("accessor must be string when default is used, not callable")
        self.accessor = Accessor(accessor) if accessor else None
        self._default = default
        self.verbose_name = verbose_name
        self.visible = visible
        self.orderable = orderable
        self.attrs = attrs or getattr(self, "attrs", {})

        # massage order_by into an OrderByTuple or None
        order_by = (order_by,) if isinstance(order_by, str) else order_by
        self.order_by = OrderByTuple(order_by) if order_by is not None else None
        if empty_values is not None:
            self.empty_values = empty_values

        self.localize = localize
        self._footer = footer
        self.exclude_from_export = exclude_from_export

        link_kwargs = None
        if callable(linkify) or hasattr(self, "get_url"):
            link_kwargs = dict(url=linkify if callable(linkify) else self.get_url)
        elif isinstance(linkify, (dict, tuple)):
            link_kwargs = dict(reverse_args=linkify)
        elif linkify is True:
            link_kwargs = dict(accessor=self.accessor)

        if link_kwargs is not None:
            self.link = LinkTransform(attrs=self.attrs.get("a", {}), **link_kwargs)

        self.initial_sort_descending = initial_sort_descending

        self.creation_counter = Column.creation_counter
        Column.creation_counter += 1

    @property
    def default(self):
        return self._default() if callable(self._default) else self._default

    @property
    def header(self):
        """
        The value used for the column heading (e.g. inside the ``<th>`` tag).

        By default this returns `~.Column.verbose_name`.

        :returns: `unicode` or `None`

        .. note::

            This property typically is not accessed directly when a table is
            rendered. Instead, `.BoundColumn.header` is accessed which in turn
            accesses this property. This allows the header to fallback to the
            column name (it is only available on a `.BoundColumn` object hence
            accessing that first) when this property doesn't return something
            useful.
        """
        return self.verbose_name

    def footer(self, bound_column, table):
        """Return the content of the footer, if specified."""
        footer_kwargs = {"column": self, "bound_column": bound_column, "table": table}

        if self._footer is not None:
            if callable(self._footer):
                return call_with_appropriate(self._footer, footer_kwargs)
            else:
                return self._footer

        if hasattr(self, "render_footer"):
            return call_with_appropriate(self.render_footer, footer_kwargs)

        return ""

    def render(self, value):
        """
        Return the content for a specific cell.

        This method can be overridden by :ref:`table.render_FOO` methods on the
        table or by subclassing `.Column`.

        If the value for this cell is in `.empty_values`, this method is
        skipped and an appropriate default value is rendered instead.
        Subclasses should set `.empty_values` to ``()`` if they want to handle
        all values in `.render`.
        """
        return value

    def value(self, **kwargs):
        """
        Return the content for a specific cell for exports.

        Similar to `.render` but without any html content.
        This can be used to get the data in the formatted as it is presented but in a
        form that could be added to a csv file.

        The default implementation just calls the `render` function but any
        subclasses where `render` returns html content should override this
        method.

        See `LinkColumn` for an example.
        """
        value = call_with_appropriate(self.render, kwargs)

        return value

    def order(self, queryset, is_descending):
        """
        Order the QuerySet of the table.

        This method can be overridden by :ref:`table.order_FOO` methods on the
        table or by subclassing `.Column`; but only overrides if second element
        in return tuple is True.

        Returns:
            Tuple (QuerySet, boolean)
        """
        return (queryset, False)

    @classmethod
    def from_field(cls, field, **kwargs):
        """
        Return a specialized column for the model field or `None`.

        Arguments:
            field (Model Field instance): the field that needs a suitable column.
            **kwargs: passed on to the column.

        Returns:
            `.Column` object or `None`

        If the column is not specialized for the given model field, it should
        return `None`. This gives other columns the opportunity to do better.

        If the column is specialized, it should return an instance of itself
        that is configured appropriately for the field.
        """
        # Since this method is inherited by every subclass, only provide a
        # column if this class was asked directly.
        if cls is Column:
            return cls(**kwargs)


class BoundColumn:
    """
    A run-time version of `.Column`.

    The difference between `.BoundColumn` and `.Column`,
    is that `.BoundColumn` objects include the relationship between a `.Column` and a `.Table`.
    In practice, this means that a `.BoundColumn` knows the *"variable name"* given to the `.Column`
    when it was declared on the `.Table`.

    Arguments:
        table (`~.Table`): The table in which this column exists
        column (`~.Column`): The type of column
        name (str): The variable name of the column used when defining the
                    `.Table`. In this example the name is ``age``::

                          class SimpleTable(tables.Table):
                              age = tables.Column()

    """

    def __init__(self, table, column, name):
        self._table = table
        self.column = column
        self.name = name
        self.link = column.link

        if not column.accessor:
            column.accessor = Accessor(self.name)
        self.accessor = column.accessor

        self.current_value = None

    def __str__(self):
        return str(self.header)

    @property
    def attrs(self):
        """
        Proxy to `.Column.attrs` but injects some values of our own.

        A ``th``, ``td`` and ``tf`` are guaranteed to be defined (irrespective
        of what is actually defined in the column attrs. This makes writing
        templates easier. ``tf`` is not actually a HTML tag, but this key name
        will be used for attributes for column's footer, if the column has one.
        """
        # prepare kwargs for computed_values()
        kwargs = {"table": self._table, "bound_column": self}
        # BoundRow.items() sets current_record and current_value when iterating over
        # the records in a table.
        if (
            getattr(self, "current_record", None) is not None
            and getattr(self, "current_value", None) is not None
        ):
            kwargs.update({"record": self.current_record, "value": self.current_value})

        # Start with table's attrs; Only 'th' and 'td' attributes will be used
        attrs = dict(self._table.attrs)

        # Update attrs to prefer column's attrs rather than table's
        attrs.update(dict(self.column.attrs))

        # we take the value for 'cell' as the basis for both the th and td attrs
        cell_attrs = attrs.get("cell", {})

        # override with attrs defined specifically for th and td respectively.
        attrs["th"] = computed_values(attrs.get("th", cell_attrs), kwargs=kwargs)
        attrs["td"] = computed_values(attrs.get("td", cell_attrs), kwargs=kwargs)
        attrs["tf"] = computed_values(attrs.get("tf", cell_attrs), kwargs=kwargs)

        # wrap in AttributeDict
        attrs["th"] = AttributeDict(attrs["th"])
        attrs["td"] = AttributeDict(attrs["td"])
        attrs["tf"] = AttributeDict(attrs["tf"])

        # Override/add classes
        attrs["th"]["class"] = self.get_th_class(attrs["th"])
        attrs["td"]["class"] = self.get_td_class(attrs["td"])
        attrs["tf"]["class"] = self.get_td_class(attrs["tf"])

        return attrs

    def _get_cell_class(self, attrs):
        """Return a set of the classes from the class key in ``attrs``."""
        classes = attrs.get("class", None)
        classes = set() if classes is None else set(classes.split(" "))

        return self._table.get_column_class_names(classes, self)

    def get_td_class(self, td_attrs):
        """Return the HTML class attribute for a data cell in this column."""
        classes = sorted(self._get_cell_class(td_attrs))
        return None if len(classes) == 0 else " ".join(classes)

    def get_th_class(self, th_attrs):
        """Return the HTML class attribute for a header cell in this column."""
        classes = self._get_cell_class(th_attrs)

        # add classes for ordering
        ordering_class = th_attrs.get("_ordering", {})
        if self.orderable:
            classes.add(ordering_class.get("orderable", "orderable"))
        if self.is_ordered:
            classes.add(
                ordering_class.get("descending", "desc")
                if self.order_by_alias.is_descending
                else ordering_class.get("ascending", "asc")
            )

        return None if len(classes) == 0 else " ".join(sorted(classes))

    @property
    def default(self):
        """Return the default value for this column."""
        value = self.column.default
        if value is None:
            value = self._table.default
        return value

    @property
    def header(self):
        """The contents of the header cell for this column."""
        # favour Column.header
        column_header = self.column.header
        if column_header:
            return column_header
        # fall back to automatic best guess
        return self.verbose_name

    @property
    def footer(self):
        """The contents of the footer cell for this column."""
        return call_with_appropriate(
            self.column.footer, {"bound_column": self, "table": self._table}
        )

    def has_footer(self):
        return self.column._footer is not None or hasattr(self.column, "render_footer")

    @property
    def order_by(self):
        """
        Return an `.OrderByTuple` of appropriately prefixed data source keys used to sort this column.

        See `.order_by_alias` for details.
        """
        if self.column.order_by is not None:
            order_by = self.column.order_by
        else:
            # default to using column accessor as data source sort key
            order_by = OrderByTuple((self.accessor,))
        return order_by.opposite if self.order_by_alias.is_descending else order_by

    @property
    def order_by_alias(self):
        """
        Return an `OrderBy` describing the current state of ordering for this column.

        The following attempts to explain the difference between `order_by`
        and `.order_by_alias`.

        `.order_by_alias` returns and `.OrderBy` instance that's based on
        the *name* of the column, rather than the keys used to order the table
        data. Understanding the difference is essential.

        Having an alias *and* a keys version is necessary because an N-tuple
        (of data source keys) can be used by the column to order the data, and
        it is ambiguous when mapping from N-tuple to column (since multiple
        columns could use the same N-tuple).

        The solution is to use order by *aliases* (which are really just
        prefixed column names) that describe the ordering *state* of the
        column, rather than the specific keys in the data source should be
        ordered.

        e.g.::

            >>> class SimpleTable(tables.Table):
            ...     name = tables.Column(order_by=("firstname", "last_name"))
            ...
            >>> table = SimpleTable([], order_by=('-name', ))
            >>> table.columns["name"].order_by_alias
            "-name"
            >>> table.columns["name"].order_by
            ("-first_name", "-last_name")

        The `OrderBy` returned has been patched to include an extra attribute
        ``next``, which returns a version of the alias that would be
        transitioned to if the user toggles sorting on this column, for example::

            not sorted -> ascending
            ascending  -> descending
            descending -> ascending

        This is useful otherwise in templates you'd need something like::

            {% if column.is_ordered %}
                {% querystring table.prefixed_order_by_field=column.order_by_alias.opposite %}
            {% else %}
                {% querystring table.prefixed_order_by_field=column.order_by_alias %}
            {% endif %}

        """
        order_by = OrderBy((self._table.order_by or {}).get(self.name, self.name))
        order_by.next = order_by.opposite if self.is_ordered else order_by
        if self.column.initial_sort_descending and not self.is_ordered:
            order_by.next = order_by.opposite
        return order_by

    @property
    def is_ordered(self):
        return self.name in (self._table.order_by or ())

    @property
    def orderable(self):
        """Return whether this column supports ordering."""
        if self.column.orderable is not None:
            return self.column.orderable
        return self._table.orderable

    @property
    def verbose_name(self):
        """
        Return the verbose name for this column.

        In order of preference, this will return:
          1) The column's explicitly defined `verbose_name`
          2) The model's `verbose_name` with the first letter capitalized (if applicable)
          3) Fall back to the column name, with first letter capitalized.

        Any `verbose_name` that was not passed explicitly in the column
        definition is returned with the first character capitalized in keeping
        with the Django convention of `verbose_name` being defined in lowercase and
        uppercased as needed by the application.

        If the table is using `QuerySet` data, then use the corresponding model
        field's `~.db.Field.verbose_name`. If it is traversing a relationship,
        then get the last field in the accessor (i.e. stop when the
        relationship turns from ORM relationships to object attributes [e.g.
        person.upper should stop at person]).
        """
        # Favor an explicit defined verbose_name
        if self.column.verbose_name is not None:
            return self.column.verbose_name

        # This is our reasonable fall back, should the next section not result
        # in anything useful.
        name = self.name.replace("_", " ")

        # Try to use a model field's verbose_name
        model = self._table.data.model
        if model:
            field = Accessor(self.accessor).get_field(model)
            if field:
                if hasattr(field, "field"):
                    name = field.field.verbose_name
                else:
                    name = getattr(field, "verbose_name", field.name)

            # If verbose_name was mark_safe()'d, return intact to keep safety
            if isinstance(name, SafeData):
                return name

        return capfirst(name)

    @property
    def visible(self):
        """Return whether this column is visible."""
        return self.column.visible

    @property
    def localize(self):
        """Return `True`, `False` or `None` as described in ``Column.localize``."""
        return self.column.localize


class BoundColumns:
    """
    Container for spawning `.BoundColumn` objects.

    This is bound to a table and provides its `.Table.columns` property.
    It provides access to those columns in different ways (iterator,
    item-based, filtered and unfiltered etc), stuff that would not be possible
    with a simple iterator in the table class.

    A `BoundColumns` object is a container for holding `BoundColumn` objects.
    It provides methods that make accessing columns easier than if they were
    stored in a `list` or `dict`. `Columns` has a similar API to a `dict` (it
    actually uses a `~collections.OrderedDict` internally).

    At the moment you'll only come across this class when you access a
    `.Table.columns` property.

    Arguments:
        table (`.Table`): the table containing the columns
    """

    def __init__(self, table, base_columns):
        self._table = table
        self.columns = OrderedDict()
        for name, column in base_columns.items():
            self.columns[name] = bound_column = BoundColumn(table, column, name)
            bound_column.render = getattr(table, "render_" + name, column.render)
            # How the value is defined: 1. value_<name> 2. render_<name> 3. column.value.
            bound_column.value = getattr(
                table, "value_" + name, getattr(table, "render_" + name, column.value)
            )
            bound_column.order = getattr(table, "order_" + name, column.order)

    def iternames(self):
        return (name for name, column in self.iteritems())

    def names(self):
        return list(self.iternames())

    def iterall(self):
        """Return an iterator that exposes all `.BoundColumn` objects, regardless of visibility or sortability."""
        return (column for name, column in self.iteritems())

    def all(self):
        return list(self.iterall())

    def iteritems(self):
        """
        Return an iterator of ``(name, column)`` pairs (where ``column`` is a `BoundColumn`).

        This method is the mechanism for retrieving columns that takes into
        consideration all of the ordering and filtering modifiers that a table
        supports (e.g. `~Table.Meta.exclude` and `~Table.Meta.sequence`).
        """
        for name in self._table.sequence:
            if name not in self._table.exclude:
                yield (name, self.columns[name])

    def items(self):
        return list(self.iteritems())

    def iterorderable(self):
        """
        `BoundColumns.all` filtered for whether they can be ordered.

        This is useful in templates, where iterating over the full
        set and checking ``{% if column.ordarable %}`` can be problematic in
        conjunction with e.g. ``{{ forloop.last }}`` (the last column might not
        be the actual last that is rendered).
        """
        return (x for x in self.iterall() if x.orderable)

    def itervisible(self):
        """
        Return `.iterorderable` filtered by visibility.

        This is geared towards table rendering.
        """
        return (x for x in self.iterall() if x.visible)

    def hide(self, name):
        """
        Hide a column.

        Arguments:
            name(str): name of the column
        """
        self.columns[name].column.visible = False

    def show(self, name):
        """
        Show a column otherwise hidden.

        Arguments:
            name(str): name of the column
        """
        self.columns[name].column.visible = True

    def __iter__(self):
        """Alias of `.itervisible` (for convenience)."""
        return self.itervisible()

    def __contains__(self, item):
        """
        Check if a column is contained within a `BoundColumns` object.

        *item* can either be a `~.BoundColumn` object, or the name of a column.
        """
        if isinstance(item, str):
            return item in self.iternames()
        else:
            # let's assume we were given a column
            return item in self.iterall()

    def __len__(self):
        """Return how many `~.BoundColumn` objects are contained (and visible)."""
        return len(list(self.itervisible()))

    def __getitem__(self, index):
        """
        Retrieve a specific `~.BoundColumn` object.

        *index* can either be 0-indexed or the name of a column

        .. code-block:: python

            columns['speed']  # returns a bound column with name 'speed'
            columns[0]        # returns the first column
        """
        if isinstance(index, int):
            try:
                return next(islice(self.iterall(), index, index + 1))
            except StopIteration:
                raise IndexError
        elif isinstance(index, str):
            for column in self.iterall():
                if column.name == index:
                    return column
            raise KeyError(
                f"Column with name '{index}' does not exist; choices are: {self.names()}"
            )
        else:
            raise TypeError(f"Column indices must be integers or str, not {type(index).__name__}")
