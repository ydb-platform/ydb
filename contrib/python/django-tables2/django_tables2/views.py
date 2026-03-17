from itertools import count
from typing import Any, Optional

from django.core.exceptions import ImproperlyConfigured
from django.views.generic.list import ListView

from . import tables
from .config import RequestConfig


class TableMixinBase:
    """Base mixin for the Single- and MultiTable class based views."""

    context_table_name = "table"
    table_pagination = None

    def get_context_table_name(self, table):
        """Return the name to use for the table's template variable."""
        return self.context_table_name

    def get_table_pagination(self, table):
        """
        Return pagination options passed to `.RequestConfig`.

         - True for standard pagination (default),
         - False for no pagination,
         - a dictionary for custom pagination.

        `ListView`s pagination attributes are taken into account, if `table_pagination` does not
        define the corresponding value.

        Override this method to further customize pagination for a `View`.
        """
        paginate = self.table_pagination
        if paginate is False:
            return False

        paginate = {}

        # Obtains and set page size from get_paginate_by
        paginate_by = self.get_paginate_by(table.data)
        if paginate_by is not None:
            paginate["per_page"] = paginate_by

        if hasattr(self, "paginator_class"):
            paginate["paginator_class"] = self.paginator_class

        if getattr(self, "paginate_orphans", 0) != 0:
            paginate["orphans"] = self.paginate_orphans

        # table_pagination overrides any MultipleObjectMixin attributes
        if self.table_pagination:
            paginate.update(self.table_pagination)

        # we have no custom pagination settings, so just use the default.
        if not paginate and self.table_pagination is None:
            return True

        return paginate

    def get_paginate_by(self, table_data) -> Optional[int]:
        """
        Determine the number of items per page, or ``None`` for no pagination.

        Args:
            table_data: The table's data.

        Returns:
            Optional[int]: Items per page or ``None`` for no pagination.
        """
        return getattr(self, "paginate_by", None)


class SingleTableMixin(TableMixinBase):
    """
    Adds a Table object to the context. Typically used with
    `.TemplateResponseMixin`.

    Attributes:
        table_class: subclass of `.Table`
        table_data: data used to populate the table, any compatible data source.
        context_table_name(str): name of the table's template variable (default:
            'table')
        table_pagination (dict): controls table pagination. If a `dict`, passed as
            the *paginate* keyword argument to `.RequestConfig`. As such, any
            Truthy value enables pagination. (default: enable pagination).

            The `dict` can be used to specify values for arguments for the call to
            `~.tables.Table.paginate`.

            If you want to use a non-standard paginator for example, you can add a key
            `paginator_class` to the dict, containing a custom `Paginator` class.

    This mixin plays nice with the Django's ``.MultipleObjectMixin`` by using
    ``.get_queryset`` as a fall back for the table data source.
    """

    table_class = None
    table_data = None

    def get_table_class(self):
        """Return the class to use for the table."""
        if self.table_class:
            return self.table_class
        if self.model:
            return tables.table_factory(self.model)

        name = type(self).__name__
        raise ImproperlyConfigured(f"You must either specify {name}.table_class or {name}.model")

    def get_table(self, **kwargs):
        """
        Return a table object to use. The table has automatic support for
        sorting and pagination.
        """
        table_class = self.get_table_class()
        table = table_class(data=self.get_table_data(), **kwargs)
        return RequestConfig(self.request, paginate=self.get_table_pagination(table)).configure(
            table
        )

    def get_table_data(self):
        """Return the table data that should be used to populate the rows."""
        if self.table_data is not None:
            return self.table_data
        elif hasattr(self, "object_list"):
            return self.object_list
        elif hasattr(self, "get_queryset"):
            return self.get_queryset()

        view_name = type(self).__name__
        raise ImproperlyConfigured(f"Table data was not specified. Define {view_name}.table_data")

    def get_table_kwargs(self):
        """
        Return the keyword arguments for instantiating the table.

        Allows passing customized arguments to the table constructor, for example,
        to remove the buttons column, you could define this method in your View::

            def get_table_kwargs(self):
                return {
                    'exclude': ('buttons', )
                }
        """
        return {}

    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        """Overridden version of `.TemplateResponseMixin` to inject the table into the template's context."""
        context = super().get_context_data(**kwargs)
        table = self.get_table(**self.get_table_kwargs())
        context[self.get_context_table_name(table)] = table
        return context


class SingleTableView(SingleTableMixin, ListView):
    """
    Generic view that renders a template and passes in a `.Table` instances.

    Mixes ``.SingleTableMixin`` with ``django.views.generic.list.ListView``.
    """


class MultiTableMixin(TableMixinBase):
    """
    Add a list with multiple Table object's to the context. Typically used with
    `.TemplateResponseMixin`.

    The `tables` attribute must be either a list of `.Table` instances or
    classes extended from `.Table` which are not already instantiated. In that
    case, `get_tables_data` must be able to return the tables data, either by
    having an entry containing the data for each table in `tables`, or by
    overriding this method in order to return this data.

    Attributes:
        tables: list of `.Table` instances or list of `.Table` child objects.
        tables_data: if defined, `tables` is assumed to be a list of table
            classes which will be instantiated with the corresponding item from
            this list of `.TableData` instances.
        table_prefix(str): Prefix to be used for each table. The string must
            contain one instance of `{}`, which will be replaced by an integer
            different for each table in the view. Default is 'table_{}-'.
        context_table_name(str): name of the table's template variable (default:
            'tables')

    .. versionadded:: 1.2.3
    """

    tables = None
    tables_data = None

    table_prefix = "table_{}-"

    # override context table name to make sense in a multiple table context
    context_table_name = "tables"

    def get_tables(self):
        """Return an array of table instances containing data."""
        if self.tables is None:
            view_name = type(self).__name__
            raise ImproperlyConfigured(f"No tables were specified. Define {view_name}.tables")
        data = self.get_tables_data()

        if data is None:
            return self.tables

        if len(data) != len(self.tables):
            view_name = type(self).__name__
            raise ImproperlyConfigured(f"len({view_name}.tables_data) != len({view_name}.tables)")
        return list(Table(data[i]) for i, Table in enumerate(self.tables))

    def get_tables_data(self):
        """Return an array of table_data that should be used to populate each table."""
        return self.tables_data

    def get_context_data(self, **kwargs: Any) -> dict[str, Any]:
        context = super().get_context_data(**kwargs)
        tables = self.get_tables()

        # apply prefixes and execute requestConfig for each table
        table_counter = count()
        for table in tables:
            table.prefix = table.prefix or self.table_prefix.format(next(table_counter))

            RequestConfig(self.request, paginate=self.get_table_pagination(table)).configure(table)

            context[self.get_context_table_name(table)] = list(tables)

        return context
