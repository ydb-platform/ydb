from django.core.paginator import EmptyPage, PageNotAnInteger


class RequestConfig:
    """
    A configurator that uses request data to setup a table.

    A single RequestConfig can be used for multiple tables in one view.

    Arguments:
        paginate (dict or bool): Indicates whether to paginate, and if so, what
            default values to use. If the value evaluates to `False`, pagination
            will be disabled. A `dict` can be used to specify default values for
            the call to `~.tables.Table.paginate` (e.g. to define a default
            `per_page` value).

            A special *silent* item can be used to enable automatic handling of
            pagination exceptions using the following logic:

             - If `~django.core.paginator.PageNotAnInteger` is raised, show the first page.
             - If `~django.core.paginator.EmptyPage` is raised, show the last page.

            For example, to use `~.LazyPaginator`::

                RequestConfig(paginate={"paginator_class": LazyPaginator}).configure(table)

    """

    def __init__(self, request, paginate=True):
        self.request = request
        self.paginate = paginate

    def configure(self, table):
        """
        Configure a table using information from the request.

        Arguments:
            table (`~.Table`): table to be configured
        """
        table.request = self.request

        order_by = self.request.GET.getlist(table.prefixed_order_by_field)
        if order_by:
            table.order_by = order_by
        if self.paginate:
            if hasattr(self.paginate, "items"):
                kwargs = dict(self.paginate)
            else:
                kwargs = {}
            # extract some options from the request
            for arg in ("page", "per_page"):
                name = getattr(table, f"prefixed_{arg}_field")
                try:
                    kwargs[arg] = int(self.request.GET[name])
                except (ValueError, KeyError):
                    pass

            silent = kwargs.pop("silent", True)
            if not silent:
                table.paginate(**kwargs)
            else:
                try:
                    table.paginate(**kwargs)
                except PageNotAnInteger:
                    table.page = table.paginator.page(1)
                except EmptyPage:
                    table.page = table.paginator.page(table.paginator.num_pages)

        return table
