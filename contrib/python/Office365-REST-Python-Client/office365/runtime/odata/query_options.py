class QueryOptions(object):
    def __init__(
        self,
        select=None,
        expand=None,
        filter_expr=None,
        order_by=None,
        top=None,
        skip=None,
        custom=None,
    ):
        """
        A query option is a set of query string parameters applied to a resource that can help control the amount
        of data being returned for the resource in the URL

        :param list[str] select: The $select system query option allows the clients to requests a limited set of
        properties for each entity or complex type.
        :param list[str] expand: The $expand system query option specifies the related resources to be included in
        line with retrieved resources.
        :param str filter_expr: The $filter system query option allows clients to filter a collection of resources
        that are addressed by a request URL.
        :param str order_by: The $orderby system query option allows clients to request resources in either ascending
        order using asc or descending order using desc
        :param int top: The $top system query option requests the number of items in the queried collection to
        be included in the result.
        :param int skip: The $skip query option requests the number of items in the queried collection that
        are to be skipped and not included in the result.
        :param dict custom: A custom query options
        """
        if expand is None:
            expand = []
        if select is None:
            select = []
        self.select = select
        self.expand = expand
        self.filter = filter_expr
        self.orderBy = order_by
        self.skip = skip
        self.top = top
        if custom is None:
            custom = {}
        self.custom = custom

    @staticmethod
    def build(client_object, properties_to_include=None):
        """
        Builds query options

        :param office365.runtime.client_object.ClientObject client_object: Client object
        :param list[str] or None properties_to_include: The list of properties to include
        """
        query_options = client_object.query_options
        if properties_to_include is None:
            return query_options

        for name in properties_to_include:
            from office365.runtime.client_object import ClientObject
            from office365.runtime.client_object_collection import (
                ClientObjectCollection,
            )

            if name in query_options.select:
                continue

            if isinstance(client_object, ClientObjectCollection):
                prop = client_object.create_typed_object().get_property(name)
            else:
                prop = client_object.get_property(name)

            if name == "Properties" or isinstance(prop, ClientObject):
                query_options.expand.append(name)
            query_options.select.append(name)
        return query_options

    def __repr__(self):
        return self.to_url()

    def __str__(self):
        return self.to_url()

    @property
    def is_empty(self):
        result = {k: v for (k, v) in self}
        return not result

    def reset(self):
        self.select = []
        self.expand = []
        self.filter = None
        self.orderBy = None
        self.skip = None
        self.top = None
        self.custom = {}

    def to_url(self):
        """Convert query options to url"""
        return "&".join(["$%s=%s" % (key, value) for (key, value) in self])

    def __iter__(self):
        for k, v in self.__dict__.items():
            if v:
                if k == "select" or k == "expand":
                    yield k, ",".join(v)
                elif k == "custom":
                    for c_k, c_v in self.custom.items():
                        yield c_k, c_v
                else:
                    yield k, v
