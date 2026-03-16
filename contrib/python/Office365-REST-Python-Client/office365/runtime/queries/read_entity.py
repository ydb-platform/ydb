from office365.runtime.odata.query_options import QueryOptions
from office365.runtime.queries.client_query import ClientQuery, T


class ReadEntityQuery(ClientQuery[T]):
    def __init__(self, return_type, properties_to_include=None):
        # type: (T, list[str]) -> None
        """
        Read client object query
        """
        super(ReadEntityQuery, self).__init__(
            return_type.context, return_type, None, None, return_type
        )
        self._query_options = QueryOptions.build(return_type, properties_to_include)

    @property
    def query_options(self):
        return self._query_options

    @property
    def url(self):
        if not self.query_options.is_empty:
            delimiter = "?"
            from office365.runtime.client_value import ClientValue
            from office365.runtime.paths.service_operation import ServiceOperationPath

            if isinstance(self.path, ServiceOperationPath) and isinstance(
                self.path.parameters, ClientValue
            ):
                delimiter = "&"
            return self.binding_type.resource_url + delimiter + str(self.query_options)
        else:
            return self.binding_type.resource_url
