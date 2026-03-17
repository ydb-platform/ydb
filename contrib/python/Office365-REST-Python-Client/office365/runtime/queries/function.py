from office365.runtime.client_object import ClientObject
from office365.runtime.client_value import ClientValue
from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.queries.client_query import ClientQuery, T


class FunctionQuery(ClientQuery[T]):
    """Function query"""

    def __init__(
        self, binding_type, method_name=None, method_params=None, return_type=None
    ):
        # type: (ClientObject, str, list|dict|ClientValue, T) -> None
        """Function query"""
        super(FunctionQuery, self).__init__(
            binding_type.context, binding_type, None, None, return_type
        )
        self._method_name = method_name
        self._method_params = method_params

    @property
    def path(self):
        return ServiceOperationPath(
            self._method_name, self._method_params, self.binding_type.resource_path
        )

    @property
    def url(self):
        orig_url = super(FunctionQuery, self).url
        return "/".join([orig_url, self.path.segment])

    @property
    def name(self):
        return self._method_name
