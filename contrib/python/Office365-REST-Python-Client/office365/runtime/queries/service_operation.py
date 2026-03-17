from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.queries.client_query import ClientQuery, T


class ServiceOperationQuery(ClientQuery[T]):
    """ "Service operation query"""

    def __init__(
        self,
        binding_type,
        method_name=None,
        method_params=None,
        parameters_type=None,
        parameters_name=None,
        return_type=None,
        is_static=False,
    ):
        super(ServiceOperationQuery, self).__init__(
            binding_type.context,
            binding_type,
            parameters_type,
            parameters_name,
            return_type,
        )
        self._method_name = method_name
        self._method_params = method_params
        self.static = is_static

    @property
    def path(self):
        if self.static:
            static_name = ".".join(
                [self.binding_type.entity_type_name, self._method_name]
            )
            return ServiceOperationPath(static_name, self._method_params)
        else:
            return ServiceOperationPath(
                self._method_name, self._method_params, self.binding_type.resource_path
            )

    @property
    def url(self):
        orig_url = super(ServiceOperationQuery, self).url
        if self.static:
            return "".join([self.context.service_root_url(), str(self.path)])
        else:
            return "/".join([orig_url, self.path.segment])

    @property
    def name(self):
        return self._method_name
