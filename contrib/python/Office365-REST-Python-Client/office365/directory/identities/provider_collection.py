from office365.directory.identities.provider import IdentityProvider
from office365.entity_collection import EntityCollection
from office365.runtime.client_result import ClientResult
from office365.runtime.queries.function import FunctionQuery
from office365.runtime.types.collections import StringCollection


class IdentityProviderCollection(EntityCollection):
    def __init__(self, context, resource_path=None):
        super(IdentityProviderCollection, self).__init__(
            context, IdentityProvider, resource_path
        )

    def available_provider_types(self):
        """
        Get all identity providers supported in a directory.

        The identityProvider API is deprecated and will stop returning data on March 2023.
        Please use the new identityProviderBase API.
        """
        return_type = ClientResult(self.context, StringCollection())
        qry = FunctionQuery(self, "availableProviderTypes", None, return_type)
        self.context.add_query(qry)
        return return_type
