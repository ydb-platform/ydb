from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.principal.type import PrincipalType
from office365.sharepoint.ui.applicationpages.peoplepicker.entity_information import (
    PickerEntityInformation,
)
from office365.sharepoint.ui.applicationpages.peoplepicker.entity_information_request import (
    PickerEntityInformationRequest,
)
from office365.sharepoint.ui.applicationpages.peoplepicker.query_parameters import (
    ClientPeoplePickerQueryParameters,
)


class ClientPeoplePickerWebServiceInterface(Entity):
    """Specifies an interface that can be used to query principals."""

    @staticmethod
    def get_search_results_by_hierarchy(
        context,
        provider_id=None,
        hierarchy_node_id=None,
        entity_types=None,
        context_url=None,
    ):
        """
        Specifies a JSON formatted CSOM String of principals found in the search grouped by hierarchy.

        :type context: office365.sharepoint.client_context.ClientContext
        :param str provider_id: The identifier of a claims provider.
        :param str hierarchy_node_id: The identifier of a node in the hierarchy. The search MUST be conducted under
            this node.
        :param str entity_types: The type of principals to search for.
        :param str context_url: The URL to use as context when searching for principals.
        """
        return_type = ClientResult(context, str())
        payload = {
            "providerID": provider_id,
            "hierarchyNodeID": hierarchy_node_id,
            "entityTypes": entity_types,
            "contextUrl": context_url,
        }
        svc = ClientPeoplePickerWebServiceInterface(context)
        qry = ServiceOperationQuery(
            svc, "GetSearchResultsByHierarchy", None, payload, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def client_people_picker_resolve_user(context, query_string):
        """
        Resolves the principals to a string of JSON representing users in people picker format.

        :param str query_string: Specifies the value to be used in the principal query.
        :param office365.sharepoint.client_context.ClientContext context: SharePoint client context

        """
        return_type = ClientResult(context, str())
        binding_type = ClientPeoplePickerWebServiceInterface(context)
        payload = {
            "queryParams": ClientPeoplePickerQueryParameters(query_string=query_string)
        }
        qry = ServiceOperationQuery(
            binding_type,
            "ClientPeoplePickerResolveUser",
            None,
            payload,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def client_people_picker_search_user(
        context, query_string, maximum_entity_suggestions=100
    ):
        """
        Returns for a string of JSON representing users in people picker format of the specified principals.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint client context
        :param str query_string: Specifies the value to be used in the principal query.
        :param int maximum_entity_suggestions: Specifies the maximum number of principals to be returned by the
        principal query.
        """
        return_type = ClientResult(context, str())
        binding_type = ClientPeoplePickerWebServiceInterface(context)
        params = ClientPeoplePickerQueryParameters(
            query_string=query_string,
            maximum_entity_suggestions=maximum_entity_suggestions,
        )
        payload = {"queryParams": params}
        qry = ServiceOperationQuery(
            binding_type,
            "ClientPeoplePickerSearchUser",
            None,
            payload,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_picker_entity_information(context, email_address):
        """
        Gets information of the specified principal.
        :param office365.sharepoint.client_context.ClientContext context: SharePoint client context
        :param str email_address: Specifies the principal for which information is being requested.

        """
        request = PickerEntityInformationRequest(
            email_address=email_address, principal_type=PrincipalType.All
        )
        return_type = PickerEntityInformation(context)
        binding_type = ClientPeoplePickerWebServiceInterface(context)
        payload = {"entityInformationRequest": request}
        qry = ServiceOperationQuery(
            binding_type,
            "GetPickerEntityInformation",
            None,
            payload,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "SP.UI.ApplicationPages.ClientPeoplePickerWebServiceInterface"


class PeoplePickerWebServiceInterface(Entity):
    @staticmethod
    def get_search_results(
        context,
        search_pattern,
        provider_id=None,
        hierarchy_node_id=None,
        entity_types=None,
    ):
        """
        Specifies a JSON formatted CSOM String of principals found in the search.

        :type context: office365.sharepoint.client_context.ClientContext
        :param str search_pattern: Specifies a pattern used to search for principals.
            The value is implementation-specific.
        :param str provider_id: The identifier of a claims provider.
        :param str hierarchy_node_id: The identifier of a node in the hierarchy. The search MUST be conducted under
            this node.
        :param str entity_types: The type of principals to search for.
        """
        return_type = ClientResult(context, str())
        payload = {
            "searchPattern": search_pattern,
            "providerID": provider_id,
            "hierarchyNodeID": hierarchy_node_id,
            "entityTypes": entity_types,
        }
        svc = PeoplePickerWebServiceInterface(context)
        qry = ServiceOperationQuery(
            svc, "GetSearchResults", None, payload, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "SP.UI.ApplicationPages.PeoplePickerWebServiceInterface"
