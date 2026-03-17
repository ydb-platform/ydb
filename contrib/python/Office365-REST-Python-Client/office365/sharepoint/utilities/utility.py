from typing import TYPE_CHECKING

from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.runtime.types.collections import StringCollection
from office365.sharepoint.entity import Entity
from office365.sharepoint.files.file import File
from office365.sharepoint.types.resource_path import ResourcePath as SPResPath
from office365.sharepoint.utilities.principal_info import PrincipalInfo

if TYPE_CHECKING:
    from office365.sharepoint.client_context import ClientContext


class Utility(Entity):
    """
    Provides tools for converting date and time formats, for obtaining information from user names,
    for modifying access to sites, and for various other tasks in managing deployment.
    """

    def __init__(self, context):
        super(Utility, self).__init__(context, ResourcePath("SP.Utilities.Utility"))

    @staticmethod
    def create_email_body_for_invitation(context, page_address):
        """Creates the contents of the e-mail message used to invite users to a document or resource in a site
        :type context: office365.sharepoint.client_context.ClientContext
        :param str page_address: Specifies part of the display name for the document or resource.
        """
        return_type = ClientResult(context, str())
        utility = Utility(context)
        payload = {
            "pageAddress": str(
                SPResPath.create_absolute(context.base_url, page_address)
            )
        }
        qry = ServiceOperationQuery(
            utility,
            "CreateEmailBodyForInvitation",
            None,
            payload,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def get_current_user_email_addresses(context):
        """
        Returns the email addresses of the current user. If more than one email address exists for the current user,
        returns a list of email addresses separated by semicolons.
        :type context: office365.sharepoint.client_context.ClientContext
        """
        result = ClientResult(context)
        utility = Utility(context)
        qry = ServiceOperationQuery(
            utility, "GetCurrentUserEmailAddresses", None, None, None, result
        )
        qry.static = True
        context.add_query(qry)
        return result

    @staticmethod
    def get_user_permission_levels(context):
        """
        Retrieves a collection of permission levels of the current user on the web.
        :type context: office365.sharepoint.client_context.ClientContext
        """
        return_type = ClientResult(context, StringCollection())
        utility = Utility(context)
        qry = ServiceOperationQuery(
            utility, "GetUserPermissionLevels", None, None, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def search_principals_using_context_web(
        context, s_input, sources, scopes, max_count, group_name=None
    ):
        """
        Returns the collection of principals that partially or uniquely matches the specified search criteria in the
        context of the current Web site
        :param str s_input: Specifies the value to be used when searching for a principal.
        :param str sources: Specifies the source to be used when searching for a principal.
        :param int scopes: Specifies the type to be used when searching for a principal.
        :param int max_count: Specifies the maximum number of principals to be returned.
        :param str or None group_name:  Specifies the name of a site collection group in the site collection that
            contains the current Web site. The collection of users in this site collection group is used when searching
            for a principal.
        :type context: office365.sharepoint.client_context.ClientContext
        """
        return_type = ClientResult(context, StringCollection())
        utility = Utility(context)
        params = {
            "input": s_input,
            "sources": sources,
            "scopes": scopes,
            "maxCount": max_count,
            "groupName": group_name,
        }
        qry = ServiceOperationQuery(
            utility, "SearchPrincipalsUsingContextWeb", params, None, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def create_wiki_page_in_context_web(context, parameters, return_type=None):
        """
        Creates a wiki page.
        :type context: office365.sharepoint.client_context.ClientContext
        :type parameters: office365.sharepoint.pages.wiki_page_creation_information.WikiPageCreationInformation
        :type return_type: File
        """
        if return_type is None:
            return_type = File(context)
        utility = Utility(context)
        payload = {"parameters": parameters}
        qry = ServiceOperationQuery(
            utility, "CreateWikiPageInContextWeb", None, payload, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def send_email(context, properties):
        """
        This method is a static method.
        :type context: office365.sharepoint.client_context.ClientContext
        :type properties: office365.sharepoint.utilities.email_properties.EmailProperties
        """
        utility = Utility(context)
        payload = {"properties": properties}
        qry = ServiceOperationQuery(
            utility, "SendEmail", None, payload, None, None, True
        )
        context.add_query(qry)
        return utility

    @staticmethod
    def unmark_discussion_as_featured(context, list_id, topic_ids):
        """
        This method is a static method.
        :type context: office365.sharepoint.client_context.ClientContext
        :type list_id: str
        :type topic_ids: str
        """
        utility = Utility(context)
        payload = {"listID": list_id, "topicIDs": topic_ids}
        qry = ServiceOperationQuery(
            utility, "UnmarkDiscussionAsFeatured", None, payload, None, None, True
        )
        context.add_query(qry)
        return utility

    @staticmethod
    def expand_groups_to_principals(context, inputs, max_count=None, return_type=None):
        """
        Expands groups to a collection of principals.
        :type context: office365.sharepoint.client_context.ClientContext
        :param list[str] inputs: A collection of groups to be expanded.
        :param int max_count: Specifies the maximum number of principals to be returned.
        :type return_type: ClientResult
        """
        binding_type = Utility(context)
        payload = {"inputs": inputs, "maxCount": max_count}
        if return_type is None:
            return_type = ClientResult(context, ClientValueCollection(PrincipalInfo))
        qry = ServiceOperationQuery(
            binding_type, "ExpandGroupsToPrincipals", None, payload, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def log_custom_app_error(context, error):
        # type: (ClientContext, str) -> ClientResult[int]
        """
        Logs an error from a SharePoint Add-in. The return value indicates the success or failure of this operation.
        These errors are of interest to administrators who monitor such apps (2).

        :type context: office365.sharepoint.client_context.ClientContext
        :param str error: Error string to log
        """
        utility = Utility(context)
        payload = {
            "error": error,
        }
        return_type = ClientResult(context)
        qry = ServiceOperationQuery(
            utility, "LogCustomAppError", None, payload, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def resolve_principal_in_current_context(
        context,
        string_input,
        scopes=None,
        sources=None,
        input_is_email_only=None,
        add_to_user_info_list=None,
        match_user_info_list=None,
    ):
        """
        Returns information about a principal that matches the specified search criteria in the context of the current
        Web site.

        :type context: office365.sharepoint.client_context.ClientContext
        :param str string_input: Specifies the value to be used when searching for a principal.
        :param int scopes: Specifies the type to be used when searching for a principal.
        :param str sources: Specifies the source to be used when searching for a principal.
        :param bool input_is_email_only: Specifies whether only the e-mail address is used when searching for
            a principal
        :param bool add_to_user_info_list: Specifies whether to add the principal to the user information list.
        :param bool match_user_info_list: Specifies whether to return the principal found in the user information list.
        """
        utility = Utility(context)
        payload = {
            "input": string_input,
            "scopes": scopes,
            "sources": sources,
            "inputIsEmailOnly": input_is_email_only,
            "addToUserInfoList": add_to_user_info_list,
            "matchUserInfoList": match_user_info_list,
        }
        return_type = ClientResult(context)
        qry = ServiceOperationQuery(
            utility,
            "ResolvePrincipalInCurrentContext",
            None,
            payload,
            None,
            return_type,
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "SP.Utilities.Utility"
