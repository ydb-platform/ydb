from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.permissions.roles.definitions.definition import RoleDefinition
from office365.sharepoint.portal.userprofiles.sharedwithme.view_item_removal_result import (
    SharedWithMeViewItemRemovalResult,
)
from office365.sharepoint.sharing.user_role_assignment import UserRoleAssignment
from office365.sharepoint.sharing.user_sharing_result import UserSharingResult


class DocumentSharingManager(Entity):
    """Specifies document sharing related methods."""

    @staticmethod
    def get_role_definition(context, role):
        """This method returns a role definition in the current web that is associated with a given Role
        (section 3.2.5.188) value.

        :type context: office365.sharepoint.client_context.ClientContext
        :param int role: A Role value for which to obtain the associated role definition object.
        """
        return_type = RoleDefinition(context)
        context.web.role_definitions.add_child(return_type)
        binding_type = DocumentSharingManager(context)
        qry = ServiceOperationQuery(
            binding_type, "GetRoleDefinition", [role], None, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def remove_items_from_shared_with_me_view(context, item_urls):
        """
        Removes an item so that it no longer shows in the current user's 'Shared With Me' view. However, this
            does not remove the user's actual permissions to the item. Up to 200 items can be provided in a single call.
            Returns a list of results indicating whether the items were successfully removed. The length of this array
            will match the length of the itemUrls array that was provided.

        :type context: office365.sharepoint.client_context.ClientContext
        :param list[str] item_urls: A list of absolute URLs of the items to be removed from the view.
            These items might belong to any site or site collection in the tenant.
        """
        return_type = ClientResult(
            context, ClientValueCollection(SharedWithMeViewItemRemovalResult)
        )
        binding_type = DocumentSharingManager(context)
        qry = ServiceOperationQuery(
            binding_type,
            "RemoveItemsFromSharedWithMeView",
            [item_urls],
            None,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def update_document_sharing_info(
        context,
        resource_address,
        user_role_assignments,
        validate_existing_permissions=None,
        additive_mode=None,
        send_server_managed_notification=None,
        custom_message=None,
        include_anonymous_links_in_notification=None,
        propagate_acl=None,
        return_type=None,
    ):
        """
        This method allows a caller with the 'ManagePermission' permission to update sharing information about a
        document to enable document sharing with a set of users. It returns an array of
        UserSharingResult (section 3.2.5.190) elements where each element contains the sharing status for each user.

        :type context: office365.sharepoint.client_context.ClientContext
        :param str resource_address: A URL that points to a securable object, which can be a document, folder or the
            root folder of a document library.
        :param list[UserRoleAssignment] user_role_assignments:An array of recipients and assigned roles on the securable
            object pointed to by the resourceAddress parameter.
        :param bool validate_existing_permissions: A Boolean flag indicating how to honor a requested permission
            for a user. If this value is "true", the protocol server will not grant the requested permission if a user
            already has sufficient permissions, and if this value is "false", the protocol server will grant the
            requested permission whether or not a user already has the same or more permissions.
            This parameter is applicable only when the parameter additiveMode is set to true.
        :param bool additive_mode: A Boolean flag indicating whether the permission setting uses the additive or strict
            mode. If this value is "true", the permission setting uses the additive mode, which means that the
            specified permission will be added to the user's current list of permissions if it is not there already,
            and if this value is "false", the permission setting uses the strict mode, which means that the specified
            permission will replace the user's current permissions.
        :param bool send_server_managed_notification: A Boolean flag to indicate whether or not to generate an email
            notification to each recipient in the "userRoleAssignments" array after the document update is completed
            successfully. If this value is "true", the protocol server will send an email notification if an email
            server is configured, and if the value is "false", no email notification will be sent.
        :param str custom_message: A custom message to be included in the email notification.
        :param bool include_anonymous_links_in_notification: A Boolean flag that indicates whether or not to include
            anonymous access links in the email notification to each recipient in the userRoleAssignments array after
            the document update is completed successfully. If the value is "true", the protocol server will include
            an anonymous access link in the email notification, and if the value is "false", no link will be included.
        :param bool propagate_acl: A flag to determine if permissions SHOULD be pushed to items with unique permission.
        :param ClientResult return_type:
        """
        if return_type is None:
            return_type = ClientResult(
                context, ClientValueCollection(UserSharingResult)
            )
        payload = {
            "resourceAddress": resource_address,
            "userRoleAssignments": user_role_assignments,
            "validateExistingPermissions": validate_existing_permissions,
            "additiveMode": additive_mode,
            "sendServerManagedNotification": send_server_managed_notification,
            "customMessage": custom_message,
            "includeAnonymousLinksInNotification": include_anonymous_links_in_notification,
            "propagateAcl": propagate_acl,
        }
        binding_type = DocumentSharingManager(context)
        qry = ServiceOperationQuery(
            binding_type,
            "UpdateDocumentSharingInfo",
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
        return "SP.Sharing.DocumentSharingManager"
