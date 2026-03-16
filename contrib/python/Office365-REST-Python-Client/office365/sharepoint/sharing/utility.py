from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.sharing.user_directory_info import UserDirectoryInfo


class SharingUtility(Entity):
    """Provides sharing related utility methods."""

    def __init__(self, context):
        super(SharingUtility, self).__init__(context, ResourcePath("SharingUtility"))

    @staticmethod
    def get_user_directory_info_by_email(context, email):
        """
        Get user information by the user’s email address in directory.

        :param str email: The email address of a user.
        :param office365.sharepoint.client_context.ClientContext context: SharePoint client context
        """
        return_type = ClientResult(context, UserDirectoryInfo())
        payload = {"email": email}
        utility = SharingUtility(context)
        qry = ServiceOperationQuery(
            utility, "GetUserDirectoryInfoByEmail", None, payload, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def validate_same_user_emails(context, primary_email, other_email, principal_name):
        """
        Validate the primary email/principal name and other email are of the same user.

        :param str primary_email: User’s primary email address
        :param str other_email: Another email address.
        :param str principal_name: User’s principal name.
        :param office365.sharepoint.client_context.ClientContext context: SharePoint client context
        """
        utility = SharingUtility(context)
        payload = {
            "primaryEmail": primary_email,
            "otherEmail": other_email,
            "principalName": principal_name,
        }
        result = ClientResult(context)
        qry = ServiceOperationQuery(
            utility, "ValidateSameUserEmails", None, payload, None, result
        )
        qry.static = True
        context.add_query(qry)
        return result
