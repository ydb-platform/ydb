from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class AppPrincipalCredential(Entity):
    """Represents a credential belonging to an app principal."""

    @staticmethod
    def create_from_symmetric_key(context, symmetric_key, not_before, not_after=None):
        """
        Create an instance of SP.AppPrincipalCredential that wraps a symmetric key.

        :type context: office365.sharepoint.client_context.ClientContext
        :param str symmetric_key: The symmetric key of the app principal credential.
        :param datetime.datetime not_before: The earliest time when the key is valid.
        :param datetime.datetime not_after: The time when the key expires.
        """
        return_type = AppPrincipalCredential(context)
        payload = {
            "symmetricKey": symmetric_key,
            "notBefore": not_before.isoformat(),
            "notAfter": not_after.isoformat(),
        }
        qry = ServiceOperationQuery(
            return_type, "CreateFromSymmetricKey", None, payload, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def create_from_key_group(context, key_group_identifier):
        """
        Create an instance of SP.AppPrincipalCredential that wraps a key group identifier.

        :type context: office365.sharepoint.client_context.ClientContext
        :param str key_group_identifier:  The key group identifier.
        """
        return_type = AppPrincipalCredential(context)
        payload = {"keyGroupIdentifier": key_group_identifier}
        qry = ServiceOperationQuery(
            return_type, "CreateFromKeyGroup", None, payload, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type
