from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class PersonalWeb(Entity):
    """Microsoft.SharePoint.Client.Sharing.PersonalWeb namespace represents methods that apply to a Web site for
    individual users. Methods act on the users default document library."""

    @staticmethod
    def fix_permission_inheritance(context):
        """
        This method fixes the permission inheritance for the default document library of the personal web when
        breakRoleInheritance didn't happen correctly during the default document library creation.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint client context
        """
        binding_type = PersonalWeb(context)
        qry = ServiceOperationQuery(
            binding_type, "FixPermissionInheritance", None, None, None, None, True
        )
        context.add_query(qry)
        return binding_type

    @property
    def entity_type_name(self):
        return "SP.Sharing.PersonalWeb"
