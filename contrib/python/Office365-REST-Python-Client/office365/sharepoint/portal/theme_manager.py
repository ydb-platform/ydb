from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class ThemeManager(Entity):
    """SharePoint site theming REST interface to perform basic create, read, update, and delete (CRUD)
    operations on site themes."""

    @property
    def entity_type_name(self):
        return "SP.Utilities.ThemeManager"

    def add_tenant_theme(self, name, theme_json):
        """
        Adds a new theme to a tenant.

        :param str name:
        :param str theme_json:
        """
        return_type = ClientResult(self.context)
        payload = {
            "name": name,
            "themeJson": theme_json,
        }
        qry = ServiceOperationQuery(
            self, "AddTenantTheme", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def delete_tenant_theme(self, name):
        """
        Removes a theme.
        """
        payload = {
            "name": name,
        }
        qry = ServiceOperationQuery(self, "DeleteTenantTheme", None, payload)
        self.context.add_query(qry)
        return self
