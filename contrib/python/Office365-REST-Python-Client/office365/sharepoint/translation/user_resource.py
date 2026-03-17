from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.translation.resource_entry import SPResourceEntry


class UserResource(Entity):
    """An object representing user-defined localizable resources."""

    def get_value_for_ui_culture(self, culture_name):
        """
        Returns the value of the resource for the requested culture. This falls back to the value specified for
        the web's UI culture.

        :param str culture_name: The culture for which value is requested.
        """
        return_type = ClientResult(self.context, str())
        payload = {"cultureName": culture_name}
        qry = ServiceOperationQuery(
            self, "GetValueForUICulture", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_resource_entries(self):
        """ """
        return_type = ClientResult(self.context, ClientValueCollection(SPResourceEntry))
        qry = ServiceOperationQuery(
            self, "GetResourceEntries", [], None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def set_value_for_ui_culture(self, culture_name, value):
        """
        Sets the value of the resource for the requested culture. This method throws an exception if the culture
        is not enabled for the web.

        :param str culture_name: The culture for which value is requested.
        :param str value: The value of the resource in the requested culture.
        """
        return_type = ClientResult(self.context, str())
        payload = {"cultureName": culture_name, "value": value}
        qry = ServiceOperationQuery(
            self, "SetValueForUICulture", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type
