from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.apps.license_collection import AppLicenseCollection
from office365.sharepoint.entity import Entity


class SPAppLicenseManager(Entity):
    def check_license(self, product_id):
        """
        :param str product_id:
        """
        return_type = ClientResult(self.context, AppLicenseCollection())
        payload = {"productId": product_id}
        qry = ServiceOperationQuery(
            self, "CheckLicense", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type
