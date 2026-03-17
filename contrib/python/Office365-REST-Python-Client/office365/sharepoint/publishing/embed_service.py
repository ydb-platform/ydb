from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.publishing.embed_data_v1 import EmbedDataV1


class EmbedService(Entity):
    """The class was used to get embed meta data for the page."""

    def embed_data(self, url, version=1):
        """The method was used to get embed meta data for the page.

        :param str url: The url of the page.
        :param int version: Version of the method.
        """
        return_type = EmbedDataV1(self.context)
        payload = {"url": url, "version": version}
        qry = ServiceOperationQuery(self, "EmbedData", None, payload, None, return_type)
        self.context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "SP.Publishing.EmbedService"
