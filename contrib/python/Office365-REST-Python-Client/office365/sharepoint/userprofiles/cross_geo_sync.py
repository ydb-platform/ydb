from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.entity import Entity
from office365.sharepoint.userprofiles.cross_geo_sync_user_data_batch import (
    CrossGeoSyncUserDataBatch,
)


class CrossGeoSync(Entity):
    """"""

    @staticmethod
    def read_full_changes_batch(context, targetInstanceId, lastRecordId, batchSize):
        # type: (ClientContext, str, str, str) -> ClientResult[CrossGeoSyncUserDataBatch]
        """ """
        payload = {
            "targetInstanceId": targetInstanceId,
            "lastRecordId": lastRecordId,
            "batchSize": batchSize,
        }
        return_type = ClientResult(context, CrossGeoSyncUserDataBatch())
        qry = ServiceOperationQuery(
            CrossGeoSync(context),
            "ReadFullChangesBatch",
            None,
            payload,
            None,
            return_type,
            is_static=True,
        )
        context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "SP.UserProfiles.CrossGeoSync"
