from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class MicroServiceManager(Entity):
    @staticmethod
    def add_microservice_work_item(context, payload, minutes, properties):
        """
        :param office365.sharepoint.client_context.ClientContext context:
        :param str or byte payload:
        :param int minutes:
        :param MicroServiceWorkItemProperties properties:
        """
        return_type = ClientResult(context)
        payload = {"payLoad": payload, "minutes": minutes, "properties": properties}
        manager = MicroServiceManager(context)
        qry = ServiceOperationQuery(
            manager, "AddMicroserviceWorkItem", None, payload, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "SP.MicroService.MicroServiceManager"
