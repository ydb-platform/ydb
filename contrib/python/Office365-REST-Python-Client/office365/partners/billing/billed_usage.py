from office365.entity import Entity
from office365.partners.billing.operation import Operation
from office365.runtime.queries.service_operation import ServiceOperationQuery


class BilledUsage(Entity):
    """Represents details for billed Azure usage data."""

    def export(self, invoice_id, attribute_set=None):
        # type: (str, str) -> Operation
        """Export the billed Azure usage data.

        :param invoice_id: 	The invoice ID for which the partner requested to export data. Required.
        :param attribute_set: Attributes that should be exported. Possible values are: full, basic, unknownFutureValue.
           The default value is full. Choose full for a complete response or basic for a subset of attributes.
        """
        payload = {"invoiceId": invoice_id, "attributeSet": attribute_set}
        return_type = Operation(self.context)
        qry = ServiceOperationQuery(self, "export", None, payload, None, return_type)
        self.context.add_query(qry)
        return return_type
