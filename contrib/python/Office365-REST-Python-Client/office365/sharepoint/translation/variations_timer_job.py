from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class VariationsTranslationTimerJob(Entity):
    """
    The VariationsTranslationTimerJob type provides methods to drive translation for list items in a variation label.
    """

    @staticmethod
    def export_items(context, list_url, item_ids, addresses_to_email):
        """
        The protocol client calls this method to export a specific set of list items.

        :type context: office365.sharepoint.client_context.ClientContext
        :param str list_url: The server-relative URL for the list containing the list items
        :param list[int] item_ids: An array containing the identifiers of the list items to be exported.
        :param list[str] addresses_to_email: An array of the e-mail addresses that will be notified when the operation
             completes.
        """
        payload = {
            "list": list_url,
            "itemIds": ClientValueCollection(int, item_ids),
            "addressesToEmail": ClientValueCollection(str, addresses_to_email),
        }
        binding_type = VariationsTranslationTimerJob(context)
        qry = ServiceOperationQuery(
            binding_type, "ExportItems", None, payload, is_static=True
        )
        context.add_query(qry)
        return binding_type

    @property
    def entity_type_name(self):
        return "SP.Translation.VariationsTranslationTimerJob"
