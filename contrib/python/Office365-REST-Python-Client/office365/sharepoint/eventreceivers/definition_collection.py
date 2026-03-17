from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.eventreceivers.definition import EventReceiverDefinition


class EventReceiverDefinitionCollection(EntityCollection[EventReceiverDefinition]):
    """
    Represents a collection of SP.EventReceiverDefinition objects that are used to enumerate the list of
    registered event receivers for Windows SharePoint Services objects that can have events.
    """

    def __init__(self, context, resource_path=None, parent=None):
        """Represents a collection of SP.EventReceiverDefinition objects that are used to enumerate the list of
        registered event receivers for Windows SharePoint Services objects that can have events.
        """
        super(EventReceiverDefinitionCollection, self).__init__(
            context, EventReceiverDefinition, resource_path, parent
        )

    def get_by_id(self, event_receiver_id):
        """Returns the event receiver with the specified identifier.

        :param str event_receiver_id:  The identifier of the event receiver.
        """
        return EventReceiverDefinition(
            self.context,
            ServiceOperationPath("GetById", [event_receiver_id], self.resource_path),
        )
