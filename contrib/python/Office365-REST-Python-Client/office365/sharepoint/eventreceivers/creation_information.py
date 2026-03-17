from office365.runtime.client_value import ClientValue


class EventReceiverDefinitionCreationInformation(ClientValue):
    """Represents the properties that can be set when creating a client-side event receiver definition."""

    def __init__(
        self, receiver_assembly=None, receiver_class=None, sequence_number=None
    ):
        """
        :param str receiver_assembly: Specifies the strong name of the assembly that is used for receiving events.
        :param str receiver_class: Specifies a string that represents the class that is used for receiving events.
        :param str sequence_number: Specifies an integer that represents the relative sequence of the event.
        """
        self.ReceiverAssembly = receiver_assembly
        self.ReceiverClass = receiver_class
        self.SequenceNumber = sequence_number
