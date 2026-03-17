from office365.runtime.client_value import ClientValue


class InternetMessageHeader(ClientValue):
    """
    A key-value pair that represents an Internet message header, as defined by RFC5322, that provides details of the
    network path taken by a message from the sender to the recipient.
    """

    def __init__(self, name=None, value=None):
        """
        :param str name: Represents the key in a key-value pair.
        :param str value:The value in a key-value pair.
        """
        self.name = name
        self.value = value
