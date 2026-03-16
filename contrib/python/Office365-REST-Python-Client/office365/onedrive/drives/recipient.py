from office365.runtime.client_value import ClientValue


class DriveRecipient(ClientValue):
    """
    The DriveRecipient resource represents a person, group, or other recipient to
    share with using the invite action.
    """

    def __init__(self, alias=None, email=None, object_id=None):
        """
        :param str alias: The alias of the domain object, for cases where an email address is unavailable
            (e.g. security groups).
        :param str email: The email address for the recipient, if the recipient has an associated email address.
        :param str object_id: The unique identifier for the recipient in the directory.
        """
        super(DriveRecipient, self).__init__()
        self.alias = alias
        self.email = email
        self.objectId = object_id

    @staticmethod
    def from_email(value):
        """
        Creates Drive recipient from email address
        :type value: str
        """
        return DriveRecipient(email=value)
