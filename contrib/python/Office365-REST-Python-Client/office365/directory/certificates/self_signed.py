from office365.runtime.client_value import ClientValue


class SelfSignedCertificate(ClientValue):
    """Contains the public part of a signing certificate."""

    def __init__(
        self,
        custom_key_identifier=None,
        display_name=None,
        end_datetime=None,
        key=None,
        key_id=None,
        start_datetime=None,
        thumbprint=None,
        type_=None,
        usage=None,
    ):
        """
        :param custom_key_identifier: 	Custom key identifier.
        :param str display_name: The friendly name for the key.
        :param end_datetime: The date and time at which the credential expires. The timestamp type represents date
             and time information using ISO 8601 format and is always in UTC time
        :param key: The value for the key credential. Should be a Base-64 encoded value.
        :param key_id: 	The unique identifier (GUID) for the key.
        :param start_datetime: The date and time at which the credential becomes valid. The timestamp type represents
             date and time information using ISO 8601 format and is always in UTC time
        :param thumbprint: 	The thumbprint value for the key.
        :param type_: 	The type of key credential. AsymmetricX509Cert.
        :param usage: A string that describes the purpose for which the key can be used. The possible value is Verify.
        """
        self.customKeyIdentifier = custom_key_identifier
        self.displayName = display_name
        self.endDateTime = end_datetime
        self.key = key
        self.keyId = key_id
        self.startDateTime = start_datetime
        self.thumbprint = thumbprint
        self.type = type_
        self.usage = usage
