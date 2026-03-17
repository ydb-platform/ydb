from office365.runtime.client_value import ClientValue


class KeyCredential(ClientValue):
    """
    Contains a key credential associated with an application .
    The keyCredentials property of the application entity is a collection of keyCredential.
    """

    def __init__(
        self,
        custom_key_identifier=None,
        display_name=None,
        end_datetime=None,
        key=None,
        key_id=None,
        start_datetime=None,
        key_type=None,
        usage=None,
    ):
        """
        :param str custom_key_identifier: A 40-character binary type that can be used to identify the credential.
           Optional. When not provided in the payload, defaults to the thumbprint of the certificate.
        :param str display_name: Friendly name for the key. Optional.
        :param datetime.datetime or str end_datetime: The date and time at which the credential expires.
            The DateTimeOffset type represents date and time information using ISO 8601 format and is always in UTC time
            For example, midnight UTC on Jan 1, 2014 is 2014-01-01T00:00:00Z.
        :param bytes or str key: The certificate's raw data in byte array converted to Base64 string.
            Returned only on $select for a single object, that is,
            GET applications/{applicationId}?$select=keyCredentials or
            GET servicePrincipals/{servicePrincipalId}?$select=keyCredentials; otherwise, it is always null.
        :param str key_id: The unique identifier (GUID) for the key.
        :param datetime.datetime or start_datetime: The date and time at which the credential becomes valid.The Timestamp
            type represents date and time information using ISO 8601 format and is always in UTC time.
            For example, midnight UTC on Jan 1, 2014 is 2014-01-01T00:00:00Z.
        :param str key_type: The type of key credential; for example, Symmetric, AsymmetricX509Cert.
        :param str usage: A string that describes the purpose for which the key can be used; for example, Verify.
        """
        self.customKeyIdentifier = custom_key_identifier
        self.displayName = display_name
        self.endDateTime = end_datetime
        self.key = key
        self.keyId = key_id
        self.startDateTime = start_datetime
        self.type = key_type
        self.usage = usage

    def __str__(self):
        return self.displayName or self.entity_type_name
