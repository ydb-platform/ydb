from office365.runtime.client_value import ClientValue


class Pkcs12CertificateInformation(ClientValue):
    """Represents the public information of a Pkcs12 certificate."""

    def __init__(
        self, thumbprint=None, is_active=None, not_after=None, not_before=None
    ):
        """
        :param str thumbprint: The certificate thumbprint
        :param long not_after: The certificate's expiry. This value is a NumericDate as defined in RFC 7519
           (A JSON numeric value representing the number of seconds from 1970-01-01T00:00:00Z UTC until the specified
           UTC date/time, ignoring leap seconds.)
        :param long not_before: The certificate's issue time (not before).
            This value is a NumericDate as defined in RFC 7519 (A JSON numeric value representing the number
            of seconds from 1970-01-01T00:00:00Z UTC until the specified UTC date/time, ignoring leap seconds.)
        """
        self.thumbprint = thumbprint
        self.isActive = is_active
        self.notAfter = not_after
        self.notBefore = not_before
