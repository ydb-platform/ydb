from office365.runtime.client_value import ClientValue


class CertificateAuthority(ClientValue):
    """Represents a certificate authority."""

    def __init__(
        self,
        certificate=None,
        certificate_revocation_list_url=None,
        is_root_authority=None,
        issuer=None,
        issuer_ski=None,
    ):
        """
        :param str certificate: The base64 encoded string representing the public certificate.
        :param str certificate_revocation_list_url: The URL of the certificate revocation list.
        :param str is_root_authority: Required. true if the trusted certificate is a root authority, false if the
            trusted certificate is an intermediate authority.
        :param str issuer: The issuer of the certificate, calculated from the certificate value
        :param str issuer_ski: The subject key identifier of the certificate, calculated from the certificate value.
        """
        super(CertificateAuthority, self).__init__()
        self.certificate = certificate
        self.certificateRevocationListUrl = certificate_revocation_list_url
        self.isRootAuthority = is_root_authority
        self.issuer = issuer
        self.issuerSki = issuer_ski
