from office365.directory.authentication.configuration_base import (
    ApiAuthenticationConfigurationBase,
)
from office365.directory.certificates.pkcs12_information import (
    Pkcs12CertificateInformation,
)
from office365.runtime.client_value_collection import ClientValueCollection


class ClientCertificateAuthentication(ApiAuthenticationConfigurationBase):
    """
    A type derived from apiAuthenticationConfigurationBase that is used to represent
    a Pkcs12-based client certificate authentication.
    This is used to retrieve the public properties of uploaded certificates.
    """

    def __init__(self, certificates=None):
        """
        :param list[Pkcs12CertificateInformation] certificates:
        """
        super(ClientCertificateAuthentication, self).__init__()
        self.certificateList = ClientValueCollection(
            Pkcs12CertificateInformation, certificates
        )
