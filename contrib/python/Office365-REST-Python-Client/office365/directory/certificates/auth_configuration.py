from office365.directory.certificates.authority import CertificateAuthority
from office365.entity import Entity
from office365.runtime.client_value_collection import ClientValueCollection


class CertificateBasedAuthConfiguration(Entity):
    """
    Certificate-based authentication enables you to be authenticated by Azure Active Directory with a client certificate
    on a Windows, Android, or iOS device when connecting your Exchange Online account to:

       - Microsoft mobile applications such as Outlook and Word
       - Exchange ActiveSync (EAS) clients

    Configuring this feature eliminates the need to enter a username and password combination into certain mail and
    Microsoft Office applications on your mobile device.
    Certificate-based authentication configuration is provided through a collection of certificate authorities.
    The certificate authorities are used to establish a trusted certificate chain which enables clients to
    be authenticated by Azure Active Directory with a client certificate.
    """

    @property
    def certificate_authorities(self):
        """Collection of certificate authorities which creates a trusted certificate chain."""
        return self.properties.get(
            "certificateAuthorities", ClientValueCollection(CertificateAuthority)
        )
