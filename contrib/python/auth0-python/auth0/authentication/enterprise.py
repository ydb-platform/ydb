from typing import Any

from .base import AuthenticationBase


class Enterprise(AuthenticationBase):

    """Enterprise endpoints.

    Args:
        domain (str): Your auth0 domain (e.g: my-domain.us.auth0.com)
    """

    def saml_metadata(self) -> Any:
        """Get SAML2.0 Metadata."""

        return self.get(
            url="{}://{}/samlp/metadata/{}".format(
                self.protocol, self.domain, self.client_id
            )
        )

    def wsfed_metadata(self) -> Any:
        """Returns the WS-Federation Metadata."""

        url = "{}://{}/wsfed/FederationMetadata/2007-06/FederationMetadata.xml"

        return self.get(url=url.format(self.protocol, self.domain))
