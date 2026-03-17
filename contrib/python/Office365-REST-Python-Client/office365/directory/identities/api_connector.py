from office365.directory.authentication.configuration_base import (
    ApiAuthenticationConfigurationBase,
)
from office365.entity import Entity
from office365.runtime.queries.service_operation import ServiceOperationQuery


class IdentityApiConnector(Entity):
    """
    Represents API connectors in an Azure Active Directory (Azure AD) tenants.

    An API connector used in your Azure AD External Identities self-service sign-up user flows allows you to call
    an API during the execution of the user flow. An API connector provides the information needed to call an API
    including an endpoint URL and authentication. An API connector can be used at a specific step in a user flow
    to affect the execution of the user flow. For example, the API response can block a user from signing up,
    show an input validation error, or overwrite user collected attributes.
    """

    def upload_client_certificate(self, pkcs12_value, password):
        """Upload a PKCS 12 format key (.pfx) to an API connector's authentication configuration.
        The input is a base-64 encoded value of the PKCS 12 certificate contents.
        This method returns an apiConnector.

        :param str pkcs12_value:
        :param str password:
        """

        payload = {"pkcs12Value": pkcs12_value, "password": password}
        qry = ServiceOperationQuery(
            self, "uploadClientCertificate", None, payload, None, None
        )
        self.context.add_query(qry)
        return self

    @property
    def authentication_configuration(self):
        """The object which describes the authentication configuration details for calling the API.
        Basic and PKCS 12 client certificate are supported."""
        return self.properties.get(
            "authenticationConfiguration", ApiAuthenticationConfigurationBase()
        )
