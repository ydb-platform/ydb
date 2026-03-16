from typing import Protocol, Union, TYPE_CHECKING
from tableauserverclient.models.oidc_item import SiteOIDCConfiguration
from tableauserverclient.server.endpoint import Endpoint
from tableauserverclient.server.request_factory import RequestFactory
from tableauserverclient.server.endpoint.endpoint import api

if TYPE_CHECKING:
    from tableauserverclient.models.site_item import SiteAuthConfiguration
    from tableauserverclient.server.server import Server


class IDPAttributes(Protocol):
    idp_configuration_id: str


class IDPProperty(Protocol):
    @property
    def idp_configuration_id(self) -> str: ...


HasIdpConfigurationID = Union[str, IDPAttributes]


class OIDC(Endpoint):
    def __init__(self, server: "Server") -> None:
        self.parent_srv = server

    @property
    def baseurl(self) -> str:
        return f"{self.parent_srv.baseurl}/sites/{self.parent_srv.site_id}/site-oidc-configuration"

    @api(version="3.24")
    def get(self) -> list["SiteAuthConfiguration"]:
        """
        Get all OpenID Connect (OIDC) configurations for the currently
        authenticated Tableau Cloud site. To get all of the configuration
        details, use the get_by_id method.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_identity_pools.htm#AuthnService_ListAuthConfigurations

        Returns
        -------
        list[SiteAuthConfiguration]
        """
        return self.parent_srv.sites.list_auth_configurations()

    @api(version="3.24")
    def get_by_id(self, id: Union[str, HasIdpConfigurationID]) -> SiteOIDCConfiguration:
        """
        Get details about a specific OpenID Connect (OIDC) configuration on the
        current Tableau Cloud site. Only retrieves configurations for the
        currently authenticated site.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_openid_connect.htm#get_openid_connect_configuration

        Parameters
        ----------
        id : Union[str, HasID]
            The ID of the OIDC configuration to retrieve. Can be either the
            ID string or an object with an id attribute.

        Returns
        -------
        SiteOIDCConfiguration
            The OIDC configuration for the specified site.
        """
        target = getattr(id, "idp_configuration_id", id)
        url = f"{self.baseurl}/{target}"
        response = self.get_request(url)
        return SiteOIDCConfiguration.from_response(response.content, self.parent_srv.namespace)

    @api(version="3.22")
    def create(self, config_item: SiteOIDCConfiguration) -> SiteOIDCConfiguration:
        """
        Create the OpenID Connect (OIDC) configuration for the currently
        authenticated Tableau Cloud site. The config_item must have the
        following attributes set, others are optional:

        idp_configuration_name
        client_id
        client_secret
        authorization_endpoint
        token_endpoint
        userinfo_endpoint
        enabled
        jwks_uri

        The secret in the returned config will be masked.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_openid_connect.htm#create_openid_connect_configuration

        Parameters
        ----------
        config : SiteOIDCConfiguration
            The OIDC configuration to create.

        Returns
        -------
        SiteOIDCConfiguration
            The created OIDC configuration.
        """
        url = self.baseurl
        create_req = RequestFactory.OIDC.create_req(config_item)
        response = self.put_request(url, create_req)
        return SiteOIDCConfiguration.from_response(response.content, self.parent_srv.namespace)

    @api(version="3.24")
    def delete_configuration(self, config: Union[str, HasIdpConfigurationID]) -> None:
        """
        Delete the OpenID Connect (OIDC) configuration for the currently
        authenticated Tableau Cloud site. The config parameter can be either
        the ID of the configuration or the configuration object itself.

        **Important**: Before removing the OIDC configuration, make sure that
        users who are set to authenticate with OIDC are set to use a different
        authentication type. Users who are not set with a different
        authentication type before removing the OIDC configuration will not be
        able to sign in to Tableau Cloud.


        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_openid_connect.htm#remove_openid_connect_configuration

        Parameters
        ----------
        config : Union[str, HasID]
            The OIDC configuration to delete. Can be either the ID of the
            configuration or the configuration object itself.
        """

        target = getattr(config, "idp_configuration_id", config)

        url = f"{self.parent_srv.baseurl}/sites/{self.parent_srv.site_id}/disable-site-oidc-configuration?idpConfigurationId={target}"
        _ = self.put_request(url)
        return None

    @api(version="3.22")
    def update(self, config: SiteOIDCConfiguration) -> SiteOIDCConfiguration:
        """
        Update the Tableau Cloud site's OpenID Connect (OIDC) configuration. The
        secret in the returned config will be masked.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_openid_connect.htm#update_openid_connect_configuration

        Parameters
        ----------
        config : SiteOIDCConfiguration
            The OIDC configuration to update. Must have the id attribute set.

        Returns
        -------
        SiteOIDCConfiguration
            The updated OIDC configuration.
        """
        url = f"{self.baseurl}/{config.idp_configuration_id}"
        update_req = RequestFactory.OIDC.update_req(config)
        response = self.put_request(url, update_req)
        return SiteOIDCConfiguration.from_response(response.content, self.parent_srv.namespace)
