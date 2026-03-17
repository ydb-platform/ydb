from tableauserverclient.models.extensions_item import ExtensionsServer, ExtensionsSiteSettings
from tableauserverclient.server.endpoint.endpoint import Endpoint
from tableauserverclient.server.endpoint.endpoint import api
from tableauserverclient.server.request_factory import RequestFactory


class Extensions(Endpoint):
    def __init__(self, parent_srv):
        super().__init__(parent_srv)

    @property
    def _server_baseurl(self) -> str:
        return f"{self.parent_srv.baseurl}/settings/extensions"

    @property
    def baseurl(self) -> str:
        return f"{self.parent_srv.baseurl}/sites/{self.parent_srv.site_id}/settings/extensions"

    @api(version="3.21")
    def get_server_settings(self) -> ExtensionsServer:
        """Lists the settings for extensions of a server

        Returns
        -------
        ExtensionsServer
            The server extensions settings
        """
        response = self.get_request(self._server_baseurl)
        return ExtensionsServer.from_response(response.content, self.parent_srv.namespace)

    @api(version="3.21")
    def update_server_settings(self, extensions_server: ExtensionsServer) -> ExtensionsServer:
        """Updates the settings for extensions of a server. Overwrites all existing settings. Any
        sites omitted from the block list will be unblocked.

        Parameters
        ----------
        extensions_server : ExtensionsServer
            The server extensions settings to update

        Returns
        -------
        ExtensionsServer
            The updated server extensions settings
        """
        req = RequestFactory.Extensions.update_server_extensions(extensions_server)
        response = self.put_request(self._server_baseurl, req)
        return ExtensionsServer.from_response(response.content, self.parent_srv.namespace)

    @api(version="3.21")
    def get(self) -> ExtensionsSiteSettings:
        """Lists the extensions settings for the site

        Returns
        -------
        ExtensionsSiteSettings
            The site extensions settings
        """
        response = self.get_request(self.baseurl)
        return ExtensionsSiteSettings.from_response(response.content, self.parent_srv.namespace)

    @api(version="3.21")
    def update(self, extensions_site_settings: ExtensionsSiteSettings) -> ExtensionsSiteSettings:
        """Updates the extensions settings for the site. Any extensions omitted
        from the safe extensions list will be removed.

        Parameters
        ----------
        extensions_site_settings : ExtensionsSiteSettings
            The site extensions settings to update

        Returns
        -------
        ExtensionsSiteSettings
            The updated site extensions settings
        """
        req = RequestFactory.Extensions.update_site_extensions(extensions_site_settings)
        response = self.put_request(self.baseurl, req)
        return ExtensionsSiteSettings.from_response(response.content, self.parent_srv.namespace)
