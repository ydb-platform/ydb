from .endpoint import Endpoint, api
from .exceptions import ServerResponseError, ServerInfoEndpointNotFoundError, EndpointUnavailableError
from ...models import ServerInfoItem
import logging

logger = logging.getLogger('tableau.endpoint.server_info')


class ServerInfo(Endpoint):
    @property
    def baseurl(self):
        return "{0}/serverInfo".format(self.parent_srv.baseurl)

    @api(version="2.4")
    def get(self):
        """ Retrieve the server info for the server.  This is an unauthenticated call """
        try:
            server_response = self.get_unauthenticated_request(self.baseurl)
        except ServerResponseError as e:
            if e.code == "404003":
                raise ServerInfoEndpointNotFoundError
            if e.code == "404001":
                raise EndpointUnavailableError

        server_info = ServerInfoItem.from_response(server_response.content, self.parent_srv.namespace)
        return server_info
