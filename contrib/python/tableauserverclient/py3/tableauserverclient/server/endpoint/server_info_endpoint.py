import logging
from typing import Union

from .endpoint import Endpoint, api
from .exceptions import ServerResponseError
from ..exceptions import (
    ServerInfoEndpointNotFoundError,
    EndpointUnavailableError,
)
from tableauserverclient.models import ServerInfoItem


class ServerInfo(Endpoint):
    def __init__(self, server):
        self.parent_srv = server
        self._info = None

    @property
    def serverInfo(self):
        if not self._info:
            self.get()
        return self._info

    def __repr__(self):
        return f"<Endpoint {self.serverInfo}>"

    @property
    def baseurl(self) -> str:
        return f"{self.parent_srv.baseurl}/serverInfo"

    @api(version="2.4")
    def get(self) -> Union[ServerInfoItem, None]:
        """
        Retrieve the build and version information for the server.

        This method makes an unauthenticated call, so no sign in or
        authentication token is required.

        Returns
        -------
        :class:`~tableauserverclient.models.ServerInfoItem`

        Raises
        ------
        :class:`~tableauserverclient.exceptions.ServerInfoEndpointNotFoundError`
            Raised when the server info endpoint is not found.

        :class:`~tableauserverclient.exceptions.EndpointUnavailableError`
            Raised when the server info endpoint is not available.

        Examples
        --------
        >>> import tableauserverclient as TSC

        >>> # create a instance of server
        >>> server = TSC.Server('https://MY-SERVER')

        >>> # set the version number > 2.3
        >>> # the server_info.get() method works in 2.4 and later
        >>> server.version = '2.5'

        >>> s_info = server.server_info.get()
        >>> print("\nServer info:")
        >>> print("\tProduct version: {0}".format(s_info.product_version))
        >>> print("\tREST API version: {0}".format(s_info.rest_api_version))
        >>> print("\tBuild number: {0}".format(s_info.build_number))
        """
        try:
            server_response = self.get_unauthenticated_request(self.baseurl)
        except ServerResponseError as e:
            if e.code == "404003":
                raise ServerInfoEndpointNotFoundError(e)
            if e.code == "404001":
                raise EndpointUnavailableError(e)
            raise e

        try:
            self._info = ServerInfoItem.from_response(server_response.content, self.parent_srv.namespace)
        except Exception as e:
            logging.getLogger(self.__class__.__name__).debug(e)
            logging.getLogger(self.__class__.__name__).debug(server_response.content)
        return self._info
