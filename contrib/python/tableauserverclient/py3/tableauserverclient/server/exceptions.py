# These errors can be thrown without even talking to Tableau Server


class ServerInfoEndpointNotFoundError(Exception):
    pass


class EndpointUnavailableError(Exception):
    pass
