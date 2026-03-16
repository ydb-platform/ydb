from office365.runtime.client_value import ClientValue


class Endpoint(ClientValue):
    """Represents an endpoint in a call. The endpoint could be a user's device, a meeting, an application/bot, etc.
    The participantEndpoint and serviceEndpoint types inherit from this type."""
