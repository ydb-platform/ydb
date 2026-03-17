from office365.runtime.client_value import ClientValue


class ResponseStatus(ClientValue):
    """Represents the response status of an attendee or organizer for a meeting request."""

    def __init__(self, response=None):
        """
        :type response: str
        """
        self.response = response
