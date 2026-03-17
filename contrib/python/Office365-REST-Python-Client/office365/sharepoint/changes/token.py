from office365.runtime.client_value import ClientValue


class ChangeToken(ClientValue):
    """Represents the unique sequential location of a change within the change log. Client applications can use the
    change token as a starting point for retrieving changes."""

    def __init__(self, string_value=None):
        """
        :param str string_value: Contains the serialized representation of the change token generated
            by the protocol server. When setting StringValue, the protocol client MUST use a value previously returned
            by the protocol server.

            Represented as a semicolon-separated list containing the following, in order:
              - The version number of the change token.
              - The change token's collection scope.
              - The collection scope GUID.
              - The time of the change token in ticks.
              - The change number.
        """
        super(ChangeToken, self).__init__()
        self.StringValue = string_value

    def __repr__(self):
        return self.StringValue or ""

    @property
    def entity_type_name(self):
        return "SP.ChangeToken"
