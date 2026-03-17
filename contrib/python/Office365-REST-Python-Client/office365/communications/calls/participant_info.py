from office365.runtime.client_value import ClientValue


class ParticipantInfo(ClientValue):
    """Contains additional properties about the participant identity"""

    def __init__(self, country_code=None):
        """
        :param str country_code: The ISO 3166-1 Alpha-2 country code of the participant's best estimated physical
            location at the start of the call.
        """
        self.countryCode = country_code
