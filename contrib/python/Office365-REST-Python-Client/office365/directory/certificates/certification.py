from office365.runtime.client_value import ClientValue


class Certification(ClientValue):
    """Represents the certification details of an application."""

    def __init__(self, certification_details_url=None):
        """
        :param str certification_details_url:
        """
        self.certificationDetailsUrl = certification_details_url
