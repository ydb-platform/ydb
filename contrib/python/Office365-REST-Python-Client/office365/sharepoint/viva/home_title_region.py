from office365.runtime.client_value import ClientValue


class VivaHomeTitleRegion(ClientValue):
    def __init__(self, image_url=None):
        """
        :param str image_url:
        """
        self.ImageUrl = image_url
