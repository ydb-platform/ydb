from office365.runtime.client_value import ClientValue


class GeoCoordinates(ClientValue):
    """
    The GeoCoordinates resource provides geographic coordinates and elevation of a location based on metadata
    contained within the file. If a DriveItem has a non-null location facet, the item represents a file with
    a known location associated with it.
    """

    def __init__(self, altitude=None, latitude=None, longitude=None):
        """
        :param float altitude: The altitude (height), in feet, above sea level for the item.
        :param float latitude: The latitude, in decimal, for the item
        :param float longitude: The longitude, in decimal, for the item
        """
        self.altitude = altitude
        self.latitude = latitude
        self.longitude = longitude
