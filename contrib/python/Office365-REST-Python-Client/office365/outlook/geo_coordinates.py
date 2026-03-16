from office365.runtime.client_value import ClientValue


class OutlookGeoCoordinates(ClientValue):
    """The geographic coordinates, elevation, and their degree of accuracy for a physical location."""

    def __init__(
        self,
        accuracy=None,
        altitude=None,
        altitude_accuracy=None,
        latitude=None,
        longitude=None,
    ):
        """
        :param float accuracy: The accuracy of the latitude and longitude. As an example, the accuracy can be measured
            in meters, such as the latitude and longitude are accurate to within 50 meters.
        :param float altitude: The altitude of the location.
        :param float altitude_accuracy: The accuracy of the altitude.
        :param float latitude: The latitude of the location.
        :param float longitude: The longitude of the location.
        """
        self.accuracy = accuracy
        self.altitude = altitude
        self.altitudeAccuracy = altitude_accuracy
        self.latitude = latitude
        self.longitude = longitude
