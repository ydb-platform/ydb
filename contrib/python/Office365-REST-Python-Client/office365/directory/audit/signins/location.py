from office365.onedrive.driveitems.geo_coordinates import GeoCoordinates
from office365.runtime.client_value import ClientValue


class SignInLocation(ClientValue):
    """Provides the city, state and country/region from where the sign-in happened."""

    def __init__(
        self,
        city=None,
        country_or_region=None,
        geo_coordinates=GeoCoordinates(),
        state=None,
    ):
        """
        :param str city: Provides the city where the sign-in originated. This is calculated using latitude/longitude
            information from the sign-in activity.
        :param str country_or_region: Provides the country code info (2 letter code) where the sign-in originated.
            This is calculated using latitude/longitude information from the sign-in activity.
        :param GeoCoordinates geo_coordinates: Provides the latitude, longitude and altitude where the sign-in
            originated.
        :param str state: Provides the State where the sign-in originated. This is calculated using latitude/longitude
            information from the sign-in activity.
        """
        self.city = city
        self.countryOrRegion = country_or_region
        self.geoCoordinates = geo_coordinates
        self.state = state
