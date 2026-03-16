from office365.outlook.geo_coordinates import OutlookGeoCoordinates
from office365.outlook.mail.physical_address import PhysicalAddress
from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection


class Location(ClientValue):
    """
    Represents location information of an event.

    There are multiple ways to create events in a calendar, for example, through an app using the create event REST API,
    or manually using the Outlook user interface. When you create an event using the user interface, you can specify
    the location as plain text (for example, "Harry's Bar"), or from the rooms list provided by Outlook,
    Bing Autosuggest, or Bing local search.
    """

    def __init__(
        self,
        address=PhysicalAddress(),
        coordinates=None,
        display_name=None,
        location_email_address=None,
        location_type=None,
        location_uri=None,
        unique_id=None,
        unique_id_type=None,
    ):
        """
        :param PhysicalAddress address: The street address of the location.
        :param list[OutlookGeoCoordinates] coordinates:
        :param str display_name: The name associated with the location.
        :param str location_email_address: Optional email address of the location.
        :param str location_type: The type of location.
        :param str location_uri: Optional URI representing the location.
        :param str unique_id: For internal use only.
        :param str unique_id_type: For internal use only.
        """
        super(Location, self).__init__()
        self.address = address
        self.coordinates = ClientValueCollection(OutlookGeoCoordinates, coordinates)
        self.displayName = display_name
        self.locationEmailAddress = location_email_address
        self.locationType = location_type
        self.locationUri = location_uri
        self.uniqueId = unique_id
        self.uniqueIdType = unique_id_type
