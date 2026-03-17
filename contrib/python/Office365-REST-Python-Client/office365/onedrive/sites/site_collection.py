from office365.onedrive.sites.archival_details import SiteArchivalDetails
from office365.runtime.client_value import ClientValue


class SiteCollection(ClientValue):
    """The siteCollection resource provides more information about a site collection."""

    def __init__(
        self,
        root=None,
        hostname=None,
        data_location_code=None,
        archival_details=SiteArchivalDetails(),
    ):
        """

        :param office365.onedrive.root.Root root: The hostname for the site collection.
        :param str hostname: The hostname for the site collection.
        :param str data_location_code: The geographic region code for where this site collection resides
        :param str archival_details: Represents the archival details of a siteCollection.
        """
        super(SiteCollection, self).__init__()
        self.root = root
        self.hostname = hostname
        self.dataLocationCode = data_location_code
        self.archivalDetails = archival_details
