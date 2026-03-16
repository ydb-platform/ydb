from office365.directory.permissions.identity_set import IdentitySet
from office365.runtime.client_value import ClientValue


class PublicationFacet(ClientValue):
    """The publicationFacet resource provides details on the published status of a driveItemVersion or driveItem
    resource."""

    def __init__(self, checked_out_by=IdentitySet(), level=None, version_id=None):
        """
        The publicationFacet resource provides details on the published status of a driveItemVersion
        or driveItem resource.

        :param str level: The state of publication for this document. Either published or checkout. Read-only.
        :param str version_id: The unique identifier for the version that is visible to the current caller. Read-only.
        """
        super(PublicationFacet, self).__init__()
        self.checkedOutBy = checked_out_by
        self.level = level
        self.versionId = version_id
