from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.sharing.inherited_from import InheritedFrom
from office365.sharepoint.sharing.links.info import SharingLinkInfo
from office365.sharepoint.sharing.principal import Principal


class LinkInfo(ClientValue):
    """This class provides metadata for the tokenized sharing link including settings details, inheritance status,
    and an optional array of members."""

    def __init__(
        self,
        inherited_from=InheritedFrom(),
        is_inherited=None,
        link_details=SharingLinkInfo(),
        link_members=None,
        link_status=None,
        total_link_members_count=None,
    ):
        """
        :param bool is_inherited: Boolean that indicates if the tokenized sharing link is present due to
             inherited permissions from a parent object.
        """
        self.inherited_from = inherited_from
        self.isInherited = is_inherited
        self.linkDetails = link_details
        self.linkMembers = ClientValueCollection(Principal, link_members)
        self.linkStatus = link_status
        self.totalLinkMembersCount = total_link_members_count
