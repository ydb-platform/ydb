from office365.runtime.client_value import ClientValue
from office365.sharepoint.sharing.direct_abilities import DirectSharingAbilities
from office365.sharepoint.sharing.link_abilities import SharingLinkAbilities


class SharingAbilities(ClientValue):
    """
    Represents the matrix of possible sharing abilities for direct sharing and tokenized sharing links along
    with the state of each capability for the current user.
    """

    def __init__(
        self,
        anonymous_link_abilities=SharingLinkAbilities(),
        anyone_link_abilities=SharingLinkAbilities(),
        direct_sharing_abilities=DirectSharingAbilities(),
        organization_link_abilities=SharingLinkAbilities(),
        people_sharing_link_abilities=SharingLinkAbilities(),
    ):
        """
        :param SharingLinkAbilities anonymous_link_abilities: Indicates abilities for anonymous access links.
        :param SharingLinkAbilities anonymous_link_abilities:
        :param DirectSharingAbilities direct_sharing_abilities: Indicates abilities for direct sharing of a document
            using the canonical URL.
        :param SharingLinkAbilities organization_link_abilities: Indicates abilities for organization access links.
        :param SharingLinkAbilities people_sharing_link_abilities: Indicates abilities for tokenized sharing links that
            are configured to support only a predefined restricted membership set.
        """
        self.anonymousLinkAbilities = anonymous_link_abilities
        self.anyoneLinkAbilities = anyone_link_abilities
        self.directSharingAbilities = direct_sharing_abilities
        self.organizationLinkAbilities = organization_link_abilities
        self.peopleSharingLinkAbilities = people_sharing_link_abilities

    @property
    def entity_type_name(self):
        return "SP.Sharing.SharingAbilities"
