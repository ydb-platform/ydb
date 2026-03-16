from office365.directory.insights.identity import InsightIdentity
from office365.directory.insights.resource_reference import ResourceReference
from office365.runtime.client_value import ClientValue


class SharingDetail(ClientValue):
    """Complex type containing properties of sharedInsight items."""

    def __init__(
        self,
        sharedBy=InsightIdentity(),
        shared_datetime=None,
        sharing_reference=ResourceReference(),
        sharing_subject=None,
        sharing_type=None,
    ):
        """
        :param datetime.datetime shared_datetime: The date and time the file was last shared.
        :param ResourceReference sharing_reference:
        :param str sharing_subject: The subject with which the document was shared.
        :param str sharing_type: Determines the way the document was shared,
            can be by a "Link", "Attachment", "Group", "Site".
        """
        self.sharedBy = sharedBy
        self.sharedDateTime = shared_datetime
        self.sharingReference = sharing_reference
        self.sharingSubject = sharing_subject
        self.sharingType = sharing_type
