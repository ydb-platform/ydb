from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.activities.identity import ActivityIdentity


class GetCommentFacet(ClientValue):
    """"""

    def __init__(
        self,
        assignees=None,
        comment_id=None,
        is_reply=None,
        parent_author=ActivityIdentity(),
        parent_comment_id=None,
        participants=None,
    ):
        """
        :param list[ActivityIdentity] assignees:
        :param str comment_id:
        :param bool is_reply:
        :param ActivityIdentity parent_author: Gets or sets the parent author.
        :param str parent_comment_id:
        :param list[ActivityIdentity] participants:
        """
        self.assignees = ClientValueCollection(ActivityIdentity, assignees)
        self.commentId = comment_id
        self.isReply = is_reply
        self.parentAuthor = parent_author
        self.parentCommentId = parent_comment_id
        self.participants = ClientValueCollection(ActivityIdentity, participants)

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Activities.GetCommentFacet"
