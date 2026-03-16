from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.social.thread import SocialThread


class SocialFeed(ClientValue):
    """
    The SocialFeed class specifies a feed, which contains an array of SocialThread (section 3.1.5.42), each of which
    specifies a root SocialPost object (section 3.1.5.26) and an array of response SocialPost objects.
    """

    def __init__(
        self,
        attributes=None,
        newest_processed=None,
        oldest_processed=None,
        threads=None,
        unread_mention_count=None,
    ):
        """
        :param in attributes: The Attributes property specifies attributes of the returned feed.
            The attributes specify if the requested feed has additional threads that were not included in the returned
            thread. See section 3.1.5.18 for details on the SocialFeedAttributes type.
        :param str newest_processed: The NewestProcessed property returns the date-time of the most recent post that
            was requested. If the current user does not have access to the post, the most recent post that was
            requested can be removed from the feed, and the feed does not contain the post with the date specified
            in this property.
        :param str oldest_processed: The OldestProcessed property returns the date-time of the oldest post that was
            requested. If the current user does not have access to the post, the oldest post that was requested can
            be removed from the feed and the feed does not contain the post with the date specified in this property.
        :param list[SocialThread] threads:
        :param int unread_mention_count: he UnreadMentionCount property returns the number of mentions of the current
            user that have been added to the feed on the server since the time that the unread mention count
            was cleared for the current user.
            The GetMentions method (see section 3.1.5.19.2.1.7) optionally clears the unread mention count for
            the current user.
            The UnreadMentionCount property is available only for social feeds returned by the GetFeed method
            (see section 3.1.5.19.2.1.4).
        """
        self.Attributes = attributes
        self.NewestProcessed = newest_processed
        self.OldestProcessed = oldest_processed
        self.Threads = ClientValueCollection(SocialThread, threads)
        self.UnreadMentionCount = unread_mention_count

    @property
    def entity_type_name(self):
        return "SP.Social.SocialFeed"
