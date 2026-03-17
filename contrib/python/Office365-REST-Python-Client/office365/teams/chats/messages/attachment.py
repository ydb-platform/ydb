from office365.runtime.client_value import ClientValue


class ChatMessageAttachment(ClientValue):
    """
    Represents an attachment to a chat message entity.

    An entity of type chatMessageAttachment is returned as part of the Get channel messages API, as a part of
    chatMessage entity.
    """

    def __init__(
        self,
        id_=None,
        name=None,
        content=None,
        content_type=None,
        content_url=None,
        teams_app_id=None,
        thumbnail_url=None,
    ):
        """
        :param str content: The content of the attachment. If the attachment is a rich card, set the property to the
             rich card object. This property and contentUrl are mutually exclusive.
        :param str content_type: The media type of the content attachment. It can have the following values:
             reference: Attachment is a link to another file. Populate the contentURL with the link to the object.
             Any contentTypes supported by the Bot Framework's Attachment object
             application/vnd.microsoft.card.codesnippet: A code snippet.
             application/vnd.microsoft.card.announcement: An announcement header.
        :param str content_url: URL for the content of the attachment. Supported protocols: http, https, file and data.
        :param str name: Name of the attachment.
        :param str teams_app_id: The ID of the Teams app that is associated with the attachment. The property is
             specifically used to attribute a Teams message card to the specified app.
        :param str thumbnail_url: URL to a thumbnail image that the channel can use if it supports using an alternative,
             smaller form of content or contentUrl. For example, if you set contentType to application/word and set
             contentUrl to the location of the Word document, you might include a thumbnail image that represents
             the document. The channel could display the thumbnail image instead of the document. When the user clicks
             the image, the channel would open the document.
        """
        self.id = id_
        self.name = name
        self.content = content
        self.contentType = content_type
        self.contentUrl = content_url
        self.teamsAppId = teams_app_id
        self.thumbnailUrl = thumbnail_url
