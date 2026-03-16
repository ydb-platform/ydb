from office365.runtime.client_value import ClientValue


class SocialPostCreationData(ClientValue):
    """
    The SocialPostCreationData object specifies the content of a post in the SocialFeedManager.CreatePost method
    (see section 3.1.5.19.2.1.1). The post consists of a text message, which can optionally include social tags,
    mentions of users, and links.
    """

    def __init__(self, content_text=None):
        """
        :param str content_text: The ContentText string contains the text body of the post. It can optionally contain
            one or more substitution references to elements in the zero-based SocialDataItems array. A substitution
            reference consists of a series of characters that consist of an open-brace character ({) followed by one
            of more digits in the range 0 to 9 and terminated by a close-brace character (}).
            The substitution reference is replaced by the text value of the element in the in the array at the offset
            specified by the value of the digits. For example, the text string "{0}" is replaced by the first element
            in the SocialDataItems array.
            Although it is not required by this interchange protocol, substitution references to mentions can be
            preceded by an at sign (@) in the ContentText and substitution references to social tags can be preceded
            by a hash mark (#) in the ContentText. The at sign and hash mark are not required by the protocol but are
            helpful if the post is displayed to a user by a client.
        """
        self.ContentText = content_text

    @property
    def entity_type_name(self):
        return "SP.Social.SocialPostCreationData"
