from office365.runtime.client_value import ClientValue


class SocialAttachment(ClientValue):
    """The SocialAttachment class represents an image, document preview, or video preview attachment."""

    @property
    def entity_type_name(self):
        return "SP.Social.SocialAttachment"
