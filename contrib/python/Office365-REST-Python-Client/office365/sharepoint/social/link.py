from office365.runtime.client_value import ClientValue


class SocialLink(ClientValue):
    """The SocialLink class defines a link that includes a URI and text representation. This class is used to represent
    the location of a web site."""

    @property
    def entity_type_name(self):
        return "SP.Social.SocialLink"
