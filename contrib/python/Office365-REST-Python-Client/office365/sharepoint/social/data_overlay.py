from office365.runtime.client_value import ClientValue


class SocialDataOverlay(ClientValue):
    """
    The SocialDataOverlay class provides information about an overlay. An overlay is a substring in a post that
    represents a user, document, site, tag, or link. The SocialPost class (see section 3.1.5.26) contains an array of
    SocialDataOverlay objects. Each of the SocialDataOverlay objects specifies a link or one or more actors.
    """
