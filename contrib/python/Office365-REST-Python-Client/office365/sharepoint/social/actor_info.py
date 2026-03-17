from office365.runtime.client_value import ClientValue


class SocialActorInfo(ClientValue):
    """e SocialActorInfo type identifies an actor to the server. An actor can be a user, document, site, or tag."""

    def __init__(self, account_name=None):
        """
        :param str account_name: The AccountName property specifies the user's account name. Users can be identified
            by this property.
        """
        self.AccountName = account_name

    @property
    def entity_type_name(self):
        return "SP.Social.SocialActorInfo"
