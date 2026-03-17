from office365.runtime.client_value import ClientValue


class TeamsTabConfiguration(ClientValue):
    """The settings that determine the content of a tab.
    When a tab is interactively configured, this information is set by the tab provider application.
    In addition to the properties below, some tab provider applications specify additional custom properties.
    """

    def __init__(
        self, content_url=None, entity_id=None, remove_url=None, website_url=None
    ):
        """
        :param str content_url: Url used for rendering tab contents in Teams.
        :param str entity_id: Identifier for the entity hosted by the tab provider.
        :param str remove_url: Url called by Teams client when a Tab is removed using the Teams Client.
        :param str website_url: Url for showing tab contents outside of Teams.
        """
        self.contentUrl = content_url
        self.entityId = entity_id
        self.removeUrl = remove_url
        self.websiteUrl = website_url
