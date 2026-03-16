from office365.runtime.client_value import ClientValue


class TeamworkActivityTopic(ClientValue):
    """Represents the topic of an activity feed notification."""

    def __init__(self, source=None, value=None, web_url=None):
        self.source = source
        self.value = value
        self.webUrl = web_url
