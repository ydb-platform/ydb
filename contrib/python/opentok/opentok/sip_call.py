import json


class SipCall(object):
    """
    Represents data from a SIP call
    """

    def __init__(self, kwargs):
        self.id = kwargs.get("id")
        self.connectionId = kwargs.get("connectionId")
        self.streamId = kwargs.get("streamId")

    def json(self):
        """
        Returns a JSON representation of the SIP call
        """
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)
