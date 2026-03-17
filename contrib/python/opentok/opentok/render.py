import json
from six import iteritems

class Render:
    """Represents an Experience Composer render of an OpenTok session."""

    def __init__(self, kwargs):
        self.id = kwargs.get("id")
        self.sessionId = kwargs.get("sessionId")
        self.projectId = kwargs.get("projectId")
        self.createdAt = kwargs.get("createdAt")
        self.updatedAt = kwargs.get("updatedAt")
        self.url = kwargs.get("url")
        self.resolution = kwargs.get("resolution")
        self.status = kwargs.get("status")
        self.streamId = kwargs.get("streamId") or None
        self.reason = kwargs.get("reason") or None

    def json(self):
        """Returns a JSON representation of the render."""
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)

    def attrs(self):
        """
        Returns a dictionary of the render's attributes.
        """
        return dict((k, v) for k, v in iteritems(self.__dict__))

class RenderList:
    """Object that represents a list of renders."""
    def __init__(self, sdk, values):
        self.count = values.get("count")
        self.items = list(map(lambda x: Render(x), values.get("items", [])))

    def attrs(self):
        return {"count": self.count, "items": map(Render.attrs, self.items)}

    def json(self):
        return json.dumps(self.attrs(), indent=4)
