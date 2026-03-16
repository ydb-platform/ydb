import json


class Captions:
    """Represents information about a captioning session."""

    def __init__(self, kwargs):
        self.captions_id = kwargs.get("captionsId")

    def json(self):
        """Returns a JSON representation of the captioning session information."""
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)
