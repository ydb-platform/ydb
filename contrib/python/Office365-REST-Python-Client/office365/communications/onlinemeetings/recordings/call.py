from office365.entity import Entity


class CallRecording(Entity):
    """Represents a recording associated with an online meeting."""

    @property
    def call_id(self):
        """The unique identifier for the call that is related to this recording. Read-only."""
        return self.properties.get("callId", None)
