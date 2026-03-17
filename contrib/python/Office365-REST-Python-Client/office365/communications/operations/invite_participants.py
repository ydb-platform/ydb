from office365.communications.calls.invitation_participant_info import (
    InvitationParticipantInfo,
)
from office365.communications.operations.comms import CommsOperation
from office365.runtime.client_value_collection import ClientValueCollection


class InviteParticipantsOperation(CommsOperation):
    """Represents the status of a long-running participant invitation operation,
    triggered by a call to the participant-invite API."""

    @property
    def participants(self):
        """
        The participants to invite.
        """
        return self.properties.get(
            "participants", ClientValueCollection(InvitationParticipantInfo)
        )
