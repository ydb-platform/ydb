from typing import Optional

from office365.communications.calls.invitation_participant_info import (
    InvitationParticipantInfo,
)
from office365.communications.calls.participant_info import ParticipantInfo
from office365.communications.onlinemeetings.restricted import OnlineMeetingRestricted
from office365.communications.operations.invite_participants import (
    InviteParticipantsOperation,
)
from office365.communications.operations.start_hold_music import StartHoldMusicOperation
from office365.communications.operations.stop_hold_music import StopHoldMusicOperation
from office365.entity import Entity
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.queries.service_operation import ServiceOperationQuery


class Participant(Entity):
    """Represents a participant in a call."""

    def invite(self, participants, client_context):
        """Invite participants to the active call.

        :param list[InvitationParticipantInfo] participants: Unique Client Context string. Max limit is 256 chars.
        :param str client_context: Unique Client Context string. Max limit is 256 chars.
        """
        return_type = InviteParticipantsOperation(self.context)
        payload = {
            "participants": ClientValueCollection(
                InvitationParticipantInfo, participants
            ),
            "clientContext": client_context,
        }
        qry = ServiceOperationQuery(self, "invite", None, payload, None, return_type)
        self.context.add_query(qry)
        return return_type

    def start_hold_music(self, custom_prompt=None, client_context=None):
        """
        Put a participant on hold and play music in the background.

        :param str or None custom_prompt: Audio prompt the participant will hear when placed on hold.
        :param str or None client_context: Unique client context string. Can have a maximum of 256 characters.
        """
        return_type = StartHoldMusicOperation(self.context)
        payload = {"customPrompt": custom_prompt, "clientContext": client_context}
        qry = ServiceOperationQuery(
            self, "startHoldMusic", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def stop_hold_music(self, client_context=None):
        """
        Reincorporate a participant previously put on hold to the call.

        :param str or None client_context: Unique client context string. Can have a maximum of 256 characters.
        """
        return_type = StopHoldMusicOperation(self.context)
        payload = {"clientContext": client_context}
        qry = ServiceOperationQuery(
            self, "stopHoldMusic", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @property
    def info(self):
        """Information about the participant."""
        return self.properties.get("info", ParticipantInfo())

    @property
    def metadata(self):
        # type: () -> Optional[str]
        """A blob of data provided by the participant in the roster."""
        return self.properties.get("metadata", None)

    @property
    def restricted_experience(self):
        """Information about the reason or reasons media content from a participant is restricted."""
        return self.properties.get("restrictedExperience", OnlineMeetingRestricted())
