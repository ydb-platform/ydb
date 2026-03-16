from office365.communications.onlinemeetings.participant_info import (
    MeetingParticipantInfo,
)
from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection


class MeetingParticipants(ClientValue):
    """Participants in a meeting."""

    def __init__(self, organizer=MeetingParticipantInfo(), attendees=None):
        """
        :param MeetingParticipantInfo organizer:
        :param list[MeetingParticipantInfo] attendees:
        """
        super(MeetingParticipants, self).__init__()
        self.organizer = organizer
        self.attendees = ClientValueCollection(MeetingParticipantInfo, attendees)
