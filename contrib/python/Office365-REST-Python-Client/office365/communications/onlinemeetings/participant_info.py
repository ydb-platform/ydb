from office365.directory.permissions.identity_set import IdentitySet
from office365.runtime.client_value import ClientValue


class MeetingParticipantInfo(ClientValue):
    """Information about a participant in a meeting."""

    def __init__(self, identity=IdentitySet(), role=None, upn=None):
        """
        :param IdentitySet identity: Identity information of the participant.
        :param str role: Specifies the participant's role in the meeting.
        :param str upn: User principal name of the participant.
        """
        self.identity = identity
        self.role = role
        self.upn = upn
