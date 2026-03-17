from office365.directory.permissions.identity_set import IdentitySet
from office365.runtime.client_value import ClientValue


class InvitationParticipantInfo(ClientValue):
    """This resource is used to represent the entity that is being invited to a group call."""

    def __init__(
        self,
        hidden=None,
        identity=IdentitySet(),
        participant_id=None,
        remove_from_default_audio_routing_group=None,
        replaces_call_id=None,
    ):
        """
        :param bool hidden: Optional. Whether to hide the participant from the roster.
        :param IdentitySet identity: The identitySet associated with this invitation.
        :param str participant_id: Optional. The ID of the target participant.
        :param bool remove_from_default_audio_routing_group: Optional. Whether to remove them from the main mixer.
        :param str replaces_call_id: Optional. The call which the target identity is currently a part of.
            For peer-to-peer case, the call will be dropped once the participant is added successfully.
        """
        self.hidden = hidden
        self.identity = identity
        self.participantId = participant_id
        self.removeFromDefaultAudioRoutingGroup = (
            remove_from_default_audio_routing_group
        )
        self.replacesCallId = replaces_call_id
