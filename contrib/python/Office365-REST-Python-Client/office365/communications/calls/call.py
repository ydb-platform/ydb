from office365.communications.calls.incoming_context import IncomingContext
from office365.communications.calls.participant import Participant
from office365.communications.calls.route import CallRoute
from office365.communications.operations.cancel_media_processing import (
    CancelMediaProcessingOperation,
)
from office365.communications.operations.comms import CommsOperation
from office365.communications.operations.unmute_participant import (
    UnmuteParticipantOperation,
)
from office365.communications.operations.update_recording_status import (
    UpdateRecordingStatusOperation,
)
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery


class Call(Entity):
    """
    The call resource is created when there is an incoming call for the application or the application creates a
    new outgoing call via a POST on app/calls.
    """

    def cancel_media_processing(self, client_context=None):
        """
        Cancels processing for any in-progress media operations.

        Media operations refer to the IVR operations playPrompt and recordResponse, which are by default queued
        to process in order. The cancelMediaProcessing method cancels any operation that is in-process as well as
        operations that are queued. For example, this method can be used to clean up the IVR operation queue for
        a new media operation. However, it will not cancel a subscribeToTone operation because it operates independent
        of any operation queue.


        :param str client_context: The client context.
        """
        return_type = CancelMediaProcessingOperation(self.context)
        payload = {
            "clientContext": client_context,
        }
        qry = ServiceOperationQuery(
            self, "cancelMediaProcessing", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def reject(self, reason=None, callback_uri=None):
        """
        Enable a bot to reject an incoming call. The incoming call request can be an invite from a participant in
        a group call or a peer-to-peer call. If an invite to a group call is received, the notification will contain
        the chatInfo and meetingInfo parameters.

        The bot is expected to answer or reject the call before the call times out. The current timeout value is
        15 seconds.

        This API does not end existing calls that have already been answered. Use delete call to end a call.

        :param str reason: The rejection reason. Possible values are None, Busy and Forbidden
        :param str callback_uri: This allows bots to provide a specific callback URI for the current call to receive
            later notifications. If this property has not been set, the bot's global callback URI will be used instead.
            This must be https.
        """
        payload = {"reason": reason, "callbackUri": callback_uri}
        qry = ServiceOperationQuery(self, "reject", None, payload)
        self.context.add_query(qry)
        return self

    def delete(self):
        """
        Delete or hang up an active call. For group calls, this will only delete your call leg and the underlying
        group call will still continue."""
        return super(Call, self).delete_object()

    def update_recording_status(self, status, client_context):
        """
        Update the application's recording status associated with a call.
        This requires the use of the Teams policy-based recording solution.

        :param str status: The recording status. Possible values are: notRecording, recording, or failed.
        :param str client_context: Unique client context string. Max limit is 256 chars.
        """
        return_type = UpdateRecordingStatusOperation(self.context)
        payload = {"status": status, "clientContext": client_context}
        qry = ServiceOperationQuery(
            self, "updateRecordingStatus", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def unmute(self, client_context):
        """
        Allow the application to unmute itself.

        This is a server unmute, meaning that the server will start sending audio packets for this participant
        to other participants again.

        :param str client_context: Unique Client Context string. Max limit is 256 chars.
        """
        return_type = UnmuteParticipantOperation(self.context)
        payload = {"clientContext": client_context}
        qry = ServiceOperationQuery(self, "unmute", None, payload, None, return_type)
        self.context.add_query(qry)
        return return_type

    @property
    def callback_uri(self):
        """The callback URL on which callbacks will be delivered. Must be https."""
        return self.properties.get("callbackUri", None)

    @property
    def call_routes(self):
        """The routing information on how the call was retargeted. Read-only."""
        return self.properties.get("callRoutes", ClientValueCollection(CallRoute))

    @property
    def incoming_context(self):
        """Call context associated with an incoming call."""
        return self.properties.get("incomingContext", IncomingContext())

    @property
    def participants(self):
        """
        Participant collection
        """
        return self.properties.get(
            "participants",
            EntityCollection(
                self.context,
                Participant,
                ResourcePath("participants", self.resource_path),
            ),
        )

    @property
    def operations(self):
        """
        CommsOperation collection
        """
        return self.properties.get(
            "operations",
            EntityCollection(
                self.context,
                CommsOperation,
                ResourcePath("operations", self.resource_path),
            ),
        )
