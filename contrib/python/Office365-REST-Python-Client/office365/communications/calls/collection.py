from office365.communications.calls.call import Call
from office365.entity_collection import EntityCollection
from office365.runtime.queries.service_operation import ServiceOperationQuery


class CallCollection(EntityCollection):
    def __init__(self, context, resource_path=None):
        super(CallCollection, self).__init__(context, Call, resource_path)

    def create(self, callback_uri):
        """
        Create call enables your bot to create a new outgoing peer-to-peer or group call, or join an existing meeting

        :param str callback_uri: The callback URL on which callbacks will be delivered. Must be https.
        """
        return super(CallCollection, self).add(callbackUri=callback_uri)

    def log_teleconference_device_quality(self, quality=None):
        """
        Log video teleconferencing device quality data.
        The Cloud Video Interop (CVI) bot represents video teleconferencing (VTC) devices and acts as a back-to-back
        agent for a VTC device in a conference call. Because a CVI bot is in the middle of the VTC and Microsoft Teams
        infrastructure as a VTC proxy, it has two media legs. One media leg is between the CVI bot
        and Teams infrastructure, such as Teams conference server or a Teams client. The other media leg is between
        the CVI bot and the VTC device.

        The third-party partners own the VTC media leg and the Teams infrastructure cannot access the quality
        data of the third-party call leg. This method is only for the CVI partners to provide their media quality data.

        :param TeleconferenceDeviceQuality quality : Quality data of VTC media leg.
        """
        qry = ServiceOperationQuery(
            self, "logTeleconferenceDeviceQuality", None, quality
        )
        self.context.add_query(qry)
        return self
