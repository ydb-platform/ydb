import random
import string
import uuid

from office365.delta_collection import DeltaCollection
from office365.intune.devices.alternative_security_id import AlternativeSecurityId
from office365.intune.devices.device import Device
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.queries.create_entity import CreateEntityQuery


class DeviceCollection(DeltaCollection[Device]):
    """Device's collection"""

    def __init__(self, context, resource_path=None):
        super(DeviceCollection, self).__init__(context, Device, resource_path)

    def add(
        self,
        display_name,
        operating_system,
        operating_system_version,
        account_enabled=False,
        alternative_security_id=None,
        device_id=None,
    ):
        """Create and register a new device in the organization.
        :param str display_name: The display name for the device
        :param str operating_system:
        :param str operating_system_version:
        :param bool account_enabled:
        :param AlternativeSecurityId alternative_security_id:
        :param str device_id:
        """

        if alternative_security_id is None:
            key_id = "base64" + "".join(
                random.choice(string.ascii_lowercase + string.digits) for _ in range(10)
            )
            alternative_security_id = AlternativeSecurityId(2, key_id)
        if device_id is None:
            device_id = str(uuid.uuid4())

        return_type = Device(self.context)
        return_type.set_property("displayName", display_name)
        return_type.set_property("operatingSystem", operating_system)
        return_type.set_property("operatingSystemVersion", operating_system_version)
        return_type.set_property("accountEnabled", account_enabled)
        return_type.set_property("deviceId", device_id)
        return_type.set_property(
            "alternativeSecurityIds",
            ClientValueCollection(AlternativeSecurityId, [alternative_security_id]),
        )
        self.add_child(return_type)
        qry = CreateEntityQuery(self, return_type, return_type)
        self.context.add_query(qry)
        return return_type
