import struct
from enum import IntEnum

# Quoted descriptions were copied or paraphrased from ISO-13400-2-2019 (E).


class DoIPMessage:
    """Base class for DoIP messages implementing common features like comparison,
    and representation"""

    def __repr__(self):
        formatted_field_values = []
        for field in self._fields:
            value = getattr(self, "_" + field)
            if type(value) == str:
                formatted_field_values.append(f'"{value}"')
            else:
                formatted_field_values.append(str(value))
        args = ", ".join(formatted_field_values)
        classname = type(self).__name__
        return f"{classname}({args})"

    def __str__(self):
        formatted_field_values = []
        for field in self._fields:
            value = getattr(self, field)
            if type(value) == str:
                formatted_field_values.append(f'{field}: "{value}"')
            else:
                formatted_field_values.append(f"{field} : {str(value)}")
        args = ", ".join(formatted_field_values)
        classname = type(self).__name__
        if args:
            return f"{classname} (0x{self.payload_type:X}): {{ {args} }}"
        else:
            return f"{classname} (0x{self.payload_type:X})"

    def __eq__(self, other):
        return (type(self) == type(other)) and (self.pack() == other.pack())


class ReservedMessage(DoIPMessage):
    """DoIP message whose payload ID is reserved either for manufacturer use or future
    expansion of DoIP protocol"""

    @classmethod
    def unpack(cls, payload_type, payload_bytes, payload_length):
        return ReservedMessage(payload_type, payload_bytes)

    def pack(self):
        self._payload

    _fields = ["payload_type", "payload"]

    def __init__(self, payload_type, payload):
        self._payload_type = payload_type
        self._payload = payload

    @property
    def payload(self):
        """Raw payload bytes"""
        return self._payload

    @property
    def payload_type(self):
        """Raw payload type (ID)"""
        return self._payload_type


class GenericDoIPNegativeAcknowledge(DoIPMessage):
    """Generic header negative acknowledge structure. See Table 18"""

    payload_type = 0x0000

    class NackCodes(IntEnum):
        """Generic DoIP header NACK codes. See Table 19"""

        IncorrectPatternFormat = 0x00
        UnknownPayloadType = 0x01
        MessageTooLarge = 0x02
        OutOfMemory = 0x03
        InvalidPayloadLength = 0x04

    @classmethod
    def unpack(cls, payload_bytes, payload_length):
        return GenericDoIPNegativeAcknowledge(*struct.unpack_from("!B", payload_bytes))

    def pack(self):
        return struct.pack("!B", self._nack_code)

    _fields = ["nack_code"]

    def __init__(self, nack_code):
        self._nack_code = nack_code

    @property
    def nack_code(self):
        """Generic DoIP header NACK code

        Description: "The generic header negative acknowledge code indicates the specific error,
        detected in the generic DoIP header, or it indicates an unsupported payload or a memory
        overload condition."
        """
        return self._nack_code


class AliveCheckRequest(DoIPMessage):
    """Alive check request - Table 27"""

    payload_type = 0x0007

    _fields = []

    @classmethod
    def unpack(cls, payload_bytes, payload_length):
        return AliveCheckRequest()

    def pack(self):
        return bytearray()


class AliveCheckResponse(DoIPMessage):
    """Alive check resopnse - Table 28"""

    payload_type = 0x0008

    @classmethod
    def unpack(cls, payload_bytes, payload_length):
        return AliveCheckResponse(*struct.unpack_from("!H", payload_bytes))

    def pack(self):
        return struct.pack("!H", self._source_address)

    _fields = ["source_address"]

    def __init__(self, source_address):
        self._source_address = source_address

    @property
    def source_address(self):
        """Source address (SA)

        Description: "Contains the logical address of the client DoIP entity
        that is currently active on this TCP_DATA socket"

        Values: From Table 13

        * 0x0000 = ISO/SAE reserved
        * 0x0001 to 0x0DFF = VM specific
        * 0x0E00 to 0x0FFF = Reserved for addresses of client
        * 0x1000 to 0x7FFF = VM Specific
        * 0x8000 to 0xE3FF = Reserved
        * 0xE400 to 0xE3FF = VM defined functional group logical addresses
        * 0xF000 to 0xFFFF = Reserved
        """
        return self._source_address


class DoipEntityStatusRequest(DoIPMessage):
    """DoIP entity status request - Table 10"""

    payload_type = 0x4001

    _fields = []

    @classmethod
    def unpack(cls, payload_bytes, payload_length):
        return DoipEntityStatusRequest()

    def pack(self):
        return bytearray()


class DiagnosticPowerModeRequest(DoIPMessage):
    """Diagnostic power mode information request - Table 8"""

    payload_type = 0x4003

    _fields = []

    @classmethod
    def unpack(cls, payload_bytes, payload_length):
        return DiagnosticPowerModeRequest()

    def pack(self):
        return bytearray()


class DiagnosticPowerModeResponse(DoIPMessage):
    """Diagnostic power mode information response - Table 9"""

    payload_type = 0x4004

    _fields = ["diagnostic_power_mode"]

    class DiagnosticPowerMode(IntEnum):
        """Diagnostic power mode - See Table 9"""

        NotReady = 0x00
        Ready = 0x01
        NotSupported = 0x02

    @classmethod
    def unpack(cls, payload_bytes, payload_length):
        return DiagnosticPowerModeResponse(*struct.unpack_from("!B", payload_bytes))

    def pack(self):
        return struct.pack("!B", self._diagnostic_power_mode)

    def __init__(self, diagnostic_power_mode):
        self._diagnostic_power_mode = diagnostic_power_mode

    @property
    def diagnostic_power_mode(self):
        """Diagnostic power mode

        Description: "Identifies whether or not the
        vehicle is in diagnostic power mode and ready to perform
        reliable diagnostics.
        """
        return DiagnosticPowerModeResponse.DiagnosticPowerMode(
            self._diagnostic_power_mode
        )


class RoutingActivationRequest(DoIPMessage):
    """Routing activation request. Table 46"""

    payload_type = 0x0005

    _fields = ["source_address", "activation_type", "reserved", "vm_specific"]

    class ActivationType(IntEnum):
        """See Table 47 - Routing activation request activation types"""

        Default = 0x00
        DiagnosticRequiredByRegulation = 0x01
        CentralSecurity = 0xE1

    @classmethod
    def unpack(cls, payload_bytes, payload_length):
        if payload_length == 7:
            return RoutingActivationRequest(*struct.unpack_from("!HBL", payload_bytes))
        else:
            return RoutingActivationRequest(*struct.unpack_from("!HBLL", payload_bytes))

    def pack(self):
        if self._vm_specific is not None:
            return struct.pack(
                "!HBLL",
                self._source_address,
                self._activation_type,
                self._reserved,
                self._vm_specific,
            )
        else:
            return struct.pack(
                "!HBL", self._source_address, self._activation_type, self._reserved
            )

    def __init__(self, source_address, activation_type, reserved=0, vm_specific=None):
        self._source_address = source_address
        self._activation_type = activation_type
        self._reserved = reserved
        self._vm_specific = vm_specific

    @property
    def source_address(self):
        """Source address (SA)

        Description: "Address of the client DoIP entity that requests routing activation.
        This is the same address that is used by the client DoIP entity when sending
        diagnostic messages on the same TCP_DATA socket."

        Values: From Table 13

        * 0x0000 = ISO/SAE reserved
        * 0x0001 to 0x0DFF = VM specific
        * 0x0E00 to 0x0FFF = Reserved for addresses of client
        * 0x1000 to 0x7FFF = VM Specific
        * 0x8000 to 0xE3FF = Reserved
        * 0xE400 to 0xE3FF = VM defined functional group logical addresses
        * 0xF000 to 0xFFFF = Reserved
        """
        return self._source_address

    @property
    def activation_type(self):
        """Activation type

        Description: "Indicates the specific type of routing activation that may
        require different types of authentication and/or confirmation."
        """
        return RoutingActivationRequest.ActivationType(self._activation_type)

    @property
    def reserved(self):
        """Reserved - should be 0x00000000"""
        return self._reserved

    @property
    def vm_specific(self):
        """Reserved for VM-specific use"""
        return self._vm_specific


class VehicleIdentificationRequest(DoIPMessage):
    """Vehicle identification request message. See Table 2"""

    payload_type = 0x0001

    _fields = []

    @classmethod
    def unpack(cls, payload_bytes, payload_length):
        return VehicleIdentificationRequest()

    def pack(self):
        return bytearray()


class VehicleIdentificationRequestWithEID(DoIPMessage):
    """Vehicle identification request message with EID. See Table 3"""

    payload_type = 0x0002

    _fields = ["eid"]

    @classmethod
    def unpack(cls, payload_bytes, payload_length):
        return VehicleIdentificationRequestWithEID(
            *struct.unpack_from("!6s", payload_bytes)
        )

    def pack(self):
        return struct.pack("!6s", self._eid)

    def __init__(self, eid):
        self._eid = eid

    @property
    def eid(self):
        """EID

        Description: "This is the DoIP entity's unique ID (e.g. network
        interface's MAC address) that shall respond to the vehicle
        identification request message."
        """
        return self._eid


class VehicleIdentificationRequestWithVIN(DoIPMessage):
    """Vehicle identification request message with VIN. See Table 4"""

    payload_type = 0x0003

    _fields = ["vin"]

    @classmethod
    def unpack(cls, payload_bytes, payload_length):
        return VehicleIdentificationRequestWithVIN(
            *struct.unpack_from("!17s", payload_bytes)
        )

    def pack(self):
        return struct.pack("!17s", self._vin.encode("ascii"))

    def __init__(self, vin):
        self._vin = vin

    @property
    def vin(self):
        """VIN

        Description: "This is the vehicle’s identification number asspecified
        in ISO 3779. This parameter is only present if the client DoIP entity
        intends toidentify the DoIP entities of an individual vehicle, the VIN
        of which is known to the client DoIP entity."

        Values: ASCII
        """
        if type(self._vin) is bytes:
            return self._vin.decode("ascii")
        else:
            return self._vin


class RoutingActivationResponse(DoIPMessage):
    """Payload type routing activation response."""

    payload_type = 0x0006

    _fields = [
        "client_logical_address",
        "logical_address",
        "response_code",
        "reserved",
        "vm_specific",
    ]

    class ResponseCode(IntEnum):
        """See Table 49"""

        DeniedUnknownSourceAddress = 0x00
        DeniedAllSocketsRegisteredActive = 0x01
        DeniedSADoesNotMatch = 0x02
        DeniedSARegistered = 0x03
        DeniedMissingAuthentication = 0x04
        DeniedRejectedConfirmation = 0x05
        DeniedUnsupportedActivationType = 0x06
        DeniedRequiresTLS = 0x07
        Success = 0x10
        SuccessConfirmationRequired = 0x11

    @classmethod
    def unpack(cls, payload_bytes, payload_length):
        if payload_length == 9:
            return RoutingActivationResponse(
                *struct.unpack_from("!HHBL", payload_bytes)
            )
        else:
            return RoutingActivationResponse(
                *struct.unpack_from("!HHBLL", payload_bytes)
            )

    def pack(self):
        if self._vm_specific is not None:
            return struct.pack(
                "!HHBLL",
                self._client_logical_address,
                self._logical_address,
                self._response_code,
                self._reserved,
                self._vm_specific,
            )
        else:
            return struct.pack(
                "!HHBL",
                self._client_logical_address,
                self._logical_address,
                self._response_code,
                self._reserved,
            )

    def __init__(
        self,
        client_logical_address,
        logical_address,
        response_code,
        reserved=0,
        vm_specific=None,
    ):
        self._client_logical_address = client_logical_address
        self._logical_address = logical_address
        self._response_code = response_code
        self._reserved = reserved
        self._vm_specific = vm_specific

    @property
    def client_logical_address(self):
        """Logical address of client DoIP entity

        Description: "Logical address of the client DoIP entity that requested routing activation."

        Values: From Table 13

        * 0x0000 = ISO/SAE reserved
        * 0x0001 to 0x0DFF = VM specific
        * 0x0E00 to 0x0FFF = Reserved for addresses of client
        * 0x1000 to 0x7FFF = VM Specific
        * 0x8000 to 0xE3FF = Reserved
        * 0xE400 to 0xE3FF = VM defined functional group logical addresses
        * 0xF000 to 0xFFFF = Reserved
        """
        return self._client_logical_address

    @property
    def logical_address(self):
        """Logical address of DoIP entity

        Description: "Logical address of the responding DoIP entity."

        Values: See client_logical_address
        """
        return self._logical_address

    @property
    def response_code(self):
        """Routing activation response code

        Description: "Response by the DoIP gateway. Routing activation denial results
        in the TCP_DATA connection being reset by the DoIP gateway. Successful routing
        activation implies that diagnostic messages can now be routed over the TCP_DATA
        connection.
        """
        return RoutingActivationResponse.ResponseCode(self._response_code)

    @property
    def reserved(self):
        """Reserved value - 0x00000000"""
        return self._reserved

    @property
    def vm_specific(self):
        """Reserved for VM-specific use

        Description: "Available for additional VM-specific use."
        """
        return self._vm_specific


class DiagnosticMessage(DoIPMessage):
    """Diagnostic Message - see Table 21 "Payload type diagnostic message structure"

    Description: Wrapper for diagnostic (UDS) payloads. The same message is used for
    TX and RX, and the ECU will confirm receipt with either a DiagnosticMessageNegativeAcknowledgement
    or a DiagnosticMessagePositiveAcknowledgement message
    """

    payload_type = 0x8001

    _fields = ["source_address", "target_address", "user_data"]

    @classmethod
    def unpack(cls, payload_bytes, payload_length):
        return DiagnosticMessage(
            *struct.unpack_from("!HH", payload_bytes), payload_bytes[4:payload_length]
        )

    def pack(self):
        return (
            struct.pack("!HH", self._source_address, self._target_address)
            + self._user_data
        )

    def __init__(self, source_address, target_address, user_data):
        self._source_address = source_address
        self._target_address = target_address
        self._user_data = user_data

    @property
    def source_address(self):
        """Source address (SA)

        Description: "Contains the logical address of the sender of a diagnostic messag
        (e.g. the client DoIP entity address)."

        Values: From Table 13

        * 0x0000 = ISO/SAE reserved
        * 0x0001 to 0x0DFF = VM specific
        * 0x0E00 to 0x0FFF = Reserved for addresses of client
        * 0x1000 to 0x7FFF = VM Specific
        * 0x8000 to 0xE3FF = Reserved
        * 0xE400 to 0xE3FF = VM defined functional group logical addresses
        * 0xF000 to 0xFFFF = Reserved
        """
        return self._source_address

    @property
    def target_address(self):
        """Target address (TA)

        Description: "Contains the logical address of the receiver of a diagnostic message
        (e.g. a specific server DoIP entity on the vehicle’s networks)."

        Values: From Table 13

        * 0x0000 = ISO/SAE reserved
        * 0x0001 to 0x0DFF = VM specific
        * 0x0E00 to 0x0FFF = Reserved for addresses of client
        * 0x1000 to 0x7FFF = VM Specific
        * 0x8000 to 0xE3FF = Reserved
        * 0xE400 to 0xE3FF = VM defined functional group logical addresses
        * 0xF000 to 0xFFFF = Reserved
        """
        return self._target_address

    @property
    def user_data(self):
        """User data (UD)

        Description: Contains the actual diagnostic data (e.g. ISO 14229-1 diagnostic
        request), which shall be routed to the destination (e.g. the ECM).

        Values: Bytes/Bytearray
        """
        return self._user_data


class DiagnosticMessageNegativeAcknowledgement(DoIPMessage):
    """A negative acknowledgement of the previously received diagnostic (UDS) message.

    Indicates that the previously received diagnostic message was rejected. Reasons could
    include a message being too large, incorrect logical addresses, etc.

    See Table 25 - "Payload type diagnostic message negative acknowledgment structure"
    """

    payload_type = 0x8003

    _fields = ["source_address", "target_address", "nack_code", "previous_message_data"]

    class NackCodes(IntEnum):
        """Diagnostic message negative acknowledge codes (See Table 26)"""

        InvalidSourceAddress = 0x02
        UnknownTargetAddress = 0x03
        DiagnosticMessageTooLarge = 0x04
        OutOfMemory = 0x05
        TargetUnreachable = 0x06
        UnknownNetwork = 0x07
        TransportProtocolError = 0x08

    @classmethod
    def unpack(cls, payload_bytes, payload_length):
        return DiagnosticMessageNegativeAcknowledgement(
            *struct.unpack_from("!HHB", payload_bytes), payload_bytes[5:payload_length]
        )

    def pack(self):
        return (
            struct.pack(
                "!HHB", self._source_address, self._target_address, self._nack_code
            )
            + self._previous_message_data
        )

    def __init__(
        self,
        source_address,
        target_address,
        nack_code,
        previous_message_data=bytearray(),
    ):
        self._source_address = source_address
        self._target_address = target_address
        self._nack_code = nack_code
        self._previous_message_data = previous_message_data

    @property
    def source_address(self):
        """Source address (SA)

        Description: "Contains the logical address of the (intended) receiver of the previous
        diagnostic message (e.g. a specific server DoIP entity on the vehicle’s networks)."

        Values: From Table 13

        * 0x0000 = ISO/SAE reserved
        * 0x0001 to 0x0DFF = VM specific
        * 0x0E00 to 0x0FFF = Reserved for addresses of client
        * 0x1000 to 0x7FFF = VM Specific
        * 0x8000 to 0xE3FF = Reserved
        * 0xE400 to 0xE3FF = VM defined functional group logical addresses
        * 0xF000 to 0xFFFF = Reserved
        """
        return self._source_address

    @property
    def target_address(self):
        """Target address (TA)

        Description: "Contains the logical address of the sender of the previous diagnostic
        message (i.e. the client DoIP entity address)."

        Values: (See source_address)
        """
        return self._target_address

    @property
    def nack_code(self):
        """NACK code

        Indicates the reason the diagnostic message was rejected
        """
        return DiagnosticMessageNegativeAcknowledgement.NackCodes(self._nack_code)

    @property
    def previous_message_data(self):
        """Previous diagnostic message data

        An optional copy of the diagnostic message which is being acknowledged.
        """
        if self._previous_message_data:
            return self._previous_message_data
        else:
            return None


class DiagnosticMessagePositiveAcknowledgement(DoIPMessage):
    """A positive acknowledgement of the previously received diagnostic (UDS) message.

    "...indicates a correctly received diagnostic message, which is processed and put into the transmission
    buffer of the destination network."

    See Table 23 - "Payload type diagnostic message acknowledgement structure"
    """

    payload_type = 0x8002

    _fields = ["source_address", "target_address", "ack_code", "previous_message_data"]

    @classmethod
    def unpack(cls, payload_bytes, payload_length):
        return DiagnosticMessagePositiveAcknowledgement(
            *struct.unpack_from("!HHB", payload_bytes), payload_bytes[5:payload_length]
        )

    def pack(self):
        return (
            struct.pack(
                "!HHB", self._source_address, self._target_address, self._ack_code
            )
            + self._previous_message_data
        )

    def __init__(
        self,
        source_address,
        target_address,
        ack_code,
        previous_message_data=bytearray(),
    ):
        self._source_address = source_address
        self._target_address = target_address
        self._ack_code = ack_code
        self._previous_message_data = previous_message_data

    @property
    def source_address(self):
        """Source address (SA)

        Description: "Contains the logical address of the (intended) receiver of the previous
        diagnostic message (e.g. a specific server DoIP entity on the vehicle’s networks)."

        Values: From Table 13

        * 0x0000 = ISO/SAE reserved
        * 0x0001 to 0x0DFF = VM specific
        * 0x0E00 to 0x0FFF = Reserved for addresses of client
        * 0x1000 to 0x7FFF = VM Specific
        * 0x8000 to 0xE3FF = Reserved
        * 0xE400 to 0xE3FF = VM defined functional group logical addresses
        * 0xF000 to 0xFFFF = Reserved
        """
        return self._source_address

    @property
    def target_address(self):
        """Target address (TA)

        Description: "Contains the logical address of the sender of the previous diagnostic
        message (i.e. the client DoIP entity address)."

        Values: (See source_address)
        """
        return self._target_address

    @property
    def ack_code(self):
        """ACK code

        Values: Required to be 0x00. All other values are reserved
        """
        return self._ack_code

    @property
    def previous_message_data(self):
        """Previous diagnostic message data

        An optional copy of the diagnostic message which is being acknowledged.
        """
        if self._previous_message_data:
            return self._previous_message_data
        else:
            return None


class EntityStatusResponse(DoIPMessage):
    """DoIP entity status response. Table 11"""

    payload_type = 0x4002

    _fields = [
        "node_type",
        "max_concurrent_sockets",
        "currently_open_sockets",
        "max_data_size",
    ]

    @classmethod
    def unpack(cls, payload_bytes, payload_length):
        if payload_length == 3:
            return EntityStatusResponse(*struct.unpack_from("!BBB", payload_bytes))
        else:
            return EntityStatusResponse(*struct.unpack_from("!BBBL", payload_bytes))

    def pack(self):
        if self.max_data_size is None:
            return struct.pack(
                "!BBB",
                self._node_type,
                self._max_concurrent_sockets,
                self._currently_open_sockets,
            )
        else:
            return struct.pack(
                "!BBBL",
                self._node_type,
                self._max_concurrent_sockets,
                self._currently_open_sockets,
                self._max_data_size,
            )

    def __init__(
        self,
        node_type,
        max_concurrent_sockets,
        currently_open_sockets,
        max_data_size=None,
    ):
        self._node_type = node_type
        self._max_concurrent_sockets = max_concurrent_sockets
        self._currently_open_sockets = currently_open_sockets
        self._max_data_size = max_data_size

    @property
    def node_type(self):
        """Node type(NT)

        Description:
        "Identifies whether the contacted DoIP instance is either a DoIP node or a DoIP gateway."

        Values:

        * 0x00: DoIP gateway
        * 0x01: DoIP node
        * 0x02 .. 0xFF: reserved
        """
        return self._node_type

    @property
    def max_concurrent_sockets(self):
        """Max. concurrent TCP_DATA sockets (MCTS)

        Description:
        "Represents the maximum number of concurrent TCP_DATA sockets allowed with this DoIP entity,
        excluding the reserve socket required for socket handling."

        Values:
        1 to 255
        """
        return self._max_concurrent_sockets

    @property
    def currently_open_sockets(self):
        """Currently open TCP_DATA sockets (NCTS)

        Description: "Number of currently established sockets."

        Values:
        0 to 255
        """
        return self._currently_open_sockets

    @property
    def max_data_size(self):
        """Max. data size (MDS)

        Description: "Maximum size of one logical request that this DoIP entity can process."

        Values:
        0 to 4GB
        """
        return self._max_data_size


class VehicleIdentificationResponse(DoIPMessage):
    """Payload type vehicle announcement/identification response message Table 5"""

    payload_type = 0x0004

    _fields = [
        "vin",
        "logical_address",
        "eid",
        "gid",
        "further_action_required",
        "vin_sync_status",
    ]

    class SynchronizationStatusCodes(IntEnum):
        """VIN/GID synchronization status code values (Table 7)

        * 0x00 = VIN and/or GID are synchronized
        * 0x01 = Reserved
        * 0x10 = Incomplete: VIN and GID are not synchronized
        * 0x11..0xff = Reserved
        """

        Synchronized = 0x00
        Incomplete = 0x10

    class FurtherActionCodes(IntEnum):
        """Further Action Code Values (Table 6)

        * 0x00 = No further action required
        * 0x01 = Reserved
        * 0x10 = Routing activation required to initiate central security
        * 0x11..0xff = available for additional VM-specific use"""

        NoFurtherActionRequired = 0x00
        RoutingActivationRequired = 0x10

    @classmethod
    def unpack(cls, payload_bytes, payload_length):
        if payload_length == 33:
            return VehicleIdentificationResponse(
                *struct.unpack_from("!17sH6s6sBB", payload_bytes)
            )
        else:
            return VehicleIdentificationResponse(
                *struct.unpack_from("!17sH6s6sB", payload_bytes)
            )

    def pack(self):
        if self._vin_sync_status is not None:
            return struct.pack(
                "!17sH6s6sBB",
                self._vin.encode("ascii"),
                self._logical_address,
                self._eid,
                self._gid,
                self._further_action_required,
                self._vin_sync_status,
            )
        else:
            return struct.pack(
                "!17sH6s6sB",
                self._vin.encode("ascii"),
                self._logical_address,
                self._eid,
                self._gid,
                self._further_action_required,
            )

    def __init__(
        self,
        vin,
        logical_address,
        eid,
        gid,
        further_action_required,
        vin_gid_sync_status=None,
    ):
        self._vin = vin
        self._logical_address = logical_address
        self._eid = eid
        self._gid = gid
        self._further_action_required = further_action_required
        self._vin_sync_status = vin_gid_sync_status

    @property
    def vin(self):
        """VIN

        Description: "This is the vehicle’s VIN as specified in ISO 3779. If the VIN is not configured at the time
        of transmission of this message, this should be indicated using the invalidity value {0x00 or 0xff}... In
        this case, the GID is used to associate DoIP nodes with a certain vehicle..."

        Values: ASCII
        """
        if type(self._vin) is bytes:
            return self._vin.decode("ascii")
        else:
            return self._vin

    @property
    def logical_address(self):
        """Logical Address

        Description: "This is the logical address that is assigned to the responding DoIP entity (see 7. 8 for further
        details). The logical address can be used, for example, to address diagnostic requests directly to the DoIP
        entity."

        Values:
        From Table 13

        * 0x0000 = ISO/SAE reserved
        * 0x0001 to 0x0DFF = VM specific
        * 0x0E00 to 0x0FFF = Reserved for addresses of client
        * 0x1000 to 0x7FFF = VM Specific
        * 0x8000 to 0xE3FF = Reserved
        * 0xE400 to 0xE3FF = VM defined functional group logical addresses
        * 0xF000 to 0xFFFF = Reserved
        """
        return self._logical_address

    @property
    def eid(self):
        """EID

        Description: "This is a unique identification of the DoIP entities in order to separate their responses
        even before the VIN is programmed to, or recognized by, the DoIP devices (e.g. during the vehicle assembly
        process). It is recommended that the MAC address information of the DoIP entity's network interface be
        used (one of the interfaces if multiple network interfaces are implemented)."

        Values: "Not set" values are 0x00 or 0xff.
        """
        return self._eid

    @property
    def gid(self):
        """GID

        Description: "This is a unique identification of a group of DoIP entities within the same vehicle in the
        case that a VIN is not configured for that vehicle... If the GID is not available at the time of
        transmission of this message, this shall be indicated using the specific invalidity" ("not set") value
        of 0x00 or 0xff.
        """
        return self._gid

    @property
    def further_action_required(self):
        """Further action required

        Description: "This is the additional information to notify the client DoIP entity that there are either
        DoIP entities with no initial connectivity or that a centralized security approach is used."
        """
        return VehicleIdentificationResponse.FurtherActionCodes(
            self._further_action_required
        )

    @property
    def vin_sync_status(self):
        """VIN/GID sync. status

        Description: "This is the additional information to notify the client DoIP entity that all DoIP entities
        have synchronized their information about the VIN or GID of the vehicle"
        """
        if self._vin_sync_status is not None:
            return VehicleIdentificationResponse.SynchronizationStatusCodes(
                self._vin_sync_status
            )
        else:
            return None


payload_type_to_message = {
    0x0000: GenericDoIPNegativeAcknowledge,
    0x0001: VehicleIdentificationRequest,
    0x0002: VehicleIdentificationRequestWithEID,
    0x0003: VehicleIdentificationRequestWithVIN,
    0x0004: VehicleIdentificationResponse,
    0x0005: RoutingActivationRequest,
    0x0006: RoutingActivationResponse,
    0x0007: AliveCheckRequest,
    0x0008: AliveCheckResponse,
    0x4001: DoipEntityStatusRequest,
    0x4002: EntityStatusResponse,
    0x4003: DiagnosticPowerModeRequest,
    0x4004: DiagnosticPowerModeResponse,
    0x8001: DiagnosticMessage,
    0x8002: DiagnosticMessagePositiveAcknowledgement,
    0x8003: DiagnosticMessageNegativeAcknowledgement,
}

payload_message_to_type = {
    message: payload_type for payload_type, message in payload_type_to_message.items()
}
