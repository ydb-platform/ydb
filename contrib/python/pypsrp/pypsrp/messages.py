# Copyright: (c) 2018, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import logging
import struct
import typing
import uuid
import warnings
import xml.etree.ElementTree as ET

from pypsrp._utils import to_string
from pypsrp.complex_objects import (
    ApartmentState,
    CommandType,
    ComplexObject,
    DictionaryMeta,
    ErrorRecord,
    GenericComplexObject,
    HostInfo,
    HostMethodIdentifier,
    InformationalRecord,
    ListMeta,
    ObjectMeta,
    Pipeline,
    ProgressRecordType,
    PSThreadOptions,
    RemoteStreamOptions,
)
from pypsrp.exceptions import SerializationError

if typing.TYPE_CHECKING:
    from pypsrp.serializer import Serializer

log = logging.getLogger(__name__)


class Destination(object):
    # The destination of a PSRP message
    CLIENT = 0x00000001
    SERVER = 0x00000002


class MessageType(object):
    """
    [MS-PSRP] 2.2.1 PowerShell Remoting Protocol Message - MessageType
    https://msdn.microsoft.com/en-us/library/dd303832.aspx

    Identifier of the message contained within a PSRP message
    """

    SESSION_CAPABILITY = 0x00010002
    INIT_RUNSPACEPOOL = 0x00010004
    PUBLIC_KEY = 0x00010005
    ENCRYPTED_SESSION_KEY = 0x00010006
    PUBLIC_KEY_REQUEST = 0x00010007
    CONNECT_RUNSPACEPOOL = 0x00010008
    RUNSPACEPOOL_INIT_DATA = 0x002100B
    RESET_RUNSPACE_STATE = 0x0002100C
    SET_MAX_RUNSPACES = 0x00021002
    SET_MIN_RUNSPACES = 0x00021003
    RUNSPACE_AVAILABILITY = 0x00021004
    RUNSPACEPOOL_STATE = 0x00021005
    CREATE_PIPELINE = 0x00021006
    GET_AVAILABLE_RUNSPACES = 0x00021007
    USER_EVENT = 0x00021008
    APPLICATION_PRIVATE_DATA = 0x00021009
    GET_COMMAND_METADATA = 0x0002100A
    RUNSPACEPOOL_HOST_CALL = 0x00021100
    RUNSPACEPOOL_HOST_RESPONSE = 0x00021101
    PIPELINE_INPUT = 0x00041002
    END_OF_PIPELINE_INPUT = 0x00041003
    PIPELINE_OUTPUT = 0x00041004
    ERROR_RECORD = 0x00041005
    PIPELINE_STATE = 0x00041006
    DEBUG_RECORD = 0x00041007
    VERBOSE_RECORD = 0x00041008
    WARNING_RECORD = 0x00041009
    PROGRESS_RECORD = 0x00041010
    INFORMATION_RECORD = 0x00041011
    PIPELINE_HOST_CALL = 0x00041100
    PIPELINE_HOST_RESPONSE = 0x00041101


class Message(object):
    def __init__(
        self,
        destination: int,
        rpid: typing.Optional[str],
        pid: typing.Optional[str],
        data: typing.Union[ComplexObject, "PipelineOutput", "PublicKeyRequest"],
        serializer: "Serializer",
    ):
        """
        [MS-PSRP] 2.2.1 PowerShell Remoting Protocol Message
        https://msdn.microsoft.com/en-us/library/dd303832.aspx

        Used to contain a PSRP message in the structure required by PSRP.

        :param destination: The Destination of the message
        :param rpid: The uuid representation of the RunspacePool
        :param pid: The uuid representation of the PowerShell pipeline
        :param data: The PSRP Message object
        :param serializer: The serializer object used to serialize the message
            when packing
        """
        self.destination = destination
        self.message_type = data.MESSAGE_TYPE  # type: ignore[union-attr]  # There's no special type right now

        empty_uuid = uuid.UUID(bytes=b"\x00" * 16)
        self.rpid = uuid.UUID(rpid) if rpid is not None else empty_uuid
        self.pid = uuid.UUID(pid) if pid is not None else empty_uuid
        self.data = data
        self._serializer = serializer

    def pack(self) -> bytes:
        msg: typing.Union[ET.Element, bytes]
        if self.message_type == MessageType.PUBLIC_KEY_REQUEST:
            msg = ET.Element("S")
        elif self.message_type == MessageType.END_OF_PIPELINE_INPUT:
            msg = b""
        elif self.message_type == MessageType.PIPELINE_INPUT:
            pipeline_input = typing.cast(PipelineInput, self.data)
            serialized_input = self._serializer.serialize(pipeline_input.data)
            msg = serialized_input if serialized_input is not None else b""
        elif self.message_type == MessageType.CONNECT_RUNSPACEPOOL and (
            self.data.min_runspaces is None and self.data.max_runspaces is None  # type: ignore[union-attr] # Checked with message_type
        ):
            msg = ET.Element("S")
        else:
            serialized_msg = self._serializer.serialize(self.data)
            msg = serialized_msg if serialized_msg is not None else b""

        if not isinstance(msg, bytes):
            message_data = ET.tostring(msg, encoding="utf-8", method="xml")
        else:
            message_data = msg
        log.debug("Packing PSRP message: %s" % to_string(message_data))

        data = struct.pack("<I", self.destination)
        data += struct.pack("<I", self.message_type)

        # .NET stores uuids/guids in bytes in the little endian form
        data += self.rpid.bytes_le
        data += self.pid.bytes_le
        data += message_data

        return data

    @staticmethod
    def unpack(
        data: bytes,
        serializer: "Serializer",
    ) -> "Message":
        destination = struct.unpack("<I", data[0:4])[0]
        message_type = struct.unpack("<I", data[4:8])[0]
        rpid = str(uuid.UUID(bytes_le=data[8:24]))
        pid = str(uuid.UUID(bytes_le=data[24:40]))

        if data[40:43] == b"\xef\xbb\xbf":
            # 40-43 is the UTF-8 BOM which we don't care about
            message_data = to_string(data[43:])
        else:
            message_data = to_string(data[40:])

        log.debug("Unpacking PSRP message of type %d: %s" % (message_type, message_data))

        message_obj = {
            MessageType.SESSION_CAPABILITY: SessionCapability,
            MessageType.INIT_RUNSPACEPOOL: InitRunspacePool,
            MessageType.PUBLIC_KEY: PublicKey,
            MessageType.ENCRYPTED_SESSION_KEY: EncryptedSessionKey,
            MessageType.PUBLIC_KEY_REQUEST: PublicKeyRequest,
            MessageType.SET_MAX_RUNSPACES: SetMaxRunspaces,
            MessageType.SET_MIN_RUNSPACES: SetMinRunspaces,
            MessageType.RUNSPACE_AVAILABILITY: RunspaceAvailability,
            MessageType.RUNSPACEPOOL_STATE: RunspacePoolStateMessage,
            MessageType.CREATE_PIPELINE: CreatePipeline,
            MessageType.GET_AVAILABLE_RUNSPACES: GetAvailableRunspaces,
            MessageType.USER_EVENT: UserEvent,
            MessageType.APPLICATION_PRIVATE_DATA: ApplicationPrivateData,
            MessageType.GET_COMMAND_METADATA: GetCommandMetadata,
            MessageType.RUNSPACEPOOL_HOST_CALL: RunspacePoolHostCall,
            MessageType.RUNSPACEPOOL_HOST_RESPONSE: RunspacePoolHostResponse,
            MessageType.PIPELINE_INPUT: PipelineInput,
            MessageType.END_OF_PIPELINE_INPUT: EndOfPipelineInput,
            MessageType.PIPELINE_OUTPUT: PipelineOutput,
            MessageType.ERROR_RECORD: ErrorRecordMessage,
            MessageType.PIPELINE_STATE: PipelineState,
            MessageType.DEBUG_RECORD: DebugRecord,
            MessageType.VERBOSE_RECORD: VerboseRecord,
            MessageType.WARNING_RECORD: WarningRecord,
            MessageType.PROGRESS_RECORD: ProgressRecord,
            MessageType.INFORMATION_RECORD: InformationRecord,
            MessageType.PIPELINE_HOST_CALL: PipelineHostCall,
            MessageType.PIPELINE_HOST_RESPONSE: PipelineHostResponse,
            MessageType.CONNECT_RUNSPACEPOOL: ConnectRunspacePool,
            MessageType.RUNSPACEPOOL_INIT_DATA: RunspacePoolInitData,
            MessageType.RESET_RUNSPACE_STATE: ResetRunspaceState,
        }[message_type]

        # PIPELINE_OUTPUT is a weird one, it contains the actual output objects
        # not encapsulated so we set it to a dynamic object and the serializer
        # will work out what is best
        message: typing.Union[ComplexObject, "PipelineOutput", "PublicKeyRequest"]
        if message_type == MessageType.PIPELINE_OUTPUT:
            # try to deserialize using our known objects, if that fails then
            # we want to get a generic object at least but raise a warning
            try:
                message_data = serializer.deserialize(message_data)
            except SerializationError as err:
                warnings.warn(
                    "Failed to deserialize msg, trying to deserialize as generic complex object: %s" % str(err)
                )
                meta = ObjectMeta("ObjDynamic", object=GenericComplexObject)
                message_data = serializer.deserialize(message_data, meta)
            message = PipelineOutput()
            message.data = message_data
        elif message_type == MessageType.PIPELINE_INPUT:
            message_data = serializer.deserialize(message_data)
            message = PipelineInput()
            message.data = message_data
        elif message_type == MessageType.PUBLIC_KEY_REQUEST:
            message = PublicKeyRequest()
        else:
            message_meta = ObjectMeta("Obj", object=message_obj)
            message = serializer.deserialize(message_data, message_meta)

        return Message(destination, rpid, pid, message, serializer)


class SessionCapability(ComplexObject):
    MESSAGE_TYPE = MessageType.SESSION_CAPABILITY

    def __init__(
        self,
        protocol_version: typing.Optional[str] = None,
        ps_version: typing.Optional[str] = None,
        serialization_version: typing.Optional[str] = None,
        time_zone: typing.Optional[bytes] = None,
    ) -> None:
        """
        [MS-PSRP] 2.2.2.1 SESSION_CAPABILITY Message
        https://msdn.microsoft.com/en-us/library/dd340636.aspx

        :param protocol_version: The PSRP version
        :param ps_version: The PowerShell version
        :param serialization_version: The serialization version
        :param time_zone: Time Zone information of the host, should be a byte
            string
        """
        super(SessionCapability, self).__init__()
        self._extended_properties = (
            ("protocol_version", ObjectMeta("Version", name="protocolversion")),
            ("ps_version", ObjectMeta("Version", name="PSVersion")),
            ("serialization_version", ObjectMeta("Version", name="SerializationVersion")),
            ("time_zone", ObjectMeta("BA", name="TimeZone", optional=True)),
        )
        self.protocol_version = protocol_version
        self.ps_version = ps_version
        self.serialization_version = serialization_version
        self.time_zone = time_zone


class InitRunspacePool(ComplexObject):
    MESSAGE_TYPE = MessageType.INIT_RUNSPACEPOOL

    def __init__(
        self,
        min_runspaces: typing.Optional[int] = None,
        max_runspaces: typing.Optional[int] = None,
        thread_options: typing.Optional[PSThreadOptions] = None,
        apartment_state: typing.Optional[ApartmentState] = None,
        host_info: typing.Optional[HostInfo] = None,
        application_arguments: typing.Optional[typing.Dict] = None,
    ) -> None:
        """
        [MS-PSRP] 2.2.2.2 INIT_RUNSPACEPOOL Message
        https://msdn.microsoft.com/en-us/library/dd359645.aspx

        :param min_runspaces: The minimum number of runspaces in the pool
        :param max_runspaces: The maximum number of runspaces in the pool
        :param thread_options: Thread options provided by the higher layer
        :param apartment_state: Apartment state provided by the higher layer
        :param host_info: The client's HostInfo details
        :param application_arguments: Application arguments provided by a
            higher layer, stored in the $PSSenderInfo variable in the pool
        """
        super(InitRunspacePool, self).__init__()
        self._extended_properties = (
            ("min_runspaces", ObjectMeta("I32", name="MinRunspaces")),
            ("max_runspaces", ObjectMeta("I32", name="MaxRunspaces")),
            ("thread_options", ObjectMeta("Obj", name="PSThreadOptions", object=PSThreadOptions)),
            ("apartment_state", ObjectMeta("Obj", name="ApartmentState", object=ApartmentState)),
            ("host_info", ObjectMeta("Obj", name="HostInfo", object=HostInfo)),
            (
                "application_arguments",
                DictionaryMeta(
                    name="ApplicationArguments",
                    dict_types=[
                        "System.Management.Automation.PSPrimitiveDictionary",
                        "System.Collections.Hashtable",
                        "System.Object",
                    ],
                ),
            ),
        )
        self.min_runspaces = min_runspaces
        self.max_runspaces = max_runspaces
        self.thread_options = thread_options
        self.apartment_state = apartment_state
        self.host_info = host_info
        self.application_arguments = application_arguments


class PublicKey(ComplexObject):
    MESSAGE_TYPE = MessageType.PUBLIC_KEY

    def __init__(
        self,
        public_key: typing.Optional[str] = None,
    ) -> None:
        """
        [MS-PSRP] 2.2.2.3 PUBLIC_KEY Message
        https://msdn.microsoft.com/en-us/library/dd644859.aspx

        :param public_key: The Base64 encoding of the public key in the PKCS1
            format.
        """
        super(PublicKey, self).__init__()
        self._extended_properties = (("public_key", ObjectMeta("S", name="PublicKey")),)
        self.public_key = public_key


class EncryptedSessionKey(ComplexObject):
    MESSAGE_TYPE = MessageType.ENCRYPTED_SESSION_KEY

    def __init__(self, session_key=None):
        """
        [MS-PSRP] 2.2.2.4 ENCRYPTED_SESSION_KEY Message
        https://msdn.microsoft.com/en-us/library/dd644930.aspx

        :param session_key: The 256-bit key for AES encryption that has been
            encrypted using the public key from the PUBLIC_KEY message using
            the RSAES-PKCS-v1_5 encryption scheme and then Base64 formatted.
        """
        super(EncryptedSessionKey, self).__init__()
        self._extended_properties = (("session_key", ObjectMeta("S", name="EncryptedSessionKey")),)
        self.session_key = session_key


class PublicKeyRequest(object):
    MESSAGE_TYPE = MessageType.PUBLIC_KEY_REQUEST

    def __init__(self) -> None:
        """
        [MS-PSRP] 2.2.2.5 PUBLIC_KEY_REQUEST Message
        https://msdn.microsoft.com/en-us/library/dd644906.aspx
        """
        super(PublicKeyRequest, self).__init__()


class SetMaxRunspaces(ComplexObject):
    MESSAGE_TYPE = MessageType.SET_MAX_RUNSPACES

    def __init__(
        self,
        max_runspaces: typing.Optional[int] = None,
        ci: typing.Optional[int] = None,
    ) -> None:
        """
        [MS-PSRP] 2.2.2.6 SET_MAX_RUNSPACES Message
        https://msdn.microsoft.com/en-us/library/dd304870.aspx

        :param max_runspaces: The maximum number of runspaces
        :param ci: The ci identifier for the CI table
        """
        super(SetMaxRunspaces, self).__init__()
        self._extended_properties = (
            ("max_runspaces", ObjectMeta("I32", name="MaxRunspaces")),
            ("ci", ObjectMeta("I64", name="CI")),
        )
        self.max_runspaces = max_runspaces
        self.ci = ci


class SetMinRunspaces(ComplexObject):
    MESSAGE_TYPE = MessageType.SET_MIN_RUNSPACES

    def __init__(
        self,
        min_runspaces: typing.Optional[int] = None,
        ci: typing.Optional[int] = None,
    ) -> None:
        """
        [MS-PSRP] 2.2.2.7 SET_MIN_RUNSPACES Message
        https://msdn.microsoft.com/en-us/library/dd340570.aspx

        :param max_runspaces: The minimum number of runspaces
        :param ci: The ci identifier for the CI table
        """
        super(SetMinRunspaces, self).__init__()
        self._extended_properties = (
            ("min_runspaces", ObjectMeta("I32", name="MinRunspaces")),
            ("ci", ObjectMeta("I64", name="CI")),
        )
        self.min_runspaces = min_runspaces
        self.ci = ci


class RunspaceAvailability(ComplexObject):
    MESSAGE_TYPE = MessageType.RUNSPACE_AVAILABILITY

    def __init__(self, response=None, ci=None):
        """
        [MS-PSRP] 2.2.2.8 RUNSPACE_AVAILABILITY Message
        https://msdn.microsoft.com/en-us/library/dd359229.aspx

        :param response: The response from the server
        :param ci: The ci identifier for the CI table
        """
        super(RunspaceAvailability, self).__init__()
        self._extended_properties = (
            ("response", ObjectMeta(name="SetMinMaxRunspacesResponse")),
            ("ci", ObjectMeta("I64", name="ci")),
        )
        self.response = response
        self.ci = ci


class RunspacePoolStateMessage(ComplexObject):
    MESSAGE_TYPE = MessageType.RUNSPACEPOOL_STATE

    def __init__(self, state=None, error_record=None):
        """
        [MS-PSRP] 2.2.2.9 RUNSPACEPOOL_STATE Message
        https://msdn.microsoft.com/en-us/library/dd303020.aspx

        :param state: The state of the runspace pool
        :param error_record:
        """
        super(RunspacePoolStateMessage, self).__init__()
        self._extended_properties = (
            ("state", ObjectMeta("I32", name="RunspaceState")),
            ("error_record", ObjectMeta("Obj", optional=True, object=ErrorRecord)),
        )
        self.state = state
        self.error_record = error_record


class CreatePipeline(ComplexObject):
    MESSAGE_TYPE = MessageType.CREATE_PIPELINE

    def __init__(
        self,
        no_input: typing.Optional[bool] = None,
        apartment_state: typing.Optional[ApartmentState] = None,
        remote_stream_options: typing.Optional[RemoteStreamOptions] = None,
        add_to_history: typing.Optional[bool] = None,
        host_info: typing.Optional[HostInfo] = None,
        pipeline: typing.Optional[Pipeline] = None,
        is_nested: typing.Optional[bool] = None,
    ) -> None:
        """
        [MS-PSRP] 2.2.2.10 CREATE_PIPELINE Message
        https://msdn.microsoft.com/en-us/library/dd340567.aspx

        :param no_input: Whether the pipeline will take input
        :param apartment_state: The ApartmentState of the pipeline
        :param remote_stream_options: The RemoteStreamOptions of the pipeline
        :param add_to_history: Whether to add the pipeline being execute to
            the history field of the runspace
        :param host_info: The HostInformation of the pipeline
        :param pipeline: The PowerShell object to create
        :param is_nested: Whether the pipeline is run in nested or
            steppable mode
        """
        super(CreatePipeline, self).__init__()
        self._extended_properties = (
            ("no_input", ObjectMeta("B", name="NoInput")),
            ("apartment_state", ObjectMeta("Obj", name="ApartmentState", object=ApartmentState)),
            ("remote_stream_options", ObjectMeta("Obj", name="RemoteStreamOptions", object=RemoteStreamOptions)),
            ("add_to_history", ObjectMeta("B", name="AddToHistory")),
            ("host_info", ObjectMeta("Obj", name="HostInfo", object=HostInfo)),
            ("pipeline", ObjectMeta("Obj", name="PowerShell", object=Pipeline)),
            ("is_nested", ObjectMeta("B", name="IsNested")),
        )

        self.no_input = no_input
        self.apartment_state = apartment_state
        self.remote_stream_options = remote_stream_options
        self.add_to_history = add_to_history
        self.host_info = host_info
        self.pipeline = pipeline
        self.is_nested = is_nested


class GetAvailableRunspaces(ComplexObject):
    MESSAGE_TYPE = MessageType.GET_AVAILABLE_RUNSPACES

    def __init__(
        self,
        ci: typing.Optional[int] = None,
    ) -> None:
        """
        [MS-PSRP] 2.2.2.11 GET_AVAILABLE_RUNSPACES Message
        https://msdn.microsoft.com/en-us/library/dd357512.aspx

        :param ci: The ci identifier for the CI table
        """
        super(GetAvailableRunspaces, self).__init__()
        self._extended_properties = (("ci", ObjectMeta("I64", name="ci")),)
        self.ci = ci


class UserEvent(ComplexObject):
    MESSAGE_TYPE = MessageType.USER_EVENT

    def __init__(
        self,
        event_id=None,
        source_id=None,
        time=None,
        sender=None,
        args=None,
        data=None,
        computer=None,
        runspace_id=None,
    ):
        """
        [MS-PSRP] 2.2.2.12 USER_EVENT Message
        https://msdn.microsoft.com/en-us/library/dd359395.aspx

        :param event_id:
        :param source_id:
        :param time:
        :param sender:
        :param args:
        :param data:
        :param computer:
        :param runspace_id:
        """
        super(UserEvent, self).__init__()
        self._extended_properties = (
            ("event_id", ObjectMeta("I32", name="PSEventArgs.EventIdentifier")),
            ("source_id", ObjectMeta("S", name="PSEventArgs.SourceIdentifier")),
            ("time", ObjectMeta("DT", name="PSEventArgs.TimeGenerated")),
            ("sender", ObjectMeta(name="PSEventArgs.Sender")),
            ("args", ObjectMeta(name="PSEventArgs.SourceArgs")),
            ("data", ObjectMeta(name="PSEventArgs.MessageData")),
            ("computer", ObjectMeta("S", name="PSEventArgs.ComputerName")),
            ("runspace_id", ObjectMeta("G", name="PSEventArgs.RunspaceId")),
        )
        self.event_id = event_id
        self.source_id = source_id
        self.time = time
        self.sender = sender
        self.args = args
        self.data = data
        self.computer = computer
        self.runspace_id = runspace_id


class ApplicationPrivateData(ComplexObject):
    MESSAGE_TYPE = MessageType.APPLICATION_PRIVATE_DATA

    def __init__(self, data=None):
        """
        [MS-PSRP] 2.2.2.13 APPLICATION_PRIVATE_DATA Message
        https://msdn.microsoft.com/en-us/library/dd644934.aspx

        :param data: A dict that contains data to sent to the PowerShell layer
        """
        super(ApplicationPrivateData, self).__init__()
        self._extended_properties = (("data", DictionaryMeta(name="ApplicationPrivateData")),)
        self.data = data


class GetCommandMetadata(ComplexObject):
    MESSAGE_TYPE = MessageType.GET_COMMAND_METADATA

    def __init__(
        self,
        names: typing.Optional[typing.List[str]] = None,
        command_type: typing.Optional[int] = None,
        namespace: typing.Optional[typing.List[str]] = None,
        argument_list: typing.Optional[typing.List] = None,
    ) -> None:
        """
        [MS-PSRP] 2.2.2.14 GET_COMMAND_METADATA Message
        https://msdn.microsoft.com/en-us/library/ee175985.aspx

        :param names:
        :param command_type:
        :param namespace:
        :param argument_list:
        """
        super(GetCommandMetadata, self).__init__()
        self._extended_properties = (
            (
                "names",
                ListMeta(
                    name="Name",
                    list_value_meta=ObjectMeta("S"),
                    list_types=["System.String[]", "System.Array", "System.Object"],
                ),
            ),
            ("command_type", ObjectMeta(name="CommandType", object=CommandType)),
            ("namespace", ObjectMeta(name="Namespace")),
            ("argument_list", ListMeta(name="ArgumentList")),
        )
        self.names = names
        self.command_type = command_type
        self.namespace = namespace
        self.argument_list = argument_list


class RunspacePoolHostCall(ComplexObject):
    MESSAGE_TYPE = MessageType.RUNSPACEPOOL_HOST_CALL

    def __init__(self, ci=None, mi=None, mp=None):
        """
        [MS-PSRP] 2.2.2.15 RUNSPACE_HOST_CALL Message
        https://msdn.microsoft.com/en-us/library/dd340830.aspx

        :param ci:
        :param mi:
        :param mp:
        """
        super(RunspacePoolHostCall, self).__init__()
        self._extended_properties = (
            ("ci", ObjectMeta("I64", name="ci")),
            ("mi", ObjectMeta("Obj", name="mi", object=HostMethodIdentifier)),
            ("mp", ListMeta(name="mp")),
        )
        self.ci = ci
        self.mi = mi
        self.mp = mp


class RunspacePoolHostResponse(ComplexObject):
    MESSAGE_TYPE = MessageType.RUNSPACEPOOL_HOST_RESPONSE

    def __init__(self, ci=None, mi=None, mr=None, me=None):
        """
        [MS-PSRP] 2.2.2.16 RUNSPACEPOOL_HOST_RESPONSE Message
        https://msdn.microsoft.com/en-us/library/dd358453.aspx

        :param ci:
        :param mi:
        :param mr:
        :param me:
        """
        super(RunspacePoolHostResponse, self).__init__()
        self._extended_properties = (
            ("ci", ObjectMeta("I64", name="ci")),
            ("mi", ObjectMeta("Obj", name="mi", object=HostMethodIdentifier)),
            ("mr", ObjectMeta(name="mr")),
            ("me", ObjectMeta("Obj", name="me", object=ErrorRecord, optional=True)),
        )
        self.ci = ci
        self.mi = mi
        self.mr = mr
        self.me = me


class PipelineInput(ComplexObject):
    MESSAGE_TYPE = MessageType.PIPELINE_INPUT

    def __init__(
        self,
        data: typing.Any = None,
    ) -> None:
        """
        [MS-PSRP] 2.2.2.17 PIPELINE_INPUT Message
        https://msdn.microsoft.com/en-us/library/dd340525.aspx

        :param data: The data to serialize and send as the input
        """
        super(PipelineInput, self).__init__()
        self.data = data


class EndOfPipelineInput(ComplexObject):
    MESSAGE_TYPE = MessageType.END_OF_PIPELINE_INPUT

    def __init__(self) -> None:
        """
        [MS-PSRP] 2.2.2.18 END_OF_PIPELINE_INPUT Message
        https://msdn.microsoft.com/en-us/library/dd342785.aspx
        """
        super(EndOfPipelineInput, self).__init__()


class PipelineOutput(object):
    MESSAGE_TYPE = MessageType.PIPELINE_OUTPUT

    def __init__(
        self,
        data: typing.Any = None,
    ) -> None:
        """
        [MS-PSRP] 2.2.2.19 PIPELINE_OUTPUT Message
        https://msdn.microsoft.com/en-us/library/dd357371.aspx
        """
        super(PipelineOutput, self).__init__()
        self.data = data


class ErrorRecordMessage(ErrorRecord):
    MESSAGE_TYPE = MessageType.ERROR_RECORD

    def __init__(self, **kwargs):
        """
        [MS-PSRP] 2.2.2.20 ERROR_RECORD Message
        https://msdn.microsoft.com/en-us/library/dd342423.aspx

        :param kwargs:
        """
        super(ErrorRecordMessage, self).__init__(**kwargs)


class PipelineState(ComplexObject):
    MESSAGE_TYPE = MessageType.PIPELINE_STATE

    def __init__(self, state=None, error_record=None):
        """
        [MS-PSRP] 2.2.2.21 PIPELINE_STATE Message
        https://msdn.microsoft.com/en-us/library/dd304923.aspx

        :param state: The state of the pipeline
        :param error_record:
        """
        super(PipelineState, self).__init__()
        self._extended_properties = (
            ("state", ObjectMeta("I32", name="PipelineState")),
            ("error_record", ObjectMeta("Obj", name="ExceptionAsErrorRecord", optional=True)),
        )
        self.state = state
        self.error_record = error_record


class DebugRecord(InformationalRecord):
    MESSAGE_TYPE = MessageType.DEBUG_RECORD

    def __init__(self, **kwargs):
        """
        [MS-PSRP] 2.2.2.22 DEBUG_RECORD Message
        https://msdn.microsoft.com/en-us/library/dd340758.aspx
        """
        super(DebugRecord, self).__init__(**kwargs)
        self._types.insert(0, "System.Management.Automation.DebugRecord")


class VerboseRecord(InformationalRecord):
    MESSAGE_TYPE = MessageType.VERBOSE_RECORD

    def __init__(self, **kwargs):
        """
        [MS-PSRP] 2.2.2.23 VERBOSE_RECORD Message
        https://msdn.microsoft.com/en-us/library/dd342930.aspx
        """
        super(VerboseRecord, self).__init__(**kwargs)
        self._types.insert(0, "System.Management.Automation.VerboseRecord")


class WarningRecord(InformationalRecord):
    MESSAGE_TYPE = MessageType.WARNING_RECORD

    def __init__(self, **kwargs):
        """
        [MS-PSRP] 2.2.2.24 WARNING_RECORD Message
        https://msdn.microsoft.com/en-us/library/dd303590.aspx
        """
        super(WarningRecord, self).__init__(**kwargs)
        self._types.insert(0, "System.Management.Automation.WarningRecord")


class ProgressRecord(ComplexObject):
    MESSAGE_TYPE = MessageType.PROGRESS_RECORD

    def __init__(
        self,
        activity=None,
        activity_id=None,
        description=None,
        current_operation=None,
        parent_activity_id=None,
        percent_complete=None,
        progress_type=None,
        seconds_remaining=None,
    ):
        """
        [MS-PSRP] 2.2.2.25 PROGRESS_RECORD Message
        https://msdn.microsoft.com/en-us/library/dd340751.aspx

        :param kwargs:
        """
        super(ProgressRecord, self).__init__()
        self._extended_properties = (
            ("activity", ObjectMeta("S", name="Activity")),
            ("activity_id", ObjectMeta("I32", name="ActivityId")),
            ("description", ObjectMeta("S", name="StatusDescription")),
            ("current_operation", ObjectMeta("S", name="CurrentOperation")),
            ("parent_activity_id", ObjectMeta("I32", name="ParentActivityId")),
            ("percent_complete", ObjectMeta("I32", name="PercentComplete")),
            ("progress_type", ObjectMeta("Obj", name="Type", object=ProgressRecordType)),
            ("seconds_remaining", ObjectMeta("I32", name="SecondsRemaining")),
        )
        self.activity = activity
        self.activity_id = activity_id
        self.description = description
        self.current_operation = current_operation
        self.parent_activity_id = parent_activity_id
        self.percent_complete = percent_complete
        self.progress_type = progress_type
        self.seconds_remaining = seconds_remaining


class InformationRecord(ComplexObject):
    MESSAGE_TYPE = MessageType.INFORMATION_RECORD

    def __init__(
        self,
        message_data=None,
        source=None,
        time_generated=None,
        tags=None,
        user=None,
        computer=None,
        pid=None,
        native_thread_id=None,
        managed_thread_id=None,
        write_information_stream=None,
    ):
        """
        [MS-PSRP] 2.2.2.26 INFORMATION_RECORD Message
        https://msdn.microsoft.com/en-us/library/mt224023.aspx

        Only in protocol_version 2.3 and above

        :param kwargs:
        """
        super(InformationRecord, self).__init__()
        self._types = ["System.Management.Automation.InformationRecord", "System.Object"]
        self._extended_properties = (
            ("message_data", ObjectMeta(name="MessageData")),
            ("source", ObjectMeta("S", name="Source")),
            ("time_generated", ObjectMeta("DT", name="TimeGenerated")),
            ("tags", ListMeta(name="Tags", list_value_meta=ObjectMeta("S"))),
            ("user", ObjectMeta("S", name="User")),
            ("computer", ObjectMeta("S", name="Computer")),
            ("pid", ObjectMeta("U32", name="ProcessId")),
            ("native_thread_id", ObjectMeta("U32", name="NativeThreadId")),
            ("managed_thread_id", ObjectMeta("U32", name="ManagedThreadId")),
            ("write_information_stream", ObjectMeta("B", name="WriteInformationStream", optional=True)),
        )
        self.message_data = message_data
        self.source = source
        self.time_generated = time_generated
        self.tags = tags
        self.user = user
        self.computer = computer
        self.pid = pid
        self.native_thread_id = native_thread_id
        self.managed_thread_id = managed_thread_id
        self.write_information_stream = write_information_stream


class PipelineHostCall(RunspacePoolHostCall):
    MESSAGE_TYPE = MessageType.PIPELINE_HOST_CALL
    """
    [MS-PSRP] 2.2.2.27 PIPELINE_HOST_CALL Message
    https://msdn.microsoft.com/en-us/library/dd356915.aspx
    """


class PipelineHostResponse(RunspacePoolHostResponse):
    MESSAGE_TYPE = MessageType.PIPELINE_HOST_RESPONSE
    """
    [MS-PSRP] 2.2.2.28 PIPELINE_HOST_RESPONSE Message
    https://msdn.microsoft.com/en-us/library/dd306168.aspx
    """


class ConnectRunspacePool(ComplexObject):
    MESSAGE_TYPE = MessageType.CONNECT_RUNSPACEPOOL

    def __init__(
        self,
        min_runspaces: typing.Optional[int] = None,
        max_runspaces: typing.Optional[int] = None,
    ) -> None:
        """
        [MS-PSRP] 2.2.2.29 CONNECT_RUNSPACEPOOL Message
        https://msdn.microsoft.com/en-us/library/hh537460.aspx

        :param min_runspaces:
        :param max_runspaces:
        """
        super(ConnectRunspacePool, self).__init__()
        self._extended_properties = (
            ("min_runspaces", ObjectMeta("I32", name="MinRunspaces", optional=True)),
            ("max_runspaces", ObjectMeta("I32", name="MaxRunspaces", optional=True)),
        )
        self.min_runspaces = min_runspaces
        self.max_runspaces = max_runspaces


class RunspacePoolInitData(ComplexObject):
    MESSAGE_TYPE = MessageType.RUNSPACEPOOL_INIT_DATA

    def __init__(self, min_runspaces=None, max_runspaces=None):
        """
        [MS-PSRP] 2.2.2.30 RUNSPACEPOOL_INIT_DATA Message
        https://msdn.microsoft.com/en-us/library/hh537788.aspx

        :param min_runspaces:
        :param max_runspaces:
        """
        super(RunspacePoolInitData, self).__init__()
        self._extended_properties = (
            ("min_runspaces", ObjectMeta("I32", name="MinRunspaces")),
            ("max_runspaces", ObjectMeta("I32", name="MaxRunspaces")),
        )
        self.min_runspaces = min_runspaces
        self.max_runspaces = max_runspaces


class ResetRunspaceState(ComplexObject):
    MESSAGE_TYPE = MessageType.RESET_RUNSPACE_STATE

    def __init__(
        self,
        ci: typing.Optional[int] = None,
    ) -> None:
        """
        [MS-PSRP] 2.2.2.31 RESET_RUNSPACE_STATE Message
        https://msdn.microsoft.com/en-us/library/mt224027.aspx

        :param ci: The call identifier
        """
        super(ResetRunspaceState, self).__init__()
        self._extended_properties = (("ci", ObjectMeta("I64", name="ci")),)
        self.ci = ci
