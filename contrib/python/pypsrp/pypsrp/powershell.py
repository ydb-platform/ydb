# Copyright: (c) 2018, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

from __future__ import annotations

import base64
import logging
import struct
import time
import types
import typing
import uuid
import warnings
import xml.etree.ElementTree as ET

import requests.exceptions
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from pypsrp import _pool_manager
from pypsrp._utils import version_equal_or_newer
from pypsrp.complex_objects import (
    ApartmentState,
    Command,
    CommandMetadata,
    CommandParameter,
    CommandType,
    ComplexObject,
    HostInfo,
    ObjectMeta,
    Pipeline,
    PipelineResultTypes,
    PSInvocationState,
    PSThreadOptions,
    RemoteStreamOptions,
    RunspacePoolState,
)
from pypsrp.exceptions import (
    FragmentError,
    InvalidPipelineStateError,
    InvalidPSRPOperation,
    InvalidRunspacePoolStateError,
    WSManFaultError,
)
from pypsrp.messages import (
    ApplicationPrivateData,
    ConnectRunspacePool,
    CreatePipeline,
    DebugRecord,
    Destination,
    EncryptedSessionKey,
    EndOfPipelineInput,
    ErrorRecordMessage,
    GetAvailableRunspaces,
    GetCommandMetadata,
    InformationRecord,
    InitRunspacePool,
    Message,
    MessageType,
    PipelineHostCall,
    PipelineHostResponse,
    PipelineInput,
    PipelineState,
    ProgressRecord,
    PublicKey,
    ResetRunspaceState,
    RunspaceAvailability,
    RunspacePoolHostCall,
    RunspacePoolHostResponse,
    RunspacePoolInitData,
    RunspacePoolStateMessage,
    SessionCapability,
    SetMaxRunspaces,
    SetMinRunspaces,
    UserEvent,
    VerboseRecord,
    WarningRecord,
)
from pypsrp.serializer import Serializer
from pypsrp.shell import SignalCode, WinRS
from pypsrp.wsman import NAMESPACES, OptionSet, SelectorSet, WSMan

if typing.TYPE_CHECKING:
    from pypsrp.host import PSHost

log = logging.getLogger(__name__)

PROTOCOL_VERSION = "2.3"
PS_VERSION = "2.0"
SERIALIZATION_VERSION = "1.1.0.1"
DEFAULT_CONFIGURATION_NAME = "Microsoft.PowerShell"


class RunspacePoolWarning(Warning):
    pass


class RunspacePool(object):
    def __init__(
        self,
        connection: WSMan,
        apartment_state: int = ApartmentState.UNKNOWN,
        thread_options: int = PSThreadOptions.DEFAULT,
        host: typing.Optional["PSHost"] = None,
        configuration_name: str = DEFAULT_CONFIGURATION_NAME,
        min_runspaces: int = 1,
        max_runspaces: int = 1,
        session_key_timeout_ms: int = 60000,
        *,
        no_profile: bool = False,
        idle_timeout: int | None = None,
    ) -> None:
        """
        Represents a Runspace pool on a remote host. This pool can contain
        one or more running PowerShell instances.

        This is meant to be a near representation of the
        System.Management.Automation.Runspaces.RunspacePool .NET class

        :param connection: The connection object used to send the command over
            to the remote host. Right now only pypsrp.wsman.WSMan is supported
        :param apartment_state: The ApartmentState enum int values that specify
            the apartment state of the remote thread
        :param thread_options: The ThreadOptions enum int values that specify
            what type of thread of create
        :param host: An implementation of pypsrp.host.PSHost that defines
            the current host and what it supports
        :param configuration_name: The PSRP configuration name to connect to,
            by default this is Microsoft.PowerShell which opens a full
            PowerShell endpoint, use this to specify the JEA configuration to
            connect to
        :param min_runspaces: The minimum number of runspaces that a pool can
            hold
        :param max_runspaces: The maximum number of runspaces that a pool can
            hold
        :param session_key_timeout_ms: The maximum time to wait for a session
            key transfer from the server
        :param no_profile: If True, the user profile will not be loaded and
            will use the machine defaults.
        :param idle_timeout: The idle timeout, in seconds, for the runspace. The
            runspace will be closed if no operations are performed in the time
            specified. If None the server default will be used.
        """
        log.info("Initialising RunspacePool object for configuration %s" % configuration_name)
        # The below are defined in some way at
        # https://msdn.microsoft.com/en-us/library/ee176015.aspx
        self.id = str(uuid.uuid4()).upper()
        self.state = RunspacePoolState.BEFORE_OPEN
        self.connection = connection
        resource_uri = "http://schemas.microsoft.com/powershell/%s" % configuration_name
        self.shell = WinRS(
            connection,
            resource_uri=resource_uri,
            id=self.id,
            input_streams="stdin pr",
            output_streams="stdout",
            no_profile=no_profile,
            idle_time_out=idle_timeout,
        )
        self.ci_table: typing.Dict = {}
        self.pipelines: typing.Dict[str, "PowerShell"] = {}
        self.session_key_timeout_ms = session_key_timeout_ms

        # Extra properties that are important and can control the RunspacePool
        # behaviour
        self.apartment_state = apartment_state
        self.thread_options = thread_options
        self.host = host
        self.protocol_version: typing.Optional[str] = None
        self.ps_version: typing.Optional[str] = None
        self.serialization_version: typing.Optional[str] = None
        self.user_events: typing.List[UserEvent] = []

        self._application_private_data: typing.Optional[ApplicationPrivateData] = None
        self._min_runspaces = min_runspaces
        self._max_runspaces = max_runspaces
        self._serializer = Serializer()
        self._fragmenter = Fragmenter(self.connection.max_payload_size, self._serializer)
        self._exchange_key: typing.Optional[rsa.RSAPrivateKeyWithSerialization] = None
        self._key_exchanged = False
        self._new_client = False
        self._ci_counter = 0

    @property
    def application_private_data(self) -> typing.Optional[ApplicationPrivateData]:
        """
        Private data to be used by applications built on top of PowerShell.
        Runspace data is gathered when creating the remote runspace pool and
        will be None if the runspace is not connected.
        """
        return self._application_private_data

    @property
    def min_runspaces(self) -> int:
        return self._min_runspaces

    @min_runspaces.setter
    def min_runspaces(
        self,
        min_runspaces: int,
    ) -> None:
        """
        Sets the minimum number of Runspaces that the pool maintains in
        anticipation of new requests.

        :param min_runspaces: The minimum number of runspaces in the pool
        """
        log.info("Setting minimum runspaces for pool to %d" % min_runspaces)
        if self.state != RunspacePoolState.OPENED:
            self._min_runspaces = min_runspaces
            return
        elif min_runspaces == self._min_runspaces:
            return

        def response_handler(response):
            if not response:
                raise InvalidPSRPOperation("Failed to set minimum runspaces")
            return response

        ci = self._ci_counter
        self._ci_counter += 1
        self.ci_table[ci] = response_handler

        set_min_runspace = SetMinRunspaces(min_runspaces=min_runspaces, ci=ci)
        data = self._fragmenter.fragment(set_min_runspace, self.id)[0]
        self.shell.send("stdin", data)

        while not isinstance(self.ci_table[ci], bool):
            self._receive()
        self._min_runspaces = min_runspaces
        del self.ci_table[ci]

    @property
    def max_runspaces(self) -> int:
        return self._max_runspaces

    @max_runspaces.setter
    def max_runspaces(
        self,
        max_runspaces: int,
    ) -> None:
        """
        Sets the maximum number of Runspaces that can be active concurrently
        in the pool. All requests above that number remain queued until
        runspaces become available.

        :param max_runspaces: The maximum number of runspaces in the pool
        """
        log.info("Setting maximum runspaces for pool to %d" % max_runspaces)
        if self.state != RunspacePoolState.OPENED:
            self._max_runspaces = max_runspaces
            return
        elif max_runspaces == self._max_runspaces:
            return

        def response_handler(response):
            if not response:
                raise InvalidPSRPOperation("Failed to set maximum runspaces")
            return response

        ci = self._ci_counter
        self._ci_counter += 1
        self.ci_table[ci] = response_handler

        set_max_runspace = SetMaxRunspaces(max_runspaces=max_runspaces, ci=ci)
        data = self._fragmenter.fragment(set_max_runspace, self.id)[0]
        self.shell.send("stdin", data)

        while not isinstance(self.ci_table[ci], bool):
            self._receive()
        self._max_runspaces = max_runspaces
        del self.ci_table[ci]

    def __enter__(self) -> "RunspacePool":
        self.open()
        return self

    def __exit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]],
        value: typing.Optional[BaseException],
        traceback: typing.Optional[types.TracebackType],
    ) -> None:
        self.close()

    def close(self) -> None:
        """
        Closes the RunspacePool and cleans all the internal resources. This
        will close all the runspaces in the runspacepool and release all the
        operations waiting for a runspace. If the pool is already closed or
        broken or closing, this will just return
        """
        log.info("Closing Runspace Pool")
        if self.state in [RunspacePoolState.CLOSED, RunspacePoolState.CLOSING, RunspacePoolState.BROKEN]:
            return

        self.shell.close()
        self.state = RunspacePoolState.CLOSED

    def connect(self) -> None:
        """
        Connects the runspace pool, Runspace pool must be in a disconnected
        state. This only supports reconnecting to a runspace pool created by
        the same client with the same SessionId value in the WSMan headers.
        """
        log.info("Connecting to Runspace Pool")
        if self.state == RunspacePoolState.OPENED:
            return
        elif self.state != RunspacePoolState.DISCONNECTED:
            raise InvalidRunspacePoolStateError(
                self.state, RunspacePoolState.DISCONNECTED, "connect to a disconnected Runspace Pool"
            )

        if self._new_client:
            log.debug("Remote Runspace Pool was created with a different client, connecting as new client")
            self._connect_new_client()
            self._new_client = False
        else:
            log.debug("Remote Runspace Pool was created with the same client, connecting as an existing client")
            self._connect_existing_client()

    def _connect_existing_client(self) -> None:
        selector_set = SelectorSet()
        selector_set.add_option("ShellId", self.shell.id or "")

        self.connection.reconnect(self.shell.resource_uri, selector_set=selector_set)
        self.state = RunspacePoolState.OPENED

    def _connect_new_client(self) -> None:
        rsp = NAMESPACES["rsp"]
        session_capability = SessionCapability(PROTOCOL_VERSION, PS_VERSION, SERIALIZATION_VERSION)
        connect_runspace = ConnectRunspacePool()
        data = self._fragmenter.fragment(session_capability, self.id)[0]
        data += self._fragmenter.fragment(connect_runspace, self.id)[0]

        connect = ET.Element("{%s}Connect" % rsp)
        selectors = SelectorSet()
        selectors.add_option("ShellId", self.id)

        options = OptionSet()
        options.add_option("protocolversion", PROTOCOL_VERSION, {"MustComply": "true"})

        open_content = ET.SubElement(connect, "connectXml", xmlns="http://schemas.microsoft.com/powershell")
        open_content.text = base64.b64encode(data).decode("utf-8")

        response = self.connection.connect(self.shell.resource_uri, connect, options, selectors)
        response_xml = response.find("rsp:ConnectResponse/pwsh:connectResponseXml", NAMESPACES)
        fragments = base64.b64decode(response_xml.text)  # type: ignore[arg-type, union-attr] # Mandated by spec

        self._parse_responses(fragments)
        self.shell.id = self.id  # need to sync up the ShellID with the rs ID
        self.shell._selector_set = selectors
        self._receive()

    def create_disconnected_power_shells(self) -> typing.List["PowerShell"]:
        """
        Creates a list of PowerShell objects that are in the Disconnected state
        for all currently disconnected running commands associated with this
        runspace pool.

        :return: List<PowerShell>: List of disconnected PowerShell objects
        """
        log.info("Getting list of disconnected PowerShells for the current Runspace Pool")
        return [s for s in self.pipelines.values() if s.state == PSInvocationState.DISCONNECTED]

    def disconnect(self) -> None:
        """
        Disconnects the runspace pool, must be in the Opened state
        """
        log.info("Disconnecting from Runspace Pool")
        if self.state == RunspacePoolState.DISCONNECTED:
            return
        elif self.state != RunspacePoolState.OPENED:
            raise InvalidRunspacePoolStateError(self.state, RunspacePoolState.OPENED, "disconnect a Runspace Pool")

        disconnect = ET.Element("{%s}Disconnect" % NAMESPACES["rsp"])
        selector_set = SelectorSet()
        selector_set.add_option("ShellId", self.shell.id or "")

        self.connection.disconnect(self.shell.resource_uri, disconnect, selector_set=selector_set)
        self.state = RunspacePoolState.DISCONNECTED
        for pipeline in self.pipelines.values():
            pipeline.state = PSInvocationState.DISCONNECTED

    def get_available_runspaces(self) -> int:
        """
        Retrieves the number of runspaces available at the time of calling this
        method.

        :return: The number of available runspaces in the pool
        """

        def response_handler(response):
            self._max_runspaces = response
            return response

        ci = self._ci_counter
        self._ci_counter += 1
        self.ci_table[ci] = response_handler

        get_runspaces = GetAvailableRunspaces(ci=ci)
        data = self._fragmenter.fragment(get_runspaces, self.id)[0]
        self.shell.send("stdin", data)

        avail_runspaces = None
        while avail_runspaces is None:
            self._receive()
            if isinstance(self.ci_table[ci], int):
                avail_runspaces = self.ci_table[ci]

        del self.ci_table[ci]
        return avail_runspaces

    def get_command_metadata(
        self,
        names: typing.Union[str, typing.List[str]],
        command_types: int = CommandType.ALL,
        namespace: typing.Optional[typing.List[str]] = None,
        arguments: typing.Optional[typing.List] = None,
    ) -> typing.List[CommandMetadata]:
        """
        Get's metadata of the higher layer command's. This is very similar to
        running the Get-Help cmdlet's but just done on the protocol layer
        instead of through a manual PowerShell pipeline.

        Any wildcard of escaping of strings must conform to the MS-PSRP
        standard which is defined here
        https://msdn.microsoft.com/en-us/library/ee175957.aspx

        :param names: A string or list of strings that specify the commands to
            get the metadata for. Each string can be a wildcard instead of the
            full name.
        :param command_types: The complex_objects.CommandType to filter by, by
            default this will be CommandType.ALL
        :param namespace: A list of wildcard patterns describing the command
            namespaces containing the commands that the server should return
        :param arguments: A list of extra arguments passed to the higher-layer
            above the PowerShell Remoting Protocol
        :return: List of CommandMetadata objects returned by the server
        """
        if self.state != RunspacePoolState.OPENED:
            raise InvalidRunspacePoolStateError(self.state, RunspacePoolState.OPENED, "get command metadata")

        if not isinstance(names, list):
            names = [names]

        get_msg = GetCommandMetadata(names, command_types, namespace, arguments)
        ps = PowerShell(self)
        ps._invoke(get_msg)
        output = ps.end_invoke()

        # first output obj is the MetadataCount obj that tells us how many
        # CommandMetadata objects there are
        _ = output.pop(0)

        # because the main type is generic the serializer will output a
        # GenericComplexObject, we will just manually serialize them to the
        # CommandMetadata object
        meta = ObjectMeta("Obj", object=CommandMetadata)
        return [self._serializer.deserialize(o._xml, meta) for o in output]

    @staticmethod
    def get_runspace_pools(
        connection: WSMan,
    ) -> typing.List["RunspacePool"]:
        """
        Queries the server for disconnected runspace pools and creates a list
        of runspace pool objects associated with each disconnected runspace
        pool on the server. Each runspace pool object in the returned array is
        in the Disconnected state and can be connected to the server by calling
        the connect() method on the runspace pool.

        :param connection: The connection object used to query the remote
            server for a list of Runspace Pools. Currently only
            pypsrp.wsman.WSMan is supported.
        :return: List<RunspacePool> objects each in the Disconnected state
        """
        log.info("Getting list of Runspace Pools on remote host")
        wsen = NAMESPACES["wsen"]
        wsmn = NAMESPACES["wsman"]

        enum_msg = ET.Element("{%s}Enumerate" % wsen)
        ET.SubElement(enum_msg, "{%s}OptimizeEnumeration" % wsmn)
        ET.SubElement(enum_msg, "{%s}MaxElements" % wsmn).text = "32000"

        # TODO: support wsman:EndOfSequence
        response = connection.enumerate("http://schemas.microsoft.com/wbem/wsman/1/windows/shell", enum_msg)
        shells = response.findall("wsen:EnumerateResponse/wsman:Items/rsp:Shell", NAMESPACES)

        runspace_pools = []
        for shell in shells:
            shell_id: str = shell.find("rsp:ShellId", NAMESPACES).text  # type: ignore # Mandated by spec
            pool = RunspacePool(connection)
            pool.id = shell_id
            pool.shell.id = shell_id
            pool.shell.opened = True
            pool._new_client = True

            # Seems like the server sends all pools not just disconnected but
            # the .NET API always sets the state to Disconnected when callling
            # GetRunspacePools so we replicate that here
            pool.state = RunspacePoolState.DISCONNECTED

            enum_msg = ET.Element("{%s}Enumerate" % wsen)
            ET.SubElement(enum_msg, "{%s}OptimizeEnumeration" % wsmn)
            ET.SubElement(enum_msg, "{%s}MaxElements" % wsmn).text = "32000"
            filter = ET.SubElement(
                enum_msg, "{%s}Filter" % wsmn, Dialect="http://schemas.dmtf.org/wbem/wsman/1/wsman/SelectorFilter"
            )
            selector_set = SelectorSet()
            selector_set.add_option("ShellId", shell_id)
            filter.append(selector_set.pack())

            resp = connection.enumerate("http://schemas.microsoft.com/wbem/wsman/1/windows/shell/Command", enum_msg)
            commands = resp.findall("wsen:EnumerateResponse/wsman:Items/rsp:Command", NAMESPACES)
            pipelines = {}
            for command in commands:
                command_id: str = command.find("rsp:CommandId", NAMESPACES).text  # type: ignore # Mandated by spec

                powershell = PowerShell(pool)
                powershell.id = command_id
                powershell._command_id = command_id
                powershell.state = PSInvocationState.DISCONNECTED
                pipelines[powershell.id] = powershell

            pool.pipelines = pipelines
            runspace_pools.append(pool)
        return runspace_pools

    def open(
        self,
        application_arguments: typing.Optional[typing.Dict] = None,
    ) -> None:
        """
        Opens the runspace pool, this step must be called before it can be
        used.

        :param application_arguments: A dictionary of variables to set for the
            runspace host. These can then be accessed in a PowerShell instance
            of the runspace with $PSSenderInfo.ApplicationArguments
        """
        log.info("Opening a new Runspace Pool on remote host")
        if self.state == RunspacePoolState.OPENED:
            return
        if self.state != RunspacePoolState.BEFORE_OPEN:
            raise InvalidRunspacePoolStateError(self.state, RunspacePoolState.BEFORE_OPEN, "open a new Runspace Pool")

        session_capability = SessionCapability(PROTOCOL_VERSION, PS_VERSION, SERIALIZATION_VERSION)
        init_runspace_pool = InitRunspacePool(
            self.min_runspaces,
            self.max_runspaces,
            PSThreadOptions(value=self.thread_options),
            ApartmentState(value=self.apartment_state),
            HostInfo(host=self.host),
            application_arguments,
        )
        msgs = [session_capability, init_runspace_pool]
        fragments = self._fragmenter.fragment_multiple(msgs, self.id)

        open_content = ET.Element("creationXml", xmlns="http://schemas.microsoft.com/powershell")
        open_content.text = base64.b64encode(fragments.pop(0)).decode("utf-8")

        options = OptionSet()
        options.add_option("protocolversion", PROTOCOL_VERSION, {"MustComply": "true"})
        self.shell.open(options, open_content)
        self.state = RunspacePoolState.NEGOTIATION_SENT

        responses = []
        while self.state == RunspacePoolState.NEGOTIATION_SENT:
            responses.extend(self._receive())

    def exchange_keys(self) -> None:
        """
        Initiate a key exchange with the server that is required when dealing
        with secure strings. This can only be run once the RunspacePool is
        open and if the key has already been exchanged then nothing will
        happen.
        """
        log.info("Starting key exchange with remote host")
        if self._key_exchanged or self._exchange_key is not None:
            # key is already exchanged or we are still in the processes of
            # exchanging it, no need to run again
            return

        # Generate a unique RSA key pair for use in this Pool only
        self._exchange_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )
        public_numbers = self._exchange_key.public_key().public_numbers()
        exponent = struct.pack("<I", public_numbers.e)
        modulus = b""
        for i in range(0, 256):
            byte_value = struct.pack("B", public_numbers.n >> (i * 8) & 0xFF)
            modulus += byte_value

        # the public key bytes follow a set structure defined in MS-PSRP
        public_key_bytes = b"\x06\x02\x00\x00\x00\xa4\x00\x00\x52\x53\x41\x31\x00\x08\x00\x00" + exponent + modulus
        public_key = base64.b64encode(public_key_bytes)

        msg = PublicKey(public_key=public_key.decode("utf-8"))
        fragments = self._fragmenter.fragment(msg, self.id)
        for fragment in fragments:
            self.shell.send("stdin", fragment)

        start = time.time()
        while not self._key_exchanged:
            elapsed = int((time.time() - start) * 1000)
            if elapsed > self.session_key_timeout_ms:
                raise InvalidPSRPOperation("Timeout while waiting for key exchange")
            self._receive()

    def reset_runspace_state(self) -> None:
        """
        Resets the variable table for the runspace to the default state.

        This is only supported for the protocol version 2.3 and above (Server
        2016/Windows 10+)
        """
        log.info("Resetting remote Runspace Pool state")
        if self.state == RunspacePoolState.BEFORE_OPEN:
            # no need to reset if the runspace has not been opened
            return
        elif self.state != RunspacePoolState.OPENED:
            raise InvalidRunspacePoolStateError(
                self.state, [RunspacePoolState.BEFORE_OPEN, RunspacePoolState.OPENED], "reset RunspacePool state"
            )
        elif not version_equal_or_newer(self.protocol_version or "", "2.3"):
            raise InvalidPSRPOperation(
                "Cannot reset runspace state on "
                "protocol versions older than 2.3, "
                "actual: %s" % self.protocol_version
            )

        def response_handler(response):
            if not response:
                raise InvalidPSRPOperation("Failed to reset runspace state")
            return response

        ci = self._ci_counter
        self._ci_counter += 1
        self.ci_table[ci] = response_handler

        reset_state = ResetRunspaceState(ci=ci)
        data = self._fragmenter.fragment(reset_state, self.id)[0]
        self.shell.send("stdin", data)

        while not isinstance(self.ci_table[ci], bool):
            self._receive()
        del self.ci_table[ci]

    def serialize(
        self,
        obj: typing.Any,
        metadata: typing.Optional[ObjectMeta] = None,
    ) -> typing.Optional[ET.Element]:
        """
        Serialize a Python object to PSRP object. This can try to automatically
        serialize based on the Python type to the closest PSRP object but
        manual coercion can be done with the metadata parameter.

        :param obj: The Python object to serialize
        :param metadata: complex_objects.ObjectMeta that defines the type of
            object to serialize to, if omitted the obj will be serialized based
            on the Python type
        :return: An XML element that can be used as part of the PSRP input
            elements like cmdlet parameters
        """
        return self._serializer.serialize(obj, metadata=metadata)

    def is_alive(
        self,
        *,
        timeout: int | None = None,
    ) -> bool:
        """Checks whether the RunspacePool is still alive.

        :param timeout: Override the connection timeout defaults for the
            request. The WSMan operation timeout will be set to this value
            and the HTTP connect/read timeouts will be this value + 2 seconds.
        :return: A bool True if the pool is still alive, False otherwise.
        """
        is_closed = False

        try:
            # We don't want to try and open a new connection if the socket
            # was closed, we treat it as the pool is not alive.
            with _pool_manager.DisableNewConnectionsContext():
                state = self.shell._get_shell(timeout=timeout)

            is_closed = state.get("State", "") == "Disconnected"
        except WSManFaultError as exc:
            if exc.code in [
                0x80338029,  # ERROR_WSMAN_OPERATION_TIMEDOUT
                0x8033805B,  # ERROR_WSMAN_UNEXPECTED_SELECTORS
            ]:
                is_closed = True
            else:
                raise

        except (_pool_manager.NewConnectionDisabled, TimeoutError, requests.exceptions.ConnectionError):
            # If a timeout or connection error occurs, treat the pool as closed.
            is_closed = True

        if is_closed:
            # Ensures that close() doesn't try and close an already closed pool
            self.state = RunspacePoolState.CLOSED
            return False

        return True

    def _receive(
        self,
        id: typing.Optional[str] = None,
        timeout: typing.Optional[int] = None,
    ) -> typing.List[typing.Tuple[int, typing.Any]]:
        """
        Sends a Receive WSMV request to the host and processes the messages
        that are received from the host (if there are any).

        :param id: If the receive is targeted to a Pipeline then this should be
            the ID of that pipeline, if None then the receive is targeted to
            the RunspacePool
        :param timeout: An override that specifies the operation timeout for
            the receive command
        :return: List of tuples where each tuple is a tuple of
            MessageType: The Message ID of the response
            Response: The return object of the response handler function for
                the message type
        """
        command_id = None
        pipeline = None

        if id:
            pipeline = self.pipelines.get(id)
            if pipeline is not None:
                command_id = pipeline._command_id

        response = self.shell.receive("stdout", command_id=command_id, timeout=timeout)[2]["stdout"]
        return self._parse_responses(response, pipeline)

    def _parse_responses(
        self,
        responses: bytes,
        pipeline: typing.Optional["PowerShell"] = None,
    ) -> typing.List[typing.Tuple[int, typing.Any]]:
        messages = self._fragmenter.defragment(responses)

        response_functions: typing.Dict[int, typing.Optional[typing.Callable[[Message], typing.Any]]] = {
            # While the docs say we should verify, they are out of date with
            # the possible responses and so we will just ignore for now
            MessageType.SESSION_CAPABILITY: self._process_session_capability,
            MessageType.ENCRYPTED_SESSION_KEY: self._process_encrypted_session_key,
            MessageType.PUBLIC_KEY_REQUEST: lambda m: self.exchange_keys,
            MessageType.RUNSPACEPOOL_INIT_DATA: self._process_runspacepool_init_data,
            MessageType.RUNSPACE_AVAILABILITY: self._process_runspacepool_availability,
            MessageType.RUNSPACEPOOL_STATE: self._process_runspacepool_state,
            MessageType.USER_EVENT: self._process_user_event,
            MessageType.APPLICATION_PRIVATE_DATA: self._process_application_private_data,
            MessageType.RUNSPACEPOOL_HOST_CALL: self._process_runspacepool_host_call,
            MessageType.WARNING_RECORD: self._process_runspacepool_warning,
        }

        if pipeline is not None:
            pipeline_response_functions: typing.Dict[int, typing.Optional[typing.Callable[[Message], typing.Any]]] = {
                # The Pipeline Output isn't processes and just returned back to
                # the receive caller
                MessageType.PIPELINE_OUTPUT: None,
                MessageType.ERROR_RECORD: pipeline._process_error_record,
                MessageType.PIPELINE_STATE: pipeline._process_pipeline_state,
                MessageType.DEBUG_RECORD: pipeline._process_debug_record,
                MessageType.VERBOSE_RECORD: pipeline._process_verbose_record,
                MessageType.WARNING_RECORD: pipeline._process_warning_record,
                MessageType.PROGRESS_RECORD: pipeline._process_progress_record,
                MessageType.INFORMATION_RECORD: pipeline._process_information_record,
                MessageType.PIPELINE_HOST_CALL: pipeline._process_pipeline_host_call,
            }
            response_functions.update(pipeline_response_functions)

        return_values = []
        for message in messages:
            if message.message_type not in response_functions:
                log.warning("Unsupported message type '%s' received" % message.message_type)
                response_function = None

            else:
                response_function = response_functions[message.message_type]

            if response_function is not None:
                return_value = response_function(message)
                return_values.append((message.message_type, return_value))
            else:
                return_values.append((message.message_type, message))

        return return_values

    def _process_host_call(
        self,
        message: Message,
        response_obj: typing.Union[typing.Type[RunspacePoolHostResponse], typing.Type[PipelineHostResponse]],
        pipeline: typing.Optional["PowerShell"] = None,
    ) -> ComplexObject:
        host_call = typing.cast(typing.Union[RunspacePoolHostCall, PipelineHostCall], message.data)

        if self.host is None:
            log.warning(
                "Cannot run host call as no host was defined for the "
                "runspace, method: %s, args: %s" % (str(host_call.mi), str(host_call.mp))
            )
            return host_call

        response = self.host.run_method(host_call.mi, host_call.mp, self, pipeline)

        if response is not None:
            msg = response_obj()
            msg.ci = host_call.ci
            msg.mi = host_call.mi
            msg.mr = response

            pid = pipeline.id if pipeline is not None else None
            fragments = self._fragmenter.fragment(msg, self.id, pid)
            for fragment in fragments:
                self.shell.send("pr", fragment)

        return host_call

    def _process_session_capability(
        self,
        message: Message,
    ) -> None:
        msg = typing.cast(SessionCapability, message.data)

        log.debug(
            "Received SessionCapability with protocol version: "
            "%s, ps version: %s, serialization version: %s"
            % (msg.protocol_version, msg.ps_version, msg.serialization_version)
        )
        self.protocol_version = msg.protocol_version
        self.ps_version = msg.ps_version
        self.serialization_version = msg.serialization_version

        # if protocol_version >= 2.2 and the max_envelope_size is still the
        # default, update the envelope size to 500KiB
        if version_equal_or_newer(msg.protocol_version or "", "2.2") and self.connection.max_envelope_size == 153600:
            self.connection.update_max_payload_size(512000)
            self._fragmenter.max_size = self.connection.max_payload_size

    def _process_runspacepool_init_data(
        self,
        message: Message,
    ) -> None:
        msg = typing.cast(RunspacePoolInitData, message.data)

        log.debug(
            "Received RunspacePoolInitData with min runspaces: %d and "
            "max runspaces: %d" % (msg.min_runspaces, msg.max_runspaces)
        )
        self._min_runspaces = msg.min_runspaces
        self._max_runspaces = msg.max_runspaces

    def _process_runspacepool_availability(
        self,
        message: Message,
    ) -> ComplexObject:
        msg = typing.cast(RunspaceAvailability, message.data)

        ci = msg.ci
        response = msg.response
        ci_handler = self.ci_table[ci]
        response = ci_handler(response)
        self.ci_table[ci] = response
        return response

    def _process_runspacepool_host_call(
        self,
        message: Message,
    ) -> ComplexObject:
        return self._process_host_call(message, RunspacePoolHostResponse)

    def _process_runspacepool_state(
        self,
        message: Message,
    ) -> ComplexObject:
        state_msg = typing.cast(RunspacePoolStateMessage, message.data)

        log.debug("Received RunspacePoolState with state: %d" % state_msg.state)
        self.state = state_msg.state
        if self.state == RunspacePoolState.BROKEN:
            raise InvalidPSRPOperation("Received a broken RunspacePoolState message: %s" % str(state_msg.error_record))
        return state_msg

    def _process_runspacepool_warning(
        self,
        message: Message,
    ) -> None:
        warnings.warn(str(message.data), RunspacePoolWarning)

    def _process_application_private_data(
        self,
        message: Message,
    ) -> None:
        self._application_private_data = typing.cast(ApplicationPrivateData, message.data)

    def _process_encrypted_session_key(
        self,
        message: Message,
    ) -> None:
        key_msg = typing.cast(EncryptedSessionKey, message.data)

        log.debug("Received EncryptedSessionKey response")
        enc_sess_key = base64.b64decode(key_msg.session_key)

        # strip off Win32 Crypto Blob Header and reverse the bytes
        encrypted_key = enc_sess_key[12:][::-1]
        pad_method = padding.PKCS1v15()
        decrypted_key = self._exchange_key.decrypt(encrypted_key, pad_method)  # type: ignore[union-attr] # Will be set

        iv = b"\x00" * 16  # PSRP doesn't use an IV
        algorithm = algorithms.AES(decrypted_key)
        mode = modes.CBC(iv)
        cipher = Cipher(algorithm, mode)

        self._serializer.cipher = cipher
        self._key_exchanged = True
        self._exchange_key = None

    def _process_user_event(
        self,
        message: Message,
    ) -> None:
        user_event = typing.cast(UserEvent, message.data)
        self.user_events.append(user_event)


class PowerShell(object):
    def __init__(
        self,
        runspace_pool: RunspacePool,
    ) -> None:
        """
        Represents a PowerShell command or script to execute against a
        RunspacePool.

        This is meant to be a near representation of the
        System.Management.Automation.PowerShell .NET class

        :param runspace_pool: The RunspacePool that the PowerShell instance
            will run over
        """
        log.info("Initialising PowerShell in remote Runspace Pool")
        self.runspace_pool = runspace_pool
        self.state = PSInvocationState.NOT_STARTED

        self.commands: typing.List[Command] = []
        self.had_errors = False
        self.history_string = None
        self.id = str(uuid.uuid4()).upper()
        self.is_nested = False
        self.streams = PSDataStreams()
        self.output: typing.List = []
        self._from_disconnect = False

        # this is not necessarily the same ID as id, this relates to the WSMan
        # CommandID that is created and we need to reference in the WSMan msgs
        self._command_id: typing.Optional[str] = None

    def __enter__(self) -> "PowerShell":
        return self

    def __exit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]],
        value: typing.Optional[BaseException],
        traceback: typing.Optional[types.TracebackType],
    ) -> None:
        self.close()

    def add_argument(
        self,
        value: typing.Any,
    ) -> "PowerShell":
        """
        Adds an argument to the last added command.

        :param value: The argument to add. If the value is a native Python
            type then it will be automatically serialized, otherwise if it is
            an already serialized object then that value will be used instead
        :return: The current PowerShell object with the argument added to the
            last added Command
        """
        command_parameter = CommandParameter(value=value)
        self.commands[-1].args.append(command_parameter)
        return self

    def add_command(
        self,
        command: Command,
    ) -> "PowerShell":
        """
        Add a Command object to the current command pipeline.

        :param command: Command to add
        :return: The current PowerShell object with the Command added
        """
        self.commands.append(command)
        return self

    def add_cmdlet(
        self,
        cmdlet: str,
        use_local_scope: typing.Optional[bool] = None,
    ) -> "PowerShell":
        """
        Add a cmdlet/command to the current command pipeline. This is similar
        to add_command but it takes in a string and constructs the Command
        object for you. For example to construct "Get-Process | Sort-Object"

        .add_cmdlet("Get-Process").add_cmdlet("Sort-Object")

        :param cmdlet: A string representing the cmdlet to add
        :param use_local_scope: Run the cmdlet under the local scope
        :return: The current PowerShell object with the cmdlet added
        """
        command = Command(
            cmdlet,
            protocol_version=self.runspace_pool.protocol_version or "",
            is_script=False,
            use_local_scope=use_local_scope,
        )
        self.commands.append(command)
        return self

    def add_parameter(
        self,
        parameter_name: str,
        value: typing.Any = None,
    ) -> "PowerShell":
        """
        Add a parameter to the last added command. For example to construct a
        command string "get-service -name service-name"

        .add_command("get-service").add_parameter("name", "service-name")

        :param parameter_name: The name of the parameter
        :param value: The value for the parameter, None means no value is set.
            If the value is a native Python type then it will be automatically
            serialized, otherwise if it is an already serialized object then
            that value will be used instead
        :return: the current PowerShell instance with the parameter added
        """
        command_parameter = CommandParameter(name=parameter_name, value=value)
        self.commands[-1].args.append(command_parameter)
        return self

    def add_parameters(
        self,
        parameters: typing.Dict[str, typing.Any],
    ) -> "PowerShell":
        """
        Adds a set of parameters to the last added command.

        :param parameters: A dictionary of parameters where the key is the
            parameter name and the value is the parameter value. A value of
            None means no value is set and the parameter is a switch
        :return: the current PowerShell instance with the parameters added
        """
        for parameter_name, value in parameters.items():
            self.add_parameter(parameter_name, value)
        return self

    def add_script(
        self,
        script: str,
        use_local_scope: typing.Optional[bool] = None,
    ) -> "PowerShell":
        """
        Add a piece of script to construct a command pipeline.

        :param script: A string representing a script
        :param use_local_scope: Run the script under the local scope
        :return: the current PowerShell instance with the command added
        """
        command = Command(
            script,
            protocol_version=self.runspace_pool.protocol_version or "",
            is_script=True,
            use_local_scope=use_local_scope,
        )
        self.commands.append(command)
        return self

    def add_statement(self) -> "PowerShell":
        """
        Set's the last command in the pipeline to be the last in that
        statement/pipeline so the next command is in a new statement.

        :return: The current PowerShell instance with the last command set
            as the last one in that statement
        """
        self.commands[-1].end_of_statement = True
        return self

    def clear_commands(self) -> "PowerShell":
        """
        Clears all the commands of the current PowerShell object.
        :return: The current PowerShell instance with all the commands cleared
        """
        self.commands = []
        return self

    def clear_streams(self) -> None:
        """
        Clears all the data streams of the current PowerShell object.
        """
        self.streams = PSDataStreams()
        self.output = []

    def close(self) -> None:
        """
        Closes the PowerShell pipeline and cleans up server resources.

        This method must be called to reuse a PowerShell object for multiple
        invocations. It sends a TERMINATE signal to the server and removes
        the pipeline from the runspace pool's pipeline registry.

        :raises InvalidPipelineStateError: If the pipeline is not in a terminal
            state (COMPLETED, STOPPED, or FAILED)
        """
        valid_states = [PSInvocationState.COMPLETED, PSInvocationState.STOPPED, PSInvocationState.FAILED]
        if self.state == PSInvocationState.NOT_STARTED:
            return
        elif self.state not in valid_states:
            raise InvalidPipelineStateError(self.state, valid_states, "close a PowerShell pipeline")

        self.runspace_pool.shell.signal(SignalCode.TERMINATE, command_id=self._command_id or self.id)
        self.runspace_pool.pipelines.pop(self.id, None)

    def connect(self):
        """
        Connects to a running command on a remote server, waits until the
        command is finished and returns the output objects.

        :return: Command output as a PSDataCollection
        """
        self.connect_async()
        return self.end_invoke()

    def connect_async(self):
        """
        Connects to a running command on a remote server, this method will
        connect to the host but will not wait until the command is finished.
        Call end_invoke() to wait until the process is complete.
        """
        if self.state != PSInvocationState.DISCONNECTED:
            raise InvalidPipelineStateError(
                self.state, PSInvocationState.DISCONNECTED, "connect to a disconnected pipeline"
            )
        rsp = NAMESPACES["rsp"]

        connect = ET.Element("{%s}Connect" % rsp, CommandId=self.id)

        self.runspace_pool.connection.connect(
            self.runspace_pool.shell.resource_uri, connect, selector_set=self.runspace_pool.shell._selector_set
        )
        self.state = PSInvocationState.RUNNING
        self._from_disconnect = True

    def create_nested_power_shell(self):
        """
        Creates a nested PowerShell within the current instance. Nested
        PowerShell is used to do simple operations like checking state of a
        variable while another command is using the runspace.

        Nested PowerShell should be invoked from the same thread as the parent
        PowerShell invocation thread. So effectively the parent PowerShell
        invocation thread is blocked until the nested invoke() operation is
        complete.

        :return: The new nested PowerShell object
        """
        if self.state != PSInvocationState.RUNNING:
            raise InvalidPipelineStateError(
                self.state, PSInvocationState.RUNNING, "create a nested PowerShell pipeline"
            )
        elif self._from_disconnect:
            raise InvalidPSRPOperation(
                "Cannot created a nested PowerShell "
                "pipeline from an existing pipeline "
                "that was connected to remotely"
            )

        ps = PowerShell(self.runspace_pool)
        ps.is_nested = True
        return ps

    def begin_invoke(
        self,
        input: typing.Optional[typing.Union[bytes, typing.Iterator]] = None,
        add_to_history: bool = False,
        apartment_state: typing.Optional[int] = None,
        redirect_shell_error_to_out: bool = False,
        remote_stream_options: typing.Optional[int] = RemoteStreamOptions.ADD_INVOCATION_INFO,
    ) -> None:
        """
        Invoke the command asynchronously, use end_invoke to get the output
        collection of return objects.

        :param input: List of inputs to the command, this will be manually
            serialized but can also be a result of runspace_pool.serialize()
        :param add_to_history: Add the commands run to the pool history
        :param apartment_state: Override the RunspacePool apartment state with
            one just for this invocation
        :param redirect_shell_error_to_out: Whether to redirect the global
            error output pipe to the commands error output pipe.
        :param remote_stream_options: Whether to return the invocation info on
            the various steams, see complex_objects.RemoteStreamOptions for the
            values. Will default to returning the invocation info on all
        """
        log.info("Beginning remote Pipeline invocation")
        valid_states = [
            PSInvocationState.NOT_STARTED,
            PSInvocationState.STOPPED,
            PSInvocationState.COMPLETED,
            PSInvocationState.FAILED,
        ]
        if self.state not in valid_states:
            raise InvalidPipelineStateError(self.state, valid_states, "start a PowerShell pipeline")

        if len(self.commands) == 0:
            raise InvalidPSRPOperation("Cannot invoke PowerShell without any commands being set")

        no_input = input is None or not input
        apartment_state = apartment_state or self.runspace_pool.apartment_state
        host_info = HostInfo(host=self.runspace_pool.host)

        pipeline = Pipeline(
            is_nested=self.is_nested,
            cmds=self.commands,
            history=self.history_string,
            redirect_err_to_out=redirect_shell_error_to_out,
        )
        create_pipeline = CreatePipeline(
            no_input,
            ApartmentState(value=apartment_state),
            RemoteStreamOptions(value=remote_stream_options),
            add_to_history,
            host_info,
            pipeline,
            self.is_nested,
        )
        self._invoke(create_pipeline)

        # finally send the input if any was specified
        if input is not None:
            if not isinstance(input, types.GeneratorType):

                def input_gen(i: bytes) -> typing.Iterator[bytes]:
                    yield i

                input = input_gen(input)  # type: ignore[arg-type]

            log.info("Sending input to remote Pipeline")
            next_input = next(input)
            while next_input is not None:
                if not isinstance(next_input, list):
                    next_input = [next_input]

                input_msgs: typing.List[ComplexObject] = [PipelineInput(data=d) for d in next_input]

                try:
                    next_input = next(input)
                except StopIteration:
                    input_msgs.append(EndOfPipelineInput())
                    next_input = None

                fragments = self.runspace_pool._fragmenter.fragment_multiple(input_msgs, self.runspace_pool.id, self.id)

                for fragment in fragments:
                    self.runspace_pool.shell.send("stdin", fragment, command_id=self._command_id)

    def end_invoke(self) -> typing.List:
        """
        Wait until the asynchronous command has finished executing and return
        the output collection of return objects.

        :return: A list of output objects
        """
        while self.state == PSInvocationState.RUNNING:
            self.poll_invoke()

        return self.output

    def invoke(
        self,
        input: typing.Optional[typing.Union[bytes, typing.Iterator]] = None,
        add_to_history: bool = False,
        apartment_state: typing.Optional[int] = None,
        redirect_shell_error_to_out: bool = False,
        remote_stream_options: typing.Optional[int] = RemoteStreamOptions.ADD_INVOCATION_INFO,
    ) -> typing.List:
        """
        Invoke the command and return the output collection of return objects.

        :param input: List of inputs to the command, this will be manually
            serialized but can also be a result of runspace_pool.serialize()
        :param add_to_history: Add the commands run to the pool history
        :param apartment_state: Override the RunspacePool apartment state with
            one just for this invocation
        :param redirect_shell_error_to_out: Whether to redirect the global
            error output pipe to the commands error output pipe.
        :param remote_stream_options: Whether to return the invocation info on
            the various steams, see complex_objects.RemoteStreamOptions for the
            values. Will default to returning the invocation info on all
        :return: A list of output objects
        """
        self.begin_invoke(input, add_to_history, apartment_state, redirect_shell_error_to_out, remote_stream_options)
        return self.end_invoke()

    def merge_previous(
        self,
        enabled: bool = False,
    ) -> "PowerShell":
        """
        Sets the MergePreviousResults of the last command in the pipeline.
        This is the same as the MergeUnclaimedPreviousCommandResults property
        used in the .NET System.Management.Automation.Runspaces.Command class.

        :param enabled: Whether to merge previous results to the Output and
            Error streams or not
        :return: The current PowerShell instance with the last command set to
            MergePreviousResults
        """
        if enabled:
            value = PipelineResultTypes.OUTPUT | PipelineResultTypes.ERROR
        else:
            value = PipelineResultTypes.NONE
        pipeline = PipelineResultTypes(protocol_version_2=True, value=value)
        self.commands[-1].merge_previous = pipeline
        return self

    def merge_all(
        self,
        to: str = "none",
    ) -> "PowerShell":
        """
        Will merge all relevant streams to the stream specified of the last
        command in the pipeline.

        :param to: Can be one of the following
            none: Do not merge the streams
            output: Send all streams to the Output stream
        :return: The current PowerSHell instance with the last command set to
            Merge<stream> to the stream desired.
        """
        self.merge_error(to)

        if version_equal_or_newer(self.runspace_pool.protocol_version or "", "2.2"):
            self.merge_debug(to)
            self.merge_verbose(to)
            self.merge_warning(to)

        if version_equal_or_newer(self.runspace_pool.protocol_version or "", "2.3"):
            self.merge_information(to)
        return self

    def merge_error(
        self,
        to: str = "none",
    ) -> "PowerShell":
        """
        Controls where the Error stream of the last command in the pipeline
        will be sent to.

        :param to: Can be one of the following
            none: Send all Error streams to the Error stream
            output: Send all Error streams to the Output stream
        :return: The current PowerShell instance with the last command set to
            MergeError to the stream desired.
        """
        self._set_merge_to("merge_error", to, ["none", "output"])

        # For V2 backwards compatibility
        if to == "none":
            my_result = self.commands[-1].merge_error
        else:
            my_result = PipelineResultTypes(value=PipelineResultTypes.ERROR)
        self.commands[-1].merge_my_result = my_result
        self.commands[-1].merge_to_result = self.commands[-1].merge_error
        return self

    def merge_warning(
        self,
        to: str = "none",
    ) -> "PowerShell":
        """
        Controls where the Warning stream of the last command in the pipeline
        will be sent to. This will throw an InvalidPSRPOperation error if the
        protocol version of the server is less than 2.2 (PSv2).

        :param to: Can be on of the following
            none: Send all Warning streams to the Warning stream
            output: Send all Warning streams to the Output stream
            null: Sends it to a null pipe (this doesn't seem to do anything)
        :return: The current PowerShell instance with the last command set to
            MergeWarning to the stream desired.
        """
        self._set_merge_to("merge_warning", to, None, "2.2")
        return self

    def merge_verbose(
        self,
        to: str = "none",
    ) -> "PowerShell":
        """
        Controls where the Verbose stream of the last command in the pipeline
        will be sent to. This will throw an InvalidPSRPOperation error if the
        protocol version of the server is less than 2.2 (PSv2).

        :param to: Can be on of the following
            none: Send all Verbose streams to the Verbose stream
            output: Send all Verbose streams to the Output stream
            null: Sends it to a null pipe (this doesn't seem to do anything)
        :return: The current PowerShell instance with the last command set to
            MergeVerbose to the stream desired.
        """
        self._set_merge_to("merge_verbose", to, None, "2.2")
        return self

    def merge_debug(
        self,
        to: str = "none",
    ) -> "PowerShell":
        """
        Controls where the Debug stream of the last command in the pipeline
        will be sent to. This will throw an InvalidPSRPOperation error if the
        protocol version of the server is less than 2.2 (PSv2).

        :param to: Can be on of the following
            none: Send all Debug streams to the Debug stream
            output: Send all Debug streams to the Output stream
            null: Sends it to a null pipe (this doesn't seem to do anything)
        :return: The current PowerShell instance with the last command set to
            MergeDebug to the stream desired.
        """
        self._set_merge_to("merge_debug", to, None, "2.2")
        return self

    def merge_information(
        self,
        to: str = "none",
    ) -> "PowerShell":
        """
        Controls where the Information stream of the last command in the
        pipeline will be sent to. This will throw an InvalidPSRPOperation error
        if the protocol version of the server is less than 2.3 (PSv5).

        :param to: Can be on of the following
            none: Send all Information streams to the Information stream
            output: Send all Information streams to the Output stream
            null: Sends it to a null pipe (this doesn't seem to do anything)
        :return: The current PowerShell instance with the last command set to
            MergeInformation to the stream desired.
        """
        self._set_merge_to("merge_information", to, None, "2.3")
        return self

    def merge_reset(self) -> "PowerShell":
        """
        Will reset the merge behaviour of all streams back to the default. This
        is done on the last command in the pipeline.

        :return: The current PowerShell instance with the last commands merge
            behaviour set to the default.
        """
        self.merge_previous(enabled=False)
        self.merge_all("none")
        return self

    def poll_invoke(
        self,
        timeout: typing.Optional[int] = None,
    ) -> None:
        """
        Poll the running process to update the output streams and the status.

        :param timeout: Override the default WSMan timeout when polling the
        pipeline.
        """
        try:
            responses = self.runspace_pool._receive(self.id, timeout=timeout)
        except WSManFaultError as err:
            # operation timeout needs to be ignored and silently tried
            # again
            if err.code == 2150858793:
                responses = []
            else:
                raise err

        for response in responses:
            if response[0] == MessageType.PIPELINE_OUTPUT:
                self.output.append(response[1].data.data)

    def stop(self) -> None:
        """
        Stop the currently running command.
        """
        log.info("Stopping remote Pipeline")
        if self.state in [PSInvocationState.STOPPING, PSInvocationState.STOPPED]:
            return
        elif self.state != PSInvocationState.RUNNING:
            raise InvalidPipelineStateError(self.state, PSInvocationState.RUNNING, "stop a running pipeline")

        self.state = PSInvocationState.STOPPING
        self.runspace_pool.shell.signal(SignalCode.PS_CTRL_C, str(self.id).upper())
        self.state = PSInvocationState.STOPPED
        self.runspace_pool.pipelines.pop(self.id, None)

    def _invoke(
        self,
        msg: ComplexObject,
    ) -> None:
        fragments = self.runspace_pool._fragmenter.fragment(msg, self.runspace_pool.id, self.id)

        self.runspace_pool.pipelines[self.id] = self

        # send first fragment as Command message
        first_frag = base64.b64encode(fragments.pop(0)).decode("utf-8")
        resp = self.runspace_pool.shell.command("", arguments=[first_frag], command_id=self.id)
        cmd_id = resp.find("rsp:CommandResponse/rsp:CommandId", NAMESPACES)
        if cmd_id is not None:
            self._command_id = cmd_id.text or ""
        self.state = PSInvocationState.RUNNING

        # send the remaining fragments with the Send message
        for fragment in fragments:
            self.runspace_pool.shell.send("stdin", fragment, command_id=self._command_id)

    def _set_merge_to(
        self,
        merge: str,
        to: str,
        valid: typing.Optional[typing.List[str]],
        min_protocol: typing.Optional[str] = None,
    ) -> None:
        valid = valid or ["none", "null", "output"]
        if to not in valid:
            raise InvalidPSRPOperation("Invalid merge to option '%s', valid values %s" % (to, ", ".join(valid)))

        if min_protocol is not None:
            if not version_equal_or_newer(self.runspace_pool.protocol_version or "", min_protocol):
                error_msg = (
                    "Merge option for '%s' is not supported in the current "
                    "protocol version %s, minimum version required %s"
                    % (merge, self.runspace_pool.protocol_version, min_protocol)
                )
                raise InvalidPSRPOperation(error_msg)

        to_value = getattr(PipelineResultTypes, to.upper())
        to_result = PipelineResultTypes(value=to_value)
        setattr(self.commands[-1], merge, to_result)

    def _process_error_record(
        self,
        message: Message,
    ) -> None:
        self.streams.error.append(typing.cast(ErrorRecordMessage, message.data))

    def _process_pipeline_host_call(
        self,
        message: Message,
    ) -> ComplexObject:
        return self.runspace_pool._process_host_call(message, PipelineHostResponse, self)

    def _process_pipeline_state(
        self,
        message: Message,
    ) -> None:
        msg_state = typing.cast(PipelineState, message.data)

        log.debug("Received PipelineState with state: %d" % msg_state.state)
        self.state = msg_state.state
        if msg_state.error_record is not None:
            self.streams.error.append(msg_state.error_record)

        if self.state == PSInvocationState.FAILED:
            self.had_errors = True

    def _process_debug_record(
        self,
        message: Message,
    ) -> None:
        self.streams.debug.append(typing.cast(DebugRecord, message.data))

    def _process_verbose_record(
        self,
        message: Message,
    ) -> None:
        self.streams.verbose.append(typing.cast(VerboseRecord, message.data))

    def _process_warning_record(
        self,
        message: Message,
    ) -> None:
        self.streams.warning.append(typing.cast(WarningRecord, message.data))

    def _process_progress_record(
        self,
        message: Message,
    ) -> None:
        self.streams.progress.append(typing.cast(ProgressRecord, message.data))

    def _process_information_record(
        self,
        message: Message,
    ) -> None:
        self.streams.information.append(typing.cast(InformationRecord, message.data))


class PSDataStreams(object):
    def __init__(self) -> None:
        """
        Streams generated by PowerShell invocations

        System.Management.Automation.PSDataStreams
        https://docs.microsoft.com/en-us/dotnet/api/system.management.automation.psdatastreams?view=powershellsdk-1.1.0
        """
        self.debug: typing.List[DebugRecord] = []
        self.error: typing.List[ErrorRecordMessage] = []
        self.information: typing.List[InformationRecord] = []
        self.progress: typing.List[ProgressRecord] = []
        self.verbose: typing.List[VerboseRecord] = []
        self.warning: typing.List[WarningRecord] = []


class Fragmenter(object):
    def __init__(
        self,
        max_size: int,
        serializer: Serializer,
    ) -> None:
        self.incoming_buffer: typing.Dict[int, typing.Dict] = {}
        self.outgoing_counter = 1
        self.max_size = max_size - 21  # take away the fragment header size
        self.serializer = serializer

    def fragment(
        self,
        data: ComplexObject,
        rpid: str,
        pid: typing.Optional[str] = None,
        remaining_size: typing.Optional[int] = None,
    ) -> typing.List[bytes]:
        msg = Message(Destination.SERVER, rpid, pid, data, self.serializer)
        msg_data = msg.pack()
        max_size = self.max_size
        start = True
        fragment_id = 0
        fragments: typing.List[bytes] = []

        if remaining_size is not None:
            msg_fragment = msg_data[:remaining_size]
            msg_data = msg_data[len(msg_fragment) :]

            end = msg_data == b""
            fragment = Fragment(self.outgoing_counter, fragment_id, msg_fragment, start, end)
            fragments.append(fragment.pack())
            fragment_id += 1
            start = False

        for msg_fragment, end in self._byte_iterator(msg_data, max_size):
            fragment = Fragment(self.outgoing_counter, fragment_id, msg_fragment, start, end)
            fragments.append(fragment.pack())
            fragment_id += 1
            start = False

        self.outgoing_counter += 1
        return fragments

    def fragment_multiple(
        self,
        blocks: typing.List[ComplexObject],
        rpid: str,
        pid: typing.Optional[str] = None,
    ) -> typing.List[bytes]:
        remaining_size = self.max_size
        fragments: typing.List[bytes] = []

        for block in blocks:
            block_fragments = self.fragment(block, rpid, pid, remaining_size=remaining_size)

            # the first fragment can fit in with the previous block fragment
            # append to the last fragment and remove from the list
            if remaining_size != self.max_size and len(fragments) > 0:
                fragments[-1] = fragments[-1] + block_fragments.pop(0)

            fragments.extend(block_fragments)

            # calculate how much data can fit into the last fragment
            remaining_size = self.max_size - len(fragments[-1])
            if remaining_size <= 0:
                remaining_size = self.max_size

        return fragments

    def defragment(
        self,
        data: bytes,
    ) -> typing.List[Message]:
        fragments = []
        while data != b"":
            frag, data = Fragment.unpack(data)
            incoming_buffer = self.incoming_buffer.get(frag.object_id)
            if incoming_buffer is None:
                incoming_buffer = {"data": b"", "id": 0}
                self.incoming_buffer[frag.object_id] = incoming_buffer

            if frag.fragment_id != incoming_buffer["id"]:
                raise FragmentError(
                    "Fragment Fragment Id: %d != Expected Fragment Id: %d" % (frag.fragment_id, incoming_buffer["id"])
                )

            if frag.start and frag.end:
                fragments.append(frag.data)
                del self.incoming_buffer[frag.object_id]
            elif frag.start:
                incoming_buffer["data"] = frag.data
                incoming_buffer["id"] += 1
            elif frag.end:
                fragments.append(incoming_buffer["data"] + frag.data)
                del self.incoming_buffer[frag.object_id]
            else:
                incoming_buffer["data"] += frag.data
                incoming_buffer["id"] += 1

        messages = [Message.unpack(fragment, self.serializer) for fragment in fragments]
        return messages

    def _byte_iterator(
        self,
        data: bytes,
        buffer_size: int,
    ) -> typing.Iterator[typing.Tuple[bytes, bool]]:
        byte_count = len(data)
        for i in range(0, byte_count, buffer_size):
            yield data[i : i + buffer_size], i + buffer_size >= byte_count


class Fragment(object):
    def __init__(
        self,
        object_id: int,
        fragment_id: int,
        data: bytes,
        start: bool = False,
        end: bool = False,
    ) -> None:
        self.object_id = object_id
        self.fragment_id = fragment_id
        self.start = start
        self.end = end
        self.data = data

    def pack(self) -> bytes:
        start_end_byte = 0
        if self.start:
            start_end_byte |= 0x1
        if self.end:
            start_end_byte |= 0x2

        data = struct.pack(">Q", self.object_id)
        data += struct.pack(">Q", self.fragment_id)
        data += struct.pack("B", start_end_byte)
        data += struct.pack(">I", len(self.data))
        data += self.data

        return data

    @staticmethod
    def unpack(data: bytes) -> typing.Tuple["Fragment", bytes]:
        object_id = struct.unpack(">Q", data[0:8])[0]
        fragment_id = struct.unpack(">Q", data[8:16])[0]

        start_end_byte = struct.unpack("B", data[16:17])[0]
        start = start_end_byte & 0x1 == 0x1
        end = start_end_byte & 0x2 == 0x2

        length = struct.unpack(">I", data[17:21])[0]
        fragment_data = data[21 : length + 21]

        fragment = Fragment(object_id, fragment_id, fragment_data, start, end)
        return fragment, data[21 + length :]
