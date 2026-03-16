"""Contains client side logic of WinRM SOAP protocol implementation"""

from __future__ import annotations

import base64
import collections.abc
import typing as t
import uuid
import xml.etree.ElementTree as ET

import xmltodict

from winrm.exceptions import (
    WinRMError,
    WinRMOperationTimeoutError,
    WinRMTransportError,
    WSManFaultError,
)
from winrm.transport import Transport

xmlns = {
    "soapenv": "http://www.w3.org/2003/05/soap-envelope",
    "soapaddr": "http://schemas.xmlsoap.org/ws/2004/08/addressing",
    "wsmanfault": "http://schemas.microsoft.com/wbem/wsman/1/wsmanfault",
    "wmierror": "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/MSFT_WmiError",
}


class Protocol(object):
    """This is the main class that does the SOAP request/response logic. There
    are a few helper classes, but pretty much everything comes through here
    first.
    """

    DEFAULT_READ_TIMEOUT_SEC = 30
    DEFAULT_OPERATION_TIMEOUT_SEC = 20
    DEFAULT_MAX_ENV_SIZE = 153600
    DEFAULT_LOCALE = "en-US"

    def __init__(
        self,
        endpoint: str,
        transport: t.Literal["auto", "basic", "certificate", "ntlm", "kerberos", "credssp", "plaintext", "ssl"] = "plaintext",
        username: str | None = None,
        password: str | None = None,
        realm: None = None,
        service: str = "HTTP",
        keytab: None = None,
        ca_trust_path: t.Literal["legacy_requests"] | str = "legacy_requests",
        cert_pem: str | None = None,
        cert_key_pem: str | None = None,
        server_cert_validation: t.Literal["validate", "ignore"] | None = "validate",
        kerberos_delegation: bool = False,
        read_timeout_sec: str | int = DEFAULT_READ_TIMEOUT_SEC,
        operation_timeout_sec: str | int = DEFAULT_OPERATION_TIMEOUT_SEC,
        kerberos_hostname_override: str | None = None,
        message_encryption: t.Literal["auto", "always", "never"] = "auto",
        credssp_disable_tlsv1_2: bool = False,
        send_cbt: bool = True,
        proxy: t.Literal["legacy_requests"] | str | None = "legacy_requests",
    ):
        """
        @param string endpoint: the WinRM webservice endpoint
        @param string transport: transport type, one of 'plaintext' (default), 'kerberos', 'ssl', 'ntlm', 'credssp'  # NOQA
        @param string username: username
        @param string password: password
        @param string realm: unused
        @param string service: the service name, default is HTTP
        @param string keytab: unused
        @param string ca_trust_path: Certification Authority trust path. If server_cert_validation is set to 'validate':
                                        'legacy_requests'(default) to use environment variables,
                                        None to explicitly disallow any additional CA trust path
                                        Any other value will be considered the CA trust path to use.
        @param string cert_pem: client authentication certificate file path in PEM format  # NOQA
        @param string cert_key_pem: client authentication certificate key file path in PEM format  # NOQA
        @param string server_cert_validation: whether server certificate should be validated on Python versions that support it; one of 'validate' (default), 'ignore' #NOQA
        @param bool kerberos_delegation: if True, TGT is sent to target server to allow multiple hops  # NOQA
        @param int read_timeout_sec: maximum seconds to wait before an HTTP connect/read times out (default 30). This value should be slightly higher than operation_timeout_sec, as the server can block *at least* that long. # NOQA
        @param int operation_timeout_sec: maximum allowed time in seconds for any single wsman HTTP operation (default 20). Note that operation timeouts while receiving output (the only wsman operation that should take any significant time, and where these timeouts are expected) will be silently retried indefinitely. # NOQA
        @param string kerberos_hostname_override: the hostname to use for the kerberos exchange (defaults to the hostname in the endpoint URL)
        @param bool message_encryption_enabled: Will encrypt the WinRM messages if set to True and the transport auth supports message encryption (Default True).
        @param string proxy: Specify a proxy for the WinRM connection to use. 'legacy_requests'(default) to use environment variables, None to disable proxies completely or the proxy URL itself.
        """

        try:
            read_timeout_sec = int(read_timeout_sec)
        except ValueError as ve:
            raise ValueError("failed to parse read_timeout_sec as int: %s" % str(ve))

        try:
            operation_timeout_sec = int(operation_timeout_sec)
        except ValueError as ve:
            raise ValueError("failed to parse operation_timeout_sec as int: %s" % str(ve))

        if operation_timeout_sec >= read_timeout_sec or operation_timeout_sec < 1:
            raise WinRMError("read_timeout_sec must exceed operation_timeout_sec, and both must be non-zero")

        self.read_timeout_sec = read_timeout_sec
        self.operation_timeout_sec = operation_timeout_sec
        self.max_env_sz = Protocol.DEFAULT_MAX_ENV_SIZE
        self.locale = Protocol.DEFAULT_LOCALE

        self.transport = Transport(
            endpoint=endpoint,
            username=username,
            password=password,
            realm=realm,
            service=service,
            keytab=keytab,
            ca_trust_path=ca_trust_path,
            cert_pem=cert_pem,
            cert_key_pem=cert_key_pem,
            read_timeout_sec=self.read_timeout_sec,
            server_cert_validation=server_cert_validation,
            kerberos_delegation=kerberos_delegation,
            kerberos_hostname_override=kerberos_hostname_override,
            auth_method=transport,
            message_encryption=message_encryption,
            credssp_disable_tlsv1_2=credssp_disable_tlsv1_2,
            send_cbt=send_cbt,
            proxy=proxy,
        )

        self.username = username
        self.password = password
        self.service = service
        self.keytab = keytab
        self.ca_trust_path = ca_trust_path
        self.server_cert_validation = server_cert_validation
        self.kerberos_delegation = kerberos_delegation
        self.kerberos_hostname_override = kerberos_hostname_override
        self.credssp_disable_tlsv1_2 = credssp_disable_tlsv1_2

    def open_shell(
        self,
        i_stream: str = "stdin",
        o_stream: str = "stdout stderr",
        working_directory: str | None = None,
        env_vars: dict[str, str] | None = None,
        noprofile: bool = False,
        codepage: int = 437,
        lifetime: None = None,
        idle_timeout: str | int | None = None,
    ) -> str:
        """
        Create a Shell on the destination host
        @param string i_stream: Which input stream to open. Leave this alone
         unless you know what you're doing (default: stdin)
        @param string o_stream: Which output stream to open. Leave this alone
         unless you know what you're doing (default: stdout stderr)
        @param string working_directory: the directory to create the shell in
        @param dict env_vars: environment variables to set for the shell. For
         instance: {'PATH': '%PATH%;c:/Program Files (x86)/Git/bin/', 'CYGWIN':
          'nontsec codepage:utf8'}
        @returns The ShellId from the SOAP response. This is our open shell
         instance on the remote machine.
        @rtype string
        """
        req = {
            "env:Envelope": self.build_wsman_header(
                resource_uri="http://schemas.microsoft.com/wbem/wsman/1/windows/shell/cmd",  # NOQA
                action="http://schemas.xmlsoap.org/ws/2004/09/transfer/Create",
            )
        }
        header = req["env:Envelope"]["env:Header"]
        header["w:OptionSet"] = {
            "w:Option": [
                {"@Name": "WINRS_NOPROFILE", "#text": str(noprofile).upper()},  # TODO remove str call
                {"@Name": "WINRS_CODEPAGE", "#text": str(codepage)},  # TODO remove str call
            ]
        }

        shell = req["env:Envelope"].setdefault("env:Body", {}).setdefault("rsp:Shell", {})
        shell["rsp:InputStreams"] = i_stream
        shell["rsp:OutputStreams"] = o_stream

        if working_directory:
            # TODO ensure that rsp:WorkingDirectory should be nested within rsp:Shell  # NOQA
            shell["rsp:WorkingDirectory"] = working_directory
            # TODO check Lifetime param: http://msdn.microsoft.com/en-us/library/cc251546(v=PROT.13).aspx  # NOQA
            # if lifetime:
            #    shell['rsp:Lifetime'] = iso8601_duration.sec_to_dur(lifetime)
        # TODO make it so the input is given in milliseconds and converted to xs:duration  # NOQA
        if idle_timeout:
            shell["rsp:IdleTimeOut"] = idle_timeout
        if env_vars:
            # the rsp:Variable tag needs to be list of variables so that all
            # environment variables in the env_vars dict are set on the shell
            env = shell.setdefault("rsp:Environment", {}).setdefault("rsp:Variable", [])
            for key, value in env_vars.items():
                env.append({"@Name": key, "#text": value})

        res = self.send_message(xmltodict.unparse(req))

        # res = xmltodict.parse(res)
        # return res['s:Envelope']['s:Body']['x:ResourceCreated']['a:ReferenceParameters']['w:SelectorSet']['w:Selector']['#text']
        root = ET.fromstring(res)
        return t.cast(str, next(node for node in root.findall(".//*") if node.get("Name") == "ShellId").text)

    # Helper method for building SOAP Header
    def build_wsman_header(
        self,
        action: str,
        resource_uri: str,
        shell_id: str | None = None,
        message_id: str | uuid.UUID | None = None,
    ) -> dict[str, t.Any]:
        """
        Builds the standard header needed for WSMan operations. The return
        value is a dictionary that can be used by xmltodict to generate the
        WSMan envelope when sending custom requests.

        @param string action: The WSMan action to perform.
        @param string resource_uri: The WSMan resource URI the request is for.
        @param string shell_id: The optional shell UUID the request is for.
        @param string message_id: A unique message UUID, if unset a random UUID
            is used.
        @returns The WSMan header as a dictionary.
        @rtype dict[str, t.Any]
        """
        if not message_id:
            message_id = uuid.uuid4()
        header: dict[str, t.Any] = {
            "@xmlns:xsd": "http://www.w3.org/2001/XMLSchema",
            "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "@xmlns:env": xmlns["soapenv"],
            "@xmlns:a": xmlns["soapaddr"],
            "@xmlns:b": "http://schemas.dmtf.org/wbem/wsman/1/cimbinding.xsd",
            "@xmlns:n": "http://schemas.xmlsoap.org/ws/2004/09/enumeration",
            "@xmlns:x": "http://schemas.xmlsoap.org/ws/2004/09/transfer",
            "@xmlns:w": "http://schemas.dmtf.org/wbem/wsman/1/wsman.xsd",
            "@xmlns:p": "http://schemas.microsoft.com/wbem/wsman/1/wsman.xsd",
            "@xmlns:rsp": "http://schemas.microsoft.com/wbem/wsman/1/windows/shell",  # NOQA
            "@xmlns:cfg": "http://schemas.microsoft.com/wbem/wsman/1/config",
            "env:Header": {
                "a:To": "http://windows-host:5985/wsman",
                "a:ReplyTo": {"a:Address": {"@mustUnderstand": "true", "#text": "http://schemas.xmlsoap.org/ws/2004/08/addressing/role/anonymous"}},  # NOQA
                "w:MaxEnvelopeSize": {"@mustUnderstand": "true", "#text": "153600"},
                "a:MessageID": "uuid:{0}".format(message_id),
                "w:Locale": {"@mustUnderstand": "false", "@xml:lang": "en-US"},
                "p:DataLocale": {"@mustUnderstand": "false", "@xml:lang": "en-US"},
                # TODO: research this a bit http://msdn.microsoft.com/en-us/library/cc251561(v=PROT.13).aspx  # NOQA
                # 'cfg:MaxTimeoutms': 600
                # Operation timeout in ISO8601 format, see http://msdn.microsoft.com/en-us/library/ee916629(v=PROT.13).aspx  # NOQA
                "w:OperationTimeout": "PT{0}S".format(int(self.operation_timeout_sec)),
                "w:ResourceURI": {"@mustUnderstand": "true", "#text": resource_uri},
                "a:Action": {"@mustUnderstand": "true", "#text": action},
            },
        }
        if shell_id:
            header["env:Header"]["w:SelectorSet"] = {"w:Selector": {"@Name": "ShellId", "#text": shell_id}}
        return header

    # For backwards compatibility with Ansible. This should not be removed
    # until all supported releases of Ansible has been updated to use the new
    # method.
    _get_soap_header = build_wsman_header

    def send_message(self, message: str) -> bytes:
        # TODO add message_id vs relates_to checking
        # TODO port error handling code
        try:
            resp = self.transport.send_message(message)
            return resp
        except WinRMTransportError as ex:
            try:
                # if response is XML-parseable, it's probably a SOAP fault; extract the details
                root = ET.fromstring(ex.response_text)
            except Exception:
                # assume some other transport error; raise the original exception
                raise ex

            fault = root.find("soapenv:Body/soapenv:Fault", xmlns)
            if fault is None:
                raise

            wsmanfault_code_raw = fault.find("soapenv:Detail/wsmanfault:WSManFault[@Code]", xmlns)
            wsmanfault_code: int | None = None
            if wsmanfault_code_raw is not None:
                wsmanfault_code = int(wsmanfault_code_raw.attrib["Code"])

                # convert receive timeout code to WinRMOperationTimeoutError
                if wsmanfault_code == 2150858793:
                    # TODO: this fault code is specific to the Receive operation; convert all op timeouts?
                    raise WinRMOperationTimeoutError()

            fault_code_raw = fault.find("soapenv:Code/soapenv:Value", xmlns)
            fault_code: str | None = None
            if fault_code_raw is not None and fault_code_raw.text:
                fault_code = fault_code_raw.text

            fault_subcode_raw = fault.find("soapenv:Code/soapenv:Subcode/soapenv:Value", xmlns)
            fault_subcode: str | None = None
            if fault_subcode_raw is not None and fault_subcode_raw.text:
                fault_subcode = fault_subcode_raw.text

            error_message_node = fault.find("soapenv:Reason/soapenv:Text", xmlns)
            reason: str | None = None
            if error_message_node is not None:
                reason = error_message_node.text

            wmi_error_code_raw = fault.find("soapenv:Detail/wmierror:MSFT_WmiError/wmierror:error_Code", xmlns)
            wmi_error_code: int | None = None
            if wmi_error_code_raw is not None and wmi_error_code_raw.text:
                wmi_error_code = int(wmi_error_code_raw.text)

            raise WSManFaultError(
                code=ex.code,
                message=ex.message,
                response=ex.response_text,
                reason=reason or "(no error message in fault)",
                fault_code=fault_code,
                fault_subcode=fault_subcode,
                wsman_fault_code=wsmanfault_code,
                wmierror_code=wmi_error_code,
            )

    def close_shell(self, shell_id: str, close_session: bool = True) -> None:
        """
        Close the shell
        @param string shell_id: The shell id on the remote machine.
         See #open_shell
        @param bool close_session: If we want to close the requests's session.
         Allows to completely close all TCP connections to the server.
        @returns This should have more error checking but it just returns true
         for now.
        @rtype bool
        """
        try:
            message_id = uuid.uuid4()
            req = {
                "env:Envelope": self.build_wsman_header(
                    resource_uri="http://schemas.microsoft.com/wbem/wsman/1/windows/shell/cmd",  # NOQA
                    action="http://schemas.xmlsoap.org/ws/2004/09/transfer/Delete",
                    shell_id=shell_id,
                    message_id=message_id,
                )
            }

            # SOAP message requires empty env:Body
            req["env:Envelope"].setdefault("env:Body", {})

            res = self.send_message(xmltodict.unparse(req))
            root = ET.fromstring(res)
            relates_to = t.cast(str, next(node for node in root.findall(".//*") if node.tag.endswith("RelatesTo")).text)
        finally:
            # Close the transport if we are done with the shell.
            # This will ensure no lingering TCP connections are thrown back into a requests' connection pool.
            if close_session:
                self.transport.close_session()

        # TODO change assert into user-friendly exception
        assert uuid.UUID(relates_to.replace("uuid:", "")) == message_id

    def run_command(
        self,
        shell_id: str,
        command: str,
        arguments: collections.abc.Iterable[str | bytes] = (),
        console_mode_stdin: bool = True,
        skip_cmd_shell: bool = False,
    ) -> str:
        """
        Run a command on a machine with an open shell
        @param string shell_id: The shell id on the remote machine.
         See #open_shell
        @param string command: The command to run on the remote machine
        @param iterable of string arguments: An array of arguments for this
         command
        @param bool console_mode_stdin: (default: True)
        @param bool skip_cmd_shell: (default: False)
        @return: The CommandId from the SOAP response.
         This is the ID we need to query in order to get output.
        @rtype string
        """
        req = {
            "env:Envelope": self.build_wsman_header(
                resource_uri="http://schemas.microsoft.com/wbem/wsman/1/windows/shell/cmd",  # NOQA
                action="http://schemas.microsoft.com/wbem/wsman/1/windows/shell/Command",  # NOQA
                shell_id=shell_id,
            )
        }
        header = req["env:Envelope"]["env:Header"]
        header["w:OptionSet"] = {
            "w:Option": [
                {"@Name": "WINRS_CONSOLEMODE_STDIN", "#text": str(console_mode_stdin).upper()},
                {"@Name": "WINRS_SKIP_CMD_SHELL", "#text": str(skip_cmd_shell).upper()},
            ]
        }
        cmd_line = req["env:Envelope"].setdefault("env:Body", {}).setdefault("rsp:CommandLine", {})
        cmd_line["rsp:Command"] = {"#text": command}
        if arguments:
            unicode_args = [a if isinstance(a, str) else a.decode("utf-8") for a in arguments]
            cmd_line["rsp:Arguments"] = " ".join(unicode_args)

        res = self.send_message(xmltodict.unparse(req))
        root = ET.fromstring(res)
        command_id = next(node for node in root.findall(".//*") if node.tag.endswith("CommandId")).text
        return t.cast(str, command_id)

    def cleanup_command(self, shell_id: str, command_id: str) -> None:
        """
        Clean-up after a command. @see #run_command
        @param string shell_id: The shell id on the remote machine.
         See #open_shell
        @param string command_id: The command id on the remote machine.
         See #run_command
        @returns: This should have more error checking but it just returns true
         for now.
        @rtype bool
        """
        message_id = uuid.uuid4()
        req = {
            "env:Envelope": self.build_wsman_header(
                resource_uri="http://schemas.microsoft.com/wbem/wsman/1/windows/shell/cmd",  # NOQA
                action="http://schemas.microsoft.com/wbem/wsman/1/windows/shell/Signal",  # NOQA
                shell_id=shell_id,
                message_id=message_id,
            )
        }

        # Signal the Command references to terminate (close stdout/stderr)
        signal = req["env:Envelope"].setdefault("env:Body", {}).setdefault("rsp:Signal", {})
        signal["@CommandId"] = command_id
        signal["rsp:Code"] = "http://schemas.microsoft.com/wbem/wsman/1/windows/shell/signal/terminate"  # NOQA

        res = self.send_message(xmltodict.unparse(req))
        root = ET.fromstring(res)
        relates_to = t.cast(str, next(node for node in root.findall(".//*") if node.tag.endswith("RelatesTo")).text)
        # TODO change assert into user-friendly exception
        assert uuid.UUID(relates_to.replace("uuid:", "")) == message_id

    def send_command_input(self, shell_id: str, command_id: str, stdin_input: str | bytes, end: bool = False) -> None:
        """
        Send input to the given shell and command.
        @param string shell_id: The shell id on the remote machine.
         See #open_shell
        @param string command_id: The command id on the remote machine.
         See #run_command
        @param string stdin_input: The input unicode string or byte string to be sent.
        @param bool end: Boolean value which will close the stdin stream. If end=True then the stdin pipe to the
        remotely running process will be closed causing the next read by the remote process to stdin to return a
        EndOfFile error; the behavior of each process when this error is encountered is defined by the process, but most
        processes ( like CMD and powershell for instance) will just exit. Setting this value to 'True' means that no
        more input will be able to be sent to the process and attempting to do so should result in an error.
        @return: None
        """
        if isinstance(stdin_input, str):
            stdin_input = stdin_input.encode("437")
        req = {
            "env:Envelope": self.build_wsman_header(
                resource_uri="http://schemas.microsoft.com/wbem/wsman/1/windows/shell/cmd",  # NOQA
                action="http://schemas.microsoft.com/wbem/wsman/1/windows/shell/Send",  # NOQA
                shell_id=shell_id,
            )
        }
        stdin_envelope = req["env:Envelope"].setdefault("env:Body", {}).setdefault("rsp:Send", {}).setdefault("rsp:Stream", {})
        stdin_envelope["@CommandId"] = command_id
        stdin_envelope["@Name"] = "stdin"
        if end:
            stdin_envelope["@End"] = "true"
        else:
            stdin_envelope["@End"] = "false"
        stdin_envelope["@xmlns:rsp"] = "http://schemas.microsoft.com/wbem/wsman/1/windows/shell"
        stdin_envelope["#text"] = base64.b64encode(stdin_input)
        self.send_message(xmltodict.unparse(req))

    def get_command_output(self, shell_id: str, command_id: str) -> tuple[bytes, bytes, int]:
        """
        Get the Output of the given shell and command. This will wait until the
        command is finished before returning the output.

        @param string shell_id: The shell id on the remote machine.
         See #open_shell
        @param string command_id: The command id on the remote machine.
         See #run_command
        @return tuple[bytes, bytes, int]: Returns a tuple with the stdout,
            stderr, and the return code of the command. The stdout and stderr
            value is a byte string and not a normal string.
        """
        stdout_buffer, stderr_buffer = [], []
        command_done = False
        while not command_done:
            try:
                stdout, stderr, return_code, command_done = self.get_command_output_raw(shell_id, command_id)
                stdout_buffer.append(stdout)
                stderr_buffer.append(stderr)
            except WinRMOperationTimeoutError:
                # this is an expected error when waiting for a long-running process, just silently retry
                pass
        return b"".join(stdout_buffer), b"".join(stderr_buffer), return_code

    def get_command_output_raw(self, shell_id: str, command_id: str) -> tuple[bytes, bytes, int, bool]:
        """
        Get the next available output of the given shell and command. This
        will wait until the issued WSMan Receive action returns data or times
        out with WinRMOperationTimeoutError.

        @param string shell_id: The shell id on the remote machine.
         See #open_shell
        @param string command_id: The command id on the remote machine.
         See #run_command
        @return tuple[bytes, bytes, int, bool]: Returns a tuple with the stdout,
            stderr, the return code of the command, and whether it has finished
            or not. The stdout and stderr value is a byte string and not a
            normal string.
        @raises WinRMOperationTimeoutError: Raised when there has been no
            output from the command
        """
        req = {
            "env:Envelope": self.build_wsman_header(
                resource_uri="http://schemas.microsoft.com/wbem/wsman/1/windows/shell/cmd",  # NOQA
                action="http://schemas.microsoft.com/wbem/wsman/1/windows/shell/Receive",  # NOQA
                shell_id=shell_id,
            )
        }

        stream = req["env:Envelope"].setdefault("env:Body", {}).setdefault("rsp:Receive", {}).setdefault("rsp:DesiredStream", {})
        stream["@CommandId"] = command_id
        stream["#text"] = "stdout stderr"

        res = self.send_message(xmltodict.unparse(req))
        root = ET.fromstring(res)
        stream_nodes = [node for node in root.findall(".//*") if node.tag.endswith("Stream")]
        stdout = []
        stderr = []
        return_code = -1
        for stream_node in stream_nodes:
            if not stream_node.text:
                continue
            if stream_node.attrib["Name"] == "stdout":
                stdout.append(base64.b64decode(stream_node.text.encode("ascii")))
            elif stream_node.attrib["Name"] == "stderr":
                stderr.append(base64.b64decode(stream_node.text.encode("ascii")))

        # We may need to get additional output if the stream has not finished.
        # The CommandState will change from Running to Done like so:
        # @example
        #   from...
        #   <rsp:CommandState CommandId="..." State="http://schemas.microsoft.com/wbem/wsman/1/windows/shell/CommandState/Running"/>
        #   to...
        #   <rsp:CommandState CommandId="..." State="http://schemas.microsoft.com/wbem/wsman/1/windows/shell/CommandState/Done">
        #     <rsp:ExitCode>0</rsp:ExitCode>
        #   </rsp:CommandState>
        command_done = len([node for node in root.findall(".//*") if node.get("State", "").endswith("CommandState/Done")]) == 1
        if command_done:
            return_code = int(next(node for node in root.findall(".//*") if node.tag.endswith("ExitCode")).text or -1)

        return b"".join(stdout), b"".join(stderr), return_code, command_done

    # While it was meant to be private it has been treated as a public API.
    # This might be removed in a future version but for now keep it as an
    # alias for the now public API method 'get_command_output_raw'.
    # https://github.com/search?q=_raw_get_command_output+language%3APython&type=code&l=Python
    _raw_get_command_output = get_command_output_raw
