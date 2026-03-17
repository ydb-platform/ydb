from __future__ import annotations

import collections.abc
import re
import typing as t
import warnings
import xml.etree.ElementTree as ET
from base64 import b64encode

from winrm.protocol import Protocol

__version__ = "0.5.0"

# Feature support attributes for multi-version clients.
# These values can be easily checked for with hasattr(winrm, "FEATURE_X"),
# "'auth_type' in winrm.FEATURE_SUPPORTED_AUTHTYPES", etc for clients to sniff features
# supported by a particular version of pywinrm
FEATURE_SUPPORTED_AUTHTYPES = ["basic", "certificate", "ntlm", "kerberos", "plaintext", "ssl", "credssp"]
FEATURE_READ_TIMEOUT = True
FEATURE_OPERATION_TIMEOUT = True
FEATURE_PROXY_SUPPORT = True


class Response(object):
    """Response from a remote command execution"""

    def __init__(self, args: tuple[bytes, bytes, int]) -> None:
        self.std_out, self.std_err, self.status_code = args

    def __repr__(self) -> str:
        # TODO put tree dots at the end if out/err was truncated
        return '<Response code {0}, out "{1!r}", err "{2!r}">'.format(self.status_code, self.std_out[:20], self.std_err[:20])


class Session(object):
    # TODO implement context manager methods
    def __init__(self, target: str, auth: tuple[str, str], **kwargs: t.Any) -> None:
        username, password = auth
        self.url = self._build_url(target, kwargs.get("transport", "plaintext"))
        self.protocol = Protocol(self.url, username=username, password=password, **kwargs)

    def run_cmd(self, command: str, args: collections.abc.Iterable[str | bytes] = ()) -> Response:
        # TODO optimize perf. Do not call open/close shell every time
        shell_id = self.protocol.open_shell()
        command_id = self.protocol.run_command(shell_id, command, args)
        rs = Response(self.protocol.get_command_output(shell_id, command_id))
        self.protocol.cleanup_command(shell_id, command_id)
        self.protocol.close_shell(shell_id)
        return rs

    def run_ps(self, script: str) -> Response:
        """base64 encodes a Powershell script and executes the powershell
        encoded script command
        """
        # must use utf16 little endian on windows
        encoded_ps = b64encode(script.encode("utf_16_le")).decode("ascii")
        rs = self.run_cmd("powershell -encodedcommand {0}".format(encoded_ps))
        if len(rs.std_err):
            # if there was an error message, clean it it up and make it human
            # readable
            rs.std_err = self._clean_error_msg(rs.std_err)
        return rs

    def _clean_error_msg(self, msg: bytes) -> bytes:
        """converts a Powershell CLIXML message to a more human readable string"""
        # TODO prepare unit test, beautify code
        # if the msg does not start with this, return it as is
        if msg.startswith(b"#< CLIXML\r\n"):
            # for proper xml, we need to remove the CLIXML part
            # (the first line)
            msg_xml = msg[11:]
            try:
                # remove the namespaces from the xml for easier processing
                msg_xml = self._strip_namespace(msg_xml)
                root = ET.fromstring(msg_xml)
                # the S node is the error message, find all S nodes
                nodes = root.findall("./S")
                new_msg = ""
                for s in nodes:
                    # append error msg string to result, also
                    # the hex chars represent CRLF so we replace with newline
                    if s.text:
                        new_msg += s.text.replace("_x000D__x000A_", "\n")
            except Exception as e:
                # if any of the above fails, the msg was not true xml
                # print a warning and return the original string
                warnings.warn("There was a problem converting the Powershell error " "message: %s" % (e))
            else:
                # if new_msg was populated, that's our error message
                # otherwise the original error message will be used
                if len(new_msg):
                    # remove leading and trailing whitespace while we are here
                    return new_msg.strip().encode("utf-8")

        # either failed to decode CLIXML or there was nothing to decode
        # just return the original message
        return msg

    def _strip_namespace(self, xml: bytes) -> bytes:
        """strips any namespaces from an xml string"""
        p = re.compile(b'xmlns=*[""][^""]*[""]')
        allmatches = p.finditer(xml)
        for match in allmatches:
            xml = xml.replace(match.group(), b"")
        return xml

    @staticmethod
    def _build_url(target: str, transport: str) -> str:
        match = re.match(r"(?i)^((?P<scheme>http[s]?)://)?(?P<host>[0-9a-z-_.]+)(:(?P<port>\d+))?(?P<path>(/)?(wsman)?)?", target)  # NOQA
        if not match:
            raise ValueError("Invalid target URL: {0}".format(target))

        scheme = match.group("scheme")
        if not scheme:
            # TODO do we have anything other than HTTP/HTTPS
            scheme = "https" if transport == "ssl" else "http"
        host = match.group("host")
        port = match.group("port")
        if not port:
            port = 5986 if transport == "ssl" else 5985
        path = match.group("path")
        if not path:
            path = "wsman"
        return "{0}://{1}:{2}/{3}".format(scheme, host, port, path.lstrip("/"))
