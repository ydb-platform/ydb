# Copyright: (c) 2018, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

from __future__ import division

import base64
import hashlib
import logging
import os
import shutil
import tempfile
import types
import typing
import warnings
import xml.etree.ElementTree as ET

from pypsrp._utils import get_pwsh_script, to_bytes, to_unicode
from pypsrp.exceptions import WinRMError
from pypsrp.powershell import (
    DEFAULT_CONFIGURATION_NAME,
    PowerShell,
    PSDataStreams,
    RunspacePool,
)
from pypsrp.serializer import Serializer
from pypsrp.shell import Process, SignalCode, WinRS
from pypsrp.wsman import WSMan

log = logging.getLogger(__name__)


class Client(object):
    def __init__(
        self,
        server: str,
        **kwargs: typing.Any,
    ) -> None:
        """
        Creates a client object used to do the following
            spawn new cmd command/process
            spawn new PowerShell Runspace Pool/Pipeline
            copy a file from localhost to the remote Windows host
            fetch a file from the remote Windows host to localhost

        This is just an easy to use layer on top of the objects WinRS and
        RunspacePool/PowerShell. It trades flexibility in favour of simplicity.

        If your use case needs some of that flexibility you can use these
        functions as a reference implementation for your own functions.

        :param server: The server/host to connect to
        :param kwargs: The various WSMan args to control the transport
            mechanism, see pypsrp.wsman.WSMan for these args
        """
        self.wsman = WSMan(server, **kwargs)

    def __enter__(self) -> "Client":
        return self

    def __exit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]],
        value: typing.Optional[BaseException],
        traceback: typing.Optional[types.TracebackType],
    ) -> None:
        self.close()

    def copy(
        self,
        src: str,
        dest: str,
        configuration_name: str = DEFAULT_CONFIGURATION_NAME,
        expand_variables: bool = False,
    ) -> str:
        """
        Copies a single file from the current host to the remote Windows host.
        This can be quite slow when it comes to large files due to the
        limitations of WinRM but it is designed to be as fast as it can be.
        During the copy process, the bytes will be stored in a temporary file
        before being copied.

        When copying it will replace the file at dest if one already exists. It
        also will verify the checksum of the copied file is the same as the
        actual file locally before copying the file to the path at dest.

        :param src: The path to the local file
        :param dest: The path to the destination file on the Windows host
        :param configuration_name: The PowerShell configuration endpoint to
            use when copying the file.
        :param expand_variables: Expand variables in path. Disabled by default
            Enable for cmd like expansion (for example %TMP% in path)
        :return: The absolute path of the file on the Windows host
        """

        def read_buffer(b_path: bytes, total_size: int, buffer_size: int) -> typing.Iterator:
            offset = 0
            sha1 = hashlib.sha1()

            with open(b_path, "rb") as src_file:
                for data in iter((lambda: src_file.read(buffer_size)), b""):
                    log.debug("Reading data of file at offset=%d with size=%d" % (offset, buffer_size))
                    offset += len(data)
                    sha1.update(data)
                    b64_data = base64.b64encode(data)

                    result = [to_unicode(b64_data)]
                    if offset == total_size:
                        result.append(to_unicode(base64.b64encode(to_bytes(sha1.hexdigest()))))

                    yield result

                # the file was empty, return empty buffer
                if offset == 0:
                    yield ["", to_unicode(base64.b64encode(to_bytes(sha1.hexdigest())))]

        if expand_variables:
            src = os.path.expanduser(os.path.expandvars(src))
        b_src = to_bytes(src)
        src_size = os.path.getsize(b_src)
        log.info("Copying '%s' to '%s' with a total size of %d" % (src, dest, src_size))

        with RunspacePool(self.wsman, configuration_name=configuration_name) as pool:
            # Get the buffer size of each fragment to send, subtract. Adjust to size of the base64 encoded bytes. Also
            # subtract 82 for the fragment, message, and other header info that PSRP adds.
            buffer_size = int((self.wsman.max_payload_size - 82) / 4 * 3)

            log.info("Creating file reader with a buffer size of %d" % buffer_size)
            read_gen = read_buffer(b_src, src_size, buffer_size)

            command = get_pwsh_script("copy.ps1")
            log.debug("Starting to send file data to remote process")
            powershell = PowerShell(pool)
            powershell.add_script(command).add_argument(dest).add_argument(expand_variables)
            powershell.invoke(input=read_gen)
            _handle_powershell_error(powershell, "Failed to copy file")

        log.debug("Finished sending file data to remote process")
        for warning in powershell.streams.warning:
            warnings.warn(str(warning))

        output_file = to_unicode(powershell.output[-1]).strip()
        log.info("Completed file transfer of '%s' to '%s'" % (src, output_file))
        return output_file

    def execute_cmd(
        self,
        command: str,
        encoding: str = "437",
        environment: typing.Optional[typing.Dict[str, str]] = None,
    ) -> typing.Tuple[str, str, int]:
        """
        Executes a command in a cmd shell and returns the stdout/stderr/rc of
        that process. This uses the raw WinRS layer and can be used to execute
        a traditional process.

        :param command: The command to execute
        :param encoding: The encoding of the output std buffers, this
            correlates to the codepage of the host and traditionally en-US
            is 437. This probably doesn't need to be modified unless you are
            running a different codepage on your host
        :param environment: A dictionary containing environment keys and
            values to set on the executing process.
        :return: A tuple of
            stdout: A unicode string of the stdout
            stderr: A unicode string of the stderr
            rc: The return code of the process

        Both stdout and stderr are returned from the server as a byte string,
        they are converted to a unicode string based on the encoding variable
        set
        """
        log.info("Executing cmd process '%s'" % command)
        with WinRS(self.wsman, environment=environment) as shell:
            process = Process(shell, command)
            process.invoke()
            process.signal(SignalCode.CTRL_C)

        rc = process.rc if process.rc is not None else -1
        return to_unicode(process.stdout, encoding), to_unicode(process.stderr, encoding), rc

    def execute_ps(
        self,
        script: str,
        configuration_name: str = DEFAULT_CONFIGURATION_NAME,
        environment: typing.Optional[typing.Dict[str, str]] = None,
    ) -> typing.Tuple[str, PSDataStreams, bool]:
        """
        Executes a PowerShell script in a PowerShell runspace pool. This uses
        the PSRP layer and is designed to run a PowerShell script and not a
        raw executable.

        Because this runs in a runspace, traditional concepts like stdout,
        stderr, rc's are no longer relevant. Instead there is a output,
        error/verbose/debug streams, and a boolean that indicates if the
        script execution came across an error. If you want the traditional
        stdout/stderr/rc, use execute_cmd instead.

        :param script: The PowerShell script to run
        :param configuration_name: The PowerShell configuration endpoint to
            use when executing the script.
        :param environment: A dictionary containing environment keys and
            values to set on the executing script.
        :return: A tuple of
            output: A unicode string of the output stream
            streams: pypsrp.powershell.PSDataStreams containing the other
                PowerShell streams
            had_errors: bool that indicates whether the script had errors
                during execution
        """
        log.info("Executing PowerShell script '%s'" % script)
        with RunspacePool(self.wsman, configuration_name=configuration_name) as pool:
            powershell = PowerShell(pool)

            if environment:
                for env_key, env_value in environment.items():
                    # Done like this for easier testing, preserves the param order
                    log.debug("Setting env var '%s' on PS script execution" % env_key)
                    powershell.add_cmdlet("New-Item").add_parameter("Path", "env:").add_parameter(
                        "Name", env_key
                    ).add_parameter("Value", env_value).add_parameter("Force", True).add_cmdlet(
                        "Out-Null"
                    ).add_statement()

            # so the client executes a powershell script and doesn't need to
            # deal with complex PS objects, we run the script in
            # Invoke-Expression and convert the output to a string
            # if a user wants to get the raw complex objects then they should
            # use RunspacePool and PowerShell directly
            powershell.add_cmdlet("Invoke-Expression").add_parameter("Command", script)
            powershell.add_cmdlet("Out-String").add_parameter("Stream")
            powershell.invoke()

        return "\n".join(powershell.output), powershell.streams, powershell.had_errors

    def fetch(
        self,
        src: str,
        dest: str,
        configuration_name: str = DEFAULT_CONFIGURATION_NAME,
        expand_variables: bool = False,
    ) -> None:
        """
        Will fetch a single file from the remote Windows host and create a
        local copy. Like copy(), this can be slow when it comes to fetching
        large files due to the limitation of WinRM.

        This method will first store the file in a temporary location before
        creating or replacing the file at dest if the checksum is correct.

        :param src: The path to the file on the remote host to fetch
        :param dest: The path on the localhost host to store the file as
        :param configuration_name: The PowerShell configuration endpoint to
            use when fetching the file.
        :param expand_variables: Expand variables in path. Disabled by default
            Enable for cmd like expansion (for example %TMP% in path)
        """
        if expand_variables:
            dest = os.path.expanduser(os.path.expandvars(dest))
        log.info("Fetching '%s' to '%s'" % (src, dest))

        with RunspacePool(self.wsman, configuration_name=configuration_name) as pool:
            script = get_pwsh_script("fetch.ps1")
            powershell = PowerShell(pool)
            powershell.add_script(script).add_argument(src).add_argument(expand_variables)

            log.debug("Starting remote process to output file data")
            powershell.invoke()
            _handle_powershell_error(powershell, "Failed to fetch file %s" % src)
            log.debug("Finished remote process to output file data")

            expected_hash = powershell.output[-1]

            temp_file, path = tempfile.mkstemp()
            try:
                file_bytes = base64.b64decode(powershell.output[0])
                os.write(temp_file, file_bytes)

                sha1 = hashlib.sha1()
                sha1.update(file_bytes)
                actual_hash = sha1.hexdigest()

                log.debug("Remote Hash: %s, Local Hash: %s" % (expected_hash, actual_hash))
                if actual_hash != expected_hash:
                    raise WinRMError(
                        "Failed to fetch file %s, hash mismatch\n"
                        "Source: %s\nFetched: %s" % (src, expected_hash, actual_hash)
                    )
                shutil.copy(path, dest)
            finally:
                os.close(temp_file)
                os.remove(path)

    def close(self) -> None:
        self.wsman.close()

    @staticmethod
    def sanitise_clixml(clixml: str) -> str:
        """
        When running a powershell script in execute_cmd (WinRS), the stderr
        stream may contain some clixml. This method will clear it up and
        replace it with the error string it would represent. This isn't done
        by default on execute_cmd for various reasons but people can call it
        manually here if they like.

        :param clixml: The clixml to parse
        :return: A unicode code string of the decoded output
        """
        output = to_unicode(clixml)
        if output.startswith("#< CLIXML"):
            # Strip off the '#< CLIXML\r\n' by finding the 2nd index of '<'
            output = output[clixml.index("<", 2) :]
            element = ET.fromstring(output)
            namespace = element.tag.replace("Objs", "")[1:-1]

            errors: typing.List[str] = []
            for error in element.findall("{%s}S[@S='Error']" % namespace):
                errors.append(error.text or "")

            output = Serializer()._deserialize_string("".join(errors))

        return output


def _handle_powershell_error(powershell: PowerShell, message: str) -> None:
    if message and powershell.had_errors:
        errors = powershell.streams.error
        error = "\n".join([str(err) for err in errors])
        raise WinRMError("%s: %s" % (message, error))
