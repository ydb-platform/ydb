# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Wraps multiple ways to communicate over SSH.
"""

import os
import re
import time
import logging
import warnings
import subprocess
from typing import List, Type, Tuple, Union, Optional, cast
from os.path import join as pjoin
from os.path import split as psplit

from libcloud.utils.py3 import StringIO, b
from libcloud.utils.logging import ExtraLogFormatter

have_paramiko = False

try:
    import paramiko

    have_paramiko = True

    PARAMIKO_VERSION_TUPLE = tuple(int(x) for x in paramiko.__version__.split("."))
except ImportError:
    PARAMIKO_VERSION_TUPLE = ()

# Depending on your version of Paramiko, it may cause a deprecation
# warning on Python 2.6.
# Ref: https://bugs.launchpad.net/paramiko/+bug/392973


__all__ = [
    "BaseSSHClient",
    "ParamikoSSHClient",
    "ShellOutSSHClient",
    "SSHCommandTimeoutError",
]

SUPPORTED_KEY_TYPES_URL = "https://libcloud.readthedocs.io/en/latest/compute/deployment.html#supported-private-ssh-key-types"  # NOQA

# Set it to False to disable backward compatibility mode when running
# paramiko >= 2.9.0. In backward compatibility mode we try to disable newer
# SHA-2 based public key algorithms in case server returns auth error. This
# way it works correctly with older OpenSSH servers which don't support those
# algorithms aka the behavior is the same as with paramiko < 2.9.0.
# In case users only talk to newer OpenSSH servers which support those
# algorithms, they may want to disable this workaround.
LIBCLOUD_PARAMIKO_SHA2_BACKWARD_COMPATIBILITY = os.environ.get(
    "LIBCLOUD_PARAMIKO_SHA2_BACKWARD_COMPATIBILITY", "true"
).lower() in [
    "true",
    "1",
]

SHA2_PUBKEY_NOT_SUPPORTED_AUTH_ERROR_MSG = """
Received authentication error from the server. Disabling SHA-2 variants of RSA
key verification algorithm for backward compatibility reasons and trying
connecting again.

You can disable this behavior by setting
LIBCLOUD_PARAMIKO_SHA2_BACKWARD_COMPATIBILITY environment variable to "false".
""".strip()


class SSHCommandTimeoutError(Exception):
    """
    Exception which is raised when an SSH command times out.
    """

    def __init__(self, cmd, timeout, stdout=None, stderr=None):
        # type: (str, float, Optional[str], Optional[str]) -> None
        self.cmd = cmd
        self.timeout = timeout
        self.stdout = stdout
        self.stderr = stderr

        self.message = "Command didn't finish in %s seconds" % (timeout)
        super().__init__(self.message)

    def __repr__(self):
        return '<SSHCommandTimeoutError: cmd="{}",timeout={})>'.format(
            self.cmd,
            self.timeout,
        )

    def __str__(self):
        return self.__repr__()


class BaseSSHClient:
    """
    Base class representing a connection over SSH/SCP to a remote node.
    """

    def __init__(
        self,
        hostname,  # type: str
        port=22,  # type: int
        username="root",  # type: str
        password=None,  # type: Optional[str]
        key=None,  # type: Optional[str]
        key_files=None,  # type: Optional[Union[str, List[str]]]
        timeout=None,  # type: Optional[float]
    ):
        """
        :type hostname: ``str``
        :keyword hostname: Hostname or IP address to connect to.

        :type port: ``int``
        :keyword port: TCP port to communicate on, defaults to 22.

        :type username: ``str``
        :keyword username: Username to use, defaults to root.

        :type password: ``str``
        :keyword password: Password to authenticate with or a password used
                           to unlock a private key if a password protected key
                           is used.

        :param key: Deprecated in favor of ``key_files`` argument.

        :type key_files: ``str`` or ``list``
        :keyword key_files: A list of paths to the private key files to use.
        """
        if key is not None:
            message = (
                'You are using deprecated "key" argument which has '
                'been replaced with "key_files" argument'
            )
            warnings.warn(message, DeprecationWarning)

            # key_files has precedent
            key_files = key if not key_files else key_files

        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.key_files = key_files
        self.timeout = timeout

    def connect(self):
        # type: () -> bool
        """
        Connect to the remote node over SSH.

        :return: True if the connection has been successfully established,
                 False otherwise.
        :rtype: ``bool``
        """
        raise NotImplementedError("connect not implemented for this ssh client")

    def put(self, path, contents=None, chmod=None, mode="w"):
        # type: (str, Optional[Union[str, bytes]], Optional[int], str) -> str
        """
        Upload a file to the remote node.

        :type path: ``str``
        :keyword path: File path on the remote node.

        :type contents: ``str``
        :keyword contents: File Contents.

        :type chmod: ``int``
        :keyword chmod: chmod file to this after creation.

        :type mode: ``str``
        :keyword mode: Mode in which the file is opened.

        :return: Full path to the location where a file has been saved.
        :rtype: ``str``
        """
        raise NotImplementedError("put not implemented for this ssh client")

    def putfo(self, path, fo=None, chmod=None):
        """
        Upload file like object to the remote server.

        :param path: Path to upload the file to.
        :type path: ``str``

        :param fo: File like object to read the content from.
        :type fo: File handle or file like object.

        :type chmod: ``int``
        :keyword chmod: chmod file to this after creation.

        :return: Full path to the location where a file has been saved.
        :rtype: ``str``
        """
        raise NotImplementedError("putfo not implemented for this ssh client")

    def delete(self, path):
        # type: (str) -> bool
        """
        Delete/Unlink a file on the remote node.

        :type path: ``str``
        :keyword path: File path on the remote node.

        :return: True if the file has been successfully deleted, False
                 otherwise.
        :rtype: ``bool``
        """
        raise NotImplementedError("delete not implemented for this ssh client")

    def run(self, cmd, timeout=None):
        # type: (str, Optional[float]) -> Tuple[str, str, int]
        """
        Run a command on a remote node.

        :type cmd: ``str``
        :keyword cmd: Command to run.

        :return ``list`` of [stdout, stderr, exit_status]
        """
        raise NotImplementedError("run not implemented for this ssh client")

    def close(self):
        # type: () -> bool
        """
        Shutdown connection to the remote node.

        :return: True if the connection has been successfully closed, False
                 otherwise.
        :rtype: ``bool``
        """
        raise NotImplementedError("close not implemented for this ssh client")

    def _get_and_setup_logger(self):
        # type: () -> logging.Logger
        logger = logging.getLogger("libcloud.compute.ssh")
        path = os.getenv("LIBCLOUD_DEBUG")

        if path:
            handler = logging.FileHandler(path)
            handler.setFormatter(ExtraLogFormatter())
            logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)

        return logger


class ParamikoSSHClient(BaseSSHClient):
    """
    A SSH Client powered by Paramiko.
    """

    # Maximum number of bytes to read at once from a socket
    CHUNK_SIZE = 4096

    # How long to sleep while waiting for command to finish (to prevent busy
    # waiting)
    SLEEP_DELAY = 0.2

    def __init__(
        self,
        hostname,  # type: str
        port=22,  # type: int
        username="root",  # type: str
        password=None,  # type: Optional[str]
        key=None,  # type: Optional[str]
        key_files=None,  # type: Optional[Union[str, List[str]]]
        key_material=None,  # type: Optional[str]
        timeout=None,  # type: Optional[float]
        keep_alive=None,  # type: Optional[int]
        use_compression=False,  # type: bool
    ):
        """
        Authentication is always attempted in the following order:

        - The key passed in (if key is provided)
        - Any key we can find through an SSH agent (only if no password and
          key is provided)
        - Any "id_rsa" or "id_dsa" key discoverable in ~/.ssh/ (only if no
          password and key is provided)
        - Plain username/password auth, if a password was given (if password is
          provided)

        :param keep_alive: Optional keep alive internal (in seconds) to use.
        :type keep_alive: ``int``

        :param use_compression: True to use compression.
        :type use_compression: ``bool``
        """
        if key_files and key_material:
            raise ValueError("key_files and key_material arguments are " "mutually exclusive")

        super().__init__(
            hostname=hostname,
            port=port,
            username=username,
            password=password,
            key=key,
            key_files=key_files,
            timeout=timeout,
        )

        self.key_material = key_material
        self.keep_alive = keep_alive
        self.use_compression = use_compression

        self.client = paramiko.SSHClient()
        # Long term we should switch to a more secure default, but this would break
        # a lot  of non-interactive deployment scripts
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # nosec
        self.logger = self._get_and_setup_logger()

        # This object is lazily created on first SFTP operation (e.g. put()
        # method call)
        self.sftp_client = None

    def connect(self):
        conninfo = {
            "hostname": self.hostname,
            "port": self.port,
            "username": self.username,
            "allow_agent": False,
            "look_for_keys": False,
        }

        if self.password:
            conninfo["password"] = self.password

        if self.key_files:
            conninfo["key_filename"] = self.key_files

        if self.key_material:
            conninfo["pkey"] = self._get_pkey_object(key=self.key_material, password=self.password)

        if not self.password and not (self.key_files or self.key_material):
            conninfo["allow_agent"] = True
            conninfo["look_for_keys"] = True

        if self.timeout:
            conninfo["timeout"] = self.timeout

        # This is a workaround for paramiko only supporting key files in
        # format staring with "BEGIN RSA PRIVATE KEY".
        # If key_files are provided and a key looks like a PEM formatted key
        # we try to convert it into a format supported by paramiko
        if (
            self.key_files
            and not isinstance(self.key_files, (list, tuple))
            and os.path.isfile(self.key_files)
        ):
            with open(self.key_files) as fp:
                key_material = fp.read()

            try:
                pkey = self._get_pkey_object(key=key_material, password=self.password)
            except paramiko.ssh_exception.PasswordRequiredException as e:
                raise e
            except Exception:
                pass
            else:
                # It appears key is valid, but it was passed in in an invalid
                # format. Try to use the converted key directly
                del conninfo["key_filename"]
                conninfo["pkey"] = pkey

        extra = {
            "_hostname": self.hostname,
            "_port": self.port,
            "_username": self.username,
            "_timeout": self.timeout,
        }

        if self.password:
            extra["_auth_method"] = "password"
        else:
            extra["_auth_method"] = "key_file"

            if self.key_files:
                extra["_key_file"] = self.key_files

        self.logger.debug("Connecting to server", extra=extra)

        try:
            self.client.connect(**conninfo)
        except paramiko.ssh_exception.AuthenticationException as e:
            # Special case to handle paramiko >= 2.9.0 which supports SHA-2
            # variants of the RSA key verification algorithm which don't work
            # with older OpenSSH server versions (e.g. default setup on Ubuntu
            # 14.04).
            # Sadly there is no way for us to catch and retry on more specific
            # / granular exception.
            # See https://www.paramiko.org/changelog.html for details.
            if (
                PARAMIKO_VERSION_TUPLE >= (2, 9, 0)
                and LIBCLOUD_PARAMIKO_SHA2_BACKWARD_COMPATIBILITY
            ):
                self.logger.warn(SHA2_PUBKEY_NOT_SUPPORTED_AUTH_ERROR_MSG)

                conninfo["disabled_algorithms"] = {"pubkeys": ["rsa-sha2-256", "rsa-sha2-512"]}
                self.client.connect(**conninfo)
            else:
                raise e

        return True

    def put(self, path, contents=None, chmod=None, mode="w"):
        extra = {"_path": path, "_mode": mode, "_chmod": chmod}
        self.logger.debug("Uploading file", extra=extra)

        sftp = self._get_sftp_client()

        # less than ideal, but we need to mkdir stuff otherwise file() fails
        head, tail = psplit(path)

        if path[0] == "/":
            sftp.chdir("/")
        else:
            # Relative path - start from a home directory (~)
            sftp.chdir(".")

        for part in head.split("/"):
            if part != "":
                try:
                    sftp.mkdir(part)
                except OSError:
                    # so, there doesn't seem to be a way to
                    # catch EEXIST consistently *sigh*
                    pass
                sftp.chdir(part)

        cwd = sftp.getcwd()
        cwd = self._sanitize_cwd(cwd=cwd)

        ak = sftp.file(tail, mode=mode)
        ak.write(contents)
        if chmod is not None:
            ak.chmod(chmod)
        ak.close()

        file_path = self._sanitize_file_path(cwd=cwd, file_path=path)
        return file_path

    def putfo(self, path, fo=None, chmod=None):
        """
        Upload file like object to the remote server.

        Unlike put(), this method operates on file objects and not directly on
        file content which makes it much more efficient for large files since
        it utilizes pipelining.
        """
        extra = {"_path": path, "_chmod": chmod}
        self.logger.debug("Uploading file", extra=extra)

        sftp = self._get_sftp_client()

        # less than ideal, but we need to mkdir stuff otherwise file() fails
        head, tail = psplit(path)

        if path[0] == "/":
            sftp.chdir("/")
        else:
            # Relative path - start from a home directory (~)
            sftp.chdir(".")

        for part in head.split("/"):
            if part != "":
                try:
                    sftp.mkdir(part)
                except OSError:
                    # so, there doesn't seem to be a way to
                    # catch EEXIST consistently *sigh*
                    pass
                sftp.chdir(part)

        cwd = sftp.getcwd()
        cwd = self._sanitize_cwd(cwd=cwd)

        sftp.putfo(fo, path)
        if chmod is not None:
            ak = sftp.file(tail)
            ak.chmod(chmod)
            ak.close()

        file_path = self._sanitize_file_path(cwd=cwd, file_path=path)
        return file_path

    def delete(self, path):
        extra = {"_path": path}
        self.logger.debug("Deleting file", extra=extra)

        sftp = self.client.open_sftp()
        sftp.unlink(path)
        sftp.close()
        return True

    def run(self, cmd, timeout=None):
        # type: (str, Optional[float]) -> Tuple[str, str, int]
        """
        Note: This function is based on paramiko's exec_command()
        method.

        :param timeout: How long to wait (in seconds) for the command to
                        finish (optional).
        :type timeout: ``float``
        """
        extra1 = {"_cmd": cmd}
        self.logger.debug("Executing command", extra=extra1)

        # Use the system default buffer size
        bufsize = -1

        transport = self._get_transport()

        chan = transport.open_session()

        start_time = time.time()
        chan.exec_command(cmd)

        stdout = StringIO()
        stderr = StringIO()

        # Create a stdin file and immediately close it to prevent any
        # interactive script from hanging the process.
        stdin = chan.makefile("wb", bufsize)
        stdin.close()

        # Receive all the output
        # Note #1: This is used instead of chan.makefile approach to prevent
        # buffering issues and hanging if the executed command produces a lot
        # of output.
        #
        # Note #2: If you are going to remove "ready" checks inside the loop
        # you are going to have a bad time. Trying to consume from a channel
        # which is not ready will block for indefinitely.
        exit_status_ready = chan.exit_status_ready()

        if exit_status_ready:
            # It's possible that some data is already available when exit
            # status is ready
            stdout.write(self._consume_stdout(chan).getvalue())
            stderr.write(self._consume_stderr(chan).getvalue())

        while not exit_status_ready:
            current_time = time.time()
            elapsed_time = current_time - start_time

            if timeout and (elapsed_time > timeout):
                # TODO: Is this the right way to clean up?
                chan.close()

                stdout_str = stdout.getvalue()  # type: str
                stderr_str = stderr.getvalue()  # type: str
                raise SSHCommandTimeoutError(
                    cmd=cmd, timeout=timeout, stdout=stdout_str, stderr=stderr_str
                )

            stdout.write(self._consume_stdout(chan).getvalue())
            stderr.write(self._consume_stderr(chan).getvalue())

            # We need to check the exist status here, because the command could
            # print some output and exit during this sleep below.
            exit_status_ready = chan.exit_status_ready()

            if exit_status_ready:
                break

            # Short sleep to prevent busy waiting
            time.sleep(self.SLEEP_DELAY)

        # Receive the exit status code of the command we ran.
        status = chan.recv_exit_status()  # type: int

        stdout_str = stdout.getvalue()
        stderr_str = stderr.getvalue()

        extra2 = {"_status": status, "_stdout": stdout_str, "_stderr": stderr_str}
        self.logger.debug("Command finished", extra=extra2)

        result = (stdout_str, stderr_str, status)  # type: Tuple[str, str, int]
        return result

    def close(self):
        self.logger.debug("Closing server connection")

        if self.client:
            self.client.close()

        if self.sftp_client:
            self.sftp_client.close()

        return True

    def _consume_stdout(self, chan):
        """
        Try to consume stdout data from chan if it's receive ready.
        """
        stdout = self._consume_data_from_channel(
            chan=chan, recv_method=chan.recv, recv_ready_method=chan.recv_ready
        )
        return stdout

    def _consume_stderr(self, chan):
        """
        Try to consume stderr data from chan if it's receive ready.
        """
        stderr = self._consume_data_from_channel(
            chan=chan,
            recv_method=chan.recv_stderr,
            recv_ready_method=chan.recv_stderr_ready,
        )
        return stderr

    def _consume_data_from_channel(self, chan, recv_method, recv_ready_method):
        """
        Try to consume data from the provided channel.

        Keep in mind that data is only consumed if the channel is receive
        ready.
        """
        result = StringIO()
        result_bytes = bytearray()

        if recv_ready_method():
            data = recv_method(self.CHUNK_SIZE)
            result_bytes += b(data)

            while data:
                ready = recv_ready_method()

                if not ready:
                    break

                data = recv_method(self.CHUNK_SIZE)
                result_bytes += b(data)

        # We only decode data at the end because a single chunk could contain
        # a part of multi byte UTF-8 character (whole multi bytes character
        # could be split over two chunks)
        result.write(result_bytes.decode("utf-8", errors="ignore"))
        return result

    def _get_pkey_object(self, key, password=None):
        """
        Try to detect private key type and return paramiko.PKey object.

        # NOTE: Paramiko only supports key in PKCS#1 PEM format.
        """
        key_types = [
            (paramiko.RSAKey, "RSA"),
            (paramiko.DSSKey, "DSA"),
            (paramiko.ECDSAKey, "EC"),
        ]

        paramiko_version = getattr(paramiko, "__version__", "0.0.0")
        paramiko_version = tuple(int(c) for c in paramiko_version.split("."))

        if paramiko_version >= (2, 2, 0):
            # Ed25519 is only supported in paramiko >= 2.2.0
            key_types.append((paramiko.ed25519key.Ed25519Key, "Ed25519"))

        for cls, key_type in key_types:
            # Work around for paramiko not recognizing keys which start with
            # "----BEGIN PRIVATE KEY-----"
            # Since key is already in PEM format, we just try changing the
            # header and footer
            key_split = key.strip().splitlines()
            if (
                key_split[0] == "-----BEGIN PRIVATE KEY-----"
                and key_split[-1] == "-----END PRIVATE KEY-----"
            ):
                key_split[0] = "-----BEGIN %s PRIVATE KEY-----" % (key_type)
                key_split[-1] = "-----END %s PRIVATE KEY-----" % (key_type)

                key_value = "\n".join(key_split)
            else:
                # Already a valid key, us it as is
                key_value = key

            try:
                key = cls.from_private_key(StringIO(key_value), password)
            except paramiko.ssh_exception.PasswordRequiredException as e:
                raise e
            except (paramiko.ssh_exception.SSHException, AssertionError) as e:
                if "private key file checkints do not match" in str(e).lower():
                    msg = "Invalid password provided for encrypted key. " "Original error: %s" % (
                        str(e)
                    )
                    # Indicates invalid password for password protected keys
                    raise paramiko.ssh_exception.SSHException(msg)

                # Invalid key, try other key type
                pass
            else:
                return key

        msg = (
            "Invalid or unsupported key type (only RSA, DSS, ECDSA and"
            " Ed25519 keys"
            " in PEM format are supported). For more information on "
            " supported key file types, see %s" % (SUPPORTED_KEY_TYPES_URL)
        )
        raise paramiko.ssh_exception.SSHException(msg)

    def _sanitize_cwd(self, cwd):
        # type: (str) -> str
        # getcwd() returns an invalid path when executing commands on Windows
        # so we need a special case for that scenario
        # For example, we convert /C:/Users/Foo -> C:/Users/Foo
        if re.match(r"^\/\w\:.*$", str(cwd)):
            cwd = str(cwd[1:])

        return cwd

    def _sanitize_file_path(self, cwd, file_path):
        # type: (str, str) -> str
        """
        Sanitize the provided file path and ensure we always return an
        absolute path, even if relative path is passed to to this function.
        """

        if file_path[0] in ["/", "\\"] or re.match(r"^\w\:.*$", file_path):
            # If it's an absolute path we return path as is
            # NOTE: We assume it's a Windows absolute path if it's starts with
            # a drive letter - e.g. C:\\..., D:\\, etc. or with \
            pass
        else:
            if re.match(r"^\w\:.*$", cwd):
                # Windows path
                file_path = cwd + "\\" + file_path
            else:
                file_path = pjoin(cwd, file_path)

        return file_path

    def _get_transport(self):
        """
        Return transport object taking into account keep alive and compression
        options passed to the constructor.
        """
        transport = self.client.get_transport()

        if self.keep_alive:
            transport.set_keepalive(self.keep_alive)

        if self.use_compression:
            transport.use_compression(compress=True)

        return transport

    def _get_sftp_client(self):
        """
        Create SFTP client from the underlying SSH client.

        This method tries to reuse the existing self.sftp_client (if it
        exists) and it also tries to verify the connection is opened and if
        it's not, it will try to re-establish it.
        """
        if not self.sftp_client:
            self.sftp_client = self.client.open_sftp()

        sftp_client = self.sftp_client

        # Verify the connection is still open, if it's not, try to
        # re-establish it.
        # We do that, by calling listdir(). If it returns "Socket is closed"
        # error we assume the connection is closed and we try to re-establish
        # it.
        try:
            sftp_client.listdir(".")
        except OSError as e:
            if "socket is closed" in str(e).lower():
                self.sftp_client = self.client.open_sftp()
            elif "no such file" in str(e).lower():
                # Not a fatal exception, means connection is still open
                pass
            else:
                raise e

        return self.sftp_client


class ShellOutSSHClient(BaseSSHClient):
    """
    This client shells out to "ssh" binary to run commands on the remote
    server.

    Note: This client should not be used in production.
    """

    def __init__(
        self,
        hostname,  # type: str
        port=22,  # type: int
        username="root",  # type: str
        password=None,  # type: Optional[str]
        key=None,  # type: Optional[str]
        key_files=None,  # type: Optional[str]
        timeout=None,  # type: Optional[float]
    ):
        super().__init__(
            hostname=hostname,
            port=port,
            username=username,
            password=password,
            key=key,
            key_files=key_files,
            timeout=timeout,
        )
        if self.password:
            raise ValueError("ShellOutSSHClient only supports key auth")

        child = subprocess.Popen(["ssh"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        child.communicate()

        if child.returncode == 127:
            raise ValueError("ssh client is not available")

        self.logger = self._get_and_setup_logger()

    def connect(self):
        """
        This client doesn't support persistent connections establish a new
        connection every time "run" method is called.
        """
        return True

    def run(self, cmd, timeout=None):
        return self._run_remote_shell_command([cmd])

    def put(self, path, contents=None, chmod=None, mode="w"):
        if mode == "w":
            redirect = ">"
        elif mode == "a":
            redirect = ">>"
        else:
            raise ValueError("Invalid mode: " + mode)

        cmd = ['echo "{}" {} {}'.format(contents, redirect, path)]
        self._run_remote_shell_command(cmd)
        return path

    def putfo(self, path, fo=None, chmod=None):
        content = fo.read()
        return self.put(path=path, contents=content, chmod=chmod)

    def delete(self, path):
        cmd = ["rm", "-rf", path]
        self._run_remote_shell_command(cmd)
        return True

    def close(self):
        return True

    def _get_base_ssh_command(self):
        # type: () -> List[str]
        cmd = ["ssh"]

        if self.key_files:
            self.key_files = cast(str, self.key_files)
            cmd += ["-i", self.key_files]

        if self.timeout:
            cmd += ["-oConnectTimeout=%s" % (self.timeout)]

        cmd += ["{}@{}".format(self.username, self.hostname)]

        return cmd

    def _run_remote_shell_command(self, cmd):
        # type: (List[str]) -> Tuple[str, str, int]
        """
        Run a command on a remote server.

        :param      cmd: Command to run.
        :type       cmd: ``list`` of ``str``

        :return: Command stdout, stderr and status code.
        :rtype: ``tuple``
        """
        base_cmd = self._get_base_ssh_command()
        full_cmd = base_cmd + [" ".join(cmd)]

        self.logger.debug('Executing command: "%s"' % (" ".join(full_cmd)))

        child = subprocess.Popen(full_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = child.communicate()

        stdout_str = cast(str, stdout)
        stderr_str = cast(str, stdout)

        return (stdout_str, stderr_str, child.returncode)


class MockSSHClient(BaseSSHClient):
    pass


SSHClient = ParamikoSSHClient  # type: Type[BaseSSHClient]
if not have_paramiko:
    SSHClient = MockSSHClient  # type: ignore
