import paramiko
import socket
import time

import typing

from functools import wraps
from collections.abc import Sequence
from dataclasses import dataclass

from .base import UnboundPlug

class SSHError(Exception):
    pass

class SSHTimeoutError(SSHError):
    pass

def _ssh_client_connected(function):
    @wraps(function)
    def check_connected(*args, **kwargs):
        _self = args[0]
        if not _self.is_connected():
            _self.logger.info("Connection is dead, reopening...")
            _self.open()
        return function(*args, **kwargs)

    return check_connected

class SSHInterface(UnboundPlug):
    """ An interface to an SSH Server.
    """
    @dataclass()
    class SSHResponse():
        exit_code: int #: The command exit code
        err_output: str #: The command stderr output
        std_output: str #: The command stdout output
        
        @property
        def output(self):
            """Combines both the err output and the std_output."""
            return self.err_output + '\n' + self.std_output

    def __init__(self, addr, username, password, create_timeout=3, port=22):
        super().__init__()
        self.ssh = None
        self.addr = addr
        self.username = username
        self.password = password
        self.create_timeout = create_timeout
        self.port = port

    def open(self, _client=None):
        self.logger.info("(Initiating SSH connection at %s)", self.addr)
        self.logger.info("(addr={}:{}, user={!r}, password={!r})".format(self.addr, self.port, self.username, self.password))
        try:
            # _client allows to pass in a mock for testing
            if _client is None: _client = paramiko.SSHClient()
            self.ssh = _client
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh.connect(self.addr, port=self.port, username=self.username, password=self.password, timeout=self.create_timeout)
            self.ssh.get_transport().set_keepalive(5) #Send keepalive packet every 5 seconds
        except Exception as e:
            raise SSHError("%s %s" % (e.__class__.__name__, str(e)))

    
    def is_connected(self):
        if self.ssh is None or self.ssh.get_transport() is None:
            return False
        else:
            return self.ssh.get_transport().is_active()

    def close(self):
        self.logger.info("(Closing SSH connection)")
        self.ssh.close()

    @_ssh_client_connected
    def execute_command(self, 
            command:str, 
            timeout:float=60, 
            stdin:typing.List=[], 
            get_pty:bool=False, 
            assertexitcode:typing.Union[typing.List[int], int, None]=0
        ):
        """ Send a :obj:`command` and wait for it to execute.

        Args:
            command: The command to send. End of lines are automatically managed. For example execute_command('ls')
                will executed the ls command.
            timeout: The timeout in second to wait for the command to finish executing.
            stdin: A list of inputs to send into stdin after the command is started. Each entry in this list will
                be separated with an r'\\n' character.
            get_pty: Usually required when providing :obj:`stdin`.
            assertexitcode: Unless this is None, defines one or a list of exit codes that are expected. After the 
                command is executed, an :class:`SSHError` will be raised if the exit code is not as expected.

        Raises:
            SSHTimeoutError:
                Raised when :obj:`timeout` is reached.
            SSHError:
                Raised when the exit code of the command is not in :obj:`assertexitcode` and :obj:`assertexitcode` is not None.
        """
        output = ""
        err_output = ""
        exit_code = None
        if command != "":
            self.logger.info("(Timeout %.1fs, TTY=%s)" % (timeout, str(get_pty)))
            ssh_stdin, ssh_stdout, ssh_stderr = self.ssh.exec_command(command, get_pty=get_pty)
    
            self.logger.debug("> {!r}".format(command))
            for stdin_element in stdin:
                self.logger.debug("> {!r}".format(stdin_element))
                ssh_stdin.write('%s\n'%stdin_element)
                ssh_stdin.flush()

            ssh_stdin.close()# Sends eof

            try:
                self.wait_stdout_with_timeout(ssh_stdout, timeout)
                exit_code = ssh_stdout.channel.recv_exit_status()
            finally:
                ssh_stdout.channel.close()
                ssh_stderr.channel.close()
                output = ssh_stdout.read().decode('utf-8', 'ignore')
                err_output = ssh_stderr.read().decode('utf-8', 'ignore')


            self.logger.debug("(Exit code={}, Response:)\n{}".format(exit_code, output.strip()))
            if err_output:
                self.logger.debug("(STDERR:)\n{}".format(err_output.strip()))
        else:
            pass
        
        if assertexitcode is not None:
            assert_exit_code(exit_code, expected=assertexitcode)

        response = self.SSHResponse(exit_code=exit_code, err_output=err_output, std_output=output)

        return response
    
    def wait_stdout_with_timeout(self, stdout, timeout_seconds):
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            if stdout.channel.eof_received:
                break
            time.sleep(0)
        else:
            raise SSHTimeoutError(f'Client command timeout reached ({timeout_seconds}s)')



def assert_exit_code(exit_code, expected):
    if not isinstance(expected, Sequence):
        expected = [expected]
        
    if exit_code not in expected:
        raise SSHError('Exit code {} not in expected list {}'.format(exit_code, expected))
        

