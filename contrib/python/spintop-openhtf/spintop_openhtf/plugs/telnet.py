from .base import UnboundPlug

import typing
from functools import wraps
from collections.abc import Sequence

import telnetlib


class TelnetError(Exception):
    pass


def _telnet_client_connected(function):
    @wraps(function)
    def check_connected(*args, **kwargs):
        _self = args[0]
        if not _self.is_connected():
            _self.logger.info("Connection is no longer alive")
            _self.open()
        return function(*args, **kwargs)

    return check_connected


class TelnetInterface(UnboundPlug):

    def __init__(self, addr, port=4343):
        super().__init__()
        self.tn = None
        self.addr = addr
        self.port = port

    def open(self, _client=None):
        # _client allows to pass in a mock for testing
        self.logger.info("(Initiating Telnet connection at %s)", self.addr)
        self.logger.info("(addr={}:{})".format(self.addr, self.port))
        try:
            if _client is None:
                _client = telnetlib.Telnet(self.addr, self.port)
            self.tn = _client
            self.tn.open(self.addr, port=self.port)
        except Exception as e:
            raise TelnetError("Unable to connect to Telnet host: " + str(e))

    def close(self):
        try:
            self.logger.info("Closing Telnet connection")
            self.tn.close()
        except:
            pass

    def is_connected(self):
        try:
            self.tn.write("\r\n".encode())
            # If the Telnet object is not connected, an AttributeError is raised
        except AttributeError:
            return False
        else:
            return True

    @_telnet_client_connected
    def execute_command(self, command: str, expected_output: str = "", until_timeout: float = 5):
        """Send a :obj:`command` and wait for it to execute.

        Args:
            command (str): The command to send. End of lines are automatically managed. For example execute_command('ls')
            will executed the ls command.
            expected_output (str, optional): If not none, the command executor will read until a given string, expected_output, is encountered or until until_timeout (s) have passed.
            until_timeout (float, optional): The timeout in seconds to wait for the command to finish reading output. Defaults to 5.

        Returns:
            output (str): output response from the Telnet client

        Raises:
            TelnetError:
                Raised when reading the output goes wrong.
        """

        output = ""
        if command != "":
            self.logger.info("(Timeout %.1fs)" % (until_timeout))
            self.tn.write((command).encode() + b"\r\n")
            self.logger.debug("> {!r}".format(command))

            try:
                if expected_output == "":
                    output = self.tn.read_until(b'\n\n', until_timeout)
                    output = output.decode()
                else:
                    expected_output = expected_output.encode()
                    output = self.tn.read_until(expected_output, until_timeout)
                    output = output.decode()
            except Exception as e:
                raise TelnetError(e)
        else:
            pass

        return output
