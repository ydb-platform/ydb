"""Pymodbus Exceptions.

Custom exceptions to be used in the Modbus code.
"""

__all__ = [
    "ConnectionException",
    "InvalidMessageReceivedException",
    "MessageRegisterException",
    "ModbusIOException",
    "NoSuchSlaveException",
    "NotImplementedException",
    "ParameterException",
]


class ModbusException(Exception):
    """Base modbus exception."""

    def __init__(self, string):
        """Initialize the exception.

        :param string: The message to append to the error
        """
        self.string = string
        super().__init__(string)

    def __str__(self):
        """Return string representation."""
        return f"Modbus Error: {self.string}"

    def isError(self):
        """Error"""
        return True


class ModbusIOException(ModbusException):
    """Error resulting from data i/o."""

    def __init__(self, string="", function_code=None):
        """Initialize the exception.

        :param string: The message to append to the error
        """
        self.fcode = function_code
        self.message = f"[Input/Output] {string}"
        ModbusException.__init__(self, self.message)


class ParameterException(ModbusException):
    """Error resulting from invalid parameter."""

    def __init__(self, string=""):
        """Initialize the exception.

        :param string: The message to append to the error
        """
        message = f"[Invalid Parameter] {string}"
        ModbusException.__init__(self, message)


class NoSuchSlaveException(ModbusException):
    """Error resulting from making a request to a slave that does not exist."""

    def __init__(self, string=""):
        """Initialize the exception.

        :param string: The message to append to the error
        """
        message = f"[No Such Slave] {string}"
        ModbusException.__init__(self, message)


class NotImplementedException(ModbusException):
    """Error resulting from not implemented function."""

    def __init__(self, string=""):
        """Initialize the exception.

        :param string: The message to append to the error
        """
        message = f"[Not Implemented] {string}"
        ModbusException.__init__(self, message)


class ConnectionException(ModbusException):
    """Error resulting from a bad connection."""

    def __init__(self, string=""):
        """Initialize the exception.

        :param string: The message to append to the error
        """
        message = f"[Connection] {string}"
        ModbusException.__init__(self, message)


class InvalidMessageReceivedException(ModbusException):
    """Error resulting from invalid response received or decoded."""

    def __init__(self, string=""):
        """Initialize the exception.

        :param string: The message to append to the error
        """
        message = f"[Invalid Message] {string}"
        ModbusException.__init__(self, message)


class MessageRegisterException(ModbusException):
    """Error resulting from failing to register a custom message request/response."""

    def __init__(self, string=""):
        """Initialize."""
        message = f"[Error registering message] {string}"
        ModbusException.__init__(self, message)
