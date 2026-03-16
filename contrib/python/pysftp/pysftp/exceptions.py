"""pysftp specific exceptions"""


class ConnectionException(Exception):
    """Exception raised for connection problems

    Attributes:
        message  -- explanation of the error
    """

    def __init__(self, host, port):
        # Call the base class constructor with the parameters it needs
        Exception.__init__(self, host, port)
        self.message = 'Could not connect to host:port.  %s:%s'


class CredentialException(Exception):
    """Exception raised for credential problems

    Attributes:
        message  -- explanation of the error
    """

    def __init__(self, message):
        # Call the base class constructor with the parameters it needs
        Exception.__init__(self, message)
        self.message = message


class HostKeysException(Exception):
    '''raised when a problem with HostKeys is encountered'''
    pass
