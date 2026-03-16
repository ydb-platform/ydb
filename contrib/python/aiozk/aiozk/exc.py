class ZKError(Exception):
    pass


class ConnectError(ZKError):
    def __init__(self, host, port, server_id=None):
        self.host = host
        self.port = port
        self.server_id = server_id

    def __str__(self):
        return 'Error connecting to %s:%s' % (self.host, self.port)


class NoServersError(ZKError):
    pass


class SessionLost(ZKError):
    pass


class InvalidClientState(ZKError):
    pass


class TimeoutError(ZKError):
    pass


class UnfinishedRead(ZKError):
    pass


class FailedRetry(ZKError):
    pass


class InvalidStateTransition(ZKError):
    pass


class TransactionFailed(ZKError):
    pass


response_error_xref = {}


class ResponseErrorMeta(type):
    def __new__(cls, name, bases, attrs):
        new_class = super(ResponseErrorMeta, cls).__new__(cls, name, bases, attrs)

        response_error_xref[new_class.error_code] = new_class

        return new_class


class ResponseError(ZKError, metaclass=ResponseErrorMeta):
    error_code = None

    def __str__(self):
        return self.__class__.__name__


class UnknownError(ResponseError):
    def __init__(self, error_code):
        print(repr(error_code))
        self.error_code = error_code

    def __str__(self):
        return 'Unknown error code: %s' % self.error_code


def get_response_error(error_code):
    if error_code not in response_error_xref:
        return UnknownError(error_code)

    return response_error_xref[error_code]()


class RolledBack(ResponseError):
    error_code = 0


class ZKSystemError(ResponseError):
    error_code = -1


class RuntimeInconsistency(ResponseError):
    error_code = -2


class DataInconsistency(ResponseError):
    error_code = -3


class ConnectionLoss(ResponseError):
    error_code = -4


class MarshallingError(ResponseError):
    error_code = -5


class Unimplemented(ResponseError):
    error_code = -6


class OperationTimeout(ResponseError):
    error_code = -7


class BadArguments(ResponseError):
    error_code = -8


class UnknownSession(ResponseError):
    error_code = -12


class NewConfigNoQuorum(ResponseError):
    error_code = -13


class ReconfigInProcess(ResponseError):
    error_code = -14


class APIError(ResponseError):
    error_code = -100


class NoNode(ResponseError):
    error_code = -101


class NoAuth(ResponseError):
    error_code = -102


class BadVersion(ResponseError):
    error_code = -103


class NoChildrenForEphemerals(ResponseError):
    error_code = -108


class NodeExists(ResponseError):
    error_code = -110


class NotEmpty(ResponseError):
    error_code = -111


class SessionExpired(ResponseError):
    error_code = -112


class InvalidCallback(ResponseError):
    error_code = -113


class InvalidACL(ResponseError):
    error_code = -114


class AuthFailed(ResponseError):
    error_code = -115


class SessionMoved(ResponseError):
    error_code = -118


class NotReadOnly(ResponseError):
    error_code = -119


class EphemeralOnLocalSession(ResponseError):
    error_code = -120


class NoWatcher(ResponseError):
    error_code = -121
