class ApiException(Exception):
    pass


class MalformedRequestData(ApiException, ValueError):
    pass


class MalformedResponse(ApiException, ValueError):
    pass


class ClientLibraryError(ApiException, RuntimeError):
    pass


class HttpStatusError(ApiException, RuntimeError):
    def __init__(self, status_code):
        self.status_code = status_code


class ClientError(HttpStatusError):
    pass


class ServerError(HttpStatusError):
    pass
