class ScramException(Exception):
    def __init__(self, message, server_error=None):
        super().__init__(message)
        self.server_error = server_error

    def __str__(self):
        s_str = "" if self.server_error is None else f": {self.server_error}"
        return super().__str__() + s_str
