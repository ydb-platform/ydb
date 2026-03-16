class OperationFailed(RuntimeError):
    pass


class OperationTimeout(RuntimeError, TimeoutError):
    pass
