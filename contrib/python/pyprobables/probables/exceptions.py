"""PyProbables Exceptions"""


class ProbablesBaseException(Exception):
    """Base ProbablesBaseException

    Args:
        message (str): The error message to be reported"""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message


class InitializationError(ProbablesBaseException):
    """Initialization Exception

    Args:
        message (str): The initialization error messge"""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class NotSupportedError(ProbablesBaseException):
    """Not Supported Functionality Exception

    Args:
        message (str): The error message to be reported"""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class SimilarityError(ProbablesBaseException):
    """Similarity Exception

    Args:
        message (str): The error message to be reported"""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class CuckooFilterFullError(ProbablesBaseException):
    """Cuckoo Filter Full Exception

    Args:
        message (str): The error message to be reported"""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class RotatingBloomFilterError(ProbablesBaseException):
    """RotatingBloomFilter unable to rotate Blooms Exceptions

    Args:
        message (str): The error message to be reported"""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class CountMinSketchError(ProbablesBaseException):
    """CountMinSketch Exception

    Args:
        message (str): The error message to be reported"""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)


class QuotientFilterError(ProbablesBaseException):
    """Quotient Filter Exception

    Args:
        message (str): The error message to be reported"""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)
