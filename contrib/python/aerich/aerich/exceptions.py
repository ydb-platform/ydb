class AerichError(Exception):
    pass


class NotSupportError(AerichError):
    """
    raise when features not support
    """


class DowngradeError(AerichError):
    """
    raise when downgrade error
    """


class NotInitedError(AerichError):
    """
    raise when Tortoise not inited
    """
