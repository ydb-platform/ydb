class Empty(Exception):
    """Exception raised when an operation is attempted on an empty queue."""
    pass


class Full(Exception):
    """Exception raised when an attempt is made to add an item to a full
       container."""
    pass
