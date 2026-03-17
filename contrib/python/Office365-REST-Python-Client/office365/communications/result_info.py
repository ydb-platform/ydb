from office365.runtime.client_value import ClientValue


class ResultInfo(ClientValue):
    """
    This contains success and failure specific result information.

    The code specifies if the result is a generic success or failure. If the code is 2xx it's a success,
    if it's a 4xx it's a client error, and if it's 5xx, it's a server error.

    The sub-codes provide supplementary information related to the type of success or failure
    (e.g. a call transfer was successful)
    """
