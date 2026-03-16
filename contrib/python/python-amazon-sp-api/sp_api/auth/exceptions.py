class AuthorizationError(Exception):
    """
    Authorization Error

    Parameters:

        error_code: str Error code from amazon auth api
        error_msg: str Error sm
        status_code: integer Response status code from amazon auth api
    """

    def __init__(self, error_code, error_msg, status_code):
        self.error_code = error_code
        self.message = error_msg
        self.status_code = status_code
