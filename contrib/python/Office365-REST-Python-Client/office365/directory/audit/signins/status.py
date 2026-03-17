from office365.runtime.client_value import ClientValue


class SignInStatus(ClientValue):
    """Provides the sign-in status (Success or Failure) of the sign-in."""

    def __init__(self, additional_details=None, error_code=None, failure_reason=None):
        """
        :param str additional_details: Provides additional details on the sign-in activity
        :param int error_code: Provides the 5-6 digit error code that's generated during a sign-in failure.
        :param str failure_reason: Provides the error message or the reason for failure for
            the corresponding sign-in activity.
        """
        super(SignInStatus, self).__init__()
        self.additionalDetails = additional_details
        self.errorCode = error_code
        self.failureReason = failure_reason
