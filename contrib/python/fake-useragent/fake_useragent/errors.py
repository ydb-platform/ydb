"""All specific exceptions for the library."""


class FakeUserAgentError(Exception):
    """Exception for any problems that are library specific."""


# common alias
UserAgentError = FakeUserAgentError
