import six


class LXDAPIException(Exception):
    """A generic exception for representing unexpected LXD API responses.

    LXD API responses are clearly documented, and are either a standard
    return value, and background operation, or an error. This exception
    is raised on an error case, or when the response status code is
    not expected for the response.

    This exception should *only* be raised in cases where the LXD REST
    API has returned something unexpected.
    """

    def __init__(self, response):
        super(LXDAPIException, self).__init__()
        self.response = response

    def __str__(self):
        if self.response.status_code == 200:  # Operation failure
            try:
                return self.response.json()['metadata']['err']
            except (ValueError, KeyError):
                pass

        try:
            data = self.response.json()
            return data['error']
        except (ValueError, KeyError):
            pass
        return self.response.content.decode('utf-8')


class NotFound(LXDAPIException):
    """An exception raised when an object is not found."""


class LXDAPIExtensionNotAvailable(Exception):
    """An exception raised when requested LXD API Extension is not present
    on current host."""

    def __init__(self, name, *args, **kwargs):
        """Custom exception handling of the message is to convert the name into
        a friendly error string.

        :param name: the api_extension that was needed.
        :type name: str
        """
        super(LXDAPIExtensionNotAvailable, self).__init__(
            "LXD API extension '{}' is not available".format(name),
            *args, **kwargs)


class ClientConnectionFailed(Exception):
    """An exception raised when the Client connection fails."""


if six.PY2:
    class NotADirectoryError(Exception):
        """ An exception raised when not a directory for python2 """
