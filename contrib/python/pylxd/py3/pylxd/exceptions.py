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
        super().__init__()
        self.response = response

    def __str__(self):
        if self.response.status_code == 200:  # Operation failure
            try:
                json_response = self.response.json()
                metadata = json_response.get("metadata")
                if metadata and isinstance(metadata, dict) and "err" in metadata:
                    return json_response["metadata"]["err"]
                if "error" in json_response:
                    return json_response["error"]
                return str(json_response)
            except (ValueError, KeyError):
                pass

        try:
            data = self.response.json()
            return data["error"]
        except (ValueError, KeyError):
            pass
        return self.response.content.decode("utf-8")


class NotFound(LXDAPIException):
    """An exception raised when an object is not found."""


class Conflict(LXDAPIException):
    """An exception raised when there is a conflict."""


class LXDAPIExtensionNotAvailable(Exception):
    """An exception raised when requested LXD API Extension is not present
    on current host."""

    def __init__(self, name, *args, **kwargs):
        """Custom exception handling of the message is to convert the name into
        a friendly error string.

        :param name: the api_extension that was needed.
        :type name: str
        """
        super().__init__(
            f"LXD API extension '{name}' is not available", *args, **kwargs
        )


class ClientConnectionFailed(Exception):
    """An exception raised when the Client connection fails."""
