from typing import Any


class DockerError(Exception):
    """Base exception for all aiodocker errors.

    This is the root of the exception hierarchy. All exceptions raised by
    aiodocker are subclasses of this exception, making it easy to catch
    all aiodocker-related errors with a single except clause.

    Attributes:
        status: The HTTP status code from the Docker API response (if applicable).
        message: The error message.
    """

    def __init__(self, status: int, message: str, *args: Any) -> None:
        super().__init__(message, *args)
        self.status = status
        self.message = message

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.status}, {self.message!r})"

    def __str__(self) -> str:
        return f"[{self.status}] {self.message}"


class DockerContainerError(DockerError):
    """Exception raised for container-specific API errors.

    Attributes:
        status: The HTTP status code from the Docker API response.
        message: The error message from the Docker API.
        container_id: The ID of the container that caused the error.
    """

    def __init__(
        self, status: int, message: str, container_id: str, *args: Any
    ) -> None:
        super().__init__(status, message, *args)
        self.container_id = container_id

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"{self.status}, {self.message!r}, {self.container_id!r})"
        )

    def __str__(self) -> str:
        return f"[{self.status}] {self.message} (container: {self.container_id})"


class DockerContextError(DockerError):
    """Base exception for Docker context configuration errors.

    This exception and its subclasses are raised when there are issues with
    Docker context configuration files (e.g., ~/.docker/contexts/).

    Attributes:
        status: Always 0 for context errors (no HTTP status).
        message: Description of the error.
        context_name: The name of the Docker context, if known.
    """

    def __init__(self, message: str, context_name: str | None = None) -> None:
        super().__init__(0, message)
        self.context_name = context_name

    def __repr__(self) -> str:
        if self.context_name:
            return (
                f"{self.__class__.__name__}("
                f"{self.message!r}, context={self.context_name!r})"
            )
        return f"{self.__class__.__name__}({self.message!r})"

    def __str__(self) -> str:
        if self.context_name:
            return f"{self.message} (context: {self.context_name})"
        return self.message


class DockerContextInvalidError(DockerContextError):
    """Raised when Docker context configuration contains invalid data.

    This exception is raised when:
    - The Docker config.json contains invalid JSON
    - A context metadata file (meta.json) is missing or contains invalid JSON
    - Required fields are missing from the context configuration
    """

    pass


class DockerContextTLSError(DockerContextError):
    """Raised when there is an error loading TLS certificates from a Docker context.

    This exception is raised when:
    - A TLS certificate file exists but cannot be read
    - A TLS certificate is invalid or malformed
    """

    pass
