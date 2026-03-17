from .auth import UserToken as UserToken, UserTokenCollection as UserTokenCollection
from .utils import UnexpectedResponse as UnexpectedResponse
from _typeshed import Incomplete
from logfire.exceptions import LogfireConfigError as LogfireConfigError
from logfire.version import VERSION as VERSION
from typing import Any
from typing_extensions import Self

UA_HEADER: Incomplete

class ProjectAlreadyExists(Exception): ...

class InvalidProjectName(Exception):
    reason: Incomplete
    def __init__(self, reason: str, /) -> None: ...

class LogfireClient:
    """A Logfire HTTP client to interact with the API.

    Args:
        user_token: The user token to use when authenticating against the API.
    """
    base_url: Incomplete
    def __init__(self, user_token: UserToken) -> None: ...
    @classmethod
    def from_url(cls, base_url: str | None) -> Self:
        """Create a client from the provided base URL.

        Args:
            base_url: The base URL to use when looking for a user token. If `None`, will prompt
                the user into selecting a token from the token collection (or, if only one available,
                use it directly). The token collection will be created from the `~/.logfire/default.toml`
                file (or an empty one if no such file exists).
        """
    def get_user_organizations(self) -> list[dict[str, Any]]:
        """Get the organizations of the logged-in user."""
    def get_user_information(self) -> dict[str, Any]:
        """Get information about the logged-in user."""
    def get_user_projects(self) -> list[dict[str, Any]]:
        """Get the projects of the logged-in user."""
    def create_new_project(self, organization: str, project_name: str):
        """Create a new project.

        Args:
            organization: The organization that should hold the new project.
            project_name: The name of the project to be created.

        Returns:
            The newly created project.
        """
    def create_write_token(self, organization: str, project_name: str) -> dict[str, Any]:
        """Create a write token for the given project in the given organization."""
    def create_read_token(self, organization: str, project_name: str) -> dict[str, Any]:
        """Create a read token for the given project in the given organization."""
    def get_prompt(self, organization: str, project_name: str, issue: str) -> dict[str, Any]:
        """Get a prompt to be used with your favorite LLM."""
