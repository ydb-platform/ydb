from __future__ import annotations

from typing import Any, Dict, List, Optional, TYPE_CHECKING

from hypercorn.typing import WWWScope
from werkzeug.datastructures import Headers
from werkzeug.sansio.request import Request as SansIORequest

from ..globals import current_app

if TYPE_CHECKING:
    from ..routing import QuartRule  # noqa


class BaseRequestWebsocket(SansIORequest):
    """This class is the basis for Requests and websockets..

    Attributes:
        routing_exception: If an exception is raised during the route
            matching it will be stored here.
        url_rule: The rule that this request has been matched too.
        view_args: The keyword arguments for the view from the route
            matching.
    """

    routing_exception: Optional[Exception] = None
    url_rule: Optional["QuartRule"] = None
    view_args: Optional[Dict[str, Any]] = None

    def __init__(
        self,
        method: str,
        scheme: str,
        path: str,
        query_string: bytes,
        headers: Headers,
        root_path: str,
        http_version: str,
        scope: WWWScope,
    ) -> None:
        """Create a request or websocket base object.

        Arguments:
            method: The HTTP verb.
            scheme: The scheme used for the request.
            path: The full unquoted path of the request.
            query_string: The raw bytes for the query string part.
            headers: The request headers.
            root_path: The root path that should be prepended to all
                routes.
            http_version: The HTTP version of the request.
            scope: Underlying ASGI scope dictionary.

        Attributes:
            args: The query string arguments.
            scheme: The URL scheme, http or https.
        """
        super().__init__(
            method,
            scheme,
            scope.get("server"),
            root_path,
            path,
            query_string,
            headers,
            headers.get("Remote-Addr"),
        )
        self.http_version = http_version
        self.scope = scope

    @property
    def max_content_length(self) -> Optional[int]:
        """Read-only view of the ``MAX_CONTENT_LENGTH`` config key."""
        if current_app:
            return current_app.config["MAX_CONTENT_LENGTH"]
        else:
            return None

    @property
    def endpoint(self) -> Optional[str]:
        """Returns the corresponding endpoint matched for this request.

        This can be None if the request has not been matched with a
        rule.
        """
        if self.url_rule is not None:
            return self.url_rule.endpoint
        else:
            return None

    @property
    def blueprint(self) -> Optional[str]:
        """Returns the blueprint the matched endpoint belongs to.

        This can be None if the request has not been matched or the
        endpoint is not in a blueprint.
        """
        if self.endpoint is not None and "." in self.endpoint:
            return self.endpoint.rsplit(".", 1)[0]
        else:
            return None

    @property
    def blueprints(self) -> List[str]:
        """Return the names of the current blueprints.
        The returned list is ordered from the current blueprint,
        upwards through parent blueprints.
        """
        # Avoid circular import
        from ..helpers import _split_blueprint_path

        if self.blueprint is not None:
            return _split_blueprint_path(self.blueprint)
        else:
            return []

    @property
    def script_root(self) -> str:
        return self.root_path

    @property
    def url_root(self) -> str:
        return self.root_url
