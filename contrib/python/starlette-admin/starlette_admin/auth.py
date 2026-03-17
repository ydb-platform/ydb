import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Optional, Sequence, Union

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.routing import Match, Mount, Route, WebSocketRoute
from starlette.status import (
    HTTP_400_BAD_REQUEST,
    HTTP_422_UNPROCESSABLE_ENTITY,
)
from starlette_admin.exceptions import FormValidationError, LoginFailed
from starlette_admin.helpers import wrap_endpoint_with_kwargs
from starlette_admin.i18n import lazy_gettext as _

if TYPE_CHECKING:
    from starlette_admin.base import BaseAdmin

from urllib.parse import urlencode

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import RedirectResponse, Response
from starlette.status import (
    HTTP_303_SEE_OTHER,
)
from starlette.types import ASGIApp


def login_not_required(
    endpoint: Callable[..., Any],
) -> Callable[..., Any]:
    """Decorators for endpoints that do not require login."""

    endpoint._login_not_required = True  # type: ignore[attr-defined]

    return endpoint


@dataclass
class AdminUser:
    username: str = field(default_factory=lambda: _("Administrator"))
    photo_url: Optional[str] = None


@dataclass
class AdminConfig:
    logo_url: Optional[str] = None
    app_title: Optional[str] = None


class BaseAuthProvider(ABC):
    """
    Base class for implementing the Authentication into your admin interface

    Args:
        login_path: The path for the login page.
        logout_path: The path for the logout page.
        allow_paths: A list of paths that are allowed without authentication.
        allow_routes: A list of route names that are allowed without authentication.

    Warning:
        - The usage of `allow_paths` is deprecated. It is recommended to use `allow_routes`
          that specifies the route names instead.

    """

    def __init__(
        self,
        login_path: str = "/login",
        logout_path: str = "/logout",
        allow_paths: Optional[Sequence[str]] = None,
        allow_routes: Optional[Sequence[str]] = None,
    ) -> None:
        self.login_path = login_path
        self.logout_path = logout_path
        self.allow_paths = allow_paths
        self.allow_routes = allow_routes

        if allow_paths:
            warnings.warn(
                "`allow_paths` is deprecated. Use `allow_routes` instead.",
                DeprecationWarning,
                stacklevel=2,
            )

    @abstractmethod
    def setup_admin(self, admin: "BaseAdmin") -> None:
        """
        This method is an abstract method that must be implemented in subclasses.
        It allows custom configuration and setup of the admin interface
        related to authentication and authorization.
        """
        raise NotImplementedError()

    def get_middleware(self, admin: "BaseAdmin") -> Middleware:
        """
        This method returns the authentication middleware required for the admin interface
        to enable authentication
        """
        return Middleware(AuthMiddleware, provider=self)

    async def is_authenticated(self, request: Request) -> bool:
        """
        This method will be called to validate each incoming request.
        You can also save the connected user information into the
        request state and use it later to restrict access to some part
        of your admin interface

        Returns:
            True: to accept the request
            False: to redirect to login page

        Examples:
            ```python
            async def is_authenticated(self, request: Request) -> bool:
                if request.session.get("username", None) in users:
                    # Save user object in state
                    request.state.user = my_users_db.get(request.session["username"])
                    return True
                return False
            ```
        """
        return False

    def get_admin_config(self, request: Request) -> Optional[AdminConfig]:
        """
        Override this method to display custom `logo_url` and/or `app_title`

        Returns:
            AdminConfig: The admin interface config

        Examples:
            ```python
            def get_admin_config(self, request: Request) -> AdminConfig:
                user = request.state.user  # Retrieve current user (previously saved in the request state)
                return AdminConfig(
                    logo_url=request.url_for("static", path=user["company_logo_url"]),
                )
            ```

            ```python
            def get_admin_config(self, request: Request) -> AdminConfig:
                user = request.state.user  # Retrieve current user (previously saved in the request state)
                return AdminConfig(
                    app_title="Hello, " + user["name"] + "!",
                )
            ```
        """
        return None  # pragma: no cover

    def get_admin_user(self, request: Request) -> Optional[AdminUser]:
        """
        Override this method to display connected user `name` and/or `profile`

        Returns:
            AdminUser: The connected user info

        Examples:
            ```python
            def get_admin_user(self, request: Request) -> AdminUser:
                user = request.state.user  # Retrieve current user (previously saved in the request state)
                return AdminUser(username=user["name"], photo_url=user["photo_url"])
            ```
        """
        return None  # pragma: no cover


class AuthProvider(BaseAuthProvider):
    async def login(
        self,
        username: str,
        password: str,
        remember_me: bool,
        request: Request,
        response: Response,
    ) -> Response:
        """
        This method will be called to validate user credentials

        Returns:
            response: return the response back

        Raises:
            FormValidationError: when form values is not valid
            LoginFailed: to display general error

        Examples:
            ```python
            async def login(
                self,
                username: str,
                password: str,
                remember_me: bool,
                request: Request,
                response: Response,
            ) -> Response:
                if len(username) < 3:
                    # Form data validation
                    raise FormValidationError(
                        {"username": "Ensure username has at least 03 characters"}
                    )

                if username in my_users_db and password == "password":
                    # Save username in session
                    request.session.update({"username": username})
                    return response

                raise LoginFailed("Invalid username or password")
            ```
        """
        raise LoginFailed("Not Implemented")

    async def logout(self, request: Request, response: Response) -> Response:
        """
        Implement logout logic (clear sessions, cookies, ...) here
        and return the response back

        Returns:
            response: return the response back

        Examples:
            ```python
            async def logout(self, request: Request, response: Response) -> Response:
                request.session.clear()
                return response
            ```
        """
        raise NotImplementedError()

    async def render_login(self, request: Request, admin: "BaseAdmin") -> Response:
        """Render the default login page for username & password authentication."""
        if request.method == "GET":
            return admin.templates.TemplateResponse(
                request=request,
                name="login.html",
                context={"_is_login_path": True},
            )
        form = await request.form()
        try:
            return await self.login(
                form.get("username"),  # type: ignore
                form.get("password"),  # type: ignore
                form.get("remember_me") == "on",
                request,
                RedirectResponse(
                    request.query_params.get("next")
                    or request.url_for(admin.route_name + ":index"),
                    status_code=HTTP_303_SEE_OTHER,
                ),
            )
        except FormValidationError as errors:
            return admin.templates.TemplateResponse(
                request=request,
                name="login.html",
                context={"form_errors": errors, "_is_login_path": True},
                status_code=HTTP_422_UNPROCESSABLE_ENTITY,
            )
        except LoginFailed as error:
            return admin.templates.TemplateResponse(
                request=request,
                name="login.html",
                context={"error": error.msg, "_is_login_path": True},
                status_code=HTTP_400_BAD_REQUEST,
            )

    async def render_logout(self, request: Request, admin: "BaseAdmin") -> Response:
        """Render the default logout page."""
        return await self.logout(
            request,
            RedirectResponse(
                request.url_for(admin.route_name + ":index"),
                status_code=HTTP_303_SEE_OTHER,
            ),
        )

    def get_login_route(self, admin: "BaseAdmin") -> Route:
        """
        Get the login route for the admin interface.
        """
        return Route(
            self.login_path,
            wrap_endpoint_with_kwargs(self.render_login, admin=admin),
            methods=["GET", "POST"],
        )

    def get_logout_route(self, admin: "BaseAdmin") -> Route:
        """
        Get the logout route for the admin interface.
        """
        return Route(
            self.logout_path,
            wrap_endpoint_with_kwargs(self.render_logout, admin=admin),
            methods=["GET"],
        )

    def setup_admin(self, admin: "BaseAdmin") -> None:
        """
        Set up the admin interface by adding necessary middleware and routes.
        """
        admin.middlewares.append(self.get_middleware(admin=admin))
        login_route = self.get_login_route(admin=admin)
        logout_route = self.get_logout_route(admin=admin)
        login_route.name = "login"
        logout_route.name = "logout"
        admin.routes.extend([login_route, logout_route])


class AuthMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app: ASGIApp,
        provider: "BaseAuthProvider",
        allow_paths: Optional[Sequence[str]] = None,
        allow_routes: Optional[Sequence[str]] = None,
    ) -> None:
        super().__init__(app)
        self.provider = provider
        self.allow_paths = list(allow_paths) if allow_paths is not None else []
        self.allow_paths.extend(
            self.provider.allow_paths if self.provider.allow_paths is not None else []
        )

        self.allow_routes = list(allow_routes) if allow_routes is not None else []
        self.allow_routes.extend(["login", "statics"])
        self.allow_routes.extend(
            self.provider.allow_routes if self.provider.allow_routes is not None else []
        )

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        """This middleware checks if the requested admin endpoint requires login.
        If login is required, it redirects to the login page when the user is
        not authenticated.

        Endpoints are authorized without login if:

        - They are decorated with `@login_not_required`
        - Their path is in `allow_paths`
        - Their name is in `allow_routes`
        - The user is already authenticated
        """
        _admin_app: Starlette = request.scope["app"]
        current_route: Optional[Union[Route, Mount, WebSocketRoute]] = None
        for route in _admin_app.routes:
            match, _ = route.matches(request.scope)
            if match == Match.FULL:
                assert isinstance(route, (Route, Mount, WebSocketRoute))
                current_route = route
                break
        if (
            (current_route is not None and current_route.path in self.allow_paths)
            or (current_route is not None and current_route.name in self.allow_routes)
            or (
                current_route is not None
                and hasattr(current_route, "endpoint")
                and getattr(current_route.endpoint, "_login_not_required", False)
            )
            or await self.provider.is_authenticated(request)
        ):
            return await call_next(request)
        return RedirectResponse(
            "{url}?{query_params}".format(
                url=request.url_for(request.app.state.ROUTE_NAME + ":login"),
                query_params=urlencode({"next": str(request.url)}),
            ),
            status_code=HTTP_303_SEE_OTHER,
        )
