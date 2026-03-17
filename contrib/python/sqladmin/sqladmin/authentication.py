from __future__ import annotations

import functools
import inspect
from typing import Any, Callable

from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import RedirectResponse, Response


class AuthenticationBackend:
    """Base class for implementing the Authentication into SQLAdmin.
    You need to inherit this class and override the methods:
    `login`, `logout` and `authenticate`.
    """

    def __init__(self, secret_key: str) -> None:
        from starlette.middleware.sessions import SessionMiddleware

        self.middlewares = [
            Middleware(SessionMiddleware, secret_key=secret_key),
        ]

    async def login(self, request: Request) -> bool:
        """Implement login logic here.
        You can access the login form data `await request.form()`
        andvalidate the credentials.
        """
        raise NotImplementedError()

    async def logout(self, request: Request) -> Response | bool:
        """Implement logout logic here.
        This will usually clear the session with `request.session.clear()`.

        If a `Response` or `RedirectResponse` is returned,
        that response is returned to the user,
        otherwise the user will be redirected to the index page.
        """
        raise NotImplementedError()

    async def authenticate(self, request: Request) -> Response | bool:
        """Implement authenticate logic here.
        This method will be called for each incoming request
        to validate the authentication.

        If a `Response` or `RedirectResponse` is returned,
        that response is returned to the user,
        otherwise a True/False is expected.
        """
        raise NotImplementedError()


def login_required(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator to check authentication of Admin routes.
    If no authentication backend is setup, this will do nothing.
    """

    @functools.wraps(func)
    async def wrapper_decorator(*args: Any, **kwargs: Any) -> Any:
        view, request = args[0], args[1]
        admin = getattr(view, "_admin_ref", view)
        auth_backend = getattr(admin, "authentication_backend", None)
        if auth_backend is not None:
            response = await auth_backend.authenticate(request)
            if isinstance(response, Response):
                return response
            if not bool(response):
                return RedirectResponse(request.url_for("admin:login"), status_code=302)

        if inspect.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        return func(*args, **kwargs)

    return wrapper_decorator
