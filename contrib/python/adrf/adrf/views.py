import asyncio
from typing import List, Optional

from asgiref.sync import async_to_sync, sync_to_async
from rest_framework.permissions import BasePermission
from rest_framework.request import Request
from rest_framework.throttling import BaseThrottle
from rest_framework.views import APIView as DRFAPIView

from adrf.requests import AsyncRequest


class APIView(DRFAPIView):
    def sync_dispatch(self, request, *args, **kwargs):
        """
        `.sync_dispatch()` is pretty much the same as Django's regular dispatch,
        but with extra hooks for startup, finalize, and exception handling.
        """
        self.args = args
        self.kwargs = kwargs
        request = self.initialize_request(request, *args, **kwargs)
        self.request = request
        self.headers = self.default_response_headers  # deprecate?

        try:
            self.initial(request, *args, **kwargs)

            # Get the appropriate handler method
            if request.method.lower() in self.http_method_names:
                handler = getattr(
                    self, request.method.lower(), self.http_method_not_allowed
                )
            else:
                handler = self.http_method_not_allowed

            response = handler(request, *args, **kwargs)

        except Exception as exc:
            response = self.handle_exception(exc)

        self.response = self.finalize_response(request, response, *args, **kwargs)
        return self.response

    async def async_dispatch(self, request, *args, **kwargs):
        """
        `.async_dispatch()` is pretty much the same as Django's regular dispatch,
        except for awaiting the handler function and with extra hooks for startup,
        finalize, and exception handling.
        """
        self.args = args
        self.kwargs = kwargs
        request = self.initialize_request(request, *args, **kwargs)
        self.request = request
        self.headers = self.default_response_headers  # deprecate?

        try:
            await sync_to_async(self.initial)(request, *args, **kwargs)

            # Get the appropriate handler method
            if request.method.lower() in self.http_method_names:
                handler = getattr(
                    self, request.method.lower(), self.http_method_not_allowed
                )
            else:
                handler = self.http_method_not_allowed

            if asyncio.iscoroutinefunction(handler):
                response = await handler(request, *args, **kwargs)
            else:
                response = await sync_to_async(handler)(request, *args, **kwargs)

        except Exception as exc:
            response = self.handle_exception(exc)

        self.response = self.finalize_response(request, response, *args, **kwargs)
        return self.response

    def dispatch(self, request, *args, **kwargs):
        """
        Dispatch checks if the view is async or not and uses the respective
        async or sync dispatch method.
        """
        if getattr(self, "view_is_async", False):
            return self.async_dispatch(request, *args, **kwargs)
        else:
            return self.sync_dispatch(request, *args, **kwargs)

    def initialize_request(self, request, *args, **kwargs):
        """
        Returns the initial request object.
        """
        parser_context = self.get_parser_context(request)

        return AsyncRequest(
            request,
            parsers=self.get_parsers(),
            authenticators=self.get_authenticators(),
            negotiator=self.get_content_negotiator(),
            parser_context=parser_context,
        )

    def check_permissions(self, request: Request) -> None:
        permissions = self.get_permissions()

        if not permissions:
            return

        sync_permissions, async_permissions = [], []

        for permission in permissions:
            if asyncio.iscoroutinefunction(permission.has_permission):
                async_permissions.append(permission)
            else:
                sync_permissions.append(permission)

        if async_permissions:
            async_to_sync(self.check_async_permissions)(request, async_permissions)

        if sync_permissions:
            self.check_sync_permissions(request, sync_permissions)

    async def check_async_permissions(
        self, request: AsyncRequest, permissions: List[BasePermission]
    ) -> None:
        """
        Check if the request should be permitted asynchronously.
        Raises an appropriate exception if the request is not permitted.
        """

        has_permissions = await asyncio.gather(
            *[permission.has_permission(request, self) for permission in permissions],
            return_exceptions=True,
        )

        for has_permission in has_permissions:
            if isinstance(has_permission, Exception):
                raise has_permission
            elif not has_permission:
                self.permission_denied(
                    request,
                    message=getattr(has_permission, "detail", None),
                    code=getattr(has_permission, "code", None),
                )

    def check_sync_permissions(
        self, request: Request, permissions: List[BasePermission]
    ) -> None:
        """
        Check if the request should be permitted synchronously.
        Raises an appropriate exception if the request is not permitted.
        """

        for permission in permissions:
            if not permission.has_permission(request, self):
                self.permission_denied(
                    request,
                    message=getattr(permission, "detail", None),
                    code=getattr(permission, "code", None),
                )

    def check_object_permissions(self, request: Request, obj) -> None:
        permissions = self.get_permissions()

        if not permissions:
            return

        sync_permissions, async_permissions = [], []

        for permission in permissions:
            if asyncio.iscoroutinefunction(permission.has_object_permission):
                async_permissions.append(permission)
            else:
                sync_permissions.append(permission)

        if async_permissions:
            async_to_sync(self.check_async_object_permissions)(
                request, async_permissions, obj
            )

        if sync_permissions:
            self.check_sync_object_permissions(request, sync_permissions, obj)

    async def check_async_object_permissions(
        self, request: AsyncRequest, permissions: List[BasePermission], obj
    ) -> None:
        """
        Check if the request should be permitted asynchronously.
        Raises an appropriate exception if the request is not permitted.
        """

        has_object_permissions = await asyncio.gather(
            *[
                permission.has_object_permission(request, self, obj)
                for permission in permissions
            ],
            return_exceptions=True,
        )

        for has_object_permission in has_object_permissions:
            if isinstance(has_object_permission, Exception):
                raise has_object_permission
            elif not has_object_permission:
                self.permission_denied(
                    request,
                    message=getattr(has_object_permission, "detail", None),
                    code=getattr(has_object_permission, "code", None),
                )

    def check_sync_object_permissions(
        self, request: Request, permissions: List[BasePermission], obj
    ) -> None:
        """
        Check if the request should be permitted synchronously.
        Raises an appropriate exception if the request is not permitted.
        """

        for permission in permissions:
            if not permission.has_object_permission(request, self, obj):
                self.permission_denied(
                    request,
                    message=getattr(permission, "detail", None),
                    code=getattr(permission, "code", None),
                )

    def check_throttles(self, request: Request) -> None:
        """
        Check if the request should be throttled.
        Raises an appropriate exception if the request is throttled.
        """
        throttles = self.get_throttles()

        if not throttles:
            return

        throttle_durations = []

        sync_throttles, async_throttles = [], []

        for throttle in throttles:
            if asyncio.iscoroutinefunction(throttle.allow_request):
                async_throttles.append(throttle)
            else:
                sync_throttles.append(throttle)

        throttle_durations.extend(self.check_sync_throttles(request, sync_throttles))

        throttle_durations.extend(
            async_to_sync(self.check_async_throttles)(request, async_throttles)
        )

        if throttle_durations:
            # Filter out `None` values which may happen in case of config / rate
            # changes, see #1438
            durations = [
                duration for duration in throttle_durations if duration is not None
            ]

            duration = max(durations, default=None)
            self.throttled(request, duration)

    async def check_async_throttles(
        self, request: AsyncRequest, throttles: List[BaseThrottle]
    ) -> List[Optional[float]]:
        """
        Check if the request should be throttled asynchronously.
        Raises an appropriate exception if the request is throttled.
        """

        throttle_durations = []

        for throttle in throttles:
            if not await throttle.allow_request(request, self):
                throttle_durations.append(throttle.wait())

        return throttle_durations

    def check_sync_throttles(
        self, request: Request, throttles: List[BaseThrottle]
    ) -> List[Optional[float]]:
        """
        Check if the request should be throttled synchronously.
        Raises an appropriate exception if the request is throttled.
        """

        throttle_durations = []

        for throttle in throttles:
            if not throttle.allow_request(request, self):
                throttle_durations.append(throttle.wait())

        return throttle_durations
