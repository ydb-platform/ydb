import asyncio
import types

from adrf.views import APIView


def api_view(http_method_names=None):
    """
    Decorator that converts a function-based view into an APIView subclass.
    Takes a list of allowed methods for the view as an argument.
    """
    http_method_names = ["GET"] if (http_method_names is None) else http_method_names

    def decorator(func):
        WrappedAPIView = type("WrappedAPIView", (APIView,), {"__doc__": func.__doc__})

        # Note, the above allows us to set the docstring.
        # It is the equivalent of:
        #
        #     class WrappedAPIView(APIView):
        #         pass
        #     WrappedAPIView.__doc__ = func.doc    <--- Not possible to do this

        # api_view applied without (method_names)
        assert not (
            isinstance(http_method_names, types.FunctionType)
        ), "@api_view missing list of allowed HTTP methods"

        # api_view applied with eg. string instead of list of strings
        assert isinstance(http_method_names, (list, tuple)), (
            "@api_view expected a list of strings, received %s"
            % type(http_method_names).__name__
        )

        allowed_methods = set(http_method_names) | {"options"}
        WrappedAPIView.http_method_names = [
            method.lower() for method in allowed_methods
        ]

        view_is_async = asyncio.iscoroutinefunction(func)

        if view_is_async:

            async def handler(self, *args, **kwargs):
                return await func(*args, **kwargs)

        else:

            def handler(self, *args, **kwargs):
                return func(*args, **kwargs)

        for method in http_method_names:
            setattr(WrappedAPIView, method.lower(), handler)

        WrappedAPIView.__name__ = func.__name__
        WrappedAPIView.__module__ = func.__module__

        WrappedAPIView.renderer_classes = getattr(
            func, "renderer_classes", APIView.renderer_classes
        )

        WrappedAPIView.parser_classes = getattr(
            func, "parser_classes", APIView.parser_classes
        )

        WrappedAPIView.authentication_classes = getattr(
            func, "authentication_classes", APIView.authentication_classes
        )

        WrappedAPIView.throttle_classes = getattr(
            func, "throttle_classes", APIView.throttle_classes
        )

        WrappedAPIView.permission_classes = getattr(
            func, "permission_classes", APIView.permission_classes
        )

        WrappedAPIView.content_negotiation_class = getattr(
            func, "content_negotiation_class", APIView.content_negotiation_class
        )

        WrappedAPIView.metadata_class = getattr(
            func, "metadata_class", APIView.metadata_class
        )

        WrappedAPIView.versioning_class = getattr(
            func, "versioning_class", APIView.versioning_class
        )

        WrappedAPIView.schema = getattr(func, "schema", APIView.schema)

        return WrappedAPIView.as_view()

    return decorator
