from __future__ import annotations

from typing import Any, Callable, List, Optional

from .globals import current_app, request
from .typing import ResponseReturnValue

http_method_funcs = frozenset(["get", "post", "head", "options", "delete", "put", "trace", "patch"])


class View:
    """Use to define routes within a class structure.

    A View subclass must implement the :meth:`dispatch_request` in
    order to respond to requests. For automatic method finding based on
    the request HTTP Verb see :class:`MethodView`.

    An example usage is,

    .. code-block:: python

          class SimpleView:
              methods = ['GET']

              async def dispatch_request(id):
                  return f"ID is {id}"

          app.add_url_rule('/<id>', view_func=SimpleView.as_view('simple'))

    Note that class

    Attributes:
        decorators: A list of decorators to apply to a view
            method. The decorators are applied in the order of
            the list.
        methods: List of methods this view allows.
        provide_automatic_options: Override automatic OPTIONS
            if set, to either True or False.
    """

    decorators: List[Callable] = []
    methods: Optional[List[str]] = None
    provide_automatic_options: Optional[bool] = None

    async def dispatch_request(self, *args: Any, **kwargs: Any) -> ResponseReturnValue:
        """Override and return a Response.

        This will be called with the request view_args, i.e. any url
        parameters.
        """
        raise NotImplementedError()

    @classmethod
    def as_view(cls, name: str, *class_args: Any, **class_kwargs: Any) -> Callable:
        async def view(*args: Any, **kwargs: Any) -> ResponseReturnValue:
            self = view.view_class(*class_args, **class_kwargs)  # type: ignore
            return await current_app.ensure_async(self.dispatch_request)(*args, **kwargs)

        if cls.decorators:
            view.__name__ = name
            view.__module__ = cls.__module__
            for decorator in cls.decorators:
                view = decorator(view)

        view.view_class: View = cls  # type: ignore
        view.__name__ = name
        view.__doc__ = cls.__doc__
        view.__module__ = cls.__module__
        view.methods = cls.methods  # type: ignore
        view.provide_automatic_options = cls.provide_automatic_options  # type: ignore
        return view


class MethodViewType(type):
    def __init__(cls, name, bases, attributes):  # type: ignore # noqa
        super().__init__(name, bases, attributes)

        if "methods" not in attributes:
            methods = []

            for key in http_method_funcs:
                if hasattr(cls, key):
                    methods.append(key.upper())

            if methods:
                cls.methods = methods


class MethodView(View, metaclass=MethodViewType):
    """A HTTP Method (verb) specific view class.

    This has an implementation of :meth:`dispatch_request` such that
    it calls a method based on the verb i.e. GET requests are handled
    by a `get` method. For example,

    .. code-block:: python

        class SimpleView(MethodView):
            async def get(id):
                return f"Get {id}"

            async def post(id):
                return f"Post {id}"

          app.add_url_rule('/<id>', view_func=SimpleView.as_view('simple'))
    """

    async def dispatch_request(self, *args: Any, **kwargs: Any) -> ResponseReturnValue:
        handler = getattr(self, request.method.lower(), None)

        if handler is None and request.method == "HEAD":
            handler = getattr(self, "get", None)

        return await current_app.ensure_async(handler)(*args, **kwargs)
