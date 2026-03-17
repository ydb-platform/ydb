import inspect
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Callable, ClassVar, TypeVar, final

from ..type_tools import get_all_type_hints, is_subclass_soft, normalize_type, strip_tags
from .essential import Mediator, Provider, Request, RequestChecker, RequestHandlerRegisterRecord
from .request_checkers import AlwaysTrueRequestChecker

__all__ = ("MethodsProvider", "method_handler")


P = TypeVar("P", bound=Any)
R = TypeVar("R", bound=Request)
T = TypeVar("T")
MethodHandler = Callable[[P, Mediator[T], R], T]

_METHOD_HANDLER_REQUEST_CLS = "_method_handler_request_cls"


def method_handler(func: MethodHandler[P, T, R], /) -> MethodHandler[P, T, R]:
    """Marks method as request handler. See :class:`MethodsProvider` for details"""
    request_cls = _infer_request_cls(func)
    setattr(func, _METHOD_HANDLER_REQUEST_CLS, request_cls)
    return func


def _infer_request_cls(func) -> type[Request]:
    signature = inspect.signature(func)

    params = list(signature.parameters.values())

    if len(params) < 3:  # noqa: PLR2004
        raise ValueError("Cannot infer request class from callable")

    if params[2].annotation == signature.empty:
        raise ValueError("Cannot infer request class from callable")

    type_hints = get_all_type_hints(func)
    request_tp = strip_tags(normalize_type(type_hints[params[2].name]))

    if is_subclass_soft(request_tp.origin, Request):
        return request_tp.source

    raise TypeError("Request parameter must be subclass of Request")


class MethodsProvider(Provider):
    _mp_cls_request_to_method_name: ClassVar[Mapping[type[Request], str]] = {}

    def __init_subclass__(cls, **kwargs):
        own_spa = _collect_class_own_request_cls_dict(cls)

        parent_request_cls_dicts = [
            parent._mp_cls_request_to_method_name
            for parent in cls.__bases__
            if issubclass(parent, MethodsProvider)
        ]
        cls._mp_cls_request_to_method_name = _merge_request_cls_dicts(cls, [*parent_request_cls_dicts, own_spa])
        for request_cls in cls._mp_cls_request_to_method_name:
            cls._validate_request_cls(request_cls)

    @classmethod
    def _validate_request_cls(cls, request_cls: type[Request]) -> None:
        pass

    def _get_request_checker(self) -> RequestChecker:
        return AlwaysTrueRequestChecker()

    @final
    def get_request_handlers(self) -> Sequence[RequestHandlerRegisterRecord]:
        request_checker = self._get_request_checker()
        return [
            (request_cls, request_checker, getattr(self, method_name))
            for request_cls, method_name in self._mp_cls_request_to_method_name.items()
        ]


def _request_cls_attached_to_several_method_handlers(
    cls: type,
    name1: str,
    name2: str,
    request_cls: type[Request],
):
    return TypeError(
        f"The {cls} has several @method_handler"
        " that attached to the same Request class"
        f" ({name1!r} and {name2!r} attached to {request_cls})",
    )


def _method_handler_has_different_request_cls(
    cls: type,
    name: str,
    request_cls1: type[Request],
    request_cls2: type[Request],
):
    return TypeError(
        f"The {cls} has @method_handler"
        " that attached to the different Request class"
        f" ({name!r} attached to {request_cls1} and {request_cls2})",
    )


_RequestClsToMethodName = dict[type[Request], str]


def _collect_class_own_request_cls_dict(cls) -> _RequestClsToMethodName:
    mapping: _RequestClsToMethodName = {}

    for attr_name in vars(cls):
        try:
            attr_value = getattr(cls, attr_name)
        except AttributeError:
            continue
        if hasattr(attr_value, _METHOD_HANDLER_REQUEST_CLS):
            request_cls = getattr(attr_value, _METHOD_HANDLER_REQUEST_CLS)
            if request_cls in mapping:
                old_name = mapping[request_cls]
                raise _request_cls_attached_to_several_method_handlers(
                    cls,
                    attr_name,
                    old_name,
                    request_cls,
                )

            mapping[request_cls] = attr_name

    return mapping


def _merge_request_cls_dicts(cls: type, dict_iter: Iterable[_RequestClsToMethodName]) -> _RequestClsToMethodName:
    name_to_request_cls: dict[str, type[Request]] = {}
    request_cls_to_name: _RequestClsToMethodName = {}
    for dct in dict_iter:
        for request_cls, name in dct.items():
            if request_cls in request_cls_to_name and request_cls_to_name[request_cls] != name:
                raise _request_cls_attached_to_several_method_handlers(
                    cls,
                    request_cls_to_name[request_cls],
                    name,
                    request_cls,
                )

            if name in name_to_request_cls and request_cls != name_to_request_cls[name]:
                raise _method_handler_has_different_request_cls(
                    cls,
                    name,
                    name_to_request_cls[name],
                    request_cls,
                )

            request_cls_to_name[request_cls] = name
            name_to_request_cls[name] = request_cls

    return request_cls_to_name
