import functools
import hmac
import logging
import sys
import os
import re
import warnings

from stripe._api_mode import ApiMode

from urllib.parse import quote_plus

from typing_extensions import Type, TYPE_CHECKING
from typing import (
    Callable,
    TypeVar,
    Union,
    overload,
    Dict,
    List,
    cast,
    Any,
    Optional,
    Mapping,
)
import typing_extensions


# Used for global variables
import stripe  # noqa: IMP101

if TYPE_CHECKING:
    from stripe._stripe_response import StripeResponse
    from stripe._stripe_object import StripeObject
    from stripe._api_requestor import _APIRequestor

STRIPE_LOG = os.environ.get("STRIPE_LOG")

logger: logging.Logger = logging.getLogger("stripe")

if TYPE_CHECKING:
    deprecated = typing_extensions.deprecated
else:
    _T = TypeVar("_T")

    # Copied from python/typing_extensions, as this was added in typing_extensions 4.5.0 which is incompatible with
    # python 3.6. We still need `deprecated = typing_extensions.deprecated` in addition to this fallback, as
    # IDEs (pylance) specially detect references to symbols defined in `typing_extensions`
    #
    # https://github.com/python/typing_extensions/blob/5d20e9eed31de88667542ba5a6f66e6dc439b681/src/typing_extensions.py#L2289-L2370
    def deprecated(
        __msg: str,
        *,
        category: Optional[Type[Warning]] = DeprecationWarning,
        stacklevel: int = 1,
    ) -> Callable[[_T], _T]:
        def decorator(__arg: _T) -> _T:
            if category is None:
                __arg.__deprecated__ = __msg
                return __arg
            elif isinstance(__arg, type):
                original_new = __arg.__new__
                has_init = __arg.__init__ is not object.__init__

                @functools.wraps(original_new)
                def __new__(cls, *args, **kwargs):
                    warnings.warn(
                        __msg, category=category, stacklevel=stacklevel + 1
                    )
                    if original_new is not object.__new__:
                        return original_new(cls, *args, **kwargs)
                    # Mirrors a similar check in object.__new__.
                    elif not has_init and (args or kwargs):
                        raise TypeError(f"{cls.__name__}() takes no arguments")
                    else:
                        return original_new(cls)

                __arg.__new__ = staticmethod(__new__)
                __arg.__deprecated__ = __new__.__deprecated__ = __msg
                return __arg
            elif callable(__arg):

                @functools.wraps(__arg)
                def wrapper(*args, **kwargs):
                    warnings.warn(
                        __msg, category=category, stacklevel=stacklevel + 1
                    )
                    return __arg(*args, **kwargs)

                __arg.__deprecated__ = wrapper.__deprecated__ = __msg
                return wrapper
            else:
                raise TypeError(
                    "@deprecated decorator with non-None category must be applied to "
                    f"a class or callable, not {__arg!r}"
                )

        return decorator


def is_appengine_dev():
    return "APPENGINE_RUNTIME" in os.environ and "Dev" in os.environ.get(
        "SERVER_SOFTWARE", ""
    )


def _console_log_level():
    if stripe.log in ["debug", "info"]:
        return stripe.log
    elif STRIPE_LOG in ["debug", "info"]:
        return STRIPE_LOG
    else:
        return None


def log_debug(message, **params):
    msg = logfmt(dict(message=message, **params))
    if _console_log_level() == "debug":
        print(msg, file=sys.stderr)
    logger.debug(msg)


def log_info(message, **params):
    msg = logfmt(dict(message=message, **params))
    if _console_log_level() in ["debug", "info"]:
        print(msg, file=sys.stderr)
    logger.info(msg)


def _test_or_live_environment():
    if stripe.api_key is None:
        return
    match = re.match(r"sk_(live|test)_", stripe.api_key)
    if match is None:
        return
    return match.groups()[0]


def dashboard_link(request_id):
    return "https://dashboard.stripe.com/{env}/logs/{reqid}".format(
        env=_test_or_live_environment() or "test", reqid=request_id
    )


def logfmt(props):
    def fmt(key, val):
        # Handle case where val is a bytes or bytesarray
        if hasattr(val, "decode"):
            val = val.decode("utf-8")
        # Check if val is already a string to avoid re-encoding into
        # ascii. Since the code is sent through 2to3, we can't just
        # use unicode(val, encoding='utf8') since it will be
        # translated incorrectly.
        if not isinstance(val, str):
            val = str(val)
        if re.search(r"\s", val):
            val = repr(val)
        # key should already be a string
        if re.search(r"\s", key):
            key = repr(key)
        return "{key}={val}".format(key=key, val=val)

    return " ".join([fmt(key, val) for key, val in sorted(props.items())])


# Borrowed from Django's source code
if hasattr(hmac, "compare_digest"):
    # Prefer the stdlib implementation, when available.
    def secure_compare(val1, val2):
        return hmac.compare_digest(val1, val2)

else:

    def secure_compare(val1, val2):
        """
        Returns True if the two strings are equal, False otherwise.
        The time taken is independent of the number of characters that match.
        For the sake of simplicity, this function executes in constant time
        only when the two strings have the same length. It short-circuits when
        they have different lengths.
        """
        if len(val1) != len(val2):
            return False
        result = 0
        if isinstance(val1, bytes) and isinstance(val2, bytes):
            for x, y in zip(val1, val2):
                result |= x ^ y
        else:
            for x, y in zip(val1, val2):
                result |= ord(cast(str, x)) ^ ord(cast(str, y))
        return result == 0


Resp = Union["StripeResponse", Dict[str, Any], List["Resp"]]


@overload
def convert_to_stripe_object(
    resp: Union["StripeResponse", Dict[str, Any]],
    api_key: Optional[str] = None,
    stripe_version: Optional[str] = None,
    stripe_account: Optional[str] = None,
    params: Optional[Mapping[str, Any]] = None,
    klass_: Optional[Type["StripeObject"]] = None,
    *,
    api_mode: ApiMode = "V1",
) -> "StripeObject": ...


@overload
def convert_to_stripe_object(
    resp: List[Resp],
    api_key: Optional[str] = None,
    stripe_version: Optional[str] = None,
    stripe_account: Optional[str] = None,
    params: Optional[Mapping[str, Any]] = None,
    klass_: Optional[Type["StripeObject"]] = None,
    *,
    api_mode: ApiMode = "V1",
) -> List["StripeObject"]: ...


def convert_to_stripe_object(
    resp: Resp,
    api_key: Optional[str] = None,
    stripe_version: Optional[str] = None,
    stripe_account: Optional[str] = None,
    params: Optional[Mapping[str, Any]] = None,
    klass_: Optional[Type["StripeObject"]] = None,
    *,
    api_mode: ApiMode = "V1",
) -> Union["StripeObject", List["StripeObject"]]:
    from stripe._api_requestor import _APIRequestor

    return _convert_to_stripe_object(
        resp=resp,
        params=params,
        klass_=klass_,
        requestor=_APIRequestor._global_with_options(
            api_key=api_key,
            stripe_version=stripe_version,
            stripe_account=stripe_account,
        ),
        api_mode=api_mode,
    )


@overload
def _convert_to_stripe_object(
    *,
    resp: Union["StripeResponse", Dict[str, Any]],
    params: Optional[Mapping[str, Any]] = None,
    klass_: Optional[Type["StripeObject"]] = None,
    requestor: "_APIRequestor",
    api_mode: ApiMode,
    is_v2_deleted_object: bool = False,
) -> "StripeObject": ...


@overload
def _convert_to_stripe_object(
    *,
    resp: List[Resp],
    params: Optional[Mapping[str, Any]] = None,
    klass_: Optional[Type["StripeObject"]] = None,
    requestor: "_APIRequestor",
    api_mode: ApiMode,
    is_v2_deleted_object: bool = False,
) -> List["StripeObject"]: ...


def _convert_to_stripe_object(
    *,
    resp: Resp,
    params: Optional[Mapping[str, Any]] = None,
    klass_: Optional[Type["StripeObject"]] = None,
    requestor: "_APIRequestor",
    api_mode: ApiMode,
    # if true, we should ignore the `object` field for finding the class name. This is set by the API requestor
    is_v2_deleted_object: bool = False,
) -> Union["StripeObject", List["StripeObject"]]:
    # If we get a StripeResponse, we'll want to return a
    # StripeObject with the last_response field filled out with
    # the raw API response information
    stripe_response = None

    # Imports here at runtime to avoid circular dependencies
    from stripe._stripe_response import StripeResponse
    from stripe._stripe_object import StripeObject

    if isinstance(resp, StripeResponse):
        stripe_response = resp
        resp = cast(Resp, stripe_response.data)

    if isinstance(resp, list):
        return [
            _convert_to_stripe_object(
                resp=cast("Union[StripeResponse, Dict[str, Any]]", i),
                requestor=requestor,
                api_mode=api_mode,
                klass_=klass_,
            )
            for i in resp
        ]
    elif isinstance(resp, dict) and not isinstance(resp, StripeObject):
        resp = resp.copy()
        klass_name = resp.get("object")
        if isinstance(klass_name, str):
            if is_v2_deleted_object:
                # circular import
                from stripe.v2._deleted_object import DeletedObject

                klass = DeletedObject
            elif api_mode == "V2" and klass_name == "v2.core.event":
                from stripe.events._event_classes import get_v2_event_class

                event_type = resp.get("type", "")
                klass = get_v2_event_class(event_type)
            else:
                from stripe._object_classes import get_object_class

                klass = get_object_class(api_mode, klass_name)
        # TODO: this is a horrible hack. The API needs
        # to return something for `object` here.

        elif "data" in resp and "next_page_url" in resp:
            klass = stripe.v2.ListObject
        elif klass_ is not None:
            klass = klass_
        else:
            klass = StripeObject

        obj = klass._construct_from(
            values=resp,
            last_response=stripe_response,
            requestor=requestor,
            api_mode=api_mode,
        )

        # We only need to update _retrieve_params when special params were
        # actually passed. Otherwise, leave it as is as the list / search result
        # constructors will instantiate their own params.
        if (
            params is not None
            and hasattr(obj, "object")
            and (
                (getattr(obj, "object") == "list")
                or (getattr(obj, "object") == "search_result")
            )
        ):
            obj._retrieve_params = params

        return obj
    else:
        return cast("StripeObject", resp)


def convert_to_dict(obj):
    """Converts a StripeObject back to a regular dict.

    Nested StripeObjects are also converted back to regular dicts.

    :param obj: The StripeObject to convert.

    :returns: The StripeObject as a dict.
    """
    if isinstance(obj, list):
        return [convert_to_dict(i) for i in obj]
    # This works by virtue of the fact that StripeObjects _are_ dicts. The dict
    # comprehension returns a regular dict and recursively applies the
    # conversion to each value.
    elif isinstance(obj, dict):
        return {k: convert_to_dict(v) for k, v in obj.items()}
    else:
        return obj


@overload
def populate_headers(
    idempotency_key: str,
) -> Dict[str, str]: ...


@overload
def populate_headers(idempotency_key: None) -> None: ...


def populate_headers(
    idempotency_key: Union[str, None],
) -> Union[Dict[str, str], None]:
    if idempotency_key is not None:
        return {"Idempotency-Key": idempotency_key}
    return None


T = TypeVar("T")


def merge_dicts(x, y):
    z = x.copy()
    z.update(y)
    return z


def sanitize_id(id):
    quotedId = quote_plus(id)
    return quotedId


def get_api_mode(url: str) -> ApiMode:
    if url.startswith("/v2"):
        return "V2"

    # if urls aren't explicitly marked as v1, they're assumed to be v1
    else:
        return "V1"


class class_method_variant(object):
    def __init__(self, class_method_name):
        self.class_method_name = class_method_name

    T = TypeVar("T")

    method: Any

    def __call__(self, method: T) -> T:
        self.method = method
        return cast(T, self)

    def __get__(self, obj, objtype: Optional[Type[Any]] = None):
        @functools.wraps(self.method)
        def _wrapper(*args, **kwargs):
            if obj is not None:
                # Method was called as an instance method, e.g.
                # instance.method(...)
                return self.method(obj, *args, **kwargs)
            elif (
                len(args) > 0
                and objtype is not None
                and isinstance(args[0], objtype)
            ):
                # Method was called as a class method with the instance as the
                # first argument, e.g. Class.method(instance, ...) which in
                # Python is the same thing as calling an instance method
                return self.method(args[0], *args[1:], **kwargs)
            else:
                # Method was called as a class method, e.g. Class.method(...)
                class_method = getattr(objtype, self.class_method_name)
                return class_method(*args, **kwargs)

        return _wrapper
