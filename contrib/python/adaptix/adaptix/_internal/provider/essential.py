import typing
from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Generic, Optional, TypeVar, final

from ..common import VarTuple
from ..compat import CompatExceptionGroup
from ..feature_requirement import HAS_NATIVE_EXC_GROUP
from ..utils import with_module

T = TypeVar("T")


@dataclass(frozen=True)
class Request(Generic[T]):
    """An object that contains data to be processed by :class:`Provider`.

    Generic argument indicates which object should be
    returned after request processing.

    Request must always be a hashable object
    """


@with_module("adaptix")
class CannotProvide(Exception):
    def __init__(
        self,
        message: str = "",
        *,
        is_terminal: bool = False,
        is_demonstrative: bool = False,
        parent_notes_gen: Optional[Callable[[], Sequence[str]]] = None,
    ):
        self.message = message
        self.is_terminal = is_terminal
        self.is_demonstrative = is_demonstrative
        self.parent_notes_gen = parent_notes_gen

    def __repr__(self):
        return (
            f"{type(self).__name__}"
            f"(message={self.message!r}, is_terminal={self.is_terminal}, is_demonstrative={self.is_demonstrative})"
        )


@with_module("adaptix")
class AggregateCannotProvide(CompatExceptionGroup[CannotProvide], CannotProvide):  # type: ignore[misc]
    def __init__(
        self,
        message: str,
        exceptions: Sequence[CannotProvide],
        *,
        is_terminal: bool = False,
        is_demonstrative: bool = False,
        parent_notes_gen: Optional[Callable[[], Sequence[str]]] = None,
    ):
        # Parameter `message` is saved by `__new__` of CompatExceptionGroup
        self.is_terminal = is_terminal
        self.is_demonstrative = is_demonstrative
        self.parent_notes_gen = parent_notes_gen

    if not HAS_NATIVE_EXC_GROUP:
        def __new__(
            cls,
            message: str,
            exceptions: Sequence[CannotProvide],
            *,
            is_terminal: bool = False,
            is_demonstrative: bool = False,
            parent_notes_gen: Optional[Callable[[], Sequence[str]]] = None,
        ):
            return super().__new__(cls, message, exceptions)

    if TYPE_CHECKING:
        exceptions: VarTuple[CannotProvide]

    def derive(self, excs: Sequence[CannotProvide]) -> "AggregateCannotProvide":  # type: ignore[override]
        return AggregateCannotProvide(
            self.message,
            excs,
            is_terminal=self.is_terminal,
            is_demonstrative=self.is_demonstrative,
            parent_notes_gen=self.parent_notes_gen,
        )

    def derive_upcasting(self, excs: Sequence[CannotProvide]) -> CannotProvide:
        """Same as method ``derive`` but allow passing an empty sequence"""
        return self.make(
            self.message,
            excs,
            is_terminal=self.is_terminal,
            is_demonstrative=self.is_demonstrative,
            parent_notes_gen=self.parent_notes_gen,
        )

    @classmethod
    def make(
        cls,
        message: str,
        exceptions: Sequence[CannotProvide],
        *,
        is_terminal: bool = False,
        is_demonstrative: bool = False,
        parent_notes_gen: Optional[Callable[[], Sequence[str]]] = None,
    ) -> CannotProvide:
        if exceptions:
            return AggregateCannotProvide(
                message,
                exceptions,
                is_terminal=is_terminal,
                is_demonstrative=is_demonstrative,
                parent_notes_gen=parent_notes_gen,
            )
        return CannotProvide(
            message,
            is_terminal=is_terminal,
            is_demonstrative=is_demonstrative,
            parent_notes_gen=parent_notes_gen,
        )


class DirectMediator(ABC):
    """Mediator is an object that gives provider access to other providers
    and that stores the state of the current search.

    Mediator is a proxy to providers of retort.
    """

    @abstractmethod
    def provide(self, request: Request[T]) -> T:
        """Get response of sent request.

        :param request: A request instance
        :return: Result of the request processing
        :raise CannotProvide: A provider able to process the request does not be found
        """

    @final
    def delegating_provide(
        self,
        request: Request[T],
        error_describer: Optional[Callable[[CannotProvide], str]] = None,
    ) -> T:
        try:
            return self.provide(request)
        except CannotProvide as e:
            raise AggregateCannotProvide(
                "" if error_describer is None else error_describer(e),
                [e],
                is_terminal=False,
                is_demonstrative=error_describer is not None,
            ) from None

    @final
    def mandatory_provide(
        self,
        request: Request[T],
        error_describer: Optional[Callable[[CannotProvide], str]] = None,
    ) -> T:
        try:
            return self.provide(request)
        except CannotProvide as e:
            raise AggregateCannotProvide(
                "" if error_describer is None else error_describer(e),
                [e],
                is_terminal=True,
                is_demonstrative=True,
            ) from None

    @final
    def mandatory_provide_by_iterable(
        self,
        requests: Iterable[Request[T]],
        error_describer: Optional[Callable[[], str]] = None,
    ) -> Sequence[T]:
        results = []
        exceptions = []
        for request in requests:
            try:
                result = self.provide(request)
            except CannotProvide as e:
                exceptions.append(e)
            else:
                results.append(result)
        if exceptions:
            raise AggregateCannotProvide.make(
                "" if error_describer is None else error_describer(),
                exceptions,
                is_demonstrative=True,
                is_terminal=True,
            )
        return results


def mandatory_apply_by_iterable(
    func: Callable[..., T],
    args_iterable: Iterable[VarTuple[Any]],
    error_describer: Optional[Callable[[], str]] = None,
) -> Iterable[T]:
    results = []
    exceptions = []
    for args in args_iterable:
        try:
            result = func(*args)
        except CannotProvide as e:
            exceptions.append(e)
        else:
            results.append(result)
    if exceptions:
        raise AggregateCannotProvide.make(
            "" if error_describer is None else error_describer(),
            exceptions,
            is_demonstrative=True,
            is_terminal=True,
        )
    return results


ResponseT = TypeVar("ResponseT")


if TYPE_CHECKING:
    P = typing.ParamSpec("P")


class Mediator(DirectMediator, ABC, Generic[ResponseT]):
    """Mediator is an object that gives provider access to other providers
    and that stores the state of the current search.

    Mediator is a proxy to providers of retort.
    """

    @abstractmethod
    def provide_from_next(self) -> ResponseT:
        """Forward current request to providers
        that placed after current provider at the recipe.
        """

    if TYPE_CHECKING:
        @abstractmethod
        def cached_call(self, func: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
            ...
    else:
        @abstractmethod
        def cached_call(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
            ...


RequestT = TypeVar("RequestT", bound=Request)
RequestHandler = Callable[[Mediator[ResponseT], RequestT], ResponseT]


class RequestChecker(ABC, Generic[RequestT]):
    @abstractmethod
    def check_request(self, mediator: DirectMediator, request: RequestT, /) -> bool:
        ...


RequestHandlerRegisterRecord = tuple[type[Request], RequestChecker, RequestHandler]


class Provider(ABC):
    """An object that can process Request instances"""

    @abstractmethod
    def get_request_handlers(self) -> Sequence[RequestHandlerRegisterRecord]:
        ...
