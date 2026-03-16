from ._core import Future
from typing import Callable, Generic, Optional, TypeVar, Union

_T = TypeVar("_T")

class ThenableFuture(Future[_T], Generic[_T]):
    def then(self,
        on_success: Optional[Callable[[_T], Union[_T, Future[_T]]]] = None,
        on_failure: Optional[Callable[[BaseException], Union[_T, BaseException]]] = None,
    ) -> ThenableFuture[_T]: ...
    def catch(self,
        on_failure: Optional[Callable[[BaseException], Union[_T, BaseException]]] = None,
    ) -> ThenableFuture[_T]: ...

def then(future: Future[_T],
    on_success: Optional[Callable[[_T], Union[_T, Future[_T]]]] = None,
    on_failure: Optional[Callable[[BaseException], Union[_T, BaseException]]] = None,
) -> Future[_T]: ...

def catch(future: Future[_T],
    on_failure: Optional[Callable[[BaseException], Union[_T, BaseException]]] = None,
) -> Future[_T]: ...
