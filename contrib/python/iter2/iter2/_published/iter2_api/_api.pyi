import typing as tp

from ..._internal.iterator_proxy import Iterator2Proxy

from ..interfaces import Iterator2
from ..interfaces import Iterable2

from .. import interfaces as m_interfaces
from ... import algo as m_algo
from ... import option as m_option
from .. import operators as m_operators


# ---

class Iter2Api:

    # --- constructor ---

    @staticmethod
    @tp.overload
    def __call__[SomeIterator2: Iterator2](
        iterable: Iterable2[SomeIterator2],
    ) -> SomeIterator2: ...


    @staticmethod
    @tp.overload
    def __call__[Item](
        iterable: tp.Iterable[Item],
    ) -> Iterator2Proxy[Item]: ...

    @staticmethod
    def __call__(iterable): ...

    # ---

    interfaces = m_interfaces
    option = m_option

    algo = m_algo
    op = m_operators


# ---

iter2 = Iter2Api()
