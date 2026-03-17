from dataclasses import dataclass

from ..._internal.iterator_constructor import construct_iterator2


from .. import interfaces as m_interfaces
from .. import algo as m_algo
from .. import box as m_box
from .. import option as m_option
from .. import operators as m_operators


# ---

@dataclass(frozen=True, slots=True)
class Iter2Api:
    # --- modules ---

    interfaces: object
    box: object
    option: object

    # --- shortcut modules ---

    algo: object
    op: object

    # --- constructor ---
    __call__: object



# ---

iter2 = Iter2Api(

    # --- modules ---

    interfaces=m_interfaces,
    box=m_box,
    option=m_option,

    algo=m_algo,
    op=m_operators,

    # --- constructor ---

    __call__=construct_iterator2,
)
