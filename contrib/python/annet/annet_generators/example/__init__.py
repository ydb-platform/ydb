import itertools
from typing import List

from annet.generators import BaseGenerator
from annet.storage import Storage

from . import hostname, lldp


def get_generators(store: Storage) -> List[BaseGenerator]:
    return list(
        itertools.chain.from_iterable(
            [
                hostname.get_generators(store),
                lldp.get_generators(store),
            ]
        )
    )
