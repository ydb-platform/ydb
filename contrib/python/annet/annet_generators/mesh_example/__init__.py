from typing import List

from annet.generators import BaseGenerator
from annet.storage import Storage

from . import bgp


def get_generators(store: Storage) -> List[BaseGenerator]:
    return bgp.get_generators(store)
