from annet.generators import BaseGenerator
from annet.storage import Storage

from . import generator


def get_generators(storage: Storage) -> list[BaseGenerator]:
    return generator.get_generators(storage)
