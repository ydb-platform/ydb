from abc import ABC, abstractmethod
from typing import List, Type

from beanie.odm.documents import Document


class BaseMigrationController(ABC):
    def __init__(self, function):
        self.function = function

    @abstractmethod
    async def run(self, session):
        pass

    @property
    @abstractmethod
    def models(self) -> List[Type[Document]]:
        pass
