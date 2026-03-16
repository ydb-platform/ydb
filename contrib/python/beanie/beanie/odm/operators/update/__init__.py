from abc import abstractmethod
from typing import Any, Mapping

from beanie.odm.operators import BaseOperator


class BaseUpdateOperator(BaseOperator):
    @property
    @abstractmethod
    def query(self) -> Mapping[str, Any]: ...
