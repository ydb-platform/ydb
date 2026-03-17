from typing import Dict, ForwardRef, Type, Union

from pydantic import BaseModel


class DocsRegistry:
    _registry: Dict[str, Type[BaseModel]] = {}

    @classmethod
    def register(cls, name: str, doc_type: Type[BaseModel]):
        cls._registry[name] = doc_type

    @classmethod
    def get(cls, name: str) -> Type[BaseModel]:
        return cls._registry[name]

    @classmethod
    def evaluate_fr(cls, forward_ref: Union[ForwardRef, Type]):
        """
        Evaluate forward ref

        :param forward_ref: ForwardRef - forward ref to evaluate
        :return: Type[BaseModel] - class of the forward ref
        """
        if (
            isinstance(forward_ref, ForwardRef)
            and forward_ref.__forward_arg__ in cls._registry
        ):
            return cls._registry[forward_ref.__forward_arg__]
        else:
            return forward_ref
