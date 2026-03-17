from typing import Dict, TypeVar, Callable

L = TypeVar("L")
R = TypeVar("R")


def values_map(v_map: Dict[L, R]) -> Callable[[L], R]:
    def converter(left: L):
        if left in v_map:
            return v_map[left]
        else:
            raise ValueError(f"Value {left} is missing in value map.")

    return converter
