from typing import Dict
from typing import List
from typing import Callable
from _typeshed import Incomplete


class GlobalProfiler:
    setup_config: Dict[str, List[str]]
    output_prefix: str
    write_config: Dict[str, bool]
    show_config: Dict[str, bool]
    enabled: bool | None

    def __init__(self) -> None:
        ...

    def enable(self, output_prefix: Incomplete | None = ...) -> None:
        ...

    def disable(self) -> None:
        ...

    def __call__(self, func: Callable) -> Callable:
        ...

    def show(self) -> None:
        ...


profile: Incomplete
