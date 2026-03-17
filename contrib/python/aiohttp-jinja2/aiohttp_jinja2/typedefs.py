from typing import Callable, Iterable, Mapping, Tuple, Union

Filter = Callable[..., str]
Filters = Union[Iterable[Tuple[str, Filter]], Mapping[str, Filter]]
