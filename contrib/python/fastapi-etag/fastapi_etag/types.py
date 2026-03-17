from typing import Callable, Union, Awaitable, Optional
from starlette.requests import Request

EtagGen = Callable[[Request], Union[Optional[str], Awaitable[Optional[str]]]]
