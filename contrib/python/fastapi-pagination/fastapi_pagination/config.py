__all__ = [
    "Config",
]

from dataclasses import dataclass
from typing import Any

from .bases import AbstractPage


@dataclass
class Config:
    """
    Configuration for the pagination.

    For this moment, only `page_cls` is available, which is a class that will be used to create pages.
    But in the future, more options might be added.
    """

    page_cls: type[AbstractPage[Any]] | None = None
