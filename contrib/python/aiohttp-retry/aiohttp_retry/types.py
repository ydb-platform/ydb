from typing import Union

from aiohttp import ClientSession

from .client import RetryClient

ClientType = Union[ClientSession, RetryClient]
