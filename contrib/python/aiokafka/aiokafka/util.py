from __future__ import annotations

import asyncio
import os
from asyncio import AbstractEventLoop
from collections.abc import Awaitable, Coroutine
from typing import (
    Any,
    TypeVar,
    cast,
)

import async_timeout
from packaging.version import Version

from .structs import OffsetAndMetadata, TopicPartition

__all__ = [
    "INTEGER_MAX_VALUE",
    "INTEGER_MIN_VALUE",
    "NO_EXTENSIONS",
    "create_future",
    "create_task",
]


T = TypeVar("T")


def create_task(coro: Coroutine[Any, Any, T]) -> asyncio.Task[T]:
    loop = get_running_loop()
    return loop.create_task(coro)


def create_future(loop: AbstractEventLoop | None = None) -> asyncio.Future[T]:
    if loop is None:
        loop = get_running_loop()
    return loop.create_future()


async def wait_for(fut: Awaitable[T], timeout: None | int | float = None) -> T:
    # A replacement for buggy (since 3.8.6) `asyncio.wait_for()`
    # https://bugs.python.org/issue42130
    async with async_timeout.timeout(timeout):
        return await fut


def parse_kafka_version(api_version: str) -> tuple[int, int, int]:
    parsed = Version(api_version).release
    if not 2 <= len(parsed) <= 3:
        raise ValueError(api_version)
    version = cast(tuple[int, int, int], (*parsed, 0)[:3])

    if not (0, 9) <= version < (3, 0):
        raise ValueError(api_version)
    return version


def commit_structure_validate(
    offsets: dict[TopicPartition, int | tuple[int, str] | OffsetAndMetadata],
) -> dict[TopicPartition, OffsetAndMetadata]:
    # validate `offsets` structure
    if not offsets or not isinstance(offsets, dict):
        raise ValueError(offsets)

    formatted_offsets = {}
    for tp, offset_and_metadata in offsets.items():
        if not isinstance(tp, TopicPartition):
            raise TypeError("Key should be TopicPartition instance")

        if isinstance(offset_and_metadata, int):
            offset, metadata = offset_and_metadata, ""
        else:
            try:
                offset, metadata = offset_and_metadata
            except Exception as exc:
                raise ValueError(offsets) from exc

            if not isinstance(metadata, str):
                raise TypeError("Metadata should be a string")

        formatted_offsets[tp] = OffsetAndMetadata(offset, metadata)
    return formatted_offsets


def get_running_loop() -> asyncio.AbstractEventLoop:
    loop = asyncio.get_event_loop()
    if not loop.is_running():
        raise RuntimeError(
            "The object should be created within an async function or "
            "provide loop directly."
        )
    return loop


NO_EXTENSIONS = bool(os.environ.get("AIOKAFKA_NO_EXTENSIONS"))

INTEGER_MAX_VALUE = 2**31 - 1
INTEGER_MIN_VALUE = -(2**31)
