# SPDX-FileCopyrightText: 2022 Hynek Schlawack <hs@ox.cx>
#
# SPDX-License-Identifier: MIT

from __future__ import annotations

from collections.abc import Iterable

from ._data import RetryHook, RetryHookFactory
from ._logging import LoggingOnRetryHook
from ._prometheus import PrometheusOnRetryHook
from ._structlog import StructlogOnRetryHook


def init_hooks(
    maybe_delayed: tuple[RetryHook | RetryHookFactory, ...],
) -> tuple[RetryHook, ...]:
    """
    Execute delayed hook factories and return a tuple of finalized hooks.
    """
    hooks = []
    for hook_or_factory in maybe_delayed:
        if isinstance(hook_or_factory, RetryHookFactory):
            hooks.append(hook_or_factory.hook_factory())
        else:
            hooks.append(hook_or_factory)

    return tuple(hooks)


def get_default_hooks() -> tuple[RetryHookFactory, ...]:
    """
    Return the default hooks according to availability.
    """
    hooks = []

    try:
        import prometheus_client  # noqa: F401

        hooks.append(PrometheusOnRetryHook)
    except ImportError:
        pass

    try:
        import structlog  # noqa: F401

        hooks.append(StructlogOnRetryHook)
    except ImportError:
        hooks.append(LoggingOnRetryHook)

    return tuple(hooks)


def set_on_retry_hooks(
    hooks: Iterable[RetryHook | RetryHookFactory] | None,
) -> None:
    """
    Set hooks that are called after a retry has been scheduled.

    Args:
        hooks:
            Hooks to call after a retry has been scheduled. Passing None resets
            to default. To deactivate instrumentation, pass an empty iterable.

    .. versionadded:: 23.2.0
    """
    from .._config import CONFIG

    CONFIG.on_retry = tuple(hooks) if hooks is not None else hooks


def get_on_retry_hooks() -> tuple[RetryHook, ...]:
    """
    Get hooks that are called after a retry has been scheduled.

    Returns:
        Hooks that will run if a retry is scheduled. Factories are called if
        they haven't already.

    .. versionadded:: 23.2.0
    """
    from .._config import CONFIG

    return CONFIG.on_retry
