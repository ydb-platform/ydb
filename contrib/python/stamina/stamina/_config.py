# SPDX-FileCopyrightText: 2022 Hynek Schlawack <hs@ox.cx>
#
# SPDX-License-Identifier: MIT

from __future__ import annotations

from collections.abc import Callable
from threading import Lock
from types import TracebackType

from .instrumentation import RetryHookFactory
from .instrumentation._hooks import get_default_hooks, init_hooks
from .typing import RetryHook


class _Testing:
    """
    Test mode specification.

    Strictly private.
    """

    __slots__ = ("attempts", "cap")

    attempts: int
    cap: bool

    def __init__(self, attempts: int, cap: bool) -> None:
        self.attempts = attempts
        self.cap = cap

    def get_attempts(self, non_testing_attempts: int | None) -> int:
        """
        Get the number of attempts to use.

        Args:
            non_testing_attempts: The number of attempts specified by the user.

        Returns:
            The number of attempts to use.
        """
        if self.cap:
            return min(self.attempts, non_testing_attempts or self.attempts)

        return self.attempts


class _Config:
    """
    Global stamina configuration.

    Strictly private.
    """

    __slots__ = (
        "_get_on_retry",
        "_is_active",
        "_on_retry",
        "_testing",
        "lock",
    )

    lock: Lock
    _is_active: bool
    _testing: _Testing | None
    _on_retry: (
        tuple[RetryHook, ...] | tuple[RetryHook | RetryHookFactory, ...] | None
    )
    _get_on_retry: Callable[[], tuple[RetryHook, ...]]

    def __init__(self, lock: Lock) -> None:
        self.lock = lock
        self._is_active = True
        self._testing = None

        # Prepare delayed initialization.
        self._on_retry = None
        self._get_on_retry = self._init_on_first_retry

    @property
    def is_active(self) -> bool:
        return self._is_active

    @is_active.setter
    def is_active(self, value: bool) -> None:
        with self.lock:
            self._is_active = value

    @property
    def testing(self) -> _Testing | None:
        return self._testing

    @testing.setter
    def testing(self, value: _Testing | None) -> None:
        with self.lock:
            self._testing = value

    @property
    def on_retry(self) -> tuple[RetryHook, ...]:
        return self._get_on_retry()

    @on_retry.setter
    def on_retry(
        self, value: tuple[RetryHook | RetryHookFactory, ...] | None
    ) -> None:
        with self.lock:
            self._get_on_retry = self._init_on_first_retry
            self._on_retry = value

    def _init_on_first_retry(self) -> tuple[RetryHook, ...]:
        """
        Perform delayed initialization of on_retry hooks.
        """
        with self.lock:
            # Ensure hooks didn't init while waiting for the lock.
            if self._get_on_retry == self._init_on_first_retry:
                if self._on_retry is None:
                    self._on_retry = get_default_hooks()

                self._on_retry = init_hooks(self._on_retry)

                self._get_on_retry = lambda: self._on_retry  # type: ignore[assignment, return-value]

        return self._on_retry  # type: ignore[return-value]


CONFIG = _Config(Lock())


def is_active() -> bool:
    """
    Check whether retrying is active.

    Returns:
        Whether retrying is active.
    """
    return CONFIG.is_active


def set_active(active: bool) -> None:
    """
    Activate or deactivate retrying.

    Is idempotent and can be called repeatedly with the same value.
    """
    CONFIG.is_active = bool(active)


def is_testing() -> bool:
    """
    Check whether test mode is enabled.

    .. versionadded:: 24.3.0
    """
    return CONFIG.testing is not None


class _RestoreTestingCM:
    def __init__(self, old: _Testing | None) -> None:
        self.old = old

    def __enter__(self) -> None:
        pass

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        CONFIG.testing = self.old


def set_testing(
    testing: bool, *, attempts: int = 1, cap: bool = False
) -> _RestoreTestingCM:
    """
    Activate or deactivate test mode.

    In testing mode, backoffs are disabled, and attempts are set to *attempts*.

    If *cap* is True, the number of attempts is not set but capped at
    *attempts*. This means that if *attempts* is greater than the number of
    attempts specified by the user, the user's value is used.

    Is idempotent and can be called repeatedly with the same values.

    .. versionadded:: 24.3.0
    .. versionadded:: 25.1.0 *cap*
    .. versionadded:: 25.1.0 Can be used as a context manager.
    """
    old = CONFIG.testing
    CONFIG.testing = _Testing(attempts, cap) if testing else None

    return _RestoreTestingCM(old)
