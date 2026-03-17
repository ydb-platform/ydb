# SPDX-FileCopyrightText: 2022 Hynek Schlawack <hs@ox.cx>
#
# SPDX-License-Identifier: MIT

from __future__ import annotations

from typing import TYPE_CHECKING

from ._data import RetryDetails, RetryHook, RetryHookFactory, guess_name


if TYPE_CHECKING:
    from prometheus_client import Counter


RETRIES_TOTAL = None


def init_prometheus() -> RetryHook:
    """
    Initialize Prometheus instrumentation.
    """
    from prometheus_client import Counter

    global RETRIES_TOTAL  # noqa: PLW0603

    # Mostly for testing so we can call init_prometheus more than once.
    if RETRIES_TOTAL is None:
        RETRIES_TOTAL = Counter(
            "stamina_retries_total",
            "Total number of retries.",
            ("callable", "retry_num", "error_type"),
        )

    def count_retries(details: RetryDetails) -> None:
        """
        Count and log retries for callable *name*.
        """
        RETRIES_TOTAL.labels(
            callable=details.name,
            retry_num=details.retry_num,
            error_type=guess_name(details.caused_by.__class__),
        ).inc()

    return count_retries


def get_prometheus_counter() -> Counter | None:
    """
    Return the Prometheus counter for the number of retries.

    Returns:
        If active, the Prometheus `counter
        <https://github.com/prometheus/client_python>`_ for the number of
        retries. None otherwise.

    .. versionadded:: 23.2.0
    """
    from . import get_on_retry_hooks

    # Finalize the hooks if not done yet.
    get_on_retry_hooks()

    return RETRIES_TOTAL


PrometheusOnRetryHook = RetryHookFactory(init_prometheus)
