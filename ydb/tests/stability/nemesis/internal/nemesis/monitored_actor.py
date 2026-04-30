"""Agent-side chaos actor base (metrics via tools ``AbstractMonitoredNemesis``)."""

from __future__ import annotations

import logging
from typing import ClassVar

from ydb.tests.tools.nemesis.library import base

NEMESIS_EXECUTION_LOGGER = "ydb.tests.stability.nemesis.execution"


class MonitoredAgentActor(base.AbstractMonitoredNemesis):
    """
    Agent-side execution only: AbstractMonitoredNemesis counters.
    Planning stays on the orchestrator (NemesisPlannerBase subclasses).

    Each subclass gets a child logger ``<NEMESIS_EXECUTION_LOGGER>.<ClassName>``
    so that log records carry the concrete runner class name.  Python logging
    propagation ensures the ``ThreadLocalHandler`` attached to the parent
    ``NEMESIS_EXECUTION_LOGGER`` in :class:`NemesisManager` still captures
    every record.
    """

    # True if the runner can safely run inside a single-host local cluster
    # (e.g. ydb/tests/tools/local_cluster). Default is False — opt-in only,
    # because many runners rely on systemd, /Berkanavt paths, real disks,
    # multi-host networking, or destructive system-wide operations.
    supports_local_mode: ClassVar[bool] = False

    def __init__(self, scope: str = "node") -> None:
        base.AbstractMonitoredNemesis.__init__(self, scope=scope)
        self._logger = logging.getLogger(
            f"{NEMESIS_EXECUTION_LOGGER}.{self.__class__.__name__}"
        )
        self._logger.setLevel(logging.DEBUG)

    @property
    def nemesis_description(self) -> str:
        return self.__class__.__doc__ or ""
