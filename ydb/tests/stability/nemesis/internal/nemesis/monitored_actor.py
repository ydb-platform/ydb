"""Agent-side chaos actor base (metrics via tools ``AbstractMonitoredNemesis``)."""

from __future__ import annotations

import logging

from ydb.tests.tools.nemesis.library import base

NEMESIS_EXECUTION_LOGGER = "ydb.tests.stability.nemesis.execution"


class MonitoredAgentActor(base.AbstractMonitoredNemesis):
    """
    Agent-side execution only: AbstractMonitoredNemesis counters.
    Planning stays on the orchestrator (NemesisPlannerBase subclasses).
    """

    def __init__(self, scope: str = "node") -> None:
        base.AbstractMonitoredNemesis.__init__(self, scope=scope)
        self._logger = logging.getLogger(NEMESIS_EXECUTION_LOGGER)
        self._logger.setLevel(logging.DEBUG)

    @property
    def nemesis_description(self) -> str:
        return self.__class__.__doc__ or ""
