from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


@dataclass
class Errors:
    failures: list = field(default_factory=list)
    freshness_failures_only: bool = True
    stale_last_usable_at: Optional[datetime] = None

    def append(self, msg: str, revinfo: Any, is_freshness_failure=False):
        self.failures.append((msg, revinfo))
        self.freshness_failures_only &= is_freshness_failure

    def update_stale(self, dt: Optional[datetime]):
        if dt is not None:
            self.stale_last_usable_at = (
                dt
                if self.stale_last_usable_at is None
                else max(self.stale_last_usable_at, dt)
            )
