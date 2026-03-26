"""
Process-wide mutable state set at app startup (avoids circular imports with routers).
"""

from __future__ import annotations

from typing import Any

# Set from app.initialize_app (orchestrator mode).
healthcheck_reporter: Any = None
