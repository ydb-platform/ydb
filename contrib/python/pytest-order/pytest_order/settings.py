from enum import Enum
from warnings import warn

from _pytest.config import Config


class Scope(Enum):
    CLASS = 1
    MODULE = 2
    SESSION = 3


class Settings:
    """Holds all configuration settings."""

    valid_scopes = {
        "class": Scope.CLASS,
        "module": Scope.MODULE,
        "session": Scope.SESSION,
    }

    def __init__(self, config: Config) -> None:
        self.sparse_ordering: bool = config.getoption("sparse_ordering")
        self.order_dependencies: bool = config.getoption("order_dependencies")
        self.marker_prefix: str = config.getoption("order_marker_prefix")
        self.error_on_failed_ordering: str = config.getoption(
            "error_on_failed_ordering"
        )
        scope: str = config.getoption("order_scope")
        if scope in self.valid_scopes:
            self.scope: Scope = self.valid_scopes[scope]
        else:
            if scope is not None:
                warn(
                    f"Unknown order scope '{scope}', ignoring it. "
                    "Valid scopes are 'session', 'module' and 'class'."
                )
            self.scope = Scope.SESSION
        scope_level: int = config.getoption("order_scope_level") or 0
        if scope_level != 0 and self.scope != Scope.SESSION:
            warn(
                f"order-scope-level cannot be used together with --order-scope={scope}"
            )
            scope_level = 0
        self.scope_level: int = scope_level
        group_scope: str = config.getoption("order_group_scope")
        if group_scope in self.valid_scopes:
            self.group_scope: Scope = self.valid_scopes[group_scope]
        else:
            if group_scope is not None:
                warn(
                    f"Unknown order group scope '{group_scope}', ignoring it. "
                    "Valid scopes are 'session', 'module' and 'class'."
                )
            self.group_scope = self.scope
        if self.group_scope.value > self.scope.value:
            warn("Group scope is larger than order scope, ignoring it.")
            self.group_scope = self.scope
        try:
            auto_mark_dep = config.getini("automark_dependency")
            if isinstance(auto_mark_dep, str):
                auto_mark_dep = auto_mark_dep.lower() in (
                    "1",
                    "yes",
                    "y",
                    "true",
                    "t",
                    "on",
                )
        except ValueError:
            auto_mark_dep = False
        self.auto_mark_dep = auto_mark_dep
