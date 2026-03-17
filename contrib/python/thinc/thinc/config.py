import catalogue
import confection
from confection import VARIABLE_RE, Config, ConfigValidationError, Promise

from .types import Decorator


class registry(confection.registry):
    # fmt: off
    optimizers: Decorator = catalogue.create("thinc", "optimizers", entry_points=True)
    schedules: Decorator = catalogue.create("thinc", "schedules", entry_points=True)
    layers: Decorator = catalogue.create("thinc", "layers", entry_points=True)
    losses: Decorator = catalogue.create("thinc", "losses", entry_points=True)
    initializers: Decorator = catalogue.create("thinc", "initializers", entry_points=True)
    datasets: Decorator = catalogue.create("thinc", "datasets", entry_points=True)
    ops: Decorator = catalogue.create("thinc", "ops", entry_points=True)
    # fmt: on

    @classmethod
    def create(cls, registry_name: str, entry_points: bool = False) -> None:
        """Create a new custom registry."""
        if hasattr(cls, registry_name):
            raise ValueError(f"Registry '{registry_name}' already exists")
        reg: Decorator = catalogue.create(
            "thinc", registry_name, entry_points=entry_points
        )
        setattr(cls, registry_name, reg)


__all__ = ["Config", "registry", "ConfigValidationError", "Promise", "VARIABLE_RE"]
