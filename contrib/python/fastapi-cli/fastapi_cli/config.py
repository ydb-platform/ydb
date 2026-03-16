import logging
from pathlib import Path
from typing import Any

from pydantic import BaseModel, StrictStr

logger = logging.getLogger(__name__)


class FastAPIConfig(BaseModel):
    entrypoint: StrictStr | None = None

    @classmethod
    def _read_pyproject_toml(cls) -> dict[str, Any]:
        """Read FastAPI configuration from pyproject.toml in current directory."""
        pyproject_path = Path.cwd() / "pyproject.toml"

        if not pyproject_path.exists():
            return {}

        try:
            import tomllib  # type: ignore[import-not-found, unused-ignore]
        except ImportError:
            try:
                import tomli as tomllib  # type: ignore[no-redef, import-not-found, unused-ignore]
            except ImportError:  # pragma: no cover
                logger.debug("tomli not available, skipping pyproject.toml")
                return {}

        with open(pyproject_path, "rb") as f:
            data = tomllib.load(f)

            return data.get("tool", {}).get("fastapi", {})  # type: ignore

    @classmethod
    def resolve(cls, entrypoint: str | None = None) -> "FastAPIConfig":
        config = cls._read_pyproject_toml()

        if entrypoint is not None:
            config["entrypoint"] = entrypoint

        return cls.model_validate(config)
