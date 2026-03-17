from typing import Any


class DishkaError(Exception):
    pass


class InvalidMarkerError(DishkaError):
    def __init__(self, marker: Any) -> None:
        self.marker = marker
        self.source_name: str | None = None

    def __str__(self) -> str:
        msg = f"Cannot use {self.marker!r} as marker."
        if self.source_name:
            msg += f"\nUsed in: {self.source_name}"
        return msg
