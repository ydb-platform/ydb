from typing import Any, Optional, TYPE_CHECKING

from ..cloudpath import CloudPath, NoStatError


if TYPE_CHECKING:
    from .localclient import LocalClient


class LocalPath(CloudPath):
    """Abstract CloudPath for accessing objects the local filesystem. Subclasses are as a
    monkeypatch substitutes for normal CloudPath subclasses when writing tests."""

    client: "LocalClient"

    def is_dir(self, follow_symlinks=True) -> bool:
        return self.client._is_dir(self, follow_symlinks=follow_symlinks)

    def is_file(self, follow_symlinks=True) -> bool:
        return self.client._is_file(self, follow_symlinks=follow_symlinks)

    def stat(self, follow_symlinks=True):
        try:
            meta = self.client._stat(self)
        except FileNotFoundError:
            raise NoStatError(
                f"No stats available for {self}; it may be a directory or not exist."
            )
        return meta

    def touch(self, exist_ok: bool = True, mode: Optional[Any] = None):
        self.client._touch(self, exist_ok)
