from __future__ import annotations

import os
import warnings
from itertools import chain
from typing import Any

from fsspec.asyn import sync

from upath._compat import FSSpecAccessorShim as _FSSpecAccessorShim
from upath._stat import UPathStatResult
from upath.core import UPath

__all__ = ["HTTPPath"]

# accessors are deprecated
_HTTPAccessor = _FSSpecAccessorShim


class HTTPPath(UPath):

    @classmethod
    def _transform_init_args(
        cls,
        args: tuple[str | os.PathLike, ...],
        protocol: str,
        storage_options: dict[str, Any],
    ) -> tuple[tuple[str | os.PathLike, ...], str, dict[str, Any]]:
        # allow initialization via a path argument and protocol keyword
        if args and not str(args[0]).startswith(protocol):
            args = (f"{protocol}://{str(args[0]).lstrip('/')}", *args[1:])
        return args, protocol, storage_options

    @property
    def root(self) -> str:  # type: ignore[override]
        return super().root or "/"

    def __str__(self):
        return super(UPath, self).__str__()

    def is_file(self):
        try:
            next(super().iterdir())
        except (StopIteration, NotADirectoryError):
            return True
        except FileNotFoundError:
            return False
        else:
            return False

    def is_dir(self):
        try:
            next(super().iterdir())
        except (StopIteration, NotADirectoryError):
            return False
        except FileNotFoundError:
            return False
        else:
            return True

    def stat(self, follow_symlinks: bool = True):
        if not follow_symlinks:
            warnings.warn(
                f"{type(self).__name__}.stat(follow_symlinks=False):"
                " is currently ignored.",
                UserWarning,
                stacklevel=2,
            )
        info = self.fs.info(self.path)
        if "url" in info:
            info["type"] = "directory" if info["url"].endswith("/") else "file"
        return UPathStatResult.from_info(info)

    def iterdir(self):
        if self.parts[-1:] == ("",):
            yield from self.parent.iterdir()
        else:
            it = iter(super().iterdir())
            try:
                item0 = next(it)
            except (StopIteration, NotADirectoryError):
                raise NotADirectoryError(str(self))
            except FileNotFoundError:
                raise FileNotFoundError(str(self))
            else:
                yield from chain([item0], it)

    def resolve(
        self: HTTPPath,
        strict: bool = False,
        follow_redirects: bool = True,
    ) -> HTTPPath:
        """Normalize the path and resolve redirects."""
        # Normalise the path
        resolved_path = super().resolve(strict=strict)
        # if the last part is "..", then it's a directory
        if self.parts[-1:] == ("..",):
            resolved_path = resolved_path.joinpath("")

        if follow_redirects:
            # Get the fsspec fs
            fs = self.fs
            url = str(self)
            # Ensure we have a session
            session = sync(fs.loop, fs.set_session)
            # Use HEAD requests if the server allows it, falling back to GETs
            for method in (session.head, session.get):
                r = sync(fs.loop, method, url, allow_redirects=True)
                try:
                    r.raise_for_status()
                except Exception as exc:
                    if method == session.get:
                        raise FileNotFoundError(self) from exc
                else:
                    resolved_path = HTTPPath(str(r.url))
                    break

        return resolved_path
