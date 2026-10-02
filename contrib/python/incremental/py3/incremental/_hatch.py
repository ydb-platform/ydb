# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

import os
import shlex
from typing import Any, Dict, List, Type, TypedDict

from hatchling.plugin import hookimpl
from hatchling.version.source.plugin.interface import VersionSourceInterface

from incremental import _existing_version, _load_pyproject_toml


class _VersionData(TypedDict):
    version: str


class IncrementalVersionSource(VersionSourceInterface):
    PLUGIN_NAME = "incremental"

    def get_version_data(self) -> _VersionData:  # type: ignore[override]
        path = os.path.join(self.root, "./pyproject.toml")
        config = _load_pyproject_toml(path)
        return {"version": _existing_version(config.version_path).public()}

    def set_version(self, version: str, version_data: Dict[Any, Any]) -> None:
        path = os.path.join(self.root, "./pyproject.toml")  # TODO: #111 Delete this.
        config = _load_pyproject_toml(path)
        raise NotImplementedError(
            f"Run `incremental update {shlex.quote(config.package)} --newversion"
            f" {shlex.quote(version)}` to set the version.\n\n"
            f" See `incremental --help` for more options."
        )


@hookimpl
def hatch_register_version_source() -> List[Type[VersionSourceInterface]]:
    return [IncrementalVersionSource]
