"""Argparse action classes for argclass."""

import argparse
import json
import logging
from argparse import Action
from pathlib import Path
from types import MappingProxyType
from typing import Any, Iterable, List, Mapping, Optional, Sequence, Union

try:
    import tomllib

    toml_load = tomllib.load
except ImportError:  # pragma: no cover
    try:
        import tomli  # type: ignore[import-not-found]

        toml_load = tomli.load  # type: ignore[assignment]
    except ImportError:
        toml_load = None  # type: ignore[assignment]

from .utils import read_ini_configs


class ConfigAction(Action):
    """Base action for loading configuration files."""

    def __init__(
        self,
        option_strings: Sequence[str],
        dest: str,
        search_paths: Iterable[Union[str, Path]] = (),
        type: MappingProxyType = MappingProxyType({}),
        help: str = "",
        required: bool = False,
        default: Any = None,
    ):
        if not isinstance(type, MappingProxyType):
            raise ValueError("type must be MappingProxyType")

        super().__init__(
            option_strings,
            dest,
            type=Path,
            help=help,
            default=default,
            required=required,
        )
        self.search_paths: List[Path] = list(map(Path, search_paths))
        self._result: Optional[Any] = None

    def parse(self, *files: Path) -> Any:
        result = {}
        for file in files:
            try:
                result.update(self.parse_file(file))
            except Exception as e:
                logging.warning("Failed to parse config file %s: %s", file, e)
        return result

    def parse_file(self, file: Path) -> Any:
        raise NotImplementedError()

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Optional[Union[str, Any]],
        option_string: Optional[str] = None,
    ) -> None:
        if not self._result:
            filenames: Sequence[Path] = list(self.search_paths)
            if values:
                filenames = [Path(values)] + list(filenames)
            filenames = list(filter(lambda x: x.exists(), filenames))

            if self.required and not filenames:
                raise argparse.ArgumentError(
                    argument=self,
                    message="is required but no one config loaded",
                )
            if filenames:
                self._result = self.parse(*filenames)
        setattr(namespace, self.dest, MappingProxyType(self._result or {}))


class INIConfigAction(ConfigAction):
    """Action for loading INI configuration files."""

    def parse(self, *files: Path) -> Mapping[str, Any]:
        result, filenames = read_ini_configs(*files)
        return result


class JSONConfigAction(ConfigAction):
    """Action for loading JSON configuration files."""

    def parse_file(self, file: Path) -> Any:
        with file.open("r") as fp:
            return json.load(fp)


class TOMLConfigAction(ConfigAction):
    """Action for loading TOML configuration files.

    Uses stdlib tomllib (Python 3.11+) or tomli package as fallback.
    """

    def parse_file(self, file: Path) -> Any:
        if toml_load is None:
            raise RuntimeError(
                "TOML support requires Python 3.11+ (tomllib) "
                "or 'tomli' package: pip install tomli"
            )
        with file.open("rb") as fp:
            return toml_load(fp)
