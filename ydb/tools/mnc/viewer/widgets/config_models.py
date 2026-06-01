import os
from dataclasses import dataclass
from typing import Optional

import yaml

import ydb.tools.mnc.scheme as mnc_scheme


@dataclass
class ConfigCandidate:
    name: str
    path: str


def _config_candidate_name_from_path(path: str) -> str:
    name = os.path.basename(path)
    base, ext = os.path.splitext(name)
    return base if ext else name


def _config_path_for_name(candidate: ConfigCandidate, name: str) -> str:
    name = name.strip()
    base_name = os.path.basename(name)
    base, ext = os.path.splitext(base_name)
    if not ext:
        ext = os.path.splitext(candidate.path)[1] or ".yaml"
        base_name = base_name + ext
    return os.path.join(os.path.dirname(candidate.path), base_name)


@dataclass
class ConfigValidation:
    errors: list[str]
    config: Optional[dict] = None

    @property
    def ok(self) -> bool:
        return not self.errors


def _validate_multinode_config(path: str) -> ConfigValidation:
    try:
        with open(path) as file:
            config = yaml.safe_load(file)
    except Exception as error:
        return ConfigValidation([f"Failed to parse config: {error}"])

    try:
        validated, errors = mnc_scheme.apply_scheme(config, mnc_scheme.multinode.scheme)
    except Exception as error:
        return ConfigValidation([f"Failed to validate config: {error}"])

    if validated is None:
        return ConfigValidation(errors or ["Config does not match multinode scheme"])
    return ConfigValidation([], validated)


@dataclass
class SelectedClusterConfig:
    candidate: ConfigCandidate
    validation: ConfigValidation
