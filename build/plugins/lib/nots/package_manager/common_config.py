"""Explicit, source-relative NOTS settings shared by injected modules."""

import copy
import os
import re
from functools import lru_cache

try:
    import ymakeyaml as yaml
except ImportError:
    import yaml

T0_SETTINGS_PATH = "devtools/frontend_build_platform/nots/constants/src/tier0-settings.yaml"


def load_common_config(pj, sources_root, include_settings=False):
    """Read graph inputs; semantic validation is performed by CLI prepare."""
    config_path = pj.data.get("nots", {}).get("commonConfigPath")
    catalogs = {}
    settings = {}
    relative_path = None
    if config_path is not None:
        absolute_path = os.path.normpath(os.path.join(os.path.dirname(pj.path), config_path))
        relative_path = os.path.relpath(absolute_path, sources_root)
        with open(absolute_path) as stream:
            data = yaml.load(stream, Loader=yaml.CSafeLoader)
        settings = select_pnpm_settings(data)
        catalogs = data.get("catalogs", {})
    return (relative_path, catalogs, settings) if include_settings else (relative_path, catalogs)


_tier0_sources_root = None


def set_tier0_sources_root(sources_root):
    global _tier0_sources_root
    _tier0_sources_root = sources_root


def load_tier0_settings():
    sources_root = _tier0_sources_root
    if sources_root is None:
        # Plugins run from sources; Python tests supply the source root via yatest.
        sources_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../../"))
        if not os.path.isfile(os.path.join(sources_root, T0_SETTINGS_PATH)):
            import yatest.common

            sources_root = yatest.common.source_path()
    return _read_tier0_settings(os.path.join(sources_root, T0_SETTINGS_PATH))


@lru_cache(maxsize=None)
def _read_tier0_settings(policy_path):
    with open(policy_path) as policy_file:
        return yaml.load(policy_file, Loader=yaml.CSafeLoader)


def pnpm_setting_types():
    return load_tier0_settings()["pnpm"]["supportedCustomSettings"]


def pnpm_settings():
    return tuple(key for key in pnpm_setting_types() if key != "catalogs")


def select_pnpm_settings(source):
    """Copy only supported settings; unknown pnpm options have no effect in NOTS."""
    if not isinstance(source, dict):
        return {}
    return {key: copy.deepcopy(source[key]) for key in pnpm_settings() if key in source}


def rebase_pnpm_settings(settings, source_dir, target_dir):
    result = copy.deepcopy(settings)
    if os.path.abspath(source_dir) == os.path.abspath(target_dir):
        return result

    def rebase(value, patch=False):
        match = re.match(r"^(workspace:|file:|link:)(.*)$", value)
        if not patch and (
            not match or os.path.isabs(match[2]) or (match[1] == "workspace:" and not re.match(r"^\.\.?/", match[2]))
        ):
            return value
        relative = os.path.relpath(
            os.path.normpath(os.path.join(source_dir, value if patch else match[2])), target_dir
        ).replace(os.sep, "/")
        return relative if patch else match[1] + (relative if relative.startswith(".") else "./" + relative)

    for key in pnpm_settings():
        if pnpm_setting_types()[key] not in ("dependency-map", "patch-map", "package-extensions"):
            continue
        for name, value in result.get(key, {}).items():
            if pnpm_setting_types()[key] == "package-extensions":
                for section, dependencies in value.items():
                    if section != "peerDependenciesMeta":
                        value[section] = {dep: rebase(spec) for dep, spec in dependencies.items()}
            else:
                result[key][name] = rebase(value, pnpm_setting_types()[key] == "patch-map")
    return result


def workspace_settings_paths(settings):
    overrides = list(settings.get("overrides", {}).values())
    specs = list(overrides)
    for extension in settings.get("packageExtensions", {}).values():
        for section in ("dependencies", "optionalDependencies", "peerDependencies"):
            specs.extend(extension.get(section, {}).values())
    paths = [spec[len("workspace:") :] for spec in specs if re.match(r"^workspace:\.\.?/", spec)]
    for spec in overrides:
        if not spec.startswith("file:"):
            continue
        paths.append(spec[len("file:") :])
    return paths


def common_config_inputs(pj, sources_root):
    """Only the target's explicit config and effective patches are build inputs."""
    inputs = set()
    filename, _, settings = load_common_config(pj, sources_root, include_settings=True)
    directory = os.path.dirname(pj.path)
    if filename:
        inputs.add(filename)
        settings = rebase_pnpm_settings(settings, os.path.join(sources_root, os.path.dirname(filename)), directory)
    settings = join_pnpm_settings(settings, select_pnpm_settings(pj.data.get("pnpm")))
    inputs.update(
        os.path.relpath(os.path.normpath(os.path.join(directory, p)), sources_root)
        for p in settings.get("patchedDependencies", {}).values()
    )
    return sorted(inputs)


def join_pnpm_settings(common, target):
    """Join dependency maps recursively; target leaves win and rule lists combine."""
    if isinstance(common, dict) and isinstance(target, dict):
        result = copy.deepcopy(common)
        for key, value in target.items():
            result[key] = join_pnpm_settings(result.get(key), value)
        return result
    if isinstance(common, list) and isinstance(target, list):
        return list(dict.fromkeys(common + target))
    return copy.deepcopy(target)
