"""Explicit, source-relative NOTS settings shared by injected modules."""

import copy
import logging
import os
import re

try:
    import ymakeyaml as yaml
except ImportError:
    import yaml

logger = logging.getLogger(__name__)
_reported_ignored_fields = set()

# npm range syntax: partial versions, comparators, unions and hyphen ranges.
_NUM = r"(?:0|[1-9][0-9]*)"
_PART = r"(?:" + _NUM + r"|[xX*])"
_IDENT = r"(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)"
_BUILD = r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?"
_VERSION = (
    r"v?(?:"
    + _NUM
    + r"\."
    + _NUM
    + r"\."
    + _NUM
    + r"(?:-"
    + _IDENT
    + r"(?:\."
    + _IDENT
    + r")*)?|"
    + _PART
    + r"(?:\."
    + _PART
    + r"){0,2})"
    + _BUILD
)
_COMPARATOR = r"(?:[<>]=?|=|~>?|\^)?\s*" + _VERSION
_RANGE = re.compile(r"(?:" + _VERSION + r"\s+-\s+" + _VERSION + r"|" + _COMPARATOR + r"(?:\s+" + _COMPARATOR + r")*)")


def is_version_range(value):
    return (
        isinstance(value, str)
        and bool(value.strip())
        and all(_RANGE.fullmatch(part.strip()) for part in value.split("||"))
    )


# ymake exposes a fixed native loader; builder uses PyYAML and checks duplicate keys.
_ConfigLoader = yaml.CSafeLoader
if yaml.__name__ != "ymakeyaml":

    class _UniqueKeyLoader(yaml.CSafeLoader):
        def construct_mapping(self, node, deep=False):
            seen = set()
            for key, _ in node.value:
                if key.id == "scalar" and key.tag != "tag:yaml.org,2002:merge":
                    value = self.construct_object(key, deep=deep)
                    if value in seen:
                        raise ValueError("Duplicate YAML key {!r} at line {}".format(value, key.start_mark.line + 1))
                    seen.add(value)
            return super().construct_mapping(node, deep=deep)

    _ConfigLoader = _UniqueKeyLoader


def load_common_config(pj, sources_root, include_settings=False):
    """Return (Arcadia-relative config path, catalogs); never rewrite the manifest."""
    settings = pj.data.get("nots", {})
    if not isinstance(settings, dict):
        raise ValueError("{}: nots must be a mapping".format(pj.path))
    has_config = "commonConfigPath" in settings
    config_path = settings.get("commonConfigPath")

    catalogs = {}
    settings = {}
    relative_path = None
    if has_config:
        if not isinstance(config_path, str) or not config_path or os.path.isabs(config_path):
            raise ValueError("{}: commonConfigPath must be a nonempty relative path".format(pj.path))
        module_directory = os.path.abspath(os.path.dirname(pj.path))
        absolute_path = os.path.normpath(os.path.join(module_directory, config_path))
        source_root = os.path.abspath(sources_root)
        if os.path.commonpath([source_root, absolute_path]) != source_root:
            raise ValueError("{}: commonConfigPath escapes Arcadia: {}".format(pj.path, config_path))
        config_directory = os.path.dirname(absolute_path)
        if (
            os.path.commonpath([source_root, config_directory]) != source_root
            or os.path.commonpath([module_directory, config_directory]) != config_directory
        ):
            raise ValueError(
                "{}: commonConfigPath must be in the module directory or a parent directory: {}".format(
                    pj.path, config_path
                )
            )
        current = source_root
        for component in os.path.relpath(absolute_path, source_root).split(os.sep):
            current = os.path.join(current, component)
            if os.path.islink(current):
                raise ValueError("{}: commonConfigPath must not contain symlinks: {}".format(pj.path, config_path))
        relative_path = os.path.relpath(absolute_path, source_root)
        try:
            with open(absolute_path) as stream:
                data = yaml.load(stream, Loader=_ConfigLoader)
        except Exception as error:
            raise ValueError("{}: cannot read common config {}: {}".format(pj.path, absolute_path, error)) from error
        if not isinstance(data, dict):
            raise ValueError("{}: {} must contain a mapping".format(pj.path, absolute_path))
        ignored = [str(key) for key in data if key not in ("catalogs",) + PNPM_SETTINGS]
        warning_key = (pj.path, absolute_path, tuple(ignored))
        if ignored and warning_key not in _reported_ignored_fields:
            _reported_ignored_fields.add(warning_key)
            logger.warning("%s: ignoring common config fields: %s", pj.path, ", ".join(ignored))
        settings = validate_pnpm_settings(data)
        validate_config_paths(settings, os.path.dirname(absolute_path), source_root)
        catalogs = data.get("catalogs", {})
        if not isinstance(catalogs, dict):
            raise ValueError("{}: catalogs must be a mapping in {}".format(pj.path, absolute_path))
        directory = os.path.dirname(relative_path).replace(os.sep, "/")
        prefix = directory + "/" if directory else ""
        for group, entries in catalogs.items():
            if (
                not isinstance(group, str)
                or not group.startswith(prefix)
                or not group[len(prefix) :]
                or group == "default"
            ):
                raise ValueError(
                    "{}: invalid catalog group {!r} in {}; expected prefix {!r}".format(
                        pj.path, group, absolute_path, prefix
                    )
                )
            if not isinstance(entries, dict):
                raise ValueError("{}: catalog {} must be a mapping in {}".format(pj.path, group, absolute_path))
            for name, value in entries.items():
                if not isinstance(name, str) or not name or not is_version_range(value):
                    raise ValueError(
                        "{}: invalid catalog entry {} / {!r}: {!r} in {}".format(
                            pj.path, group, name, value, absolute_path
                        )
                    )

    for name, spec in pj.dependencies_iter():
        if isinstance(spec, str) and spec.startswith("catalog:"):
            group = spec[len("catalog:") :]
            if not group or group == "default" or name not in catalogs.get(group, {}):
                raise ValueError(
                    "{}: unresolved {} for {} in common config {}".format(pj.path, spec, name, relative_path)
                )
    return (relative_path, catalogs, settings) if include_settings else (relative_path, catalogs)


def load_tier0_settings():
    # The builder embeds the policy; ymake plugins read the same source file.
    try:
        import library.python.resource as resource

        content = resource.find("nots/tier0-settings.yaml")
    except ImportError:
        content = None
    if content is None:
        policy_path = os.path.join(
            os.path.dirname(__file__),
            "../../../../../devtools/frontend_build_platform/nots/constants/src/tier0-settings.yaml",
        )
        with open(policy_path) as policy_file:
            return yaml.load(policy_file, Loader=yaml.CSafeLoader)
    return yaml.load(content, Loader=yaml.CSafeLoader)


PNPM_SETTING_TYPES = load_tier0_settings()["pnpm"]["supportedCustomSettings"]
PNPM_SETTINGS = tuple(key for key in PNPM_SETTING_TYPES if key != "catalogs")


def select_pnpm_settings(source):
    """Copy only supported settings; unknown pnpm options have no effect in NOTS."""
    if not isinstance(source, dict):
        return {}
    return validate_pnpm_settings(source)


def validate_pnpm_settings(data):
    settings = {}
    for key in PNPM_SETTINGS:
        if key not in data:
            continue
        entries = data[key]
        if PNPM_SETTING_TYPES[key] == "boolean":
            if not isinstance(entries, bool):
                raise ValueError("{} must be a boolean".format(key))
            settings[key] = entries
            continue
        if PNPM_SETTING_TYPES[key] == "non-negative-integer":
            if not isinstance(entries, int) or isinstance(entries, bool) or not 0 <= entries <= 9007199254740991:
                raise ValueError("{} must be a non-negative integer".format(key))
            settings[key] = entries
            continue
        if PNPM_SETTING_TYPES[key] == "string-list":
            if not isinstance(entries, list) or any(not isinstance(entry, str) for entry in entries):
                raise ValueError("{} must be a list of strings".format(key))
            settings[key] = copy.deepcopy(entries)
            continue
        if not isinstance(entries, dict):
            raise ValueError("{} must be a mapping".format(key))
        for name, value in entries.items():
            if not isinstance(name, str) or not name:
                raise ValueError("invalid {} selector".format(key))
            if PNPM_SETTING_TYPES[key] == "peer-rules":
                if name in ("ignoreMissing", "allowAny"):
                    if not isinstance(value, list) or any(not isinstance(v, str) for v in value):
                        raise ValueError("invalid peerDependencyRules {}".format(name))
                elif name == "allowedVersions":
                    if not isinstance(value, dict) or any(
                        not isinstance(k, str) or not isinstance(v, str) for k, v in value.items()
                    ):
                        raise ValueError("invalid allowedVersions")
                else:
                    raise ValueError("invalid peerDependencyRules {}".format(name))
                continue
            if PNPM_SETTING_TYPES[key] != "package-extensions":
                if not isinstance(value, str) or not value:
                    raise ValueError("invalid {} value for {}".format(key, name))
                continue
            if not isinstance(value, dict):
                raise ValueError("package extension must be a mapping")
            for section, dependencies in value.items():
                if section not in (
                    "dependencies",
                    "optionalDependencies",
                    "peerDependencies",
                    "peerDependenciesMeta",
                ) or not isinstance(dependencies, dict):
                    raise ValueError("invalid package extension section: {}".format(section))
                for dep, spec in dependencies.items():
                    if not isinstance(dep, str) or not dep:
                        raise ValueError("invalid extension dependency")
                    if section == "peerDependenciesMeta":
                        if not isinstance(spec, dict) or any(
                            k != "optional" or not isinstance(v, bool) for k, v in spec.items()
                        ):
                            raise ValueError("invalid peerDependenciesMeta")
                    elif not isinstance(spec, str) or not spec:
                        raise ValueError("invalid extension dependency specifier")
        settings[key] = copy.deepcopy(entries)
    return settings


def rebase_pnpm_settings(settings, source_dir, target_dir):
    result = copy.deepcopy(settings)

    def rebase(value, patch=False):
        match = re.match(r"^(workspace:|file:|link:)(\.\.?/.*)$", value)
        if not patch and not match:
            return value
        relative = os.path.relpath(
            os.path.normpath(os.path.join(source_dir, value if patch else match[2])), target_dir
        ).replace(os.sep, "/")
        return relative if patch else match[1] + (relative if relative.startswith(".") else "./" + relative)

    for key in PNPM_SETTINGS:
        if PNPM_SETTING_TYPES[key] not in ("dependency-map", "patch-map", "package-extensions"):
            continue
        for name, value in result.get(key, {}).items():
            if PNPM_SETTING_TYPES[key] == "package-extensions":
                for section, dependencies in value.items():
                    if section != "peerDependenciesMeta":
                        value[section] = {dep: rebase(spec) for dep, spec in dependencies.items()}
            else:
                result[key][name] = rebase(value, PNPM_SETTING_TYPES[key] == "patch-map")
    return result


def workspace_settings_paths(settings):
    specs = list(settings.get("overrides", {}).values())
    for extension in settings.get("packageExtensions", {}).values():
        for section in ("dependencies", "optionalDependencies", "peerDependencies"):
            specs.extend(extension.get(section, {}).values())
    return [spec[len("workspace:") :] for spec in specs if re.match(r"^workspace:\.\.?/", spec)]


def validate_config_paths(settings, directory, root):
    paths = list(settings.get("patchedDependencies", {}).values()) + workspace_settings_paths(settings)
    for value in paths:
        absolute = os.path.normpath(os.path.join(directory, value))
        if (
            os.path.isabs(value)
            or os.path.commonpath([root, absolute]) != root
            or os.path.commonpath([os.path.realpath(root), os.path.realpath(absolute)]) != os.path.realpath(root)
        ):
            raise ValueError("common config path escapes Arcadia: {}".format(value))


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
