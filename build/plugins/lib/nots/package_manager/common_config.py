"""Explicit, source-relative NOTS settings shared by injected modules."""

import logging
import os
import re

try:
    import ymakeyaml as yaml
except ImportError:
    import yaml

logger = logging.getLogger(__name__)

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


def load_common_config(pj, sources_root, inject_peers):
    """Return (Arcadia-relative config path, catalogs); never rewrite the manifest."""
    settings = pj.data.get("nots", {})
    if not isinstance(settings, dict):
        raise ValueError("{}: nots must be a mapping".format(pj.path))
    has_config = "commonConfigPath" in settings
    config_path = settings.get("commonConfigPath")
    if not inject_peers:
        if has_config:
            logger.warning("%s: commonConfigPath requires injected peers; ignoring config", pj.path)
        return None, {}

    catalogs = {}
    relative_path = None
    if has_config:
        if not isinstance(config_path, str) or not config_path or os.path.isabs(config_path):
            raise ValueError("{}: commonConfigPath must be a nonempty relative path".format(pj.path))
        absolute_path = os.path.normpath(os.path.join(os.path.dirname(pj.path), config_path))
        source_root = os.path.abspath(sources_root)
        if os.path.commonpath([source_root, absolute_path]) != source_root or os.path.commonpath(
            [os.path.realpath(source_root), os.path.realpath(absolute_path)]
        ) != os.path.realpath(source_root):
            raise ValueError("{}: commonConfigPath escapes Arcadia: {}".format(pj.path, config_path))
        relative_path = os.path.relpath(absolute_path, source_root)
        try:
            with open(absolute_path) as stream:
                data = yaml.load(stream, Loader=_ConfigLoader)
        except Exception as error:
            raise ValueError("{}: cannot read common config {}: {}".format(pj.path, absolute_path, error)) from error
        if not isinstance(data, dict):
            raise ValueError("{}: {} must contain a mapping".format(pj.path, absolute_path))
        for key in data:
            if key != "catalogs":
                logger.warning("%s: ignoring unknown common config key %s in %s", pj.path, key, absolute_path)
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
    return relative_path, catalogs


def merge_config_sources(target, incoming):
    """Map source file paths to group names; preserve ownership across peer workspaces."""
    for filename, groups in incoming.items():
        for previous, previous_groups in target.items():
            if previous == filename:
                continue
            if os.path.dirname(previous) == os.path.dirname(filename):
                raise ValueError("Conflicting common configs: {} and {}".format(previous, filename))
            overlap = set(groups).intersection(previous_groups)
            if overlap:
                raise ValueError(
                    "Conflicting catalog groups {} in {} and {}".format(sorted(overlap), previous, filename)
                )
        target[filename] = sorted(set(target.get(filename, [])).union(groups))
