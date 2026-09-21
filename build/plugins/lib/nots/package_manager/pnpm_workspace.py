import copy
import os

from .common_config import merge_config_sources

try:
    import ymakeyaml as yaml
except Exception:
    import yaml


def _as_string_list(value):
    return value if isinstance(value, list) else []


def _pnpm_workspace_settings(package_json):
    """Adapt package.json#pnpm settings to the pnpm 11 workspace schema.

    The generated package.json remains unchanged for pnpm 10 compatibility.
    """
    source = package_json.data.get("pnpm") or {}
    if not isinstance(source, dict):
        return {}

    settings = copy.deepcopy(source)

    explicit_allow_builds = settings.pop("allowBuilds", {})
    allow_builds = {}
    for package_name in _as_string_list(settings.pop("onlyBuiltDependencies", [])):
        allow_builds[package_name] = True
    for key in ("neverBuiltDependencies", "ignoredBuiltDependencies"):
        for package_name in _as_string_list(settings.pop(key, [])):
            allow_builds[package_name] = False
    if isinstance(explicit_allow_builds, dict):
        allow_builds.update(explicit_allow_builds)
    settings.pop("onlyBuiltDependenciesFile", None)
    settings.pop("ignoreDepScripts", None)
    if allow_builds:
        settings["allowBuilds"] = allow_builds
    else:
        settings.pop("allowBuilds", None)

    if "allowNonAppliedPatches" in settings:
        settings.setdefault("allowUnusedPatches", settings.pop("allowNonAppliedPatches"))
    settings.pop("ignorePatchFailures", None)

    pm_on_fail = settings.get("pmOnFail")
    manage_versions = settings.pop("managePackageManagerVersions", None)
    package_manager_strict = settings.pop("packageManagerStrict", None)
    package_manager_strict_version = settings.pop("packageManagerStrictVersion", None)
    if pm_on_fail is None:
        if package_manager_strict_version is True:
            pm_on_fail = "error"
        elif package_manager_strict is False:
            pm_on_fail = "warn"
        elif manage_versions is not None:
            pm_on_fail = "download" if manage_versions else "ignore"
    if pm_on_fail is not None:
        settings["pmOnFail"] = pm_on_fail

    settings.pop("useNodeVersion", None)
    execution_env = settings.get("executionEnv")
    if isinstance(execution_env, dict):
        execution_env.pop("nodeVersion", None)
        if not execution_env:
            settings.pop("executionEnv")

    audit_config = settings.get("auditConfig")
    if isinstance(audit_config, dict):
        # CVE identifiers cannot be mechanically converted to GHSA identifiers.
        audit_config.pop("ignoreCves", None)
        if not audit_config:
            settings.pop("auditConfig")

    return settings


class PnpmWorkspace(object):
    @classmethod
    def load(cls, path):
        ws = cls(path)
        ws.read()

        return ws

    def __init__(self, path):
        if not os.path.isabs(path):
            raise TypeError("Absolute path required, given: {}".format(path))

        self.path = path
        # NOTE: pnpm requires relative workspace paths.
        self.packages = set()
        self.catalogs = {}
        self.common_config_sources = {}
        self.settings = {}

    def read(self):
        with open(self.path) as f:
            parsed = yaml.load(f, Loader=yaml.CSafeLoader) or {}
            self.packages = set(parsed.get("packages", []))
            self.catalogs = parsed.get("catalogs", {})
            self.common_config_sources = parsed.get("notsCommonConfigSources", {})
            self.settings = {
                key: value
                for key, value in parsed.items()
                if key not in ("packages", "catalogs", "notsCommonConfigSources")
            }

    def write(self, path=None):
        if not path:
            path = self.path

        with open(path, "w") as f:
            data = copy.deepcopy(self.settings)
            data["packages"] = sorted(self.packages)
            if self.catalogs:
                data["catalogs"] = self.catalogs
            if self.common_config_sources:
                data["notsCommonConfigSources"] = self.common_config_sources
            yaml.dump(data, f, Dumper=yaml.CSafeDumper)

    def get_paths(self, base_path=None, ignore_self=False):
        """
        Returns absolute paths of the workspace packages.
        :param base_path: base path to resolve relative dep paths
        :type base_path: str
        :param ignore_self: whether path of the current module will be excluded (if present)
        :type ignore_self: bool
        :rtype: list of str
        """
        if base_path is None:
            base_path = os.path.dirname(self.path)

        return [
            os.path.normpath(os.path.join(base_path, pkg_path))
            for pkg_path in self.packages
            if not ignore_self or pkg_path != "."
        ]

    def set_from_package_json(self, package_json):
        """
        Sets packages to "workspace" deps from given package.json.
        :param package_json: package.json of workspace
        :type package_json: PackageJson
        """
        if os.path.dirname(package_json.path) != os.path.dirname(self.path):
            raise TypeError(
                "package.json should be in workspace directory {}, given: {}".format(
                    os.path.dirname(self.path), package_json.path
                )
            )

        self.packages = set(path for _, path in package_json.get_workspace_dep_spec_paths())
        # Add relative path to self.
        self.packages.add(".")
        self.settings = _pnpm_workspace_settings(package_json)

    def merge(self, ws):
        """
        Adds `ws`'s packages to the workspace.
        :param ws: workspace to merge
        :type ws: PnpmWorkspace
        """
        dir_path = os.path.dirname(self.path)
        ws_dir_path = os.path.dirname(ws.path)

        merge_config_sources(self.common_config_sources, ws.common_config_sources)
        for group, entries in ws.catalogs.items():
            if group in self.catalogs and self.catalogs[group] != entries:
                raise ValueError("Conflicting catalog group: {}".format(group))
            self.catalogs[group] = entries

        for p_rel_path in ws.packages:
            p_path = os.path.normpath(os.path.join(ws_dir_path, p_rel_path))
            self.packages.add(os.path.relpath(p_path, dir_path))
