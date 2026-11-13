import json
import logging
import os
import re

logger = logging.getLogger(__name__)


class PackageJsonWorkspaceError(RuntimeError):
    pass


class PackageJson(object):
    DEP_KEY = "dependencies"
    DEV_DEP_KEY = "devDependencies"
    PEER_DEP_KEY = "peerDependencies"
    OPT_DEP_KEY = "optionalDependencies"
    PNPM_OVERRIDES_KEY = "pnpm.overrides"
    DEP_KEYS = (DEP_KEY, DEV_DEP_KEY, PEER_DEP_KEY, OPT_DEP_KEY, PNPM_OVERRIDES_KEY)

    WORKSPACE_SCHEMA = "workspace:"

    @classmethod
    def load(cls, path):
        """
        :param path: package.json path
        :type path: str
        :rtype: PackageJson
        """
        pj = cls(path)
        pj.read()

        return pj

    def __init__(self, path):
        # type: (str) -> None
        if not os.path.isabs(path):
            raise TypeError("Absolute path required, given: {}".format(path))

        self.path = path
        self.data = None
        self.indent = "  "

    def read(self):
        with open(self.path, 'rb') as f:
            content = f.read()
            self.data = json.loads(content)
            indent = re.search(rb'^[ \t]+(?=")', content, re.MULTILINE)
            self.indent = indent.group().decode('ascii') if indent else "  "

    def write(self, path=None):
        """
        :param path: path to store package.json, defaults to original path
        :type path: str
        """
        if path is None:
            path = self.path

        directory = os.path.dirname(path)
        if not os.path.exists(directory):
            os.mkdir(directory)

        with open(path, "w") as f:
            json.dump(self.data, f, indent=self.indent, ensure_ascii=False)
            f.write('\n')  # it's better for diff algorithm in arc
            logger.debug("Written {}".format(path))

    def get_name(self):
        # type: () -> str
        name = self.data.get("name")

        if not name:
            name = os.path.dirname(self.path).replace("/", "-").strip("-")

        return name

    def get_version(self):
        return self.data["version"]

    def get_description(self):
        return self.data.get("description")

    def get_use_prebuilder(self):
        return self.data.get("usePrebuilder", False)

    def get_nodejs_version(self):
        return self.data.get("engines", {}).get("node")

    def get_dep_specifier(self, dep_name):
        for name, spec in self.dependencies_iter():
            if dep_name == name:
                return spec
        return None

    def dependencies_iter(self):
        for key in self.DEP_KEYS:
            if key == self.PNPM_OVERRIDES_KEY:
                deps = self.data.get("pnpm", {}).get("overrides", {})
            else:
                deps = self.data.get(key)

            if not deps:
                continue

            for name, spec in deps.items():
                yield name, spec

    def has_dependencies(self):
        first_dep = next(self.dependencies_iter(), None)
        return first_dep is not None

    def get_files(self) -> list[str]:
        return self.data.get("files", [])

    def bins_iter(self):
        bins = self.data.get("bin")
        if isinstance(bins, str):
            yield bins
        elif isinstance(bins, dict):
            for bin in bins.values():
                yield bin

    def get_workspace_dep_spec_paths(self) -> list[tuple[str, str]]:
        """
        Returns names and paths from specifiers of the defined workspace dependencies.
        :rtype: list[tuple[str, str]]
        """
        return list(self._workspace_dep_spec_paths_iter())

    def _workspace_dep_spec_paths_iter(self):
        # file: can use an alias or an override selector unrelated to the manifest name.
        schema = self.WORKSPACE_SCHEMA
        schema_len = len(schema)

        for name, spec in self.dependencies_iter():
            if spec.startswith("file:"):
                relative = spec[len("file:") :]
                directory = os.path.normpath(os.path.join(os.path.dirname(self.path), relative))
                if not os.path.isabs(relative) and (
                    os.path.isfile(os.path.join(directory, "ya.make"))
                    or os.path.isfile(os.path.join(directory, "output.tar.uuid"))
                ):
                    yield name, relative
                continue
            if not spec.startswith(schema):
                continue

            spec_path = spec[schema_len:]
            if not (spec_path.startswith(".") or spec_path.startswith("..")):
                raise PackageJsonWorkspaceError(
                    "Expected relative path specifier for workspace dependency, but got '{}' for {} in {}".format(
                        spec, name, self.path
                    )
                )

            yield name, spec_path

    def get_workspace_dep_paths(self, base_path=None):
        """
        Returns paths of the defined workspace dependencies.
        :param base_path: base path to resolve relative dep paths
        :type base_path: str
        :rtype: list of str
        """
        if base_path is None:
            base_path = os.path.dirname(self.path)

        return [os.path.normpath(os.path.join(base_path, p)) for _, p in self.get_workspace_dep_spec_paths()]

    def validate_prebuilds(self, common_overrides=None):
        pnpm_overrides = dict(common_overrides or {})
        pnpm_overrides.update(self.data.get("pnpm", {}).get("overrides", {}))
        use_prebuild_flags = self.data.get("@yatool/prebuilder", {}).get("usePrebuild", {})
        missing_overrides = [
            key for key, enabled in use_prebuild_flags.items() if enabled and key not in pnpm_overrides
        ]
        messages = []
        if missing_overrides:
            messages.append("These packages have addons but overrides are not set:")
            messages.extend(f"  - {key}" for key in missing_overrides)
        return (not messages, messages)

    def get_pnpm_patched_dependencies(self) -> dict[str, str]:
        return self.data.get("pnpm", {}).get("patchedDependencies", {})
