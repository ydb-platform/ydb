import base64
import os
import io
import re

from .common_config import load_tier0_settings
from .platform import package_supports_platform

try:
    import ymakeyaml as yaml
except Exception:
    import yaml

LOCKFILE_VERSION = "lockfileVersion"
WS_PREFIX = "workspace:"


class LockfilePackageMetaInvalidError(RuntimeError):
    pass


class LockfilePackageMeta:
    """A registry archive derived from a package key and integrity."""

    __slots__ = ("key", "tarball_url", "integrity", "integrity_algorithm", "tarball_path")

    def __init__(self, key, tarball_url, tarball_path, integrity, integrity_algorithm):
        self.key = key
        self.tarball_url = tarball_url
        self.tarball_path = tarball_path
        self.integrity = integrity
        self.integrity_algorithm = integrity_algorithm

    def to_uri(self):
        return f"{self.tarball_url}#integrity={self.integrity_algorithm}-{self.integrity}"


class PnpmLockfileHelper:
    """
    Helpers for package and snapshot identifiers in pnpm lockfile v9.
    """

    @staticmethod
    def package_id_from_snapshot_id(snapshot_id):
        """
        Parses the snapshot_id from v9 lockfile.
        In v9 we have "snapshots" with keys like "@optional_scope/package_name@version(optional dep it came from)".
        e.g. '@babel/plugin-syntax-class-properties@7.12.13(@babel/core@7.25.8)'
        In v9 we have "packages" with keys like "@optional_scope/package_name@version".
        e.g. '@babel/plugin-syntax-class-properties@7.12.13'
        So we need to take only the part before first round bracket

        Args:
            snapshot_id (str): snapshot_id from v9 lockfile.

        Raises:
            Exception: in case of invalid snapshot_id

        Returns:
            str: package_id that can be used in v9 lockfile
        """
        package_id = snapshot_id.split("(", 2)[0]
        if not package_id:
            raise Exception(f"Can't get package id from snapshot id: '{snapshot_id}'")

        return package_id

    @staticmethod
    def package_name_from_package_id(package_id):
        """
        In v9 we have "packages" with keys like "@optional_scope/package_name@version".
        e.g. '@babel/plugin-syntax-class-properties@7.12.13'
        In v9 we have "dependencies" with keys like "@optional_scope/package_name".
        e.g. '@babel/plugin-syntax-class-properties'
        So we need to take only the part before last '@', and we can have one '@' for scope:

        Args:
            package_id (str): package_id from v9 lockfile.

        Raises:
            Exception: in case of invalid package_id

        Returns:
            str: package_name that can be used in v9 lockfile
        """
        package_specifier_elements = package_id.split("@", 3)
        if len(package_specifier_elements) > 1:
            package_specifier_elements.pop(-1)
        package_name = "@".join(package_specifier_elements)
        if not package_name:
            raise Exception(f"Can't get package name from package id: '{package_id}'")

        return package_name


class Lockfile(object):
    @classmethod
    def load(cls, path):
        """
        :param path: lockfile path
        :type path: str
        :rtype: Lockfile
        """
        pj = cls(path)
        pj.read()

        return pj

    def __init__(self, path):
        if not os.path.isabs(path):
            raise TypeError("Absolute path required, given: {}".format(path))

        self.path = path
        self.data = None

    def read(self):
        try:
            with io.open(self.path, "rb") as f:
                content = f.read()
                # ymakeyaml accepts a file stream or a filename, not YAML bytes.
                f.seek(0)
                data = yaml.load(f, Loader=yaml.CSafeLoader) if content.strip() else None
        except FileNotFoundError:
            data = None

        # Packages without dependencies may omit the lockfile or keep an empty one.
        self.data = {LOCKFILE_VERSION: "9.0"} if data is None or data == {} else data
        self._validate_version()

    def _validate_version(self):
        version = (
            str(self.data.get(LOCKFILE_VERSION, "<no-version>")) if isinstance(self.data, dict) else "<no-version>"
        )
        if version != "9.0":
            raise ValueError(
                f"Error of project configuration: {self.path} has lockfileVersion: {version}.\n"
                "Only lockfileVersion 9.0 is supported. Regenerate pnpm-lock.yaml using "
                "`ya tool nots --clean update-lockfile`"
            )
        self.data[LOCKFILE_VERSION] = "9.0"

    def write(self, path=None):
        """
        :param path: path to store lockfile, defaults to original path
        :type path: str
        """
        if path is None:
            path = self.path

        with open(path, "w") as f:
            yaml.dump(self.data, f, Dumper=yaml.CSafeDumper)

    def get_packages_meta(self, platform=None, supported_architectures=None):
        """
        Extracts packages meta from lockfile.
        :rtype: list of LockfilePackageMeta
        """
        selected = self._select_packages_for_platform(platform, supported_architectures)
        mapped = (_parse_package_meta(key, meta) for key, meta in selected)
        return filter(lambda x: x is not None, mapped)

    def _select_packages_for_platform(self, platform, supported_architectures):
        packages = self.data.get("packages", {})
        if not platform:
            return packages.items()

        optional_in_all_contexts = {}
        for key, snapshot in self.data.get("snapshots", {}).items():
            package_id = PnpmLockfileHelper.package_id_from_snapshot_id(key)
            optional_in_all_contexts[package_id] = (
                optional_in_all_contexts.get(package_id, True) and snapshot.get("optional") is True
            )

        # Generated TS_PROTO workspaces reuse the owner's tarballs with default pnpm settings.
        return (
            (key, metadata)
            for key, metadata in packages.items()
            if not optional_in_all_contexts.get(key)
            or package_supports_platform(metadata, platform)
            or (
                supported_architectures is not None
                and package_supports_platform(metadata, platform, supported_architectures)
            )
        )

    def get_importers(self):
        """Returns the v9 importers section without converting legacy root dependencies."""
        return self.data.get("importers") or {}

    def validate_importers(self):
        validate_keys = ("dependencies", "devDependencies", "peerDependencies", "optionalDependencies")
        importers = self.get_importers()
        pkg = importers.get(".", {})
        peers = set(["."])
        problem_importers = []

        for deps_key, deps in pkg.items():
            if deps_key not in validate_keys:
                continue
            for _, dep in deps.items():
                specifier = dep.get("specifier")
                if specifier and specifier.startswith(WS_PREFIX):
                    peers.add(specifier[len(WS_PREFIX) :])

        for importer in self.get_importers().keys():
            if importer not in peers:
                problem_importers.append(importer)

        if problem_importers:
            raise Exception(f"Invalid importers in lockfile: {", ".join(problem_importers)}")


def _parse_package_meta(key, meta):
    """
    :param key: uniq package key from lockfile
    :type key: string
    :param meta: package meta dict from lockfile
    :type meta: dict
    :rtype: LockfilePackageMetaInvalidError
    """
    try:
        # inject-workspace-packages case
        resolution = meta["resolution"]
        if resolution.get("type") == "directory":
            return None

        tarball_url, tarball_path = _registry_tarball_from_package_key(key)
        integrity_algorithm, integrity = _parse_package_integrity(resolution["integrity"])
    except KeyError as e:
        raise TypeError(f"Invalid package meta for '{key}', missing {e} key")
    except LockfilePackageMetaInvalidError as e:
        raise TypeError(f"Invalid package meta for '{key}', parse error: {e}")

    return LockfilePackageMeta(key, tarball_url, tarball_path, integrity, integrity_algorithm)


def _registry_tarball_from_package_key(key):
    package_id = PnpmLockfileHelper.package_id_from_snapshot_id(key)
    name, separator, version = package_id.rpartition("@")
    if (
        not separator
        # Only URL-safe npm characters; keep support for legacy uppercase/special names.
        or not re.fullmatch(r"(?:@[0-9A-Za-z._~!'()*-]+/)?[0-9A-Za-z_~!'()*-][0-9A-Za-z._~!'()*-]*", name)
        or not re.fullmatch(
            r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)"
            r"(?:-(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)"
            r"(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*)?"
            r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?",
            version,
        )
    ):
        raise LockfilePackageMetaInvalidError(f"cannot infer tarball URL from package key '{key}'")

    package_path = name.replace("/", "%2f")
    filename = f"{name.rsplit('/', 1)[-1]}-{version}.tgz"
    registry = load_tier0_settings()["pnpm"]["defaults"]["registry"].rstrip("/")
    return f"{registry}/{package_path}/-/{filename}", f"{name}/-/{filename}"


def _parse_package_integrity(integrity):
    """
    Returns tuple of algorithm and hash (hex).
    :param integrity: package integrity in format "{algo}-{base64_of_hash}"
    :type integrity: string
    :rtype: (str, str)
    """
    algo, hash_b64 = integrity.split("-", 1)

    if algo not in ("sha1", "sha512"):
        raise LockfilePackageMetaInvalidError(
            f"Invalid package integrity algorithm, expected one of ('sha1', 'sha512'), got '{algo}'"
        )

    try:
        base64.b64decode(hash_b64)
    except TypeError as e:
        raise LockfilePackageMetaInvalidError(f"Invalid package integrity encoding, integrity: {integrity}, error: {e}")

    return (algo, hash_b64)
