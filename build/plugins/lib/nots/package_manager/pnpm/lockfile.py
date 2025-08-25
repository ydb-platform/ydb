import base64
import os
import io
import re

from urllib import parse as urlparse

from ..base import PackageJson, BaseLockfile, LockfilePackageMeta, LockfilePackageMetaInvalidError

try:
    import ymakeyaml as yaml
except Exception:
    import yaml

LOCKFILE_VERSION = "lockfileVersion"
IMPORTER_KEYS = PackageJson.DEP_KEYS + ("specifiers",)
WS_PREFIX = "workspace:"


class PnpmLockfileHelper:
    """
    The class is to contain functionality for converting data structures
    from old lockfile versions to current one and from one data structure to another.
    """

    @staticmethod
    def parse_package_id_v6(package_id_v6):
        """
        Parses the package_id from v6 lockfile
        In v6 we have 'packages' with keys like '/[@scope/]package_name@version[(optional_dep_it_came_from)...]'
        e.g. '/@some-scope/some-package@1.2.3(@other-scope/some-core@2.3.4)'
        In v9 we have
        1. "snapshots" with keys like '[@scope/]package_name@version[(optional_dep_it_came_from)...]'.
           e.g. '@some-scope/some-package@1.2.3(@other-scope/some-core@2.3.4)'
        2. "packages" with keys like "[@scope/]package_name@version".
           e.g. '@some-scope/some-package@1.2.3'
        3. "dependencies" with keys like "[@scope/]package_name" e.g. '@some-scope/some-package'
           and "version" field for having full version specifier e.g. '1.2.3(@other-scope/some-core@2.3.4)'

        Args:
            package_id_v6 (str): package_id from v6 lockfile.

        Raises:
            Exception: in case of invalid package_id

        Returns:
            str[]: values for v9 lockfile: [snapshot_id, package_id, package_name, version_specifier]
        """
        snapshot_id = PnpmLockfileHelper.snapshot_id_from_v6(package_id_v6)
        package_id = PnpmLockfileHelper.package_id_from_snapshot_id(snapshot_id)
        package_name = PnpmLockfileHelper.package_name_from_package_id(package_id)
        snapshot_version = snapshot_id[len(package_name) + 1 :]

        return snapshot_id, package_id, package_name, snapshot_version

    @staticmethod
    def snapshot_id_from_v6(package_id_v6):
        """
        Parses the package_id from v6 lockfile.
        In v6 we have "packages" with keys like '/@optional_scope/package_name@version(optional_dep_it_came_from)'
        e.g. '/@babel/plugin-syntax-class-properties@7.12.13(@babel/core@7.25.8)'
        In v9 we have "snapshots" with keys like "@optional_scope/package_name@version(optional dep it came from)".
        e.g. '@babel/plugin-syntax-class-properties@7.12.13(@babel/core@7.25.8)'

        Args:
            package_id_v6 (str): package_id from v6 lockfile.

        Raises:
            Exception: in case of invalid package_id

        Returns:
            str: snapshot_id that can be used in v9 lockfile
        """
        if package_id_v6[0] != "/":
            raise Exception(f"Can't get snapshot id from package id: '{package_id_v6}'")

        return package_id_v6[1:]

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

    @staticmethod
    def ensure_v9(lockfile_data):
        """
        Checks if lockfile_data has version 9, returns lockfile_data as-is if so.
        If lockfile_data has version 6 then tries to apply transformations from v6 to v9
        """
        lockfile_version = lockfile_data.get(LOCKFILE_VERSION)

        if lockfile_version == "9.0":
            return lockfile_data

        if lockfile_version != "6.0":
            raise Exception(f"Invalid lockfile version: {lockfile_version}")

        # according to the spec
        # https://github.com/pnpm/pnpm/blob/f76ff6389b6252cca1653248444dac160ac1f052/lockfile/types/src/lockfileFileTypes.ts#L12C52-L12C154
        snapshots_data_keys = [
            "dependencies",
            "optionalDependencies",
            "patched",
            "optional",
            "transitivePeerDependencies",
            "id",
        ]
        ignore_data_list = ["dev"]

        snapshots = {}
        packages = {}

        importers_data = {}
        importer_keys_by_package_name = {}
        for importer_key in IMPORTER_KEYS:
            for package_name, data in lockfile_data.get(importer_key, {}).items():
                if importer_key not in importers_data:
                    importers_data[importer_key] = {}
                importers_data[importer_key][package_name] = data
                importer_keys_by_package_name[package_name] = importer_key

        for package_v6_specifier, data in lockfile_data.get("packages", {}).items():
            snapshot_id, package_id, package_name, snapshot_version = PnpmLockfileHelper.parse_package_id_v6(
                package_v6_specifier
            )
            package_data = packages.get(package_id, {})
            snapshot_data = {}
            for key, value in data.items():
                if key in ignore_data_list:
                    continue
                if key in snapshots_data_keys:
                    snapshot_data[key] = value
                else:
                    package_data[key] = value

            if package_data:
                packages[package_id] = package_data

            # Saving it to snapshots even if it's empty
            snapshots[snapshot_id] = snapshot_data
            if package_name in importer_keys_by_package_name:
                importer_key = importer_keys_by_package_name[package_name]
                importers_data[importer_key][package_name]["version"] = snapshot_version

        new_lockfile_data = {}
        new_lockfile_data.update(lockfile_data)

        # This part is already converted to importers_data
        for importer_key in IMPORTER_KEYS:
            if importer_key in new_lockfile_data:
                new_lockfile_data.pop(importer_key)

        new_lockfile_data["lockfileVersion"] = "9.0"
        if importers_data:
            new_lockfile_data["importers"] = {".": importers_data}
        if packages:
            new_lockfile_data["packages"] = packages
        if snapshots:
            new_lockfile_data["snapshots"] = snapshots

        return new_lockfile_data


class PnpmLockfile(BaseLockfile):

    def read(self):
        # raise Exception("Reading lock file is not supported")
        with io.open(self.path, "rb") as f:
            data = yaml.load(f, Loader=yaml.CSafeLoader) or {LOCKFILE_VERSION: "9.0"}

            lockfile_version = "<no-version>"
            if isinstance(data, dict) and LOCKFILE_VERSION in data:
                lockfile_version = str(data.get(LOCKFILE_VERSION))
            r = re.compile('^[69]\\.\\d$')
            if not lockfile_version or not r.match(lockfile_version):
                raise Exception(
                    f"Error of project configuration: {self.path} has lockfileVersion: {lockfile_version}.\n"
                    "This version is not supported. Please, delete pnpm-lock.yaml and regenerate it using "
                    "`ya tool nots --clean update-lockfile`"
                )

            self.data = PnpmLockfileHelper.ensure_v9(data)

    def write(self, path=None):
        """
        :param path: path to store lockfile, defaults to original path
        :type path: str
        """
        if path is None:
            path = self.path

        with open(path, "w") as f:
            yaml.dump(self.data, f, Dumper=yaml.CSafeDumper)

    def get_packages_meta(self):
        """
        Extracts packages meta from lockfile.
        :rtype: list of LockfilePackageMeta
        """
        packages = self.data.get("packages", {})

        return map(lambda x: _parse_package_meta(*x), packages.items())

    def update_tarball_resolutions(self, fn):
        """
        :param fn: maps `LockfilePackageMeta` instance to new `resolution.tarball` value
        :type fn: lambda
        """
        packages = self.data.get("packages", {})

        for key, meta in packages.items():
            meta["resolution"]["tarball"] = fn(_parse_package_meta(key, meta, allow_file_protocol=True))
            packages[key] = meta

    def get_importers(self):
        """
        Returns "importers" section from the lockfile or creates similar structure from "dependencies" and "specifiers".
        :rtype: dict of dict of dict of str
        """
        importers = self.data.get("importers")
        if importers is not None:
            return importers

        importer = {k: self.data[k] for k in IMPORTER_KEYS if k in self.data}

        return {".": importer} if importer else {}

    def validate_importers(self):
        validate_keys = ("dependencies", "devDependencies", "peerDependencies", "optionalDependencies")
        importers = self.get_importers()
        pkg = importers.get(".")
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

    def merge(self, lf):
        """
        Merges two lockfiles:
        1. Converts the lockfile to monorepo-like lockfile with "importers" section instead of "dependencies" and "specifiers".
        2. Merges `lf`'s dependencies and specifiers to importers.
        3. Merges `lf`'s packages to the lockfile.
        :param lf: lockfile to merge
        :type lf: PnpmLockfile
        """
        importers = self.get_importers()
        build_path = os.path.dirname(self.path)

        self.data = PnpmLockfileHelper.ensure_v9(self.data)
        lf.data = PnpmLockfileHelper.ensure_v9(lf.data)

        for importer, imports in lf.get_importers().items():
            importer_path = os.path.normpath(os.path.join(os.path.dirname(lf.path), importer))
            importer_rel_path = os.path.relpath(importer_path, build_path)
            importers[importer_rel_path] = imports

        self.data["importers"] = importers

        for k in IMPORTER_KEYS:
            self.data.pop(k, None)

        packages = self.data.get("packages", {})
        for k, v in lf.data.get("packages", {}).items():
            if k not in packages:
                packages[k] = v
        self.data["packages"] = packages

        snapshots = self.data.get("snapshots", {})
        for k, v in lf.data.get("snapshots", {}).items():
            if k not in snapshots:
                snapshots[k] = v
        self.data["snapshots"] = snapshots

    def validate_has_addons_flags(self):
        packages = self.data.get("packages", {})
        invalid_keys = []

        for key, meta in packages.items():
            if meta.get("requiresBuild") and "hasAddons" not in meta:
                invalid_keys.append(key)

        return (not invalid_keys, invalid_keys)

    # TODO: remove after dropping v6 support
    def get_requires_build_packages(self):
        packages = self.data.get("packages", {})
        requires_build_packages = []

        for pkg, meta in packages.items():
            if meta.get("requiresBuild"):
                requires_build_packages.append(pkg)

        return requires_build_packages


def _parse_package_meta(key, meta, allow_file_protocol=False):
    """
    :param key: uniq package key from lockfile
    :type key: string
    :param meta: package meta dict from lockfile
    :type meta: dict
    :rtype: LockfilePackageMetaInvalidError
    """
    try:
        tarball_url = _parse_tarball_url(meta["resolution"]["tarball"], allow_file_protocol)
        sky_id = _parse_sky_id_from_tarball_url(meta["resolution"]["tarball"])
        integrity_algorithm, integrity = _parse_package_integrity(meta["resolution"]["integrity"])
    except KeyError as e:
        raise TypeError(f"Invalid package meta for '{key}', missing {e} key")
    except LockfilePackageMetaInvalidError as e:
        raise TypeError(f"Invalid package meta for '{key}', parse error: {e}")

    return LockfilePackageMeta(key, tarball_url, sky_id, integrity, integrity_algorithm)


def _parse_tarball_url(tarball_url, allow_file_protocol):
    if tarball_url.startswith("file:") and not allow_file_protocol:
        raise LockfilePackageMetaInvalidError(f"tarball cannot point to a file, got '{tarball_url}'")

    return tarball_url.split("?")[0]


def _parse_sky_id_from_tarball_url(tarball_url):
    """
    :param tarball_url: tarball url
    :type tarball_url: string
    :rtype: string
    """
    if tarball_url.startswith("file:"):
        return ""

    rbtorrent_param = urlparse.parse_qs(urlparse.urlparse(tarball_url).query).get("rbtorrent")

    if rbtorrent_param is None:
        return ""

    return f"rbtorrent:{rbtorrent_param[0]}"


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
