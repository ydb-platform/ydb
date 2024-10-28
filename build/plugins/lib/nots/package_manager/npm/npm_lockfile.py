import base64
import json
import os
import io

from six.moves.urllib import parse as urlparse
from six import iteritems

from ..base import BaseLockfile, LockfilePackageMeta, LockfilePackageMetaInvalidError

LOCKFILE_VERSION_FIELD = "lockfileVersion"


class NpmLockfile(BaseLockfile):
    def read(self):
        with io.open(self.path, "rb") as f:
            self.data = json.load(f) or {LOCKFILE_VERSION_FIELD: 3}

            lockfile_version = self.data.get(LOCKFILE_VERSION_FIELD, "<no-version>")
            if lockfile_version != 3:
                raise Exception(
                    f'Error of project configuration: {self.path} has lockfileVersion: {lockfile_version}. '
                    + f'This version is not supported. Please, delete {os.path.basename(self.path)} and regenerate it using "ya tool nots --clean install --lockfile-only --npm"'
                )

    def write(self, path=None):
        """
        :param path: path to store lockfile, defaults to original path
        :type path: str
        """
        if path is None:
            path = self.path

        with open(path, "w") as f:
            json.dump(self.data, f, indent=2)

    def get_packages_meta(self):
        """
        Extracts packages meta from lockfile.
        :rtype: list of LockfilePackageMeta
        """
        packages = self.data.get("packages", {})

        for key, meta in packages.items():
            if self._should_skip_package(key, meta):
                continue
            yield _parse_package_meta(key, meta)

    def update_tarball_resolutions(self, fn):
        """
        :param fn: maps `LockfilePackageMeta` instance to new `resolution.tarball` value
        :type fn: lambda
        """
        packages = self.data.get("packages", {})

        for key, meta in iteritems(packages):
            if self._should_skip_package(key, meta):
                continue
            meta["resolved"] = fn(_parse_package_meta(key, meta, allow_file_protocol=True))
            packages[key] = meta

    def get_requires_build_packages(self):
        raise NotImplementedError()

    def validate_has_addons_flags(self):
        raise NotImplementedError()

    def _should_skip_package(self, key, meta):
        return not key or key.startswith(".") or meta.get("link", None) is True


def _parse_package_meta(key, meta, allow_file_protocol=False):
    """
    :param key: uniq package key from lockfile
    :type key: string
    :param meta: package meta dict from lockfile
    :type meta: dict
    :rtype: LockfilePackageMetaInvalidError
    """
    try:
        tarball_url = _parse_tarball_url(meta["resolved"], allow_file_protocol)
        sky_id = _parse_sky_id_from_tarball_url(meta["resolved"])
        integrity_algorithm, integrity = _parse_package_integrity(meta["integrity"])
    except KeyError as e:
        raise TypeError("Invalid package meta for key '{}', missing '{}' key".format(key, e))
    except LockfilePackageMetaInvalidError as e:
        raise TypeError("Invalid package meta for key '{}', parse error: '{}'".format(key, e))

    return LockfilePackageMeta(key, tarball_url, sky_id, integrity, integrity_algorithm)


def _parse_tarball_url(tarball_url, allow_file_protocol):
    if tarball_url.startswith("file:") and not allow_file_protocol:
        raise LockfilePackageMetaInvalidError("tarball cannot point to a file, got {}".format(tarball_url))
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

    return "rbtorrent:{}".format(rbtorrent_param[0])


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
        raise LockfilePackageMetaInvalidError(
            "Invalid package integrity encoding, integrity: {}, error: {}".format(integrity, e)
        )

    return (algo, hash_b64)
