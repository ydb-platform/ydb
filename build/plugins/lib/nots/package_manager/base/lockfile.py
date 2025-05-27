import os

from abc import ABCMeta, abstractmethod


class LockfilePackageMetaInvalidError(RuntimeError):
    pass


def is_tarball_url_valid(tarball_url):
    if not tarball_url.startswith("https://") and not tarball_url.startswith("http://"):
        return True

    return tarball_url.startswith("https://npm.yandex-team.ru/") or tarball_url.startswith("http://npm.yandex-team.ru/")


class LockfilePackageMeta(object):
    """
    Basic struct representing package meta from lockfile.
    """

    __slots__ = ("key", "tarball_url", "sky_id", "integrity", "integrity_algorithm", "tarball_path")

    @staticmethod
    def from_str(s):
        return LockfilePackageMeta(*s.strip().split(" "))

    def __init__(self, key, tarball_url, sky_id, integrity, integrity_algorithm):
        if not is_tarball_url_valid(tarball_url):
            raise LockfilePackageMetaInvalidError(
                "tarball can only point to npm.yandex-team.ru, got {}".format(tarball_url)
            )

        # http://npm.yandex-team.ru/@scope%2fname/-/name-0.0.1.tgz
        parts = tarball_url.split("/")

        self.key = key
        self.tarball_url = tarball_url
        self.sky_id = sky_id
        self.integrity = integrity
        self.integrity_algorithm = integrity_algorithm
        self.tarball_path = "/".join(parts[-3:]).replace("%2f", "/")  # @scope%2fname/-/name-0.0.1.tgz

    def to_str(self):
        return " ".join([self.tarball_url, self.sky_id, self.integrity, self.integrity_algorithm])

    def to_uri(self):
        tarball_url: str = self.tarball_url
        if not tarball_url.startswith("https://") and not tarball_url.startswith("http://"):
            tarball_url = "https://npm.yandex-team.ru/" + tarball_url
        pkg_uri = f"{tarball_url}#integrity={self.integrity_algorithm}-{self.integrity}"
        return pkg_uri


class BaseLockfile(object, metaclass=ABCMeta):
    @classmethod
    def load(cls, path):
        """
        :param path: lockfile path
        :type path: str
        :rtype: BaseLockfile
        """
        pj = cls(path)
        pj.read()

        return pj

    def __init__(self, path):
        if not os.path.isabs(path):
            raise TypeError("Absolute path required, given: {}".format(path))

        self.path = path
        self.data = None

    @abstractmethod
    def read(self):
        pass

    @abstractmethod
    def write(self, path=None):
        pass

    @abstractmethod
    def get_packages_meta(self):
        pass

    @abstractmethod
    def update_tarball_resolutions(self, fn):
        pass

    @abstractmethod
    def validate_has_addons_flags(self):
        pass

    @abstractmethod
    def get_requires_build_packages(self):
        pass
