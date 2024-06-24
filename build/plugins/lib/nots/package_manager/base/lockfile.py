import os

from abc import ABCMeta, abstractmethod
from six import add_metaclass


class LockfilePackageMeta(object):
    """
    Basic struct representing package meta from lockfile.
    """

    __slots__ = ("key", "tarball_url", "sky_id", "integrity", "integrity_algorithm", "tarball_path")

    @staticmethod
    def from_str(s):
        return LockfilePackageMeta(*s.strip().split(" "))

    def __init__(self, key, tarball_url, sky_id, integrity, integrity_algorithm):
        # http://npm.yandex-team.ru/@scope%2fname/-/name-0.0.1.tgz
        parts = tarball_url.split("/")

        self.key = key
        self.tarball_url = tarball_url
        self.sky_id = sky_id
        self.integrity = integrity
        self.integrity_algorithm = integrity_algorithm
        self.tarball_path = "/".join(parts[-3:])  # @scope%2fname/-/name-0.0.1.tgz

    def to_str(self):
        return " ".join([self.tarball_url, self.sky_id, self.integrity, self.integrity_algorithm])

    def to_uri(self):
        pkg_uri = f"{self.tarball_url}#integrity={self.integrity_algorithm}-{self.integrity}"
        return pkg_uri


class LockfilePackageMetaInvalidError(RuntimeError):
    pass


@add_metaclass(ABCMeta)
class BaseLockfile(object):
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
