import importlib.metadata
import itertools
import re
import sys
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Any

from .common import VarTuple


def _true():
    return True


def _false():
    return False


class Requirement(ABC):
    __slots__ = ("__bool__", "__dict__", "is_met")

    def __init__(self):
        self.is_met = self._evaluate()
        self.__bool__ = _true if self.is_met else _false

    @abstractmethod
    def _evaluate(self) -> bool:
        ...

    @property
    @abstractmethod
    def fail_reason(self) -> str:
        ...


class PythonVersionRequirement(Requirement):
    def __init__(self, min_version: VarTuple[int]):
        self.min_version = min_version
        super().__init__()

    def _evaluate(self) -> bool:
        return sys.version_info >= self.min_version

    @property
    def fail_reason(self) -> str:
        return f'Python >= {".".join(map(str, self.min_version))} is required'


class DistributionRequirement(Requirement):
    def __init__(self, distribution_name: str):
        self.distribution_name = distribution_name
        super().__init__()

    def _evaluate(self) -> bool:
        try:
            importlib.metadata.distribution(self.distribution_name)
        except importlib.metadata.PackageNotFoundError:
            return False
        return True

    @property
    def fail_reason(self) -> str:
        return f"Installed distribution {self.distribution_name!r} is required"


class DistributionVersionRequirement(DistributionRequirement):
    # Pattern from PEP 440
    _VERSION_PATTERN = r"""
        v?
        (?:
            (?:(?P<epoch>[0-9]+)!)?                           # epoch
            (?P<release>[0-9]+(?:\.[0-9]+)*)                  # release segment
            (?P<pre>                                          # pre-release
                [-_\.]?
                (?P<pre_l>(a|b|c|rc|alpha|beta|pre|preview))
                [-_\.]?
                (?P<pre_n>[0-9]+)?
            )?
            (?P<post>                                         # post release
                (?:-(?P<post_n1>[0-9]+))
                |
                (?:
                    [-_\.]?
                    (?P<post_l>post|rev|r)
                    [-_\.]?
                    (?P<post_n2>[0-9]+)?
                )
            )?
            (?P<dev>                                          # dev release
                [-_\.]?
                (?P<dev_l>dev)
                [-_\.]?
                (?P<dev_n>[0-9]+)?
            )?
        )
        (?:\+(?P<local>[a-z0-9]+(?:[-_\.][a-z0-9]+)*))?       # local version
    """

    _version_regex = re.compile(
        r"^\s*" + _VERSION_PATTERN + r"\s*$",
        re.VERBOSE | re.IGNORECASE,
    )

    def __init__(self, distribution_name: str, min_version: str):
        self.min_version = min_version
        super().__init__(distribution_name)

    def _evaluate(self) -> bool:
        try:
            distribution = importlib.metadata.distribution(self.distribution_name)
        except importlib.metadata.PackageNotFoundError:
            return False
        return (
            self._make_comparator(distribution.version)
            >=
            self._make_comparator(self.min_version)
        )

    def _remove_trailing_zeros(self, data: Iterable[int]) -> VarTuple[int]:
        return tuple(reversed(tuple(itertools.dropwhile(lambda x: x == 0, reversed(tuple(data))))))

    def _make_comparator(self, version: str) -> VarTuple[Any]:
        match = self._version_regex.match(version)
        if match is None:
            raise ValueError
        return (
            match.group("epoch") or 0,
            self._remove_trailing_zeros(int(i) for i in match.group("release").split(".")),
            match.group("pre") is not None,
            match.group("dev") is not None,
        )

    @property
    def fail_reason(self) -> str:
        return f"Installed distribution {self.distribution_name!r} of {self.min_version!r} is required"


class PythonImplementationRequirement(Requirement):
    def __init__(self, implementation_name: str):
        self.implementation_name = implementation_name
        super().__init__()

    def _evaluate(self) -> bool:
        return sys.implementation.name == self.implementation_name

    @property
    def fail_reason(self) -> str:
        return f"{self.implementation_name} is required"


HAS_PY_310 = PythonVersionRequirement((3, 10))
HAS_TYPE_UNION_OP = HAS_PY_310
HAS_TYPE_GUARD = HAS_PY_310
HAS_TYPE_ALIAS = HAS_PY_310
HAS_PARAM_SPEC = HAS_PY_310

HAS_PY_311 = PythonVersionRequirement((3, 11))
HAS_NATIVE_EXC_GROUP = HAS_PY_311
HAS_TYPED_DICT_REQUIRED = HAS_PY_311
HAS_SELF_TYPE = HAS_PY_311
HAS_TV_TUPLE = HAS_PY_311
HAS_UNPACK = HAS_PY_311

HAS_PY_312 = PythonVersionRequirement((3, 12))
HAS_TV_SYNTAX = HAS_PY_312

HAS_PY_313 = PythonVersionRequirement((3, 13))
HAS_TV_DEFAULT = HAS_PY_313

HAS_SUPPORTED_ATTRS_PKG = DistributionVersionRequirement("attrs", "21.3.0")
HAS_ATTRS_PKG = DistributionRequirement("attrs")

HAS_SUPPORTED_MSGSPEC_PKG = DistributionVersionRequirement("msgspec", "0.14.0")
HAS_MSGSPEC_PKG = DistributionRequirement("msgspec")

HAS_SUPPORTED_SQLALCHEMY_PKG = DistributionVersionRequirement("sqlalchemy", "2.0.0")
HAS_SQLALCHEMY_PKG = DistributionRequirement("sqlalchemy")

HAS_SUPPORTED_PYDANTIC_PKG = DistributionVersionRequirement("pydantic", "2.0.0")
HAS_PYDANTIC_PKG = DistributionRequirement("pydantic")

IS_CPYTHON = PythonImplementationRequirement("cpython")
IS_PYPY = PythonImplementationRequirement("pypy")
