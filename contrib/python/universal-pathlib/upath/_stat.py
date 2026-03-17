from __future__ import annotations

import os
import warnings
from datetime import datetime
from stat import S_IFDIR
from stat import S_IFLNK
from stat import S_IFREG
from typing import Any
from typing import Iterator
from typing import Mapping
from typing import Sequence

__all__ = [
    "UPathStatResult",
]


def _convert_value_to_timestamp(value: Any) -> int | float:
    """Try to convert a datetime-like value to a timestamp."""
    if isinstance(value, (int, float)):
        return value
    elif isinstance(value, str):
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value).timestamp()
    elif isinstance(value, datetime):
        return value.timestamp()
    else:
        warnings.warn(
            f"Cannot convert {value!r} of type {type(value)!r} to a timestamp."
            " Please report this at: https://github.com/fsspec/universal_path/issues",
            RuntimeWarning,
            stacklevel=2,
        )
        raise TypeError(f"Cannot convert {value!r} to a timestamp.")


def _get_stat_result_extra_fields() -> tuple[str, ...]:
    """retrieve the extra fields of the os.stat_result class."""
    # Note:
    #  The lines below let us provide a dictionary with the additional
    #  named fields of the stat_result class as keys and the internal
    #  index of the field as value.
    sr = os.stat_result(range(os.stat_result.n_fields))
    rd = sr.__reduce__()
    assert isinstance(rd, tuple), "unexpected return os.stat_result.__reduce__"
    _, (_, extra) = rd
    extra_fields = sorted(extra, key=extra.__getitem__)
    return tuple(extra_fields)


class UPathStatResult:
    """A stat_result compatible class wrapping fsspec info dicts.

    **Note**: It is unlikely that you will ever have to instantiate
      this class directly. If you want to convert and info dict,
      use: `UPathStatResult.from_info(info)`

    This object may be accessed either as a tuple of
      (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime)
    or via the attributes st_mode, st_ino, st_dev, st_nlink, st_uid, and so on.

    There's an additional method `as_info()` for accessing the info dict.
    This is useful to access additional information provided by the file system
    implementation, that's not covered by the stat_result tuple.

    """

    __slots__ = ("_seq", "_info")
    # Note:
    #   can't derive from os.stat_result at all, and can't derive from
    #   tuple and have slots. So we duck type the os.stat_result class

    # Add the fields and "extra fields" of the os.stat_result class
    _fields = (
        "st_mode",
        "st_ino",
        "st_dev",
        "st_nlink",
        "st_uid",
        "st_gid",
        "st_size",
        "st_atime",
        "st_mtime",
        "st_ctime",
    )
    _fields_extra = _get_stat_result_extra_fields()

    # Provide the n_ attributes of the os.stat_result class for compatibility
    n_sequence_fields = len(_fields)
    n_fields = len(_fields) + len(_fields_extra)
    n_unnamed_fields = len(set(_fields_extra).intersection(_fields))

    if (
        n_fields != os.stat_result.n_fields
        or n_sequence_fields != os.stat_result.n_sequence_fields
        or n_unnamed_fields != os.stat_result.n_unnamed_fields
    ):
        warnings.warn(
            "UPathStatResult: The assumed number of fields in the"
            " stat_result class is not correct. Got: "
            f" {_fields!r}, {_fields_extra!r}, {os.stat_result.n_fields}"
            " This might cause problems? Please report this issue at:"
            " https://github.com/fsspec/universal_path/issues",
            RuntimeWarning,
            stacklevel=2,
        )

    def __init__(
        self,
        stat_result_seq: Sequence[int],
        info_dict: Mapping[str, Any] | None = None,
    ) -> None:
        """init compatible with os.stat_result

        Use `UPathStatResult.from_info(info)` to instantiate from a fsspec info.
        """
        seq = tuple(stat_result_seq)
        if n := len(seq) < self.n_sequence_fields:
            raise TypeError(
                f"{self.__name__} takes at least {self.n_fields}-sequence"
                " ({n}-sequence given)"
            )
        elif n > self.n_fields:
            raise TypeError(
                f"{self.__name__} takes at most {self.n_fields}-sequence"
                " ({n}-sequence given)"
            )
        elif self.n_sequence_fields <= n < self.n_sequence_fields:
            warnings.warn(
                "UPathStatResult: The seq provided more than"
                f" {self.n_sequence_fields} items. Ignoring the extra items...",
                UserWarning,
                stacklevel=2,
            )
        self._seq = seq[: self.n_sequence_fields]
        self._info = info_dict or {}

    def __repr__(self):
        cls_name = type(self).__name__
        seq_attrs = ", ".join(map("{0[0]}={0[1]}".format, zip(self._fields, self)))
        return f"{cls_name}({seq_attrs}, info={self._info!r})"

    def __eq__(self, other):
        if not isinstance(other, UPathStatResult):
            return NotImplemented
        else:
            return self._info == other._info

    # --- access to the fsspec info dict ------------------------------

    @classmethod
    def from_info(cls, info: Mapping[str, Any]) -> UPathStatResult:
        """Create a UPathStatResult from a fsspec info dict."""
        # fill all the fallback default values with 0
        defaults = [0] * cls.n_sequence_fields
        return cls(defaults, info)

    def as_info(self) -> Mapping[str, Any]:
        """Return the fsspec info dict."""
        return self._info

    # --- guaranteed fields -------------------------------------------

    @property
    def st_mode(self) -> int:
        """protection bits"""
        mode = self._info.get("mode")
        if isinstance(mode, int):
            return mode
        elif isinstance(mode, str):
            try:
                return int(mode, 8)
            except ValueError:
                pass

        type_ = self._info.get("type")
        if type_ == "file":
            return S_IFREG  # see: stat.S_ISREG
        elif type_ == "directory":
            return S_IFDIR  # see: stat.S_ISDIR

        if self._info.get("isLink"):
            return S_IFLNK  # see: stat.S_ISLNK

        return self._seq[0]

    @property
    def st_ino(self) -> int:
        """inode"""
        ino = self._info.get("ino")
        if isinstance(ino, int):
            return ino
        return self._seq[1]

    @property
    def st_dev(self) -> int:
        """device"""
        dev = self._info.get("dev")
        if isinstance(dev, int):
            return dev
        return self._seq[2]

    @property
    def st_nlink(self) -> int:
        """number of hard links"""
        nlink = self._info.get("nlink")
        if isinstance(nlink, int):
            return nlink
        return self._seq[3]

    @property
    def st_uid(self) -> int:
        """user ID of owner"""
        for key in ["uid", "owner", "uname", "unix.owner"]:
            try:
                return int(self._info[key])
            except (ValueError, TypeError, KeyError):
                pass
        return self._seq[4]

    @property
    def st_gid(self) -> int:
        """group ID of owner"""
        for key in ["gid", "group", "gname", "unix.group"]:
            try:
                return int(self._info[key])
            except (ValueError, TypeError, KeyError):
                pass
        return self._seq[5]

    @property
    def st_size(self) -> int:
        """total size, in bytes"""
        try:
            return int(self._info["size"])
        except (ValueError, TypeError, KeyError):
            return self._seq[6]

    @property
    def st_atime(self) -> int | float:
        """time of last access"""
        for key in ["atime", "time", "last_accessed", "accessTime"]:
            try:
                raw_value = self._info[key]
            except KeyError:
                continue
            try:
                return _convert_value_to_timestamp(raw_value)
            except (TypeError, ValueError):
                pass
        return self._seq[7]

    @property
    def st_mtime(self) -> int | float:
        """time of last modification"""
        for key in [
            "mtime",
            "LastModified",
            "last_modified",
            "timeModified",
            "modificationTime",
            "modified_at",
        ]:
            try:
                raw_value = self._info[key]
            except KeyError:
                continue
            try:
                return _convert_value_to_timestamp(raw_value)
            except (TypeError, ValueError):
                pass
        return self._seq[8]

    @property
    def st_ctime(self) -> int | float:
        """time of last change"""
        try:
            raw_value = self._info["ctime"]
        except KeyError:
            pass
        else:
            try:
                return _convert_value_to_timestamp(raw_value)
            except (TypeError, ValueError):
                pass
        return self._seq[9]

    @property
    def st_birthtime(self) -> int | float:
        """time of creation"""
        for key in [
            "birthtime",
            "created",
            "creation_time",
            "timeCreated",
            "created_at",
        ]:
            try:
                raw_value = self._info[key]
            except KeyError:
                continue
            try:
                return _convert_value_to_timestamp(raw_value)
            except (TypeError, ValueError):
                pass
        raise AttributeError("birthtime")

    # --- extra fields ------------------------------------------------

    def __getattr__(self, item):
        if item in self._fields_extra:
            return 0  # fallback default value
        raise AttributeError(item)

    # --- os.stat_result tuple interface ------------------------------

    def __len__(self) -> int:
        return len(self._fields)

    def __iter__(self) -> Iterator[int]:
        """the sequence interface iterates over the guaranteed fields.

        All values are integers.
        """
        for field in self._fields:
            yield int(getattr(self, field))

    def index(self, value: int, start: int = 0, stop: int | None = None, /) -> int:
        """the sequence interface index method."""
        if stop is None:
            stop = len(self._seq)
        return self._seq.index(value, start, stop)

    def count(self, value: int) -> int:
        """the sequence interface count method."""
        return self._seq.count(value)

    # --- compatibility with the fsspec info dict interface ------------

    def __getitem__(self, item: int | str) -> Any:
        if isinstance(item, str):
            warnings.warn(
                "Access the fsspec info via `.as_info()[key]`",
                DeprecationWarning,
                stacklevel=2,
            )
            return self._info[item]
        # we need to go via the attributes and cast to int
        attr = self._fields[item]
        return int(getattr(self, attr))

    def keys(self):
        """compatibility with the fsspec info dict interface."""
        warnings.warn(
            "Access the fsspec info via `.as_info().keys()`",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._info.keys()

    def values(self):
        """compatibility with the fsspec info dict interface."""
        warnings.warn(
            "Access the fsspec info via `.as_info().values()`",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._info.values()

    def items(self):
        """compatibility with the fsspec info dict interface."""
        warnings.warn(
            "Access the fsspec info via `.as_info().items()`",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._info.items()

    def get(self, key, default=None):
        """compatibility with the fsspec info dict interface."""
        warnings.warn(
            "Access the fsspec info via `.as_info().get(key, default)`",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._info.get(key, default)

    def copy(self):
        """compatibility with the fsspec info dict interface."""
        warnings.warn(
            "Access the fsspec info via `.as_info().copy()`",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._info.copy()
