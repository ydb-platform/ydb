"""Utility Functions"""

import math
import mmap
import string
from array import array
from io import IOBase
from pathlib import Path
from struct import Struct
from typing import Literal


def is_hex_string(hex_string: str | None) -> bool:
    """check if the passed in string is really hex"""
    if hex_string is None:
        return False
    return all(c in string.hexdigits for c in hex_string)


def is_valid_file(filepath: str | Path | None) -> bool:
    """check if the passed filepath points to a real file"""
    if filepath is None:
        return False
    return Path(filepath).exists()


def resolve_path(filepath: str | Path) -> Path:
    """fully resolve the path by expanding user and resolving"""
    return Path(filepath).expanduser().resolve()


def get_x_bits(num: int, max_bits: int, num_bits: int, right_bits: bool = True) -> int:
    """ensure the correct bits are pulled from num"""
    if right_bits:
        return num & (2**num_bits - 1)
    return ((1 << num_bits) - 1) & (num >> (max_bits - num_bits))


class MMap:
    """Simplified mmap.mmap class"""

    __slots__ = ("__p", "__f", "__m", "_closed")

    def __init__(self, path: Path | str):
        self.__p = Path(path)
        self.__f = self.path.open("rb")  # noqa: SIM115
        self.__m = mmap.mmap(self.__f.fileno(), 0, access=mmap.ACCESS_READ)
        self._closed = False

    def __enter__(self) -> mmap.mmap:
        return self.__m

    def __exit__(self, *args, **kwargs) -> None:
        if self.__m and not self.map.closed:
            self.map.close()
        if self.__f:
            self.__f.close()
        self._closed = True

    @property
    def closed(self) -> bool:
        """Is the MMap closed"""
        return self._closed

    @property
    def map(self) -> mmap.mmap:
        """Return a pointer to the mmap"""
        return self.__m

    @property
    def path(self) -> Path:
        """Return the path to the mmap'd file"""
        return self.__p

    def close(self) -> None:
        """Close the MMap class includeing cleaning up open files, etc"""
        self.__exit__()

    def seek(self, pos: int, whence: Literal[0, 1, 2]) -> None:
        """Implement a method to seek on top of the MMap class"""
        self.__m.seek(pos, whence)

    def read(self, n: int = -1) -> bytes:
        """Implement a method to read from the file on top of the MMap class"""
        return self.__m.read(n)


class Bitarray:
    """Simplified, pure python bitarray implementation using as little memory as possible

    Args:
        size (int): The number of bits in the bitarray
    Returns:
        Bitarray: A bitarray
    Raises:
        TypeError:
        ValueError:"""

    def __init__(self, size: int):
        if not isinstance(size, int):
            raise TypeError(f"Bitarray size must be an int; {type(size)} was provided")
        if size <= 0:
            raise ValueError(f"Bitarray size must be larger than 1; {size} was provided")
        self._size_bytes = math.ceil(size / 8)
        self._bitarray = array("B", [0]) * self._size_bytes
        self._size = size

    @property
    def size_bytes(self) -> int:
        """The size of the bitarray in bytes"""
        return self._size_bytes

    @property
    def size(self) -> int:
        """The number of bits in the bitarray"""
        return self._size

    @property
    def bitarray(self) -> array:
        """The bitarray"""
        return self._bitarray

    def __getitem__(self, key: int) -> int:
        return self.check_bit(key)

    def __setitem__(self, idx: int, val: int):
        if val < 0 or val > 1:
            raise ValueError("Invalid bit setting; must be 0 or 1")
        if idx < 0 or idx >= self._size:
            raise IndexError(f"Bitarray index outside of range; index {idx} was provided")
        b = idx // 8
        if val == 1:
            self._bitarray[b] = self._bitarray[b] | (1 << (idx % 8))
        else:
            self._bitarray[b] = self._bitarray[b] & ~(1 << (idx % 8))

    def check_bit(self, idx: int) -> int:
        """Check if the bit idx is set

        Args:
            idx (int): The index to check
        Returns:
            int: The status of the bit, either 0 or 1"""
        if idx < 0 or idx >= self._size:
            raise IndexError(f"Bitarray index outside of range; index {idx} was provided")
        return 0 if (self._bitarray[idx // 8] & (1 << (idx % 8))) == 0 else 1

    def is_bit_set(self, idx: int) -> bool:
        """Check if the bit idx is set

        Args:
            idx (int): The index to check
        Returns:
            int: The status of the bit, either 0 or 1"""
        return bool(self.check_bit(idx))

    def set_bit(self, idx: int) -> None:
        """Set the bit at idx to 1

        Args:
            idx (int): The index to set"""
        if idx < 0 or idx >= self._size:
            raise IndexError(f"Bitarray index outside of range; index {idx} was provided")
        b = idx // 8
        self._bitarray[b] = self._bitarray[b] | (1 << (idx % 8))

    def clear_bit(self, idx: int) -> None:
        """Set the bit at idx to 0

        Args:
            idx (int): The index to clear"""
        if idx < 0 or idx >= self._size:
            raise IndexError(f"Bitarray index outside of range; index {idx} was provided")
        b = idx // 8
        self._bitarray[b] = self._bitarray[b] & ~(1 << (idx % 8))

    def clear(self):
        """Clear all bits in the bitarray"""
        for i in range(self._size_bytes):
            self._bitarray[i] = 0

    def as_string(self):
        """String representation of the bitarray

        Returns:
            str: Bitarray representation as a string"""
        return "".join(str(self.check_bit(x)) for x in range(self._size))

    def num_bits_set(self) -> int:
        """Number of bits set in the bitarray

        Returns:
            int: Number of bits set"""
        return sum(self.check_bit(x) for x in range(self._size))

    _BITARRAY_FOOTER = Struct("Q")
    _BITARRAY_BIN = Struct("B")

    def to_bytes(self) -> bytes:
        """Convert the bitarray to bytes

        Returns:
            bytes: Bitarray representation as bytes"""
        footer = self._BITARRAY_FOOTER.pack(self._size)
        return self._bitarray.tobytes() + footer

    @classmethod
    def from_bytes(cls, data: bytes) -> "Bitarray":
        """Convert bytes to a bitarray

        Args:
            data (bytes): Bytes to convert"""
        size = cls._BITARRAY_FOOTER.unpack(data[-8:])[0]
        bitarray = array("B", data[:-8])
        ba = Bitarray(size)
        ba._bitarray = bitarray
        return ba

    def export(self, file: Path | str | IOBase | mmap.mmap) -> None:
        """Export the bitarray to a file

        Args:
            filename (str): Filename to export to"""
        if not isinstance(file, IOBase | mmap.mmap):
            file = resolve_path(file)
            with open(file, "wb") as filepointer:
                self.export(filepointer)
        else:
            file.write(self.to_bytes())
