"""Counting Cuckoo Filter, python implementation
License: MIT
Author: Tyler Barrus (barrust@gmail.com)
"""

import random
from array import array
from collections.abc import ByteString
from io import IOBase
from mmap import mmap
from pathlib import Path
from struct import Struct
from typing import Union

from probables.cuckoo.cuckoo import CuckooFilter
from probables.exceptions import CuckooFilterFullError
from probables.hashes import KeyT, SimpleHashT
from probables.utilities import MMap, resolve_path


class CountingCuckooFilter(CuckooFilter):
    """Simple Counting Cuckoo Filter implementation

    Args:
        capacity (int): The number of bins
        bucket_size (int): The number of buckets per bin
        max_swaps (int): The number of cuckoo swaps before stopping
        expansion_rate (int): The rate at which to expand
        auto_expand (bool): If the filter should automatically expand
        finger_size (int): The size of the fingerprint to use in bytes \
            (between 1 and 4); exported as 4 bytes; up to the user to \
            reset the size correctly on import
        filename (str): The path to the file to load or None if no file
    Returns:
        CountingCuckooFilter: A Cuckoo Filter object"""

    __slots__ = ("__unique_elements",)

    def __init__(
        self,
        capacity: int = 10000,
        bucket_size: int = 4,
        max_swaps: int = 500,
        expansion_rate: int = 2,
        auto_expand: bool = True,
        finger_size: int = 4,
        filepath: str | Path | None = None,
        hash_function: SimpleHashT | None = None,
    ) -> None:
        """setup the data structure"""
        self.__unique_elements = 0
        super().__init__(
            capacity,
            bucket_size,
            max_swaps,
            expansion_rate,
            auto_expand,
            finger_size,
            filepath,
            hash_function,
        )

    __COUNTING_CUCKOO_FOOTER_STRUCT = Struct("II")
    __BIN_STRUCT = Struct("II")

    @classmethod
    def init_error_rate(
        cls,
        error_rate: float,
        capacity: int = 10000,
        bucket_size: int = 4,
        max_swaps: int = 500,
        expansion_rate: int = 2,
        auto_expand: bool = True,
        hash_function: SimpleHashT | None = None,
    ):
        """Initialize a simple Cuckoo Filter based on error rate

        Args:
            error_rate (float):
            capacity (int): The number of bins
            bucket_size (int): The number of buckets per bin
            max_swaps (int): The number of cuckoo swaps before stopping
            expansion_rate (int): The rate at which to expand
            auto_expand (bool): If the filter should automatically expand
            hash_function (function): Hashing strategy function to use `hf(key)`
        Returns:
            CuckooFilter: A Cuckoo Filter object"""
        cku = CountingCuckooFilter(
            capacity=capacity,
            bucket_size=bucket_size,
            auto_expand=auto_expand,
            max_swaps=max_swaps,
            expansion_rate=expansion_rate,
            hash_function=hash_function,
        )
        cku._set_error_rate(error_rate)
        return cku

    @classmethod
    def load_error_rate(cls, error_rate: float, filepath: str | Path, hash_function: SimpleHashT | None = None):
        """Initialize a previously exported Cuckoo Filter based on error rate

        Args:
            error_rate (float):
            filepath (str): The path to the file to load or None if no file
            hash_function (function): Hashing strategy function to use \
            `hf(key)`
        Returns:
            CuckooFilter: A Cuckoo Filter object
        """
        filepath = resolve_path(filepath)
        cku = CountingCuckooFilter(filepath=filepath, hash_function=hash_function)
        cku._set_error_rate(error_rate)
        return cku

    @classmethod
    def frombytes(
        cls, b: ByteString, error_rate: float | None = None, hash_function: SimpleHashT | None = None
    ) -> "CountingCuckooFilter":
        """
        Args:
            b (ByteString): The bytes to load as a Expanding Bloom Filter
            error_rate (float): The error rate of the cuckoo filter, if used to generate the original filter
            hash_function (function): Hashing strategy function to use `hf(key, number)`
        Returns:
            CountingCuckooFilter: A Bloom Filter object"""
        cku = CountingCuckooFilter(hash_function=hash_function)
        cku._load(b)

        # if error rate is provided, use it
        cku._set_error_rate(error_rate)
        return cku

    def __contains__(self, val: KeyT) -> bool:
        """setup the `in` keyword"""
        return self.check(val) > 0

    @property
    def unique_elements(self) -> int:
        """int: unique number of elements inserted"""
        return self.__unique_elements

    @property
    def buckets(self) -> list[list["CountingCuckooBin"]]:  # type: ignore
        """list(list): The buckets holding the fingerprints

        Note:
            Not settable"""
        return self._buckets

    def load_factor(self) -> float:
        """float: How full the Cuckoo Filter is currently"""
        return self.unique_elements / (self.capacity * self.bucket_size)

    def add(self, key: KeyT) -> None:
        """Add element key to the filter

        Args:
            key (str): The element to add
        Raises:
            CuckooFilterFullError: When element not inserted after maximum number of swaps or 'kicks'"""
        idx_1, idx_2, fingerprint = self._generate_fingerprint_info(key)

        is_present = self._check_if_present(idx_1, idx_2, fingerprint)
        if is_present is not None:
            for bucket in self.buckets[is_present]:
                if fingerprint in bucket:
                    bucket.increment()
                    self._inserted_elements += 1
                    return
        finger = self._insert_fingerprint_alt(fingerprint, idx_1, idx_2)
        self._deal_with_insertion(finger)

    def check(self, key: KeyT) -> int:  # type: ignore
        """Check if an element is in the filter

        Args:
            key (str): Element to check
        Returns:
            int: The number of times inserted into the filter"""
        idx_1, idx_2, fingerprint = self._generate_fingerprint_info(key)
        is_present = self._check_if_present(idx_1, idx_2, fingerprint)
        val = 0
        if is_present is not None:
            # get the count out!
            for bucket in self.buckets[is_present]:
                if fingerprint in bucket:
                    val = bucket.count
                    break
        return val

    def remove(self, key: KeyT) -> bool:
        """Remove an element from the filter

        Args:
            key (str): Element to remove"""
        idx_1, idx_2, fingerprint = self._generate_fingerprint_info(key)
        idx = self._check_if_present(idx_1, idx_2, fingerprint)
        if idx is None:
            return False
        for bucket in self.buckets[idx]:
            if fingerprint in bucket:
                bucket.decrement()
                self._inserted_elements -= 1
                if bucket.count == 0:
                    self.buckets[idx].remove(bucket)
                    self.__unique_elements -= 1
                return True
        return False  # catch this...

    def expand(self):
        """Expand the cuckoo filter"""
        self._expand_logic(None)

    def export(self, file: Path | str | IOBase | mmap) -> None:
        """Export cuckoo filter to file

        Args:
            file (str): Path to file to export"""
        if not isinstance(file, IOBase | mmap):
            file = resolve_path(file)
            with open(file, "wb") as filepointer:
                self.export(filepointer)  # type:ignore
        else:
            self.__bucket_decomposition(self.buckets, self.bucket_size).tofile(file)
            # now put out the required information at the end
            file.write(self.__COUNTING_CUCKOO_FOOTER_STRUCT.pack(self.bucket_size, self.max_swaps))

    def _insert_fingerprint_alt(
        self, fingerprint: int, idx_1: int, idx_2: int, count: int = 1
    ) -> Union["CountingCuckooBin", None]:
        """insert a fingerprint, but with a count parameter!"""
        if self.__insert_element(fingerprint, idx_1, count):
            self._inserted_elements += 1
            self.__unique_elements += 1
            return None
        if self.__insert_element(fingerprint, idx_2, count):
            self._inserted_elements += 1
            self.__unique_elements += 1
            return None

        # we didn't insert, so now we need to randomly select one index to use
        # and move things around to the other index, if possible, until we
        # either move everything around or hit the maximum number of swaps
        idx = random.choice([idx_1, idx_2])
        prv_bin = CountingCuckooBin(fingerprint, 1)
        for _ in range(self.max_swaps):
            # select one element to be swapped out...
            swap_elm = random.randint(0, self.bucket_size - 1)
            swap_finger = self.buckets[idx][swap_elm]
            prv_bin, self.buckets[idx][swap_elm] = swap_finger, prv_bin

            # now find another place to put this fingerprint
            index_1, index_2 = self._indicies_from_fingerprint(prv_bin.finger)

            idx = index_2 if idx == index_1 else index_1

            if self.__insert_element(prv_bin.finger, idx, prv_bin.count):
                self._inserted_elements += 1
                self.__unique_elements += 1
                return None

        # if we got here we have an error... we might need to know what is left
        return prv_bin

    def _check_if_present(self, idx_1: int, idx_2: int, fingerprint: int) -> int | None:
        """wrapper for checking if fingerprint is already inserted"""
        if fingerprint in [x.finger for x in self.buckets[idx_1]]:
            return idx_1
        if fingerprint in [x.finger for x in self.buckets[idx_2]]:
            return idx_2
        return None

    def _load(self, file: Path | str | IOBase | mmap | bytes | ByteString) -> None:
        """load a cuckoo filter from file"""
        if not isinstance(file, IOBase | mmap | bytes | bytearray | memoryview):
            file = resolve_path(file)
            with MMap(file) as filepointer:
                self._load(filepointer)
        else:
            self._parse_footer(file, self.__COUNTING_CUCKOO_FOOTER_STRUCT)  # type: ignore
            self._inserted_elements = 0
            self._parse_buckets(file)  # type: ignore

    def _parse_buckets(self, d: ByteString) -> None:
        """Parse bytes to pull out and set the buckets"""
        bin_size = self.__BIN_STRUCT.size
        self._cuckoo_capacity = (len(bytes(d)) - bin_size) // bin_size // self.bucket_size
        start = 0
        end = bin_size
        self._buckets = []
        for i in range(self.capacity):
            self.buckets.append([])
            for _ in range(self.bucket_size):
                finger, count = self.__BIN_STRUCT.unpack(bytes(d[start:end]))
                if finger > 0:
                    ccb = CountingCuckooBin(finger, count)
                    self.buckets[i].append(ccb)
                    self._inserted_elements += count
                    self.__unique_elements += 1
                start = end
                end += bin_size

    def _expand_logic(self, extra_fingerprint: Union["CountingCuckooBin", None]) -> None:
        """the logic to acutally expand the cuckoo filter"""
        # get all the fingerprints
        fingerprints = self._setup_expand(extra_fingerprint)
        self.__unique_elements = 0  # this needs to be reset!

        for elm in fingerprints:
            idx_1, idx_2 = self._indicies_from_fingerprint(elm.finger)
            res = self._insert_fingerprint_alt(elm.finger, idx_1, idx_2, elm.count)
            if res is not None:  # again, this *shouldn't* happen
                msg = "The CountingCuckooFilter failed to expand"
                raise CuckooFilterFullError(msg)

    def __insert_element(self, fingerprint, idx, count=1) -> bool:
        """insert an element"""
        if len(self.buckets[idx]) < self.bucket_size:
            self.buckets[idx].append(CountingCuckooBin(fingerprint, count))
            return True
        return False

    @staticmethod
    def __bucket_decomposition(buckets, bucket_size: int) -> array:
        """convert a list of buckets into a single array for export"""
        arr = array("I")
        for bucket in buckets:
            for buck in bucket:
                arr.extend(buck.get_array())
            leftover = bucket_size - len(bucket)
            arr.fromlist([0 for _ in range(leftover * 2)])
        return arr


class CountingCuckooBin:
    """A container class for the counting cuckoo filter"""

    # keep it lightweight
    __slots__ = ["__bin"]

    def __init__(self, fingerprint: int, count: int) -> None:
        """init"""
        self.__bin = array("I", [fingerprint, count])

    def __contains__(self, val: int) -> bool:
        """setup the `in` construct"""
        return self.__bin[0] == val

    def get_array(self):
        """return the array implementation"""
        return self.__bin

    @property
    def finger(self) -> int:
        """fingerprint property"""
        return self.__bin[0]

    @property
    def count(self) -> int:
        """count property"""
        return self.__bin[1]

    def __repr__(self) -> str:
        """how do we represent this?"""
        return self.__str__()

    def __str__(self) -> str:
        """convert it into a string"""
        return f"(fingerprint:{self.__bin[0]} count:{self.__bin[1]})"

    def increment(self) -> int:
        """increment"""
        self.__bin[1] += 1
        return self.__bin[1]

    def decrement(self) -> int:
        """decrement"""
        self.__bin[1] -= 1
        return self.__bin[1]
