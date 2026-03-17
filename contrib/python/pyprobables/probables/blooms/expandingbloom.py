"""Expanding and Rotating BloomFilter, python implementations
License: MIT
Author: Tyler Barrus (barrust@gmail.com)
URL: https://github.com/barrust/pyprobables
"""

from array import array
from collections.abc import ByteString
from io import BytesIO, IOBase
from mmap import mmap
from pathlib import Path
from struct import Struct

from probables.blooms.bloom import BloomFilter
from probables.exceptions import RotatingBloomFilterError
from probables.hashes import HashFuncT, HashResultsT, KeyT, default_fnv_1a
from probables.utilities import MMap, is_valid_file, resolve_path


class ExpandingBloomFilter:
    """Simple expanding Bloom Filter implementation for use in python; the
    Bloom Fiter will automatically expand, or grow, if the false
    positive rate is about to become greater than the desired false
    positive rate.

    Args:
        est_elements (int): The number of estimated elements to be added
        false_positive_rate (float): The desired false positive rate
        filepath (str): Path to file to load
        hash_function (function): Hashing strategy function to use `hf(key, number)`
    Returns:
        ExpandingBloomFilter: An expanding Bloom Filter object
    Note:
        Initialization order of operations:
            1) Filepath
            2) est_elements and false_positive_rate"""

    __slots__ = (
        "_blooms",
        "__fpr",
        "__est_elements",
        "__hash_func",
        "_added_elements",
    )

    def __init__(
        self,
        est_elements: int | None = None,
        false_positive_rate: float | None = None,
        filepath: str | Path | None = None,
        hash_function: HashFuncT | None = None,
    ):
        """initialize"""
        self._blooms = []  # type: ignore
        self.__fpr = false_positive_rate if false_positive_rate is not None else 0.0
        self.__est_elements = est_elements if est_elements is not None else 100
        self.__hash_func: HashFuncT
        self._added_elements = 0  # total added...

        if hash_function is not None:
            self.__hash_func = hash_function
        else:
            self.__hash_func = default_fnv_1a

        if filepath is not None and is_valid_file(filepath):
            self.__load(filepath)
        else:
            # add in the initial bloom filter!
            self.__add_bloom_filter()

    __FOOTER_STRUCT = Struct("QQQf")
    __S_INT64_STRUCT = Struct("Q")
    _BLOOM_ELEMENT_SIZE = Struct("B").size

    @classmethod
    def frombytes(cls, b: ByteString, hash_function: HashFuncT | None = None) -> "ExpandingBloomFilter":
        """
        Args:
            b (ByteString): The bytes to load as a Expanding Bloom Filter
            hash_function (function): Hashing strategy function to use `hf(key, number)`
        Returns:
            ExpandingBloomFilter: A Bloom Filter object
        """
        size, est_els, added_els, fpr = cls._parse_footer(b)
        blm = ExpandingBloomFilter(est_elements=est_els, false_positive_rate=fpr, hash_function=hash_function)
        blm._parse_blooms(b, size)
        blm._added_elements = added_els
        return blm

    def __contains__(self, key: KeyT) -> bool:
        """setup the `in` functionality"""
        return self.check(key)

    def __bytes__(self) -> bytes:
        """Export bloom filter to `bytes`"""

        with BytesIO() as f:
            self.export(f)
            return f.getvalue()

    @property
    def expansions(self) -> int:
        """int: The number of expansions"""
        return len(self._blooms) - 1

    @property
    def false_positive_rate(self) -> float:
        """float: The desired false positive rate of the expanding Bloom Filter"""
        return self.__fpr

    @property
    def estimated_elements(self) -> int:
        """int: The original number of elements estimated to be in the Bloom Filter"""
        return self.__est_elements

    @property
    def elements_added(self) -> int:
        """int: The total number of elements added"""
        return self._added_elements

    @property
    def hash_function(self) -> HashFuncT:
        """int: The total number of elements added"""
        return self.__hash_func

    def push(self) -> None:
        """Push a new expansion onto the Bloom Filter"""
        self.__add_bloom_filter()

    def check(self, key: KeyT) -> bool:
        """Check to see if the key is in the Bloom Filter

        Args:
            key (str): The key to check for in the Bloom Filter
        Returns:
            bool: `True` if the element is likely present; `False` if definately not present"""
        hashes = self._blooms[0].hashes(key)
        return self.check_alt(hashes)

    def check_alt(self, hashes: HashResultsT) -> bool:
        """Check to see if the hashes are in the Bloom Filter

        Args:
            hashes (list): The hash representation to check for in the Bloom Filter
        Returns:
            bool: `True` if the element is likely present; `False` if definately not present"""
        return any(blm.check_alt(hashes) for blm in self._blooms)

    def add(self, key: KeyT, force: bool = False) -> None:
        """Add the key to the Bloom Filter

        Args:
            key (str): The element to be inserted
            force (bool): `True` will force it to be inserted, even if it likely has been inserted \
                before `False` will only insert if not found in the Bloom Filter"""
        hashes = self._blooms[0].hashes(key)
        self.add_alt(hashes, force)

    def add_alt(self, hashes: HashResultsT, force: bool = False) -> None:
        """Add the element represented by hashes into the Bloom Filter

        Args:
            hashes (list): A list of integers representing the key to insert
            force (bool): `True` will force it to be inserted, even if it likely has been inserted \
                before `False` will only insert if not found in the Bloom Filter"""
        self._added_elements += 1
        if force or not self.check_alt(hashes):
            self.__check_for_growth()
            self._blooms[-1].add_alt(hashes)

    def __add_bloom_filter(self):
        """build a new bloom and add it on!"""
        blm = BloomFilter(
            est_elements=self.__est_elements,
            false_positive_rate=self.__fpr,
            hash_function=self.__hash_func,
        )
        self._blooms.append(blm)

    def __check_for_growth(self):
        """detereming if the bloom filter should automatically grow"""
        if self._blooms[-1].elements_added >= self.__est_elements:
            self.__add_bloom_filter()

    def export(self, file: Path | str | IOBase | mmap) -> None:
        """Export an expanding Bloom Filter, or subclass, to disk

        Args:
            filepath (str): The path to the file to import"""
        if not isinstance(file, IOBase | mmap):
            file = resolve_path(file)
            with open(file, "wb") as filepointer:
                self.export(filepointer)  # type:ignore
        else:
            filepointer = file  # type:ignore
            # add all the different Bloom bit arrays...
            for blm in self._blooms:
                filepointer.write(self.__S_INT64_STRUCT.pack(blm.elements_added))
                blm.bloom.tofile(filepointer)
            filepointer.write(
                self.__FOOTER_STRUCT.pack(
                    len(self._blooms),
                    self.estimated_elements,
                    self.elements_added,
                    self.false_positive_rate,
                )
            )

    def __load(self, file: Path | str | IOBase | mmap):
        """load a file"""
        if not isinstance(file, IOBase | mmap):
            file = resolve_path(file)
            with MMap(file) as filepointer:
                self.__load(filepointer)
        else:
            size, est_els, els_added, fpr = self._parse_footer(file)  # type: ignore
            self._blooms = []
            self._added_elements = els_added
            self.__fpr = fpr
            self.__est_elements = est_els
            self._parse_blooms(file, size)  # type:ignore

    @classmethod
    def _parse_footer(cls, b: ByteString) -> tuple[int, int, int, float]:
        offset = cls.__FOOTER_STRUCT.size
        size, est_els, els_added, fpr = cls.__FOOTER_STRUCT.unpack(bytes(b[-1 * offset :]))
        return int(size), int(est_els), int(els_added), float(fpr)

    def _parse_blooms(self, b: ByteString, size: int) -> None:
        # reset the bloom list
        self._blooms = []
        blm_size = 0
        start = 0
        end = 0
        for _ in range(size):
            blm = BloomFilter(
                est_elements=self.__est_elements,
                false_positive_rate=self.__fpr,
                hash_function=self.__hash_func,
            )
            if blm_size == 0:
                blm_size = self._BLOOM_ELEMENT_SIZE * blm.bloom_length
            end = start + self.__S_INT64_STRUCT.size + blm_size
            blm._els_added = int(self.__S_INT64_STRUCT.unpack(bytes(b[start : start + self.__S_INT64_STRUCT.size]))[0])
            blm._bloom = array("B", bytes(b[start + self.__S_INT64_STRUCT.size : end]))
            self._blooms.append(blm)
            start = end


class RotatingBloomFilter(ExpandingBloomFilter):
    """Simple Rotating Bloom Filter implementation that allows for the "older"
    elements added to be removed, in chunks. As the queue fills up, those
    elements inserted earlier will be bulk removed. This also provides the
    user with the oportunity to force the removal instead of it being time
    based.

    Args:
        est_elements (int): The number of estimated elements to be added
        false_positive_rate (float): The desired false positive rate
        max_queue_size (int): This is the number is used to determine the maximum number of Bloom Filters. \
            Total elements added is based on `max_queue_size * est_elements`
        filepath (str): Path to file to load
        hash_function (function): Hashing strategy function to use `hf(key, number)`
    Note:
        Initialization order of operations:
            1) Filepath
            2) est_elements and false_positive_rate
    """

    __slots__ = ("_queue_size",)

    def __init__(
        self,
        est_elements: int | None = None,
        false_positive_rate: float | None = None,
        max_queue_size: int = 10,
        filepath: str | Path | None = None,
        hash_function: HashFuncT | None = None,
    ) -> None:
        """initialize"""
        super().__init__(
            est_elements=est_elements,
            false_positive_rate=false_positive_rate,
            filepath=filepath,
            hash_function=hash_function,
        )
        self._queue_size = max_queue_size

    @classmethod
    def frombytes(  # type:ignore
        cls, b: ByteString, max_queue_size: int, hash_function: HashFuncT | None = None
    ) -> "RotatingBloomFilter":
        """
        Args:
            b (ByteString): The bytes to load as a Expanding Bloom Filter
            max_queue_size (int): This is the number is used to determine the maximum number \
                of Bloom Filters. Total elements added is based on `max_queue_size * est_elements`
            hash_function (function): Hashing strategy function to use `hf(key, number)`
        Returns:
            RotatingBloomFilter: A Bloom Filter object
        """
        size, est_els, added_els, fpr = cls._parse_footer(b)
        blm = RotatingBloomFilter(
            est_elements=est_els, false_positive_rate=fpr, max_queue_size=max_queue_size, hash_function=hash_function
        )
        blm._parse_blooms(b, size)
        blm._added_elements = added_els
        return blm

    @property
    def max_queue_size(self) -> int:
        """int: The maximum size for the queue"""
        return self._queue_size

    @property
    def current_queue_size(self) -> int:
        """int: The current size of the queue"""
        return len(self._blooms)

    def add_alt(self, hashes: HashResultsT, force: bool = False) -> None:
        """Add the element represented by hashes into the Bloom Filter

        Args:
            hashes (list): A list of integers representing the key to insert
            force (bool): `True` will force it to be inserted, even if it likely has been inserted \
                before `False` will only insert if not found in the Bloom Filter"""
        self._added_elements += 1
        if force or not self.check_alt(hashes):
            self.__rotate_bloom_filter()
            self._blooms[-1].add_alt(hashes)

    def pop(self) -> None:
        """Pop the oldest Bloom Filter off of the queue without pushing a new
        Bloom Filter onto the queue

        Raises:
            RotatingBloomFilterError: Unable to rotate the Bloom Filter"""
        if self.current_queue_size == 1:
            msg = "Popping a Bloom Filter will result in an unusable system!"
            raise RotatingBloomFilterError(msg)
        self._blooms.pop(0)

    def push(self) -> None:
        """Push a new bloom filter onto the queue and rotate if necessary"""
        self.__rotate_bloom_filter(force=True)

    def __rotate_bloom_filter(self, force: bool = False):
        """handle determining if/when the Bloom Filter queue needs to be rotated"""
        blm = self._blooms[-1]
        ready_to_rotate = blm.elements_added == blm.estimated_elements
        no_need_to_pop = self.current_queue_size < self._queue_size
        if force and no_need_to_pop:
            self.__add_bloom_filter()
        elif force:  # must need to be pop'd first!
            blm = self._blooms.pop(0)
            self.__add_bloom_filter()
        elif ready_to_rotate and no_need_to_pop:
            self.__add_bloom_filter()
        elif ready_to_rotate:
            blm = self._blooms.pop(0)
            self.__add_bloom_filter()

    def __add_bloom_filter(self):
        """build a new bloom and add it on!"""
        blm = BloomFilter(
            est_elements=self.estimated_elements,
            false_positive_rate=self.false_positive_rate,
            hash_function=self.hash_function,
        )
        self._blooms.append(blm)
