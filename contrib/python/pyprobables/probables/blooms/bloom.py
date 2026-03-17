"""BloomFilter and BloomFiter on Disk, python implementation
License: MIT
Author: Tyler Barrus (barrust@gmail.com)
URL: https://github.com/barrust/bloom
"""

import math
import os
from array import array
from binascii import hexlify, unhexlify
from collections.abc import ByteString
from io import BufferedRandom, BytesIO, IOBase
from mmap import mmap
from numbers import Number
from pathlib import Path
from shutil import copyfile
from struct import Struct
from textwrap import wrap
from typing import Union

from probables.exceptions import InitializationError, NotSupportedError, SimilarityError
from probables.hashes import HashFuncT, HashResultsT, KeyT, default_fnv_1a
from probables.utilities import MMap, is_hex_string, is_valid_file, resolve_path

MISMATCH_MSG = "The parameter second must be of type BloomFilter or a BloomFilterOnDisk"

SimpleBloomT = Union["BloomFilter", "BloomFilterOnDisk"]


def _verify_not_type_mismatch(second: SimpleBloomT) -> bool:
    """verify that there is not a type mismatch"""
    return isinstance(second, BloomFilter | BloomFilterOnDisk)


class BloomFilter:
    """Simple Bloom Filter implementation for use in python; It can read and write the
    same format as the c version (https://github.com/barrust/bloom)

    Args:
        est_elements (int): The number of estimated elements to be added
        false_positive_rate (float): The desired false positive rate
        filepath (str): Path to file to load
        hex_string (str): Hex based representation to be loaded
        hash_function (function): Hashing strategy function to use `hf(key, number)`
    Returns:
        BloomFilter: A Bloom Filter object
    Note:
        Initialization order of operations:
            1) From file
            2) From Hex String
            3) From params
    """

    __slots__ = (
        "_on_disk",
        "_type",
        "_typecode",
        "_bits_per_elm",
        "_bloom",
        "_est_elements",
        "_fpr",
        "_bloom_length",
        "_hash_func",
        "_els_added",
        "_number_hashes",
        "_num_bits",
    )

    def __init__(
        self,
        est_elements: int | None = None,
        false_positive_rate: float | None = None,
        filepath: str | Path | None = None,
        hex_string: str | None = None,
        hash_function: HashFuncT | None = None,
    ):
        # set some things up
        self._on_disk = False
        self._type = "regular"
        self._typecode = "B"
        self._fpr = 0.0
        self._bloom_length = 0
        self._est_elements = 0
        self._bits_per_elm = 8.0
        self._bloom: array
        self._hash_func: HashFuncT
        self._els_added = 0
        self._number_hashes = 0
        self._num_bits = 0

        self._load_init(filepath, hash_function, hex_string, est_elements, false_positive_rate)

    def _load_init(self, filepath, hash_function, hex_string, est_elements, false_positive_rate):
        """Handle setting params and loading everything as needed"""
        if is_valid_file(filepath):
            self._load(filepath, hash_function)
        elif is_hex_string(hex_string):
            self._load_hex(hex_string, hash_function)
        else:
            if est_elements is None or false_positive_rate is None:
                raise InitializationError("Insufecient parameters to set up the Bloom Filter")
            # calc values
            fpr, n_hashes, n_bits = self._get_optimized_params(est_elements, false_positive_rate)
            self._set_values(est_elements, fpr, n_hashes, n_bits, hash_function)
            self._bloom = array(self._typecode, [0]) * self._bloom_length

    # NOTE: these should be "FOOTERS" and not headers
    _FOOTER_STRUCT = Struct("QQf")
    _FOOTER_STRUCT_BE = Struct(">QQf")
    _FPR_STRUCT = Struct("f")
    _IMPT_STRUCT = Struct("B")

    def __contains__(self, key: KeyT) -> int | bool:
        """setup the `in` keyword"""
        return self.check(key)

    def __str__(self) -> str:
        """output statistics of the bloom filter"""
        on_disk = "no" if self.is_on_disk is False else "yes"
        return (
            "BloomFilter:\n"
            f"\tbits: {self.number_bits}\n"
            f"\testimated elements: {self.estimated_elements}\n"
            f"\tnumber hashes: {self.number_hashes}\n"
            f"\tmax false positive rate: {self.false_positive_rate:.6f}\n"
            f"\tbloom length (8 bits): {self.bloom_length}\n"
            f"\telements added: {self.elements_added}\n"
            f"\testimated elements added: {self.estimate_elements()}\n"
            f"\tcurrent false positive rate: {self.current_false_positive_rate():.6f}\n"
            f"\texport size (bytes): {self.export_size()}\n"
            f"\tnumber bits set: {self._cnt_number_bits_set()}\n"
            f"\tis on disk: {on_disk}\n"
        )

    def __bytes__(self) -> bytes:
        """Export bloom filter to `bytes`"""
        with BytesIO() as f:
            self.export(f)
            return f.getvalue()

    # Some Properties
    @property
    def false_positive_rate(self) -> float:
        """float: The maximum desired false positive rate

        Note:
            Not settable"""
        return self._fpr

    @property
    def estimated_elements(self) -> int:
        """int: The maximum number of elements estimated to be added at setup

        Note:
            Not settable"""
        return self._est_elements

    @property
    def number_hashes(self) -> int:
        """int: The number of hashes required for the Bloom Filter hashing strategy

        Note:
            Not settable"""
        return self._number_hashes

    @property
    def number_bits(self) -> int:
        """int: Number of bits in the Bloom Filter

        Note:
            Not settable"""
        return self._num_bits

    @property
    def elements_added(self) -> int:
        """int: Number of elements added to the Bloom Filter

        Note:
            Changing this can cause the current false positive rate to be reported incorrectly"""
        return self._els_added

    @elements_added.setter
    def elements_added(self, val: int):
        """set the els added"""
        self._els_added = val

    @property
    def is_on_disk(self) -> bool:
        """bool: Is the Bloom Filter on Disk or not

        Note:
            Not settable"""
        return self._on_disk

    @property
    def bloom_length(self) -> int:
        """int: Length of the Bloom Filter array

        Note:
            Not settable"""
        return self._bloom_length

    @property
    def bloom(self) -> array:
        """list(int): The bit/int array"""
        return self._bloom

    @property
    def hash_function(self) -> HashFuncT:
        """function: The hash function used

        Note:
            Not settable"""
        return self._hash_func

    # Working things
    def clear(self) -> None:
        """Clear or reset the Counting Bloom Filter"""
        self._els_added = 0
        for idx in range(self._bloom_length):
            self._bloom[idx] = 0

    def hashes(self, key: KeyT, depth: int | None = None) -> HashResultsT:
        """Return the hashes based on the provided key

        Args:
            key (str): Description of arg1
            depth (int): Number of permutations of the hash to generate; if None, generate `number_hashes`
        Returns:
            List(int): A list of the hashes for the key in int form"""
        tmp = depth if depth is not None else self._number_hashes
        return self._hash_func(key, tmp)

    def add(self, key: KeyT) -> None:
        """Add the key to the Bloom Filter

        Args:
            key (str): The element to be inserted"""
        self.add_alt(self.hashes(key))

    def add_alt(self, hashes: HashResultsT) -> None:
        """Add the element represented by hashes into the Bloom Filter

        Args:
            hashes (list): A list of integers representing the key to insert"""
        for i in range(0, self._number_hashes):
            k = hashes[i] % self._num_bits
            idx = k // 8
            self._bloom[idx] = self._bloom[idx] | (1 << (k % 8))
        self._els_added += 1

    def check(self, key: KeyT) -> bool:
        """Check if the key is likely in the Bloom Filter

        Args:
            key (str): The element to be checked
        Returns:
            bool: True if likely encountered, False if definately not"""
        return self.check_alt(self.hashes(key))

    def check_alt(self, hashes: HashResultsT) -> bool:
        """Check if the element represented by hashes is in the Bloom Filter

        Args:
            hashes (list): A list of integers representing the key to check
        Returns:
            bool: True if likely encountered, False if definately not"""
        for i in range(self._number_hashes):
            k = hashes[i] % self._num_bits
            if (self._bloom[k // 8] & (1 << (k % 8))) == 0:
                return False
        return True

    def export_hex(self) -> str:
        """Export the Bloom Filter as a hex string

        Return:
            str: Hex representation of the Bloom Filter"""
        footer_bytes = self._FOOTER_STRUCT_BE.pack(
            self.estimated_elements,
            self.elements_added,
            self.false_positive_rate,
        )
        bytes_string = hexlify(bytearray(self._bloom[: self.bloom_length])) + hexlify(footer_bytes)
        return str(bytes_string, "utf-8")

    def export(self, file: Path | str | IOBase | mmap) -> None:
        """Export the Bloom Filter to disk

        Args:
            file (str): The file or filepath to which the Bloom Filter will be written."""
        if not isinstance(file, IOBase | mmap):
            file = resolve_path(file)
            with open(file, "wb") as filepointer:
                self.export(filepointer)
        else:
            self._bloom.tofile(file)  # type: ignore
            file.write(
                self._FOOTER_STRUCT.pack(
                    self.estimated_elements,
                    self.elements_added,
                    self.false_positive_rate,
                )
            )

    def export_c_header(self, filename: str | Path) -> None:
        """Export the Bloom Filter to disk as a C header file.

        Args:
            filename (str): The filename to which the Bloom Filter will be written."""
        data = ("  " + line for line in wrap(", ".join(f"0x{e:02x}" for e in bytearray.fromhex(self.export_hex())), 80))
        bloom_type = "standard BloomFilter" if self._type in ["regular", "regular-on-disk"] else "CountingBloomFilter"

        with open(filename, "w", encoding="utf-8") as file:
            print(f"/* BloomFilter Export of a {bloom_type} */", file=file)
            print("#include <inttypes.h>", file=file)
            print("const uint64_t estimated_elements = ", self.estimated_elements, ";", sep="", file=file)
            print("const uint64_t elements_added = ", self.elements_added, ";", sep="", file=file)
            print("const float false_positive_rate = ", self.false_positive_rate, ";", sep="", file=file)
            print("const uint64_t number_bits = ", self.number_bits, ";", sep="", file=file)
            print("const unsigned int number_hashes = ", self.number_hashes, ";", sep="", file=file)
            print("const unsigned char bloom[] = {", *data, "};", sep="\n", file=file)

    @classmethod
    def frombytes(cls, b: ByteString, hash_function: HashFuncT | None = None) -> "BloomFilter":
        """
        Args:
            b (ByteString): The bytes to load as a Bloom Filter
            hash_function (function): Hashing strategy function to use `hf(key, number)`
        Returns:
            BloomFilter: A Bloom Filter object
        """
        offset = cls._FOOTER_STRUCT.size
        est_els, els_added, fpr, _, _ = cls._parse_footer(cls._FOOTER_STRUCT, bytes(b[-1 * offset :]))
        blm = BloomFilter(est_elements=est_els, false_positive_rate=fpr, hash_function=hash_function)
        blm._load(b, hash_function=blm.hash_function)
        blm._els_added = els_added
        return blm

    def estimate_elements(self) -> int:
        """Estimate the number of unique elements added

        Returns:
            int: Number of elements estimated to be inserted
        Note:
            Returns -1 if all bits in the Bloom filter are set"""
        setbits = self._cnt_number_bits_set()
        if setbits >= self.number_bits:
            return -1  # not sure this is the "best", but it would signal something is wrong
        log_n = math.log(1 - (float(setbits) / float(self.number_bits)))
        tmp = float(self.number_bits) / float(self.number_hashes)
        return int(-1 * tmp * log_n)

    def export_size(self) -> int:
        """Calculate the size of the bloom on disk

        Returns:
            int: Size of the Bloom Filter when exported to disk"""
        return (self.bloom_length * self._IMPT_STRUCT.size) + self._FOOTER_STRUCT.size

    def current_false_positive_rate(self) -> float:
        """Calculate the current false positive rate based on elements added

        Return:
            float: The current false positive rate"""
        num = self.number_hashes * -1 * self.elements_added
        dbl = num / self.number_bits
        exp = math.exp(dbl)
        return math.pow((1 - exp), self.number_hashes)

    def intersection(self, second: SimpleBloomT) -> SimpleBloomT:
        """Return a new Bloom Filter that contains the intersection of the
        two

        Args:
            second (BloomFilter): The Bloom Filter with which to take the intersection
        Returns:
            BloomFilter: The new Bloom Filter containing the intersection
        Raises:
            TypeError: When second is not either a :class:`BloomFilter` or :class:`BloomFilterOnDisk`
            SimilarityError: When second is not of the same size (false_positive_rate and est_elements)
        Note:
            `second` may be a BloomFilterOnDisk object"""
        if not _verify_not_type_mismatch(second):
            raise TypeError(MISMATCH_MSG)

        if self._verify_bloom_similarity(second) is False:
            raise SimilarityError("Bloom Filters are not similar")

        res = BloomFilter(
            self.estimated_elements,
            self.false_positive_rate,
            hash_function=self.hash_function,
        )

        for i in range(0, res.bloom_length):
            res._bloom[i] = self._get_element(i) & second._get_element(i)
        res.elements_added = res.estimate_elements()
        return res

    def union(self, second: SimpleBloomT) -> "BloomFilter":
        """Return a new Bloom Filter that contains the union of the two

        Args:
            second (BloomFilter): The Bloom Filter with which to calculate the union
        Returns:
            BloomFilter: The new Bloom Filter containing the union
        Raises:
            TypeError: When second is not either a :class:`BloomFilter` or :class:`BloomFilterOnDisk`
            SimilarityError: When second is not of the same size (false_positive_rate and est_elements)
        Note:
            `second` may be a BloomFilterOnDisk object"""
        if not _verify_not_type_mismatch(second):
            raise TypeError(MISMATCH_MSG)

        if self._verify_bloom_similarity(second) is False:
            raise SimilarityError("Bloom Filters are not similar")

        res = BloomFilter(
            self.estimated_elements,
            self.false_positive_rate,
            hash_function=self.hash_function,
        )

        for i in range(self.bloom_length):
            res._bloom[i] = self._get_element(i) | second._get_element(i)
        res.elements_added = res.estimate_elements()
        return res

    def jaccard_index(self, second: SimpleBloomT) -> float:
        """Calculate the jaccard similarity score between two Bloom Filters

        Args:
            second (BloomFilter): The Bloom Filter to compare with
        Returns:
            float: A numeric value between 0 and 1 where 1 is identical and 0 means completely different
        Raises:
            TypeError: When second is not either a :class:`BloomFilter` or :class:`BloomFilterOnDisk`
            SimilarityError: When second is not of the same size (false_positive_rate and est_elements)
        Note:
            `second` may be a BloomFilterOnDisk object"""
        if not _verify_not_type_mismatch(second):
            raise TypeError(MISMATCH_MSG)

        if self._verify_bloom_similarity(second) is False:
            raise SimilarityError("Bloom Filters are not similar")

        count_union = 0

        count_int = 0
        for i in range(0, self.bloom_length):
            el1 = self._get_element(i)
            el2 = second._get_element(i)
            t_union = el1 | el2
            t_intersection = el1 & el2
            count_union += bin(t_union).count("1")
            count_int += bin(t_intersection).count("1")
        if count_union == 0:
            return 1.0
        return count_int / count_union

    # More private functions
    @classmethod
    def _get_optimized_params(cls, estimated_elements: int, false_positive_rate: float) -> tuple[float, int, int]:
        valid_prms = isinstance(estimated_elements, Number) and estimated_elements > 0
        if not valid_prms:
            msg = "Bloom: estimated elements must be greater than 0"
            raise InitializationError(msg)
        valid_prms = isinstance(false_positive_rate, Number) and 0.0 <= false_positive_rate < 1.0
        if not valid_prms:
            msg = "Bloom: false positive rate must be between 0.0 and 1.0"
            raise InitializationError(msg)

        fpr = cls._FPR_STRUCT.pack(float(false_positive_rate))
        t_fpr = float(cls._FPR_STRUCT.unpack(fpr)[0])  # to mimic the c version!
        # optimal caluclations
        m_bt = math.ceil((-estimated_elements * math.log(t_fpr)) / 0.4804530139182)  # ln(2)^2
        number_hashes = int(round(0.6931471805599453 * m_bt / estimated_elements))  # math.log(2.0)

        if number_hashes == 0:
            raise InitializationError("Bloom: Number hashes is zero; unusable parameters provided")

        return t_fpr, number_hashes, m_bt

    def _set_values(
        self,
        est_els: int,
        fpr: float,
        n_hashes: int,
        n_bits: int,
        hash_func: HashFuncT | None,
    ) -> None:
        self._est_elements = est_els
        self._fpr = fpr
        self._bloom_length = math.ceil(n_bits / self._bits_per_elm)
        if hash_func is not None:
            self._hash_func = hash_func
        else:
            self._hash_func = default_fnv_1a
        self._els_added = 0
        self._number_hashes = n_hashes
        self._num_bits = n_bits

    def _load_hex(self, hex_string: str, hash_function: HashFuncT | None = None) -> None:
        """placeholder for loading from hex string"""
        offset = self._FOOTER_STRUCT_BE.size * 2
        est_els, els_added, fpr, n_hashes, n_bits = self._parse_footer(
            self._FOOTER_STRUCT_BE, unhexlify(hex_string[-offset:])
        )
        self._set_values(est_els, fpr, n_hashes, n_bits, hash_function)
        self._bloom = array(self._typecode, unhexlify(hex_string[:-offset]))
        self._els_added = els_added

    def _load(
        self,
        file: Path | str | IOBase | mmap | ByteString,
        hash_function: HashFuncT | None = None,
    ) -> None:
        """load the Bloom Filter from file or bytes"""
        if not isinstance(file, IOBase | mmap | bytes | bytearray | memoryview):
            file = resolve_path(file)
            with MMap(file) as filepointer:
                self._load(filepointer, hash_function)
        else:
            offset = self._FOOTER_STRUCT.size
            est_els, els_added, fpr, n_hashes, n_bits = self._parse_footer(
                self._FOOTER_STRUCT,
                file[-1 * offset :],  # type: ignore
            )
            self._set_values(est_els, fpr, n_hashes, n_bits, hash_function)
            # now read in the bit array!
            self._parse_bloom_array(file, self._IMPT_STRUCT.size * self.bloom_length)  # type: ignore
            self._els_added = els_added

    @classmethod
    def _parse_footer(cls, stct: Struct, d: ByteString) -> tuple[int, int, float, int, int]:
        """parse footer returning the data: estimated elements, elements added,
        false positive rate, hash function, number hashes, number bits"""
        e_elms, e_added, fpr = stct.unpack_from(bytearray(d))

        est_elements = e_elms
        els_added = e_added
        fpr = float(fpr)
        fpr, n_hashes, n_bits = cls._get_optimized_params(est_elements, fpr)

        return int(est_elements), int(els_added), float(fpr), int(n_hashes), int(n_bits)

    def _parse_bloom_array(self, b: ByteString, offset: int) -> None:
        """parse bytes into the bloom array"""
        self._bloom = array(self._typecode, bytes(b[:offset]))

    def _cnt_number_bits_set(self) -> int:
        """calculate the total number of set bits in the bloom"""
        setbits = 0
        for i in range(0, self.bloom_length):
            setbits += bin(self._bloom[i]).count("1")
        return setbits

    def _get_element(self, idx: int) -> int:
        """wrappper for getting an element from the Bloom Filter!"""
        return self._bloom[idx]

    def _verify_bloom_similarity(self, second: SimpleBloomT) -> bool:
        """can the blooms be used in intersection, union, or jaccard index"""
        hash_match = self.number_hashes != second.number_hashes
        same_bits = self.number_bits != second.number_bits
        next_hash = self.hashes("test") != second.hashes("test")
        return not (hash_match or same_bits or next_hash)


class BloomFilterOnDisk(BloomFilter):
    """Simple Bloom Filter implementation directly on disk for use in python;
    It can read and write the same format as the c version (https://github.com/barrust/bloom)

    Args:
        filepath (str): Path to file to load
        est_elements (int): The number of estimated elements to be added
        false_positive_rate (float): The desired false positive rate
        hex_string (str): Hex based representation to be loaded
        hash_function (function): Hashing strategy function to use \
        `hf(key, number)`
    Returns:
        BloomFilterOnDisk: A Bloom Filter object
    Raises:
        NotSupportedError: Loading using a hex string is not supported
    Note:
        Initialization order of operations:
            1) Esimated elements and false positive rate
            2) From Hex String
            3) Only filepath provided
    """

    __slots__ = ["_filepath", "__file_pointer"]

    def __init__(
        self,
        filepath: str | Path,
        est_elements: int | None = None,
        false_positive_rate: float | None = None,
        hex_string: str | None = None,
        hash_function: HashFuncT | None = None,
    ) -> None:
        # set some things up
        self._filepath = resolve_path(filepath)
        self.__file_pointer: BufferedRandom | None = None
        super().__init__(est_elements, false_positive_rate, filepath, hex_string, hash_function)

    def _load_init(self, filepath, hash_function, hex_string, est_elements, false_positive_rate):
        """Handle setting params and loading everything as needed"""
        self._type = "regular-on-disk"
        self._on_disk = True
        if is_hex_string(hex_string):
            msg = "Loading from hex_string is currently not supported by the on disk Bloom Filter"
            raise NotSupportedError(msg)
        if est_elements is not None and false_positive_rate is not None:
            fpr, n_hashes, n_bits = self._get_optimized_params(est_elements, false_positive_rate)
            self._set_values(est_elements, fpr, n_hashes, n_bits, hash_function)

            with open(self._filepath, "wb") as filepointer:
                (array(self._typecode, [0]) * self.bloom_length).tofile(filepointer)
                filepointer.write(self._FOOTER_STRUCT.pack(est_elements, 0, false_positive_rate))
                filepointer.flush()
            self._load(self._filepath, hash_function)
        elif is_valid_file(self._filepath):
            self._load(self._filepath, hash_function)
        else:
            raise InitializationError("Insufecient parameters to set up the On Disk Bloom Filter")

    def __del__(self) -> None:
        """handle if user doesn't close the on disk Bloom Filter"""
        self.close()

    def __bytes__(self) -> bytes:
        return bytes(self._bloom)

    def close(self) -> None:
        """Clean up the BloomFilterOnDisk object"""
        if self.__file_pointer is not None and not self.__file_pointer.closed:
            self.__update()
            self._bloom.close()  # type: ignore
            self.__file_pointer.close()
            self.__file_pointer = None

    def export(self, file: str | Path) -> None:  # type: ignore
        """Export to disk if a different location

        Args:
            file (str|Path): The filename to which the Bloom Filter will be exported
        Note:
            Only exported if the filename is not the original filename"""
        self.__update()
        if file and Path(file) != self._filepath:
            copyfile(self._filepath, str(file))
        # otherwise, nothing to do!

    def _load(self, file: str | Path, hash_function: HashFuncT | None = None):  # type: ignore
        """load the Bloom Filter on disk"""
        # read the file, set the optimal params
        # mmap everything
        file = resolve_path(file)
        with open(file, "r+b") as filepointer:
            offset = self._FOOTER_STRUCT.size
            filepointer.seek(offset * -1, os.SEEK_END)
            est_els, _, fpr = self._FOOTER_STRUCT.unpack_from(filepointer.read(offset))

            fpr, n_hashes, n_bits = self._get_optimized_params(est_els, fpr)
            self._set_values(est_els, fpr, n_hashes, n_bits, hash_function)
        # setup a few additional items
        self.__file_pointer = open(file, "r+b")  # noqa: SIM115
        self._bloom = mmap(self.__file_pointer.fileno(), 0)  # type: ignore
        self._on_disk = True

    def add_alt(self, hashes: HashResultsT) -> None:
        super().add_alt(hashes)
        self.__update()

    @classmethod
    def frombytes(cls, b: ByteString, hash_function: HashFuncT | None = None) -> "BloomFilterOnDisk":
        """
        Raises: NotSupportedError
        """
        msg = "Loading from bytes is currently not supported by the on disk Bloom Filter"
        raise NotSupportedError(msg)

    _EXPECTED_ELM_STRUCT = Struct("Q")
    _UPDATE_OFFSET = Struct("Qf")

    def _get_element(self, idx: int) -> int:
        """wrappper to use similar functions always!"""
        return int(self._IMPT_STRUCT.unpack(bytes([self._bloom[idx]]))[0])

    def __update(self):
        """update the on disk Bloom Filter and ensure everything is out to disk"""
        self._bloom.flush()  # type: ignore
        self.__file_pointer.seek(-1 * self._UPDATE_OFFSET.size, os.SEEK_END)  # type: ignore
        self.__file_pointer.write(self._EXPECTED_ELM_STRUCT.pack(self.elements_added))  # type: ignore
        self.__file_pointer.flush()  # type: ignore
