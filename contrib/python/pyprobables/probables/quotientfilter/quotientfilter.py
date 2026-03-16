"""Quotient Filter, python implementation
License: MIT
Author: Tyler Barrus (barrust@gmail.com)
"""

import sys
from array import array
from collections.abc import Iterator
from typing import TextIO

from probables.exceptions import QuotientFilterError
from probables.hashes import KeyT, SimpleHashT, fnv_1a_32
from probables.utilities import Bitarray


class QuotientFilter:
    """Simple Quotient Filter implementation

    Args:
        quotient (int): The size of the quotient to use
        auto_expand (bool): Automatically expand or not
        hash_function (function): Hashing strategy function to use `hf(key, number)`
    Returns:
        QuotientFilter: The initialized filter
    Raises:
        QuotientFilterError: Raised when unable to initialize
    Note:
        The size of the QuotientFilter will be 2**q"""

    __slots__ = (
        "_q",
        "_r",
        "_size",
        "_elements_added",
        "_hash_func",
        "_int_type_code",
        "_bits_per_elm",
        "_is_occupied",
        "_is_continuation",
        "_is_shifted",
        "_filter",
        "_max_load_factor",
        "_auto_resize",
        "__mod_size",
    )

    def __init__(
        self, quotient: int = 20, auto_expand: bool = True, hash_function: SimpleHashT | None = None
    ):  # needs to be parameterized
        if quotient < 3 or quotient > 31:
            raise QuotientFilterError(
                f"Invalid quotient setting; quotient must be between 3 and 31; {quotient} was provided"
            )
        self.__set_params(quotient, auto_expand, hash_function)

    def __set_params(self, quotient: int, auto_expand: bool, hash_function: SimpleHashT | None):
        self._q: int = quotient
        self._r: int = 32 - quotient
        self._size: int = 1 << self._q  # same as 2**q
        self.__mod_size: int = self._size - 1
        self._elements_added: int = 0
        self._auto_resize: bool = auto_expand
        self._hash_func: SimpleHashT = fnv_1a_32 if hash_function is None else hash_function  # type: ignore
        self._max_load_factor: float = 0.85

        # ensure we use the smallest type possible to reduce memory wastage
        if self._r <= 8:
            self._int_type_code = "B"
            self._bits_per_elm = 8
        elif self._r <= 16:
            self._int_type_code = "I"
            self._bits_per_elm = 16
        else:
            self._int_type_code = "L"
            self._bits_per_elm = 32

        self._is_occupied = Bitarray(self._size)
        self._is_continuation = Bitarray(self._size)
        self._is_shifted = Bitarray(self._size)
        self._filter = array(self._int_type_code, [0]) * self._size

    def __contains__(self, val: KeyT) -> bool:
        """setup the `in` keyword"""
        return self.check(val)

    @property
    def quotient(self) -> int:
        """int: The size of the quotient, in bits"""
        return self._q

    @property
    def remainder(self) -> int:
        """int: The size of the remainder, in bits"""
        return self._r

    @property
    def num_elements(self) -> int:
        """int: The total size of the filter"""
        return self._size

    @property
    def elements_added(self) -> int:
        """int: The number of elements added to the filter"""
        return self._elements_added

    @property
    def bits_per_elm(self) -> int:
        """int: The number of bits used per element"""
        return self._bits_per_elm

    @property
    def size(self) -> int:
        """int: The number of bins available in the filter

        Note:
            same as `num_elements`"""
        return self._size

    @property
    def load_factor(self) -> float:
        """float: The load factor of the filter"""
        return self._elements_added / self._size

    @property
    def auto_expand(self) -> bool:
        """bool: Will the quotient filter automatically expand"""
        return self._auto_resize

    @auto_expand.setter
    def auto_expand(self, val: bool):
        """change the auto expand property"""
        self._auto_resize = bool(val)

    @property
    def max_load_factor(self) -> float:
        """float: The maximum allowed load factor after which auto expanding should occur"""
        return self._max_load_factor

    @max_load_factor.setter
    def max_load_factor(self, val: float):
        """set the maximum load factor"""
        self._max_load_factor = float(val)

    def add(self, key: KeyT) -> None:
        """Add key to the quotient filter

        Args:
            key (str|bytes): The element to add
        Raises:
            QuotientFilterError: Raised when no locations are available in which to insert"""
        _hash = self._hash_func(key, 0)
        self.add_alt(_hash)

    def add_alt(self, _hash: int) -> None:
        """Add the pre-hashed value to the quotient filter

        Args:
            _hash (int): The element to add
        Raises:
            QuotientFilterError: Raised when no locations are available in which to insert"""
        if self._auto_resize and self.load_factor >= self._max_load_factor:
            self.resize()
        key_quotient = _hash >> self._r
        key_remainder = _hash & ((1 << self._r) - 1)
        if self._contained_at_loc(key_quotient, key_remainder) == -1:
            self._add(key_quotient, key_remainder)

    def remove(self, key: KeyT) -> None:
        """Remove key from the quotient filter

        Args:
            key (str|bytes): The element to remove
        """
        _hash = self._hash_func(key, 0)
        self.remove_alt(_hash)

    def remove_alt(self, _hash: int) -> None:
        """Remove key from the quotient filter

        Args:
            _hash (int): The element to remove
        """
        key_quotient = _hash >> self._r
        key_remainder = _hash & ((1 << self._r) - 1)
        self._remove_element(key_quotient, key_remainder)

    def check(self, key: KeyT) -> bool:
        """Check to see if key is likely in the quotient filter

        Args:
            key (str|bytes): The element to add
        Return:
            bool: True if likely encountered, False if definately not"""
        _hash = self._hash_func(key, 0)
        return self.check_alt(_hash)

    def check_alt(self, _hash: int) -> bool:
        """Check to see if the pre-calculated hash is likely in the quotient filter

        Args:
            _hash (int): The element to add
        Return:
            bool: True if likely encountered, False if definately not"""
        key_quotient = _hash >> self._r
        key_remainder = _hash & ((1 << self._r) - 1)
        return self._contained_at_loc(key_quotient, key_remainder) != -1

    def hashes(self) -> Iterator[int]:
        """A generator over the hashes in the quotient filter

        Yields:
            int: The next hash stored in the quotient filter"""
        queue: list[int] = []

        # find first empty location
        start = 0
        while not self._is_empty_element(start):
            start += 1

        cur_quot = 0
        for i in range(start, self._size + start):  # this will allow for wrap-arounds
            idx = i % self._size
            is_occupied = self._is_occupied.check_bit(idx)
            is_continuation = self._is_continuation.check_bit(idx)
            is_shifted = self._is_shifted.check_bit(idx)
            # Nothing here, keep going
            if is_occupied + is_continuation + is_shifted == 0:
                assert len(queue) == 0
                continue

            if is_occupied == 1:  # keep track of the indicies that match a hashed quotient
                queue.append(idx)

            #  run start
            if self._is_run_start(idx):
                cur_quot = queue.pop(0)

            yield (cur_quot << self._r) + self._filter[idx]

    def get_hashes(self) -> list[int]:
        """Get the hashes from the quotient filter as a list

        Returns:
            list(int): The hash values stored in the quotient filter"""
        return list(self.hashes())

    def resize(self, quotient: int | None = None) -> None:
        """Resize the quotient filter to use the new quotient size

        Args:
            quotient (int): The new quotient to use
        Note:
            If `None` is provided, the quotient filter will double in size (quotient + 1)
        Raises:
            QuotientFilterError: When the new quotient will not accommodate the elements already added"""
        if quotient is None:
            quotient = self._q + 1

        if self.elements_added >= (1 << quotient):
            raise QuotientFilterError("Unable to shrink since there will be too many elements in the quotient filter")
        if quotient < 3 or quotient > 31:
            raise QuotientFilterError(
                f"Invalid quotient setting; quotient must be between 3 and 31; {quotient} was provided"
            )

        hashes = self.get_hashes()

        for i in range(self._size):
            self._filter[i] = 0

        self.__set_params(quotient, self._auto_resize, self._hash_func)

        for _h in hashes:
            self.add_alt(_h)

    def merge(self, second: "QuotientFilter") -> None:
        """Merge the `second` quotient filter into the first

        Args:
            second (QuotientFilter): The quotient filter to merge
        Note:
            The hashing function between the two filters should match
        Note:
            Errors can occur if the quotient filter being inserted into does not expand (i.e., auto_expand=False)"""
        if self._hash_func("test", 0) != second._hash_func("test", 0):
            raise QuotientFilterError("Hash functions do not match")

        for _h in second.hashes():
            self.add_alt(_h)

    def _shift_insert(self, q: int, r: int, orig_idx: int, insert_idx: int, flag: int):
        """Insert the element q and r by shifting elements as needed"""
        if self._is_empty_element(insert_idx):
            self._filter[insert_idx] = r
            self._is_occupied[q] = 1
            self._is_continuation[insert_idx] = 1 if insert_idx != orig_idx else 0
            self._is_shifted[insert_idx] = 1 if insert_idx != q else 0

        else:
            next_idx = (insert_idx + 1) & self.__mod_size

            while True:
                was_empty = self._is_empty_element(next_idx)

                temp = self._is_continuation[next_idx]
                self._is_continuation[next_idx] = self._is_continuation[insert_idx]
                self._is_continuation[insert_idx] = temp

                self._is_shifted[next_idx] = 1

                temp = self._filter[next_idx]
                self._filter[next_idx] = self._filter[insert_idx]
                self._filter[insert_idx] = temp

                if was_empty:
                    break

                next_idx = (next_idx + 1) & self.__mod_size

            self._filter[insert_idx] = r
            self._is_occupied[q] = 1
            self._is_continuation[insert_idx] = 1 if insert_idx != orig_idx else 0
            self._is_shifted[insert_idx] = 1 if insert_idx != q else 0

            if flag == 1:
                self._is_continuation[(insert_idx + 1) & self.__mod_size] = 1

    def _get_start_index(self, quotient: int) -> int:
        """Get the starting index for the quotient"""
        if self._is_empty_element(quotient):
            return quotient

        j = quotient
        cnts: int = 0

        while True:
            if j == quotient or self._is_occupied[j] == 1:
                cnts += 1

            if self._is_shifted[j] == 1:
                j = (j - 1) & self.__mod_size
            else:
                break

        while True:
            if self._is_continuation[j] == 0:
                if cnts == 1:
                    break
                cnts -= 1

            j = (j + 1) & self.__mod_size

        return j

    def _add(self, q: int, r: int):
        """Add an quotient into the filter"""
        if self._size == self._elements_added:
            raise QuotientFilterError("Unable to insert the element due to insufficient space")
        if self._is_empty_element(q):
            self._filter[q] = r
            self._is_occupied[q] = 1

        else:
            start_idx = self._get_start_index(q)

            if self._is_occupied[q] == 0:
                self._shift_insert(q, r, start_idx, start_idx, 0)

            else:
                orig_start_idx = start_idx
                starts = 0
                f = (
                    self._is_occupied.check_bit(start_idx)
                    + self._is_continuation.check_bit(start_idx)
                    + self._is_shifted.check_bit(start_idx)
                )

                while starts == 0 and f != 0 and r > self._filter[start_idx]:
                    start_idx = (start_idx + 1) & self.__mod_size

                    if self._is_continuation[start_idx] == 0:
                        starts += 1

                    f = (
                        self._is_occupied.check_bit(start_idx)
                        + self._is_continuation.check_bit(start_idx)
                        + self._is_shifted.check_bit(start_idx)
                    )

                if starts == 1:
                    self._shift_insert(q, r, orig_start_idx, start_idx, 0)
                else:
                    self._shift_insert(q, r, orig_start_idx, start_idx, 1)
        self._elements_added += 1

    def _remove_element(self, q: int, r: int) -> None:
        idx = self._contained_at_loc(q, r)
        if idx == -1:
            return

        next_idx = (idx + 1) & self.__mod_size
        remove_orig_idx = self._should_remove_orig_idx(idx, next_idx)

        if self._is_empty_element(next_idx) or self._is_cluster_start(next_idx):
            self._remove_and_clear_bits(idx, q, remove_orig_idx)
            return

        min_idx = self._find_cluster_start(idx)
        idx, next_idx = self._handle_first_move(idx, next_idx)
        idx, next_idx = self._shift_elements(idx, next_idx)
        self._clear_last_element(idx)
        if remove_orig_idx:
            self._is_occupied[q] = 0
        self._fixup_cluster(min_idx, next_idx)

    def _should_remove_orig_idx(self, idx: int, next_idx: int) -> bool:
        return self._is_run_or_cluster_start(idx) and self._is_continuation[next_idx] == 0

    def _remove_and_clear_bits(self, idx: int, q: int, remove_orig_idx: bool) -> None:
        self._filter[idx] = 0
        self._is_occupied.clear_bit(idx)
        self._is_continuation.clear_bit(idx)
        self._is_shifted.clear_bit(idx)
        if remove_orig_idx:
            self._is_occupied[q] = 0

    def _find_cluster_start(self, idx: int) -> int:
        min_idx = idx
        while not self._is_cluster_start(min_idx):
            min_idx = (min_idx - 1) & self.__mod_size
        return min_idx

    def _handle_first_move(self, idx: int, next_idx: int):
        if self._is_run_or_cluster_start(idx) and self._is_continuation[next_idx] == 1:
            self._filter[idx] = self._filter[next_idx]
            self._is_continuation[idx] = 0
            self._is_shifted[idx] = self._is_shifted[next_idx]
            idx = next_idx
            next_idx = (idx + 1) & self.__mod_size
        return idx, next_idx

    def _shift_elements(self, idx: int, next_idx: int):
        while not self._is_cluster_start(next_idx) and not self._is_empty_element(next_idx):
            self._filter[idx] = self._filter[next_idx]
            self._is_continuation[idx] = self._is_continuation[next_idx]
            self._is_shifted[idx] = self._is_shifted[next_idx]
            idx = next_idx
            next_idx = (idx + 1) & self.__mod_size
        return idx, next_idx

    def _clear_last_element(self, idx: int) -> None:
        self._filter[idx] = 0
        self._is_continuation[idx] = 0
        self._is_shifted[idx] = 0
        self._is_occupied[idx] = 0

    def _fixup_cluster(self, min_idx: int, next_idx: int) -> None:
        cur_quot = -1
        queue: list[int] = []
        while min_idx != next_idx:
            if self._is_occupied[min_idx] == 1:
                queue.append(min_idx)
            if self._is_run_start(min_idx) == 1:
                cur_quot = queue.pop(0)
            if cur_quot == min_idx:
                self._is_continuation[min_idx] = 0
                self._is_shifted[min_idx] = 0
                self._is_occupied[min_idx] = 1
            min_idx = (min_idx + 1) & self.__mod_size

    def _contained_at_loc(self, q: int, r: int) -> int:
        """returns the index location of the element, or -1 if not present"""
        if self._is_occupied[q] == 0:
            return -1

        start_idx = self._get_start_index(q)
        starts = 0

        while self._is_empty_element(start_idx) is False:
            if self._is_continuation[start_idx] == 0:
                starts += 1

            if starts == 2 or self._filter[start_idx] > r:
                break

            if self._filter[start_idx] == r:
                return start_idx

            start_idx = (start_idx + 1) & self.__mod_size

        return -1

    def _is_cluster_start(self, elt: int) -> bool:
        """Does this `elt` sit at the beginning of a cluster?"""
        return self._is_occupied[elt] == 1 and self._is_continuation[elt] == 0 and self._is_shifted[elt] == 0

    def _is_run_start(self, elt: int) -> bool:
        """Does `elt` sit at the beginning of a run?"""
        return self._is_continuation[elt] != 1 and (self._is_occupied[elt] == 1 or self._is_shifted[elt] == 1)

    def _is_run_or_cluster_start(self, elt: int) -> bool:
        if self._is_cluster_start(elt):
            return True
        return bool(self._is_run_start(elt))

    def _is_empty_element(self, elt: int) -> bool:
        """Is this an empty element?"""
        return (self._is_occupied[elt] + self._is_continuation[elt] + self._is_shifted[elt]) == 0

    def print(self, file: TextIO = sys.stdout):
        """show the bits and the run/cluster/continuation/empty status, defaults to `sys.stdout`"""
        print("idx\t--\tO-C-S\tStatus", file=file)
        print("----------------------------------------", file=file)
        for i in range(self._size):
            print(
                f"{i}\t--\t{self._is_occupied[i]}-{self._is_continuation[i]}-{self._is_shifted[i]}"
                f"\t{self._element_is(i)}",
                file=file,
            )

    def validate_metadata(self, verbose=False) -> bool:
        """Check for invalid bit settings, per the wikipedia documentation

        Args:
            verbose (str): Print which index in the quotient filter failed
        Returns:
            bool: True if the metadata bits are all passing; False otherwise"""
        is_valid = True
        for i in range(self._size):
            if self._is_occupied[i] == 0 and self._is_continuation == 1 and self._is_shifted == 0:
                if verbose:
                    print(f"Row failed: {i}")
                is_valid = False
            if self._is_occupied[i] == 1 and self._is_continuation == 1 and self._is_shifted == 0:
                if verbose:
                    print(f"Row failed: {i}")
                is_valid = False
        return is_valid

    def _element_is(self, idx):
        is_a = "Continuation"
        if self._is_empty_element(idx):
            is_a = "Empty"
        elif self._is_cluster_start(idx):
            is_a = "Cluster Start"
        elif self._is_run_start(idx):
            is_a = "Run Start"
        return is_a
