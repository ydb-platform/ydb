"""Count-Min Sketch, Heavy Hitters, and Stream Threshold, python implementations
License: MIT
Author: Tyler Barrus (barrust@gmail.com)
URL: https://github.com/barrust/count-min-sketch
"""

import math
from array import array
from collections.abc import ByteString
from io import BytesIO, IOBase
from mmap import mmap
from numbers import Number
from pathlib import Path
from struct import Struct

from probables.constants import INT32_T_MAX, INT32_T_MIN, INT64_T_MAX, INT64_T_MIN
from probables.exceptions import CountMinSketchError, InitializationError, NotSupportedError
from probables.hashes import HashFuncT, HashResultsT, KeyT, default_fnv_1a
from probables.utilities import MMap, is_valid_file, resolve_path


class CountMinSketch:
    """Simple Count-Min Sketch implementation for use in python;
    It can read and write the same format as the c version
    (https://github.com/barrust/count-min-sketch)

    Args:
        width (int): The width of the count-min sketch
        depth (int): The depth of the count-min sketch
        confidence (float): The level of confidence desired
        error_rate (float): The desired error rate
        filepath (str): Path to file to load
        hash_function (function): Hashing strategy function to use `hf(key, number)`
    Returns:
        CountMinSketch: A Count-Min Sketch object

    Note:
        Initialization order of operations:
            1) From file
            2) Width and depth
            3) Confidence and error rate
    Note:
        Default query type is `min`
    Note:
        For width and depth, width may realistically be in the thousands while depth \
            is in the single digit to teens"""

    __slots__ = (
        "__width",
        "__depth",
        "__confidence",
        "__error_rate",
        "__elements_added",
        "__query_method",
        "_bins",
        "_hash_function",
    )

    def __init__(
        self,
        width: int | None = None,
        depth: int | None = None,
        confidence: float | None = None,
        error_rate: float | None = None,
        filepath: str | Path | None = None,
        hash_function: HashFuncT | None = None,
    ) -> None:
        """default initilization function"""
        # default values
        self.__width = 0
        self.__depth = 0
        self.__confidence = 0.0
        self.__error_rate = 0.0
        self.__elements_added = 0
        self.__query_method = self.__min_query

        if filepath is not None and is_valid_file(filepath):
            filepath = resolve_path(filepath)
            self.__load(filepath)
        else:
            if width is not None and depth is not None:
                valid_prms = isinstance(width, Number) and width > 0 and isinstance(depth, Number) and depth > 0
                if not valid_prms:
                    msg = "CountMinSketch: width and depth must be greater than 0"
                    raise InitializationError(msg)
                self.__width = int(width)
                self.__depth = int(depth)
                self.__confidence = 1 - (1 / math.pow(2, self.depth))
                self.__error_rate = 2 / self.width
            elif confidence is not None and error_rate is not None:
                valid_prms = (
                    isinstance(confidence, Number)
                    and confidence > 0
                    and isinstance(error_rate, Number)
                    and error_rate > 0
                )
                if not valid_prms:
                    msg = "CountMinSketch: width and depth must be greater than 0"
                    raise InitializationError(msg)
                self.__confidence = confidence
                self.__error_rate = error_rate
                self.__width = math.ceil(2 / error_rate)
                numerator = -1 * math.log(1 - confidence)
                self.__depth = math.ceil(numerator / 0.6931471805599453)

            else:
                msg = (
                    "Must provide one of the following to initialize the "
                    "Count-Min Sketch:\n"
                    "    A file to load,\n"
                    "    The width and depth,\n"
                    "    OR confidence and error rate"
                )
                raise InitializationError(msg)
            self._bins = array("i", [0]) * (self.width * self.depth)

        if hash_function is None:
            self._hash_function = default_fnv_1a
        else:
            self._hash_function = hash_function  # type: ignore

    __FOOTER_STRUCT = Struct("IIq")
    __BASIC_BIN_STRUCT = Struct("i")

    def __str__(self) -> str:
        """string representation of the count min sketch"""
        msg = (
            "Count-Min Sketch:\n"
            "\tWidth: {0}\n"
            "\tDepth: {1}\n"
            "\tConfidence: {2}\n"
            "\tError Rate: {3}\n"
            "\tElements Added: {4}"
        )
        return msg.format(
            self.width,
            self.depth,
            self.confidence,
            self.error_rate,
            self.elements_added,
        )

    def __contains__(self, key: KeyT) -> bool:
        """setup the `in` keyword"""
        return self.check(key) != 0

    def __bytes__(self) -> bytes:
        """Export countmin-sketch to `bytes`"""
        with BytesIO() as f:
            self.export(f)
            return f.getvalue()

    @classmethod
    def frombytes(cls, b: ByteString, hash_function: HashFuncT | None = None) -> "CountMinSketch":
        """
        Args:
            b (ByteString): The bytes to load as a Count-Min Sketch
            hash_function (function): Hashing strategy function to use `hf(key, number)`
        Returns:
            CountMinSketch: A count-min sketch object
        """
        offset = cls.__FOOTER_STRUCT.size
        width, depth, _ = cls.__FOOTER_STRUCT.unpack_from(bytes(b[-1 * offset :]))
        cms = CountMinSketch(width=width, depth=depth, hash_function=hash_function)
        cms._parse_bytes(b)
        return cms

    @property
    def width(self) -> int:
        """int: The width of the count-min sketch

        Note:
            Not settable"""
        return self.__width

    @property
    def depth(self) -> int:
        """int: The depth of the count-min sketch

        Note:
            Not settable"""
        return self.__depth

    @property
    def confidence(self) -> float:
        """float: The confidence of the count-min sketch

        Note:
            Not settable"""
        return self.__confidence

    @property
    def error_rate(self) -> float:
        """float: The error rate of the count-min sketch

        Note:
            Not settable"""
        return self.__error_rate

    @property
    def elements_added(self) -> int:
        """int: The number of elements added to the count-min sketch

        Note:
            Not settable"""
        return self.__elements_added

    @property
    def query_type(self) -> str:
        """str: The name of the query type being used

        Note:
            Valid values:
                * 'min' or None
                * 'mean'
                * 'mean-min'"""
        if self.__query_method == self.__mean_query:
            return "mean"
        if self.__query_method == self.__mean_min_query:
            return "mean-min"
        return "min"

    @query_type.setter
    def query_type(self, val: str):
        """set to min query Options='min', 'mean', 'mean-min'
        other values are set to min
        setting to mean is converting to a Count-Mean Sketch
        setting to mean-min is converting to a Count-Mean-Min Sketch"""
        if val is None:
            self.__query_method = self.__min_query
            return
        val = val.lower()
        if val == "mean":
            self.__query_method = self.__mean_query
        elif val == "mean-min":
            self.__query_method = self.__mean_min_query
        else:
            self.__query_method = self.__min_query

    def clear(self) -> None:
        """Reset the count-min sketch to an empty state"""
        self.__elements_added = 0
        for i, _ in enumerate(self._bins):
            self._bins[i] = 0

    def hashes(self, key: KeyT, depth: int | None = None) -> HashResultsT:
        """Return the hashes based on the provided key

        Args:
            key (str): Description of arg1
            depth (int): Number of permutations of the hash to generate; if None, generate `number_hashes`

        Returns:
            List(int): A list of the hashes for the key in int form"""
        return self._hash_function(key, self.depth if depth is None else depth)

    def add(self, key: KeyT, num_els: int = 1) -> int:
        """Insert the element `key` into the count-min sketch

        Args:
            key (str): The element to insert
            num_els (int): The number of times to insert the element
        Returns:
            int: The number of times the element was likely inserted after the insertion"""
        return self.add_alt(self.hashes(key), num_els)

    def add_alt(self, hashes: HashResultsT, num_els: int = 1) -> int:
        """Insert an element by using the hash representation

        Args:
            key (str): The element to insert
            num_els (int): The number of times to insert the element
        Returns:
            int: The number of times the element was likely inserted after the insertion"""
        bins = [(val % self.width) + (i * self.width) for i, val in enumerate(hashes)]
        vals = [self._bins[x] + num_els for x in bins]

        for i, val in enumerate(vals):
            idx = bins[i]
            if val > INT32_T_MAX:
                self._bins[idx] = INT32_T_MAX
                vals[i] = INT32_T_MAX
            else:
                self._bins[idx] = val
        self.__elements_added += num_els
        if self.elements_added > INT64_T_MAX:
            self.__elements_added = INT64_T_MAX
        return self.__query_method(sorted(vals))

    def remove(self, key: KeyT, num_els: int = 1) -> int:
        """Remove element 'key' from the count-min sketch

        Args:
            key (str): The element to remove
            num_els (int): The number of times to remove the element
        Returns:
            int: The number of times the element was likely inserted after the removal"""
        return self.remove_alt(self.hashes(key), num_els)

    def remove_alt(self, hashes: HashResultsT, num_els: int = 1) -> int:
        """Remove an element by using the hash representation

        Args:
            hashes (list): The hashes representing the element to remove
            num_els (int): The number of times to remove the element
        Returns:
            int: The number of times the element was likely inserted after the removal"""
        bins = [(v % self.width) + (i * self.width) for i, v in enumerate(hashes)]
        vals = [self._bins[x] - num_els for x in bins]
        for i, val in enumerate(vals):
            idx = bins[i]
            if val > INT32_T_MIN:
                self._bins[idx] = val
            else:
                self._bins[idx] = INT32_T_MIN
                vals[i] = INT32_T_MIN
        self.__elements_added -= num_els
        if self.elements_added < INT64_T_MIN:
            self.__elements_added = INT64_T_MIN

        return self.__query_method(sorted(vals))

    def check(self, key: KeyT) -> int:
        """Check number of times element 'key' is in the count-min sketch

        Args:
            key (str): The key to check the number of times inserted
        Returns:
            int: The number of times the element was likely inserted"""
        return self.check_alt(self.hashes(key))

    def check_alt(self, hashes: HashResultsT) -> int:
        """Check the count-min sketch for an element by using the hash representation

        Args:
            hashes (list): The hashes representing the element to check
        Returns:
            int: The number of times the element was likely inserted"""
        bins = [(val % self.width) + (i * self.width) for i, val in enumerate(hashes)]
        return self.__query_method(sorted([self._bins[i] for i in bins]))

    def export(self, file: Path | str | IOBase | mmap) -> None:
        """Export the count-min sketch to disk

        Args:
            filename (str): The filename to which the count-min sketch will be written."""
        if not isinstance(file, IOBase | mmap):
            file = resolve_path(file)
            with open(file, "wb") as filepointer:
                self.export(filepointer)  # type: ignore
        else:
            # write out the bins
            self._bins.tofile(file)  # type: ignore
            file.write(self.__FOOTER_STRUCT.pack(self.width, self.depth, self.elements_added))

    def join(self, second: "CountMinSketch") -> None:
        """Join two count-min sketchs into a single count-min sketch; the
        calling count-min sketch will have the resulting combined data

        Args:
            second (CountMinSketch): The count-min sketch to merge
        Raises:
            TypeError: When second is not either a :class:`CountMinSketch`,:class:`CountMeanSketch` \
                or :class:`CountMeanMinSketch`
            CountMinSketchError: Raised when the count-min sketches are not of the same dimensions
        Note:
            The calling count-min sketch will contain the combined data
            once complete
        """
        if not isinstance(second, (CountMinSketch)):
            msg = f"Unable to merge a count-min sketch with {type(second)}"
            raise TypeError(msg)
        if (
            (self.width != second.width)
            or (self.depth != second.depth)
            or (self.hashes("test") != second.hashes("test"))
        ):
            raise CountMinSketchError("Unable to merge as the count-min sketches are mismatched")

        size = self.width * self.depth
        for i in range(size):
            if self._bins[i] == INT32_T_MIN or self._bins[i] == INT32_T_MAX:
                continue
            tmp_els = self._bins[i] + second._bins[i]

            if tmp_els > INT32_T_MAX:
                self._bins[i] = INT32_T_MAX
            elif tmp_els < INT32_T_MIN:
                self._bins[i] = INT32_T_MIN
            else:
                self._bins[i] = tmp_els

        # handle adding and removing elements added including handling overflow
        self.__elements_added += second.elements_added

        if self.elements_added > INT64_T_MAX:
            self.__elements_added = INT64_T_MAX
        elif self.elements_added < INT64_T_MIN:
            self.__elements_added = INT64_T_MIN

    def __load(self, file: Path | str | IOBase | mmap):
        """load the count-min sketch from file"""
        if not isinstance(file, IOBase | mmap):
            file = resolve_path(file)
            with MMap(file) as filepointer:
                self.__load(filepointer)
        else:
            self._parse_bytes(file)  # type: ignore

    @classmethod
    def _parse_footer(cls, file: ByteString) -> tuple[int, int, int]:
        """return width, depth and elements added, in that order"""
        offset = cls.__FOOTER_STRUCT.size
        width, depth, elements_added = cls.__FOOTER_STRUCT.unpack_from(bytes(file[-1 * offset :]))
        return width, depth, elements_added

    def _parse_bytes(self, file: ByteString):
        """parse bytes or a mapped file to setup the CountMin-Sketch"""
        width, depth, els_added = self._parse_footer(file)
        self.__width = width
        self.__depth = depth
        self.__elements_added = els_added
        self.__confidence = 1 - (1 / math.pow(2, self.depth))
        self.__error_rate = 2 / self.width

        offset = self.__BASIC_BIN_STRUCT.size * self.width * self.depth
        self._bins = array("i", bytes(file[:offset]))

    @staticmethod
    def __min_query(results: HashResultsT) -> int:
        """generate the min query; assumes sorted list"""
        return results[0]

    def __mean_query(self, results: HashResultsT) -> int:
        """generate the mean query; assumes sorted list"""
        return sum(results) // self.depth

    def __mean_min_query(self, results: HashResultsT) -> int:
        """generate the mean-min query; assumes sorted list"""
        if results[0] == 0 and results[-1] == 0:
            return 0
        meanmin = []
        for t_bin in results:
            diff = self.elements_added - t_bin
            calc = t_bin - diff // (self.width - 1)
            meanmin.append(calc)
        meanmin.sort()
        if self.depth % 2 == 0:
            calc = meanmin[self.depth // 2] + meanmin[self.depth // 2 - 1]
            res = calc // 2
        else:
            res = meanmin[self.depth // 2]
        return res


class CountMeanSketch(CountMinSketch):
    """Simple Count-Mean Sketch implementation for use in python;
    It can read and write the same format as the c version
    (https://github.com/barrust/count-min-sketch)

    Args:
        width (int): The width of the count-min sketch
        depth (int): The depth of the count-min sketch
        confidence (float): The level of confidence desired
        error_rate (float): The desired error rate
        filepath (str): Path to file to load
        hash_function (function): Hashing strategy function to use `hf(key, number)`
    Returns:
        CountMeanSketch: A Count-Mean Sketch object
    Note:
        Initialization order of operations:
            1) From file
            2) Width and depth
            3) Confidence and error rate
    Note:
        Default query type is `mean`
    Note:
        For width and depth, width may realistically be in the thousands while depth is \
            in the single digit to teens"""

    def __init__(
        self,
        width: int | None = None,
        depth: int | None = None,
        confidence: float | None = None,
        error_rate: float | None = None,
        filepath: str | Path | None = None,
        hash_function: HashFuncT | None = None,
    ) -> None:
        super().__init__(width, depth, confidence, error_rate, filepath, hash_function)
        self.query_type = "mean"


class CountMeanMinSketch(CountMinSketch):
    """Simple Count-Mean-Min Sketch implementation for use in python;
    It can read and write the same format as the c version
    (https://github.com/barrust/count-min-sketch)

    Args:
        width (int): The width of the count-min sketch
        depth (int): The depth of the count-min sketch
        confidence (float): The level of confidence desired
        error_rate (float): The desired error rate
        filepath (str): Path to file to load
        hash_function (function): Hashing strategy function to use `hf(key, number)`
    Returns:
        CountMeanMinSketch: A Count-Mean-Min Sketch object
    Note:
        Initialization order of operations:
            1) From file
            2) Width and depth
            3) Confidence and error rate
    Note:
        Default query type is `mean-min`
    Note:
        For width and depth, width may realistically be in the thousands while depth is \
            in the single digit to teens"""

    def __init__(
        self,
        width: int | None = None,
        depth: int | None = None,
        confidence: float | None = None,
        error_rate: float | None = None,
        filepath: str | Path | None = None,
        hash_function: HashFuncT | None = None,
    ) -> None:
        super().__init__(width, depth, confidence, error_rate, filepath, hash_function)
        self.query_type = "mean-min"


class HeavyHitters(CountMinSketch):
    """Find and track those elements that are the most common, or heavy
    hitters

    Args:
        num_hitters (int): The maximum number of distinct elements to track
        width (int): The width of the count-min sketch
        depth (int): The depth of the count-min sketch
        confidence (float): The level of confidence desired
        error_rate (float): The desired error rate
        filepath (str): Path to file to load
        hash_function (function): Hashing strategy function to use `hf(key, number)`
    Returns:
        HeavyHitters: A Count-Min Sketch object
    Note:
        Initialization order of operations:
            1) From file
            2) Width and depth
            3) Confidence and error rate
    Note:
        Default query type is `min`
    Note:
        For width and depth, width may realistically be in the thousands while depth is \
            in the single digit to teens"""

    __slots__ = ["__top_x", "__top_x_size", "__num_hitters", "__smallest"]

    def __init__(
        self,
        num_hitters: int = 100,
        width: int | None = None,
        depth: int | None = None,
        confidence: float | None = None,
        error_rate: float | None = None,
        filepath: str | Path | None = None,
        hash_function: HashFuncT | None = None,
    ) -> None:
        super().__init__(width, depth, confidence, error_rate, filepath, hash_function)
        self.__top_x = {}  # type: ignore
        self.__top_x_size = 0
        self.__num_hitters = num_hitters
        self.__smallest = 0

    @classmethod
    def frombytes(  # type: ignore
        cls, b: ByteString, num_hitters: int = 100, hash_function: HashFuncT | None = None
    ) -> "HeavyHitters":
        """
        Args:
            b (ByteString): The bytes to load as a Expanding Bloom Filter
            num_hitters (int): The maximum number of distinct elements to track
            hash_function (function): Hashing strategy function to use `hf(key, number)`
        Returns:
            HeavyHitters: A Bloom Filter object
        """
        width, depth, _ = cls._parse_footer(b)
        hh = HeavyHitters(width=width, depth=depth, num_hitters=num_hitters, hash_function=hash_function)
        hh._parse_bytes(b)
        return hh

    def __str__(self) -> str:
        """heavy hitters string rep"""
        msg = super().__str__()
        return (
            f"Heavy Hitters {msg}\n"
            f"\tNumber Hitters: {self.number_heavy_hitters}\n"
            f"\tNumber Recorded: {self.__top_x_size}"
        )

    @property
    def heavy_hitters(self) -> dict[str, int]:
        """dict: Return the heavy hitters, or most common elements

        Note:
            Not settable"""
        return self.__top_x

    @property
    def number_heavy_hitters(self) -> int:
        """int: Return the maximum number of heavy hitters being tracked

        Note:
            Not settable"""
        return self.__num_hitters

    def add(self, key: str, num_els: int = 1) -> int:  # type: ignore
        """Add element to heavy hitters

        Args:
            key (str): The element to add
            num_els (int): The number of instances to add
        Returns:
            int: Number of times key has been inserted
        Note:
            Override function"""
        return self.add_alt(key, self.hashes(key), num_els)

    def add_alt(self, key: str, hashes: HashResultsT, num_els: int = 1) -> int:  # type: ignore
        """Add the element `key` represented as hashes to the HeavyHitters
        object (hence the different signature on the function!)

        Args:
            key (str): The element to add
            hashes (list): The list of integers representing the key to insert
            num_els (int): The number of instances to add
        Returns:
            int: Number of times key has been inserted
        Note:
            Different key signature than the normal :class:`CountMinSketch`
        Note:
            Override function"""
        res = super().add_alt(hashes, num_els)

        # update the heavy hitters list as necessary
        if self.__top_x_size < self.__num_hitters:  # still have room in top x
            tmp = self.__top_x.get(key, None)
            self.__top_x[key] = res
            if tmp is None:
                self.__top_x_size = len(self.__top_x)
        elif key in self.__top_x:  # easy update as it is already there
            self.__top_x[key] = res
        elif res > self.__smallest:  # something in there is smaller
            self.__top_x[key] = res
            # get the key with the smallest element
            tmp_key = min(self.__top_x, key=self.__top_x.get)  # type: ignore
            # delete this key
            self.__top_x.pop(tmp_key, None)
            new_min = min(self.__top_x, key=self.__top_x.get)  # type: ignore
            self.__smallest = self.__top_x[new_min]
        return res

    def remove_alt(self, hashes: HashResultsT, num_els: int = 1):
        """Remove element based on hashes provided; not supported in
        heavy hitters

        Raises:
            NotSupportedError: This function is not supported by the HeavyHitters class
        Note:
            Override function"""
        msg = (
            "Unable to remove elements in the HeavyHitters "
            "class as it is an un supported action (and does not"
            "make sense)!"
        )
        raise NotSupportedError(msg)

    def clear(self) -> None:
        """Clear out the heavy hitters!"""
        super().clear()
        self.__top_x = {}
        self.__top_x_size = 0
        self.__smallest = 0

    def join(self, second: "HeavyHitters"):  # type: ignore
        """Join is not supported by HeavyHitters

        Raises:
            NotSupportedError: This functionality is currently not supported"""
        msg = "Joining is not supported for heavy hitters"
        raise NotSupportedError(msg)


class StreamThreshold(CountMinSketch):
    """Find and track those elements that are above a certain threshold

    Args:
        threshold (int): The threshold above which an element will be tracked
        width (int): The width of the count-min sketch
        depth (int): The depth of the count-min sketch
        confidence (float): The level of confidence desired
        error_rate (float): The desired error rate
        filepath (str): Path to file to load
        hash_function (function): Hashing strategy function to use `hf(key, number)`
    Returns:
        StreamThreshold: A Count-Min Sketch object
    Note:
        Initialization order of operations:
            1) From file
            2) Width and depth
            3) Confidence and error rate
    Note:
        Default query type is `min`
    Note:
        For width and depth, width may realistically be in the thousands while depth is \
            in the single digit to teens"""

    __slots__ = ["__threshold", "__meets_threshold"]

    def __init__(
        self,
        threshold: int = 100,
        width: int | None = None,
        depth: int | None = None,
        confidence: float | None = None,
        error_rate: float | None = None,
        filepath: str | Path | None = None,
        hash_function: HashFuncT | None = None,
    ) -> None:
        super().__init__(width, depth, confidence, error_rate, filepath, hash_function)
        self.__threshold = threshold
        self.__meets_threshold = {}  # type: ignore

    @classmethod
    def frombytes(  # type: ignore
        cls, b: ByteString, threshold: int = 100, hash_function: HashFuncT | None = None
    ) -> "StreamThreshold":
        """
        Args:
            b (ByteString): The bytes to load as a Expanding Bloom Filter
            threshold (int): The threshold above which an element will be tracked
            hash_function (function): Hashing strategy function to use `hf(key, number)`
        Returns:
            StreamThreshold: A Bloom Filter object
        """
        width, depth, _ = cls._parse_footer(b)
        st = StreamThreshold(width=width, depth=depth, threshold=threshold, hash_function=hash_function)
        st._parse_bytes(b)
        return st

    def __str__(self) -> str:
        """stream threshold string rep"""
        msg = super().__str__()
        return (
            f"Stream Threshold {msg}\n"
            f"\tThreshold: {self.threshold}\n"
            f"\tNumber Meeting Threshold: {len(self.__meets_threshold)}"
        )

    @property
    def meets_threshold(self) -> dict[str, int]:
        """dict: Those keys that meet the required threshold (with value)"""
        return self.__meets_threshold

    @property
    def threshold(self) -> int:
        """int: The threshold at which a key is tracked"""
        return self.__threshold

    def clear(self) -> None:
        """Clear out the stream threshold!"""
        super().clear()
        self.__meets_threshold = {}

    def add(self, key: str, num_els: int = 1) -> int:  # type: ignore
        """Add the element for key into the data structure

        Args:
            key (str): The element to add
            num_els (int): The number of instances to add
        Returns:
            int: Number of times key has been inserted
        Note:
            Override function"""
        return self.add_alt(key, self.hashes(key), num_els)

    def add_alt(self, key: str, hashes: HashResultsT, num_els: int = 1) -> int:  # type: ignore
        """Add the element for key into the data structure

        Args:
            key (str): The element to add
            hashes (list): The list of integers representing the key to insert
            num_els (int): The number of instances to add
        Returns:
            int: Number of times key has been inserted
        Note:
            Different key signature than the normal :class:`CountMinSketch`
        Note:
            Override function"""
        res = super().add_alt(hashes, num_els)
        if res >= self.__threshold:
            self.__meets_threshold[key] = res
        return res

    def remove(self, key: str, num_els: int = 1) -> int:  # type: ignore
        """Remove element 'key' from the count-min sketch

        Args:
            key (str): The element to remove
            num_els (int): The number of times to remove the element
        Returns:
            int: The number of times the element was likely inserted after the removal
        Note:
            Override function"""
        return self.remove_alt(key, self.hashes(key), num_els)

    def remove_alt(self, key: str, hashes: HashResultsT, num_els: int = 1) -> int:  # type: ignore
        """Remove an element by using the hash representation

        Args:
            key (str): The key that the hashes represent
            hashes (list): The hashes representing the element to remove
            num_els (int): The number of times to remove the element
        Returns:
            int: The number of times the element was likely inserted after the removal
        Note:
            Different key signature than the normal :class:`CountMinSketch`
        Note:
            Override function"""
        res = super().remove_alt(hashes, num_els)
        if res < self.__threshold:
            self.__meets_threshold.pop(key, None)
        else:
            self.__meets_threshold[key] = res
        return res

    def join(self, second: "StreamThreshold"):  # type: ignore
        """Join is not supported by StreamThreshold

        Raises:
            NotSupportedError: This functionality is currently not supported"""
        msg = "Joining is not supported for stream threshold"
        raise NotSupportedError(msg)
