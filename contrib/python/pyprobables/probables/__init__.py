"""pyprobables module"""

from probables.blooms import (
    BloomFilter,
    BloomFilterOnDisk,
    CountingBloomFilter,
    ExpandingBloomFilter,
    RotatingBloomFilter,
)
from probables.countminsketch import CountMeanMinSketch, CountMeanSketch, CountMinSketch, HeavyHitters, StreamThreshold
from probables.cuckoo import CountingCuckooFilter, CuckooFilter
from probables.exceptions import (
    CuckooFilterFullError,
    InitializationError,
    NotSupportedError,
    ProbablesBaseException,
    RotatingBloomFilterError,
    SimilarityError,
)
from probables.quotientfilter import QuotientFilter
from probables.utilities import Bitarray

__author__ = "Tyler Barrus"
__maintainer__ = "Tyler Barrus"
__email__ = "barrust@gmail.com"
__license__ = "MIT"
__version__ = "0.7.0"
__credits__: list[str] = []
__url__ = "https://github.com/barrust/pyprobables"
__bugtrack_url__ = "https://github.com/barrust/pyprobables/issues"

__all__ = [
    "BloomFilter",
    "BloomFilterOnDisk",
    "CountingBloomFilter",
    "CountMinSketch",
    "CountMeanSketch",
    "CountMeanMinSketch",
    "HeavyHitters",
    "StreamThreshold",
    "CuckooFilter",
    "CountingCuckooFilter",
    "InitializationError",
    "NotSupportedError",
    "ProbablesBaseException",
    "CuckooFilterFullError",
    "ExpandingBloomFilter",
    "RotatingBloomFilter",
    "RotatingBloomFilterError",
    "QuotientFilter",
    "Bitarray",
    "SimilarityError",
]
