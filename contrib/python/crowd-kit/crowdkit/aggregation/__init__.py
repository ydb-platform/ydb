from typing import cast

from . import base
from .classification import (
    GLAD,
    KOS,
    MACE,
    MMSR,
    DawidSkene,
    GoldMajorityVote,
    MajorityVote,
    OneCoinDawidSkene,
    Wawa,
    ZeroBasedSkill,
)
from .embeddings import HRRASA, RASA, ClosestToAverage
from .image_segmentation import (
    SegmentationEM,
    SegmentationMajorityVote,
    SegmentationRASA,
)
from .multilabel import BinaryRelevance
from .pairwise import BradleyTerry, NoisyBradleyTerry
from .texts import ROVER, TextHRRASA, TextRASA

__all__ = [
    "base",
    "BradleyTerry",
    "ClosestToAverage",
    "DawidSkene",
    "OneCoinDawidSkene",
    "GLAD",
    "GoldMajorityVote",
    "HRRASA",
    "KOS",
    "MACE",
    "MMSR",
    "MajorityVote",
    "NoisyBradleyTerry",
    "RASA",
    "ROVER",
    "SegmentationEM",
    "SegmentationMajorityVote",
    "SegmentationRASA",
    "TextHRRASA",
    "TextRASA",
    "Wawa",
    "ZeroBasedSkill",
    "BinaryRelevance",
]


def is_arcadia() -> bool:
    try:
        import __res

        return cast(bool, __res == __res)
    except ImportError:
        return False
