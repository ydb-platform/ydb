"""
Routines for classification.
"""

__all__ = [
    "DawidSkene",
    "GLAD",
    "GoldMajorityVote",
    "KOS",
    "MACE",
    "MajorityVote",
    "MMSR",
    "OneCoinDawidSkene",
    "Wawa",
    "ZeroBasedSkill",
]

from .dawid_skene import DawidSkene, OneCoinDawidSkene
from .glad import GLAD
from .gold_majority_vote import GoldMajorityVote
from .kos import KOS
from .m_msr import MMSR
from .mace import MACE
from .majority_vote import MajorityVote
from .wawa import Wawa
from .zero_based_skill import ZeroBasedSkill
