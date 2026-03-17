# -*- coding:utf-8 -*-

#  ************************** Copyrights and license ***************************
#
# This file is part of gcovr 8.6, a parsing and reporting tool for gcov.
# https://gcovr.com/en/8.6
#
# _____________________________________________________________________________
#
# Copyright (c) 2013-2026 the gcovr authors
# Copyright (c) 2013 Sandia Corporation.
# Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
# the U.S. Government retains certain rights in this software.
#
# This software is distributed under the 3-clause BSD License.
# For more information, see the README.rst file.
#
# ****************************************************************************

from __future__ import annotations
from typing import TypeVar

from .merging import MergeOptions


LinecovCollectionKeyType = int
LinecovKeyType = str
BranchcovKeyType = tuple[int, int, int]
ConditioncovKeyType = tuple[int, int]
CallcovKeyType = tuple[int, int, int]
FunctioncovKeyType = str
_Key = TypeVar(
    "_Key",
    LinecovCollectionKeyType,
    LinecovKeyType,
    BranchcovKeyType,
    ConditioncovKeyType,
    CallcovKeyType,
    FunctioncovKeyType,
)
_T = TypeVar("_T")


class CoverageDict(dict[_Key, _T]):
    """Base class for a coverage dictionary."""

    def merge(
        self,
        other: CoverageDict[_Key, _T],
        options: MergeOptions,
    ) -> None:
        """Helper function to merge items in a dictionary."""

        # Merge other into self
        for key, item in other.items():
            if key in self:
                self[key].merge(item, options)
            else:
                self[key] = item

        # At this point, "self" contains all merged items.
        # The caller should not access "other" objects therefore we clear it.
        other.clear()
