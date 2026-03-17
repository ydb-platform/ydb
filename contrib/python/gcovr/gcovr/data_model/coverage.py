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

"""
The gcovr coverage data model.

This module represents the core data structures
and should not have dependencies on any other gcovr module,
also not on the gcovr.utils module.

The data model should contain the exact same information
as the JSON input/output format.

The types ending with ``*Coverage``
contain per-project/-line/-decision/-branch coverage.

The types ``SummarizedStats``, ``CoverageStat``, and ``DecisionCoverageStat``
report aggregated metrics/percentages.


Merge coverage data.

All of these merging function have the signature
``merge(T, T, MergeOptions) -> T``.
That is, they take two coverage data items and combine them,
returning the combined coverage.
This may change the input objects, so that they should not be used afterwards.

In a mathematical sense, all of these ``merge()`` functions
must behave somewhat like an addition operator:

* commutative: order of arguments must not matter,
  so that ``merge(a, b)`` must match ``merge(b, a)``.
* associative: order of merging must not matter,
  so that ``merge(a, merge(b, c))`` must match ``merge(merge(a, b), c)``.
* identity element: there must be an empty element,
  so that ``merge(a, empty)`` and ``merge(empty, a)`` and ``a`` all match.
  However, the empty state might be implied by “parent dict does not contain an entry”,
  or must contain matching information like the same line number.

The insertion functions insert a single coverage item into a larger structure,
for example inserting BranchCoverage into a LineCoverage object.
The target/parent structure is updated in-place,
otherwise this has equivalent semantics to merging.
In particular, if there already is coverage data in the target with the same ID,
then the contents are merged.
The insertion functions return the coverage structure that is saved in the target,
which may not be the same as the input value.
"""

from __future__ import annotations
from abc import abstractmethod
import os
import re
from typing import Any, Callable, Iterable, NoReturn, TypeVar

from ..exceptions import (
    GcovrDataAssertionError,
    GcovrMergeAssertionError,
    SanityCheckError,
)
from ..filter import is_file_excluded
from ..logging import LOGGER
from ..utils import force_unix_separator
from ..options import Options

from .coverage_dict import (
    BranchcovKeyType,
    ConditioncovKeyType,
    CallcovKeyType,
    CoverageDict,
    FunctioncovKeyType,
    LinecovCollectionKeyType,
    LinecovKeyType,
)
from .merging import DEFAULT_MERGE_OPTIONS, MergeOptions
from .stats import CoverageStat, DecisionCoverageStat, SummarizedStats


GCOVR_DATA_SOURCES = "gcovr/data_sources"
GCOVR_EXCLUDED = "gcovr/excluded"

_T = TypeVar("_T")


def _presentable_filename(filename: str, root_filter: re.Pattern[str]) -> str:
    """Mangle a filename so that it is suitable for a report."""

    normalized = root_filter.sub("", filename)
    if filename.endswith(normalized):
        # remove any slashes between the removed prefix and the normalized name
        if filename != normalized:
            while normalized.startswith(os.path.sep):
                normalized = normalized[len(os.path.sep) :]
    else:
        # Do no truncation if the filter does not start matching
        # at the beginning of the string
        normalized = filename

    return force_unix_separator(normalized)


class CoverageBase:
    """Base class for coverage information."""

    __slots__ = ("data_sources",)

    def __init__(self, data_sources: str | set[tuple[str, ...]]) -> None:
        if isinstance(data_sources, str):
            self.data_sources = set[tuple[str, ...]]([(data_sources,)])
        else:
            self.data_sources = data_sources.copy()

    def merge_base_data(
        self,
        other: CoverageBase,
    ) -> None:
        """Merge the data of the base class."""
        self.data_sources.update(other.data_sources)

    def raise_merge_error(self, msg: str, other: Any) -> NoReturn:
        """Get the exception with message extended with context."""
        location = self.location
        raise GcovrMergeAssertionError(
            "\n".join(
                [
                    msg if location is None else f"{location} {msg}",
                    "GCOV data file of merge source is:"
                    if len(other.data_sources) == 1
                    else "GCOV data files of merge source are:",
                    *[f"   {' -> '.join(e)}" for e in sorted(other.data_sources)],
                    f"and of merge target {'is' if len(self.data_sources) == 1 else 'are'}:",
                    *[f"   {' -> '.join(e)}" for e in sorted(self.data_sources)],
                ]
            )
        )

    def raise_data_error(self, msg: str) -> NoReturn:
        """Get the exception with message extended with context."""
        location = self.location
        raise GcovrDataAssertionError(
            "\n".join(
                [
                    msg if location is None else f"{location} {msg}",
                    f"GCOV data file{' is' if len(self.data_sources) == 1 else 's are'}:",
                    *[f"   {' -> '.join(e)}" for e in sorted(self.data_sources)],
                ]
            )
        )

    def _merge_property(
        self,
        other: CoverageBase,
        msg: str,
        getter: Callable[[CoverageBase], _T],
    ) -> _T:
        """Assert that the property given by name is defined the same if defined twice. Return the value of the property."""

        left = getter(self)
        right = getter(other)
        if left is not None and right is not None:
            if left != right:
                self.raise_merge_error(
                    f"{msg} must be equal, got {left} and {right}.",
                    other,
                )

        return left or right

    @property
    @abstractmethod
    def key(self) -> Any:
        """Get the key used for the dictionary to unique identify the coverage object."""

    @property
    @abstractmethod
    def location(self) -> str | None:
        """Get the source location of the coverage data."""


class BranchCoverage(CoverageBase):
    r"""Represent coverage information about a branch.

    Args:
        branchno (int):
            The branch number.
        count (int):
            Number of times this branch was followed.
        fallthrough (bool, optional):
            Whether this is a fallthrough branch. False if unknown.
        throw (bool, optional):
            Whether this is an exception-handling branch. False if unknown.
        source_block_id (int, optional):
            The block number.
        destination_block_id (int, optional):
            The destination block of the branch. None if unknown.
        excluded (bool, optional):
            Whether the branch is excluded.

        >>> filecov = FileCoverage("file.c", filename="file.c")
        >>> linecov_list = LineCoverageCollection(filecov, "not_used.gcov", lineno=11)
        >>> linecov = LineCoverage(linecov_list, "line.gcov", count=0, function_name="function")
        >>> BranchCoverage(linecov, "call.gcov", branchno=None, count=-1)
        Traceback (most recent call last):
          ...
        gcovr.exceptions.GcovrDataAssertionError: file.c:11 count must not be a negative value.
        GCOV data file is:
           call.gcov
        >>> BranchCoverage(linecov, "call.gcov", branchno=None, count=-1)
        Traceback (most recent call last):
          ...
        gcovr.exceptions.GcovrDataAssertionError: file.c:11 count must not be a negative value.
        GCOV data file is:
           call.gcov
        >>> BranchCoverage(linecov, "call.gcov", branchno=None, source_block_id=1, destination_block_id=2, count=-1)
        Traceback (most recent call last):
          ...
        gcovr.exceptions.GcovrDataAssertionError: file.c:11 (source block 1, destination block 2) count must not be a negative value.
        GCOV data file is:
           call.gcov
    """

    first_undefined_source_block_id: bool = True

    __slots__ = (
        "parent",
        "branchno",
        "count",
        "fallthrough",
        "throw",
        "source_block_id",
        "destination_block_id",
        "excluded",
    )

    def __init__(
        self,
        parent: LineCoverage,
        data_sources: str | set[tuple[str, ...]],
        *,
        branchno: int | None,
        count: int,
        fallthrough: bool = False,
        throw: bool = False,
        source_block_id: int | None = None,
        destination_block_id: int | None = None,
        excluded: bool = False,
    ) -> None:
        super().__init__(data_sources)
        self.parent = parent
        self.branchno = branchno
        self.count = count
        self.fallthrough = fallthrough
        self.throw = throw
        self.source_block_id = source_block_id
        self.destination_block_id = destination_block_id
        self.excluded = excluded

        if count < 0:
            self.raise_data_error("count must not be a negative value.")

    def serialize(
        self,
        get_data_sources: Callable[[CoverageBase], dict[str, Any]],
    ) -> dict[str, Any]:
        """Serialize the object."""
        data_dict = dict[str, Any]()
        if self.branchno is not None:
            data_dict["branchno"] = self.branchno
        data_dict.update(
            {
                "count": self.count,
                "fallthrough": self.fallthrough,
                "throw": self.throw,
            }
        )
        if self.source_block_id is not None:
            data_dict["source_block_id"] = self.source_block_id
        if self.destination_block_id is not None:
            data_dict["destination_block_id"] = self.destination_block_id
        if self.excluded:
            data_dict[GCOVR_EXCLUDED] = True
        data_dict.update(get_data_sources(self))

        return data_dict

    @classmethod
    def deserialize(
        cls,
        linecov: LineCoverage,
        get_data_sources: Callable[[dict[str, Any]], set[tuple[str, ...]]],
        data_dict: dict[str, Any],
    ) -> BranchCoverage:
        """Deserialize the object."""
        return linecov.insert_branch_coverage(
            get_data_sources(data_dict),
            branchno=data_dict.get("branchno"),
            count=data_dict["count"],
            source_block_id=data_dict.get("source_block_id"),
            fallthrough=data_dict["fallthrough"],
            throw=data_dict["throw"],
            destination_block_id=data_dict.get("destination_block_id"),
            excluded=data_dict.get(GCOVR_EXCLUDED, False),
        )

    def merge(
        self,
        other: BranchCoverage,
        _option: MergeOptions,
    ) -> None:
        """
        Merge BranchCoverage information.

        Do not use 'other' objects afterwards!

            Examples:
        >>> filecov = FileCoverage("file.gcov", filename="file.cpp")
        >>> linecov_list = LineCoverageCollection(filecov, "not_used.gcov", lineno=100)
        >>> linecov = LineCoverage(linecov_list, "line.gcov", count=2, function_name="function")
        >>> left = BranchCoverage(linecov, "left.gcov", branchno=0, count=1, source_block_id=2)
        >>> right = BranchCoverage(linecov, "right.gcov", branchno=0, count=1, source_block_id=3)
        >>> left.merge(right, DEFAULT_MERGE_OPTIONS)
        Traceback (most recent call last):
          ...
        gcovr.exceptions.GcovrMergeAssertionError: file.cpp:100 (branch 0, source block 2) Source block ID must be equal, got 2 and 3.
        GCOV data file of merge source is:
           right.gcov
        and of merge target is:
           left.gcov
        >>> left = BranchCoverage(..., "left.gcov", branchno=0, count=1)
        >>> right = BranchCoverage(..., "right.gcov", branchno=0, count=4, source_block_id=2, fallthrough=False, throw=True)
        >>> right.excluded = True
        >>> left.merge(right, DEFAULT_MERGE_OPTIONS)
        >>> left.count
        5
        >>> left.source_block_id
        2
        >>> left.fallthrough
        False
        >>> left.throw
        True
        >>> left.excluded
        True
        """

        self.count += other.count
        self.fallthrough |= other.fallthrough
        self.throw |= other.throw
        self.branchno = self._merge_property(
            other, "Branch number", lambda x: x.branchno
        )
        self.source_block_id = self._merge_property(
            other, "Source block ID", lambda x: x.source_block_id
        )
        self.destination_block_id = self._merge_property(
            other, "Destination block ID", lambda x: x.destination_block_id
        )
        self.excluded |= other.excluded
        self.merge_base_data(other)

    @property
    def key(self) -> BranchcovKeyType:
        """Get the key used for the dictionary to unique identify the coverage object."""
        return (
            -1 if self.branchno is None else int(self.branchno),
            -1 if self.source_block_id is None else int(self.source_block_id),
            -1 if self.destination_block_id is None else int(self.destination_block_id),
        )

    @property
    def location(self) -> str | None:
        """Get the source location of the coverage data."""
        branch_info = []
        if self.branchno is not None:
            branch_info.append(f"branch {self.branchno}")
        if self.source_block_id is not None:
            branch_info.append(f"source block {self.source_block_id}")
        if self.destination_block_id is not None:
            branch_info.append(f"destination block {self.destination_block_id}")
        return str(self.parent.location) + (
            f" ({', '.join(branch_info)})" if branch_info else ""
        )

    @property
    def source_block_id_or_0(self) -> int:
        """Get a valid block number (0) if there was no definition in GCOV file."""
        if self.source_block_id is None:
            self.source_block_id = 0
            if BranchCoverage.first_undefined_source_block_id:
                BranchCoverage.first_undefined_source_block_id = False
                LOGGER.info("No block number defined, assuming 0 for all undefined")

        return self.source_block_id

    @property
    def is_excluded(self) -> bool:
        """Return True if the branch is excluded."""
        return self.excluded

    @property
    def is_reportable(self) -> bool:
        """Return True if the branch is reportable."""
        return not self.excluded

    @property
    def is_covered(self) -> bool:
        """Return True if the branch is covered."""
        return self.is_reportable and self.count > 0


class ConditionCoverage(CoverageBase):
    r"""Represent coverage information about a condition.

    Args:
        conditionno (int):
            The number of the condition.
        count (int):
            Number of condition outcomes in this expression.
        covered (int):
            Number of covered condition outcomes in this expression.
        not_covered_true list[int]:
            The conditions which were not true.
        not_covered_false list[int]:
            The conditions which were not false.
        excluded (bool, optional):
            Whether the condition is excluded.

        >>> filecov = FileCoverage("file.c", filename="file.c")
        >>> linecov_list = LineCoverageCollection(filecov, "not_used.gcov", lineno=11)
        >>> linecov = LineCoverage(linecov_list, "line.gcov", count=0, function_name="function")
        >>> ConditionCoverage(linecov, "call.gcov", conditionno=1, count=-1, covered=2, not_covered_true=[1], not_covered_false=[2])
        Traceback (most recent call last):
          ...
        gcovr.exceptions.GcovrDataAssertionError: file.c:11 (condition 1) count must not be a negative value.
        GCOV data file is:
           call.gcov
        >>> ConditionCoverage(linecov, [["call_1.gcov"], ["call_2.gcov"]], conditionno=1, count=2, covered=4, not_covered_true=[1], not_covered_false=[2])
        Traceback (most recent call last):
          ...
        gcovr.exceptions.GcovrDataAssertionError: file.c:11 (condition 1) count must not be less than covered.
        GCOV data files are:
           call_1.gcov
           call_2.gcov
        >>> ConditionCoverage(linecov, "call.gcov", conditionno=1, count=4, covered=2, not_covered_true=[1], not_covered_false=[1, 2])
        Traceback (most recent call last):
          ...
        gcovr.exceptions.GcovrDataAssertionError: file.c:11 (condition 1) The sum of the covered conditions (2), the uncovered true conditions (1) and the uncovered false conditions (2) must be equal to the count of conditions (4).
        GCOV data file is:
           call.gcov
    """

    __slots__ = (
        "parent",
        "conditionno",
        "count",
        "covered",
        "not_covered_true",
        "not_covered_false",
        "excluded",
    )

    def __init__(
        self,
        parent: LineCoverage,
        data_sources: str | set[tuple[str, ...]],
        *,
        conditionno: int,
        count: int,
        covered: int,
        not_covered_true: list[int],
        not_covered_false: list[int],
        excluded: bool = False,
    ) -> None:
        super().__init__(data_sources)
        self.parent = parent
        self.conditionno = conditionno
        self.count = count
        self.covered = covered
        self.not_covered_true = not_covered_true
        self.not_covered_false = not_covered_false
        self.excluded = excluded

        if count < 0:
            self.raise_data_error("count must not be a negative value.")
        if count < covered:
            self.raise_data_error("count must not be less than covered.")
        if count != (covered + len(not_covered_true) + len(not_covered_false)):
            self.raise_data_error(
                f"The sum of the covered conditions ({covered}),"
                f" the uncovered true conditions ({len(not_covered_true)})"
                f" and the uncovered false conditions ({len(not_covered_false)})"
                f" must be equal to the count of conditions ({count})."
            )

    def serialize(
        self,
        get_data_sources: Callable[[CoverageBase], dict[str, Any]],
    ) -> dict[str, Any]:
        """Serialize the object."""
        data_dict = {
            "conditionno": self.conditionno,
            "count": self.count,
            "covered": self.covered,
            "not_covered_false": self.not_covered_false,
            "not_covered_true": self.not_covered_true,
        }
        if self.excluded:
            data_dict[GCOVR_EXCLUDED] = True
        data_dict.update(get_data_sources(self))

        return data_dict

    @classmethod
    def deserialize(
        cls,
        linecov: LineCoverage,
        get_data_sources: Callable[[dict[str, Any]], set[tuple[str, ...]]],
        data_dict: dict[str, Any],
    ) -> ConditionCoverage:
        """Deserialize the object."""
        return linecov.insert_condition_coverage(
            get_data_sources(data_dict),
            conditionno=data_dict["conditionno"],
            count=data_dict["count"],
            covered=data_dict["covered"],
            not_covered_false=data_dict["not_covered_false"],
            not_covered_true=data_dict["not_covered_true"],
            excluded=data_dict.get(GCOVR_EXCLUDED, False),
        )

    def merge(
        self,
        other: ConditionCoverage,
        _option: MergeOptions,
    ) -> None:
        """
        Merge ConditionCoverage information.

        Do not use 'other' objects afterwards!

        Examples:
        >>> filecov = FileCoverage("file.gcov", filename="file.c")
        >>> linecov_list = LineCoverageCollection(filecov, "not_used.gcov", lineno=10)
        >>> linecov = LineCoverage(linecov_list, "line.gcov", count=2, function_name="function")
        >>> left = ConditionCoverage(linecov, "left.gcov", conditionno=1, count=4, covered=2, not_covered_true=[1, 2], not_covered_false=[])
        >>> right = ConditionCoverage(linecov, "right.gcov", conditionno=2, count=4, covered=1, not_covered_true=[2], not_covered_false=[1, 3])
        >>> left.merge(right, DEFAULT_MERGE_OPTIONS)
        Traceback (most recent call last):
          ...
        gcovr.exceptions.GcovrMergeAssertionError: file.c:10 (condition 1) The condition number must be equal, got 2 and expected 1.
        GCOV data file of merge source is:
           right.gcov
        and of merge target is:
           left.gcov
        >>> left = ConditionCoverage(linecov, "left.gcov", conditionno=1, count=4, covered=2, not_covered_true=[1, 2], not_covered_false=[])
        >>> right = ConditionCoverage(linecov, "right.gcov", conditionno=1, count=4, covered=1, not_covered_true=[2], not_covered_false=[1, 3], excluded=True)
        >>> left.merge(right, DEFAULT_MERGE_OPTIONS)
        >>> left.count
        4
        >>> left.covered
        3
        >>> left.not_covered_true
        [2]
        >>> left.not_covered_false
        []
        >>> left.excluded
        True
        """
        if self.conditionno != other.conditionno:
            self.raise_merge_error(
                f"The condition number must be equal, got {other.conditionno} and expected {self.conditionno}.",
                other,
            )
        if self.count != other.count:
            self.raise_merge_error(
                f"The number of conditions must be equal, got {other.count} and expected {self.count}.",
                other,
            )

        self.not_covered_false = sorted(
            list(set(self.not_covered_false) & set(other.not_covered_false))
        )
        self.not_covered_true = sorted(
            list(set(self.not_covered_true) & set(other.not_covered_true))
        )
        self.covered = (
            self.count - len(self.not_covered_false) - len(self.not_covered_true)
        )
        self.excluded |= other.excluded
        self.merge_base_data(other)

    @property
    def key(self) -> ConditioncovKeyType:
        """Get the key used for the dictionary to unique identify the coverage object."""
        return (self.conditionno, self.count)

    @property
    def location(self) -> str | None:
        """Get the source location of the coverage data."""
        return f"{self.parent.location} (condition {self.conditionno})"

    @property
    def is_excluded(self) -> bool:
        """Return True if the branch is excluded."""
        return self.excluded

    @property
    def is_reportable(self) -> bool:
        """Return True if the branch is reportable."""
        return not self.excluded

    @property
    def is_covered(self) -> bool:
        """Return True if the condition is covered."""
        return self.is_reportable and self.covered > 0

    @property
    def is_fully_covered(self) -> bool:
        """Return True if the condition is covered."""
        return self.is_reportable and self.covered == self.count


class DecisionCoverageUncheckable(CoverageBase):
    r"""Represent coverage information about a decision."""

    __slots__ = ("parent",)

    def __init__(
        self,
        parent: LineCoverage | None,
        data_sources: str | set[tuple[str, ...]],
    ) -> None:
        super().__init__(data_sources)
        self.parent = parent

    def serialize(
        self,
        get_data_sources: Callable[[CoverageBase], dict[str, Any]],
    ) -> dict[str, Any]:
        """Serialize the object."""
        data_dict = dict[str, Any]({"type": "uncheckable"})
        data_dict.update(get_data_sources(self))

        return data_dict

    @classmethod
    def deserialize(
        cls,
        linecov: LineCoverage,
        get_data_sources: Callable[[dict[str, Any]], set[tuple[str, ...]]],
        data_dict: dict[str, Any],
    ) -> None:
        """Deserialize the object."""
        linecov.insert_decision_coverage(
            DecisionCoverageUncheckable(linecov, get_data_sources(data_dict))
        )

    def merge(self, other: DecisionCoverageUncheckable) -> None:
        """Merge the decision coverage."""
        self.merge_base_data(other)

    @property
    def key(self) -> NoReturn:
        """Get the key used for the dictionary to unique identify the coverage object."""
        raise NotImplementedError("Function not implemented for decision objects.")

    @property
    def location(self) -> str | None:
        """Get a string defining the source location for the coverage data."""
        return None if self.parent is None else self.parent.location

    @property
    def is_covered(self) -> bool:
        """Return true if the decision is covered."""
        return True

    def coverage(self) -> DecisionCoverageStat:
        """Get the coverage stat."""
        return DecisionCoverageStat(0, 1, 2)  # TODO should it be uncheckable=2?


class DecisionCoverageConditional(CoverageBase):
    r"""Represent coverage information about a decision.

    Args:
        count_true (int):
            Number of times this decision was made.

        count_false (int):
            Number of times this decision was made.

    """

    __slots__ = "parent", "count_true", "count_false"

    def __init__(
        self,
        parent: LineCoverage | None,
        data_sources: str | set[tuple[str, ...]],
        *,
        count_true: int,
        count_false: int,
    ) -> None:
        super().__init__(data_sources)
        self.parent = parent
        if count_true < 0:
            self.raise_data_error("count_true must not be a negative value.")
        self.count_true = count_true
        if count_false < 0:
            self.raise_data_error("count_true must not be a negative value.")
        self.count_false = count_false

    def serialize(
        self,
        get_data_sources: Callable[[CoverageBase], dict[str, Any]],
    ) -> dict[str, Any]:
        """Serialize the object."""
        data_dict = dict[str, Any](
            {
                "type": "conditional",
                "count_true": self.count_true,
                "count_false": self.count_false,
            }
        )
        data_dict.update(get_data_sources(self))

        return data_dict

    @classmethod
    def deserialize(
        cls,
        linecov: LineCoverage,
        get_data_sources: Callable[[dict[str, Any]], set[tuple[str, ...]]],
        data_dict: dict[str, Any],
    ) -> None:
        """Deserialize the object."""
        linecov.insert_decision_coverage(
            DecisionCoverageConditional(
                linecov,
                get_data_sources(data_dict),
                count_true=data_dict["count_true"],
                count_false=data_dict["count_false"],
            )
        )

    def merge(self, other: DecisionCoverageConditional) -> None:
        """Merge the decision coverage."""
        self.count_true += other.count_true
        self.count_false += other.count_false
        self.merge_base_data(other)

    @property
    def key(self) -> NoReturn:
        """Get the key used for the dictionary to unique identify the coverage object."""
        raise NotImplementedError("Function not implemented for decision objects.")

    @property
    def location(self) -> str | None:
        """Get the source location of the coverage data."""
        return None if self.parent is None else self.parent.location

    @property
    def is_covered(self) -> bool:
        """Return true if the decision is covered."""
        return not (self.count_true == 0 or self.count_false == 0)

    def coverage(self) -> DecisionCoverageStat:
        """Get the coverage stat."""
        covered = 0
        if self.count_true > 0:
            covered += 1
        if self.count_false > 0:
            covered += 1
        return DecisionCoverageStat(covered, 0, 2)


class DecisionCoverageSwitch(CoverageBase):
    r"""Represent coverage information about a decision.

    Args:
        count (int):
            Number of times this decision was made.
    """

    __slots__ = "parent", "count"

    def __init__(
        self,
        parent: LineCoverage | None,
        data_sources: str | set[tuple[str, ...]],
        *,
        count: int,
    ) -> None:
        super().__init__(data_sources)
        self.parent = parent
        if count < 0:
            self.raise_data_error("count must not be a negative value.")
        self.count = count

    def serialize(
        self,
        get_data_sources: Callable[[CoverageBase], dict[str, Any]],
    ) -> dict[str, Any]:
        """Serialize the object."""
        data_dict = dict[str, Any](
            {
                "type": "switch",
                "count": self.count,
            }
        )
        data_dict.update(get_data_sources(self))

        return data_dict

    @classmethod
    def deserialize(
        cls,
        linecov: LineCoverage,
        get_data_sources: Callable[[dict[str, Any]], set[tuple[str, ...]]],
        data_dict: dict[str, Any],
    ) -> None:
        """Deserialize the object."""
        linecov.insert_decision_coverage(
            DecisionCoverageSwitch(
                linecov,
                get_data_sources(data_dict),
                count=data_dict["count"],
            )
        )

    def merge(self, other: DecisionCoverageSwitch) -> None:
        """Merge the decision coverage."""
        self.count += other.count
        self.merge_base_data(other)

    @property
    def key(self) -> NoReturn:
        """Get the key used for the dictionary to unique identify the coverage object."""
        raise NotImplementedError("Function not implemented for decision objects.")

    @property
    def location(self) -> str | None:
        """Get the source location of the coverage data."""
        return None if self.parent is None else self.parent.location

    @property
    def is_covered(self) -> bool:
        """Return true if the decision is covered."""
        return self.count != 0

    def coverage(self) -> DecisionCoverageStat:
        """Get the coverage stat."""
        covered = 0
        if self.count > 0:
            covered += 1
        return DecisionCoverageStat(covered, 0, 1)


DecisionCoverage = (
    DecisionCoverageUncheckable | DecisionCoverageConditional | DecisionCoverageSwitch
)


class CallCoverage(CoverageBase):
    r"""Represent coverage information about a call.

    Args:
        callno (int, optional):
            The number of the call, only used if destination_block_id is None.
        source_block_id (int):
            The block number.
        destination_block_id (int, optional):
            The destination block of the branch. None if unknown.
        returned (int):
            How often the function call returned.
        excluded (bool, optional):
            Whether the call is excluded.

        >>> filecov = FileCoverage("file.c", filename="file.c")
        >>> linecov_list = LineCoverageCollection(filecov, "not_used.gcov", lineno=11)
        >>> linecov = LineCoverage(linecov_list, "line.gcov", count=0, function_name="function")
        >>> CallCoverage(linecov, "call.gcov", callno=None, destination_block_id=None, returned=0, source_block_id=12)
        Traceback (most recent call last):
          ...
        gcovr.exceptions.GcovrDataAssertionError: file.c:11 (call 12->None) Either callno or destination_block_id must be set.
        GCOV data file is:
           call.gcov
        >>> CallCoverage(linecov, [["call_1.gcov"], ["call_2.gcov"]], callno=1, destination_block_id=1, returned=0, source_block_id=0)
        Traceback (most recent call last):
          ...
        gcovr.exceptions.GcovrDataAssertionError: file.c:11 (call 1) One of callno or destination_block_id must be set.
        GCOV data files are:
           call_1.gcov
           call_2.gcov
    """

    __slots__ = (
        "parent",
        "callno",
        "source_block_id",
        "destination_block_id",
        "returned",
        "excluded",
    )

    def __init__(
        self,
        parent: LineCoverage,
        data_sources: str | set[tuple[str, ...]],
        *,
        callno: int | None,
        source_block_id: int,
        destination_block_id: int | None,
        returned: int,
        excluded: bool = False,
    ) -> None:
        super().__init__(data_sources)
        self.parent = parent
        self.callno = callno
        self.source_block_id = source_block_id
        self.destination_block_id = destination_block_id
        self.returned = returned
        self.excluded = excluded

        if callno is None and destination_block_id is None:
            self.raise_data_error("Either callno or destination_block_id must be set.")
        if callno is not None and destination_block_id is not None:
            self.raise_data_error("One of callno or destination_block_id must be set.")

    def serialize(
        self,
        get_data_sources: Callable[[CoverageBase], dict[str, Any]],
    ) -> dict[str, Any]:
        """Serialize the object."""
        data_dict = dict[str, Any]()
        if self.callno is not None:
            data_dict["callno"] = self.callno
        data_dict["source_block_id"] = self.source_block_id
        if self.destination_block_id is not None:
            data_dict["destination_block_id"] = self.destination_block_id
        data_dict["returned"] = self.returned
        if self.excluded:
            data_dict[GCOVR_EXCLUDED] = True
        data_dict.update(get_data_sources(self))

        return data_dict

    @classmethod
    def deserialize(
        cls,
        linecov: LineCoverage,
        get_data_sources: Callable[[dict[str, Any]], set[tuple[str, ...]]],
        data_dict: dict[str, Any],
    ) -> CallCoverage:
        """Deserialize the object."""
        return linecov.insert_call_coverage(
            get_data_sources(data_dict),
            callno=data_dict.get("callno"),
            returned=data_dict["returned"],
            source_block_id=data_dict["source_block_id"],
            destination_block_id=data_dict.get("destination_block_id"),
            excluded=data_dict.get(GCOVR_EXCLUDED, False),
        )

    def merge(
        self,
        other: CallCoverage,
        _options: MergeOptions,
    ) -> None:
        """
        Merge CallCoverage information.

        Do not use 'left' or 'right' objects afterwards!

        >>> filecov = FileCoverage("file.gcov", filename="file.c")
        >>> linecov_list = LineCoverageCollection(filecov, "not_used.gcov", lineno=10)
        >>> linecov = LineCoverage(linecov_list, "line.gcov", count=2, function_name="function")
        >>> left = CallCoverage(linecov, "left.gcov", callno=1, source_block_id=1, destination_block_id=None, returned=2)
        >>> right = CallCoverage(linecov, "right.gcov", callno=2, source_block_id=10, destination_block_id=None, returned=1, excluded=True)
        >>> left.merge(right, DEFAULT_MERGE_OPTIONS)
        Traceback (most recent call last):
          ...
        gcovr.exceptions.GcovrMergeAssertionError: file.c:10 (call 1) Call number must be equal, got 1 and 2.
        GCOV data file of merge source is:
           right.gcov
        and of merge target is:
           left.gcov
        >>> right.callno = 1
        >>> left.merge(right, DEFAULT_MERGE_OPTIONS)
        Traceback (most recent call last):
          ...
        gcovr.exceptions.GcovrMergeAssertionError: file.c:10 (call 1) Source block ID must be equal, got 1 and 10.
        GCOV data file of merge source is:
           right.gcov
        and of merge target is:
           left.gcov
        >>> right.source_block_id = 1
        >>> left.merge(right, DEFAULT_MERGE_OPTIONS)
        >>> left.callno
        1
        >>> left.source_block_id
        1
        >>> left.destination_block_id == None
        True
        >>> left.returned
        3
        >>> left.excluded
        True
        >>> left = CallCoverage(linecov, "left.gcov", callno=None, source_block_id=1, destination_block_id=2, returned=2)
        >>> right = CallCoverage(linecov, "right.gcov", callno=None, source_block_id=1, destination_block_id=3, returned=1, excluded=True)
        >>> left.merge(right, DEFAULT_MERGE_OPTIONS)
        Traceback (most recent call last):
          ...
        gcovr.exceptions.GcovrMergeAssertionError: file.c:10 (call 1->2) Destination block ID must be equal, got 2 and 3.
        GCOV data file of merge source is:
           right.gcov
        and of merge target is:
           left.gcov
        """
        self.callno = self._merge_property(other, "Call number", lambda x: x.callno)
        self.source_block_id = self._merge_property(
            other, "Source block ID", lambda x: x.source_block_id
        )
        self.destination_block_id = self._merge_property(
            other, "Destination block ID", lambda x: x.destination_block_id
        )
        self.returned += other.returned
        self.excluded |= other.excluded
        self.merge_base_data(other)

    @property
    def key(self) -> CallcovKeyType:
        """Get the key used for the dictionary to unique identify the coverage object."""
        return (
            (-1 if self.callno is None else int(self.callno)),
            (-1 if self.source_block_id is None else int(self.source_block_id)),
            (
                -1
                if self.destination_block_id is None
                else int(self.destination_block_id)
            ),
        )

    @property
    def location(self) -> str | None:
        """Get the source location of the coverage data."""
        call_info = (
            f"{self.source_block_id}->{self.destination_block_id}"
            if self.callno is None
            else self.callno
        )
        return f"{self.parent.location} (call {call_info})"

    @property
    def is_excluded(self) -> bool:
        """Return True if the call is excluded."""
        return self.excluded

    @property
    def is_reportable(self) -> bool:
        """Return True if the call is reportable."""
        return not self.excluded

    @property
    def is_covered(self) -> bool:
        """Return True if the call is covered."""
        return self.is_reportable and self.returned != 0


class LineCoverage(CoverageBase):
    r"""Represent coverage information about a line.

    Each line is either *excluded* or *reportable*.

    A *reportable* line is either *covered* or *uncovered*.

    The default state of a line is *coverable*/*reportable*/*uncovered*.

    Args:
        count (int):
            How often this line was executed at least partially.
        function_name (str):
            Mangled name of the function the line belongs to.
        block_ids (*int, optional):
            List of block ids in this line
        excluded (bool, optional):
            Whether this line is excluded by a marker.

    >>> filecov = FileCoverage("file.gcov", filename="file.c")
    >>> linecov_list = LineCoverageCollection(filecov, "not_used.gcov", lineno=1)
    >>> linecov = LineCoverage(linecov_list, "line.gcov", count=-1, function_name=None)
    Traceback (most recent call last):
        ...
    gcovr.exceptions.GcovrDataAssertionError: file.c:1 count must not be a negative value.
    GCOV data file is:
       line.gcov
    """

    __slots__ = (
        "parent",
        "count",
        "function_name",
        "demangled_function_name",
        "block_ids",
        "excluded",
        "__branches",
        "__conditions",
        "decision",
        "__calls",
    )

    def __init__(
        self,
        parent: LineCoverageCollection,
        data_sources: str | set[tuple[str, ...]],
        *,
        count: int,
        function_name: str | None,
        block_ids: list[int] | None = None,
        excluded: bool = False,
    ) -> None:
        super().__init__(data_sources)
        self.parent = parent
        self.count = count
        self.function_name = function_name
        self.demangled_function_name: str | None = None
        self.block_ids = block_ids
        self.excluded = excluded
        self.__branches = CoverageDict[BranchcovKeyType, BranchCoverage]()
        self.__conditions = CoverageDict[ConditioncovKeyType, ConditionCoverage]()
        self.decision: DecisionCoverage | None = None
        self.__calls = CoverageDict[CallcovKeyType, CallCoverage]()

        if count < 0:
            self.raise_data_error("count must not be a negative value.")

    def serialize(
        self,
        get_data_sources: Callable[[CoverageBase], dict[str, Any]],
    ) -> dict[str, Any]:
        """Serialize the object."""
        data_dict = dict[str, Any](
            {
                "line_number": self.lineno,
            }
        )
        if self.function_name is not None:
            data_dict["function_name"] = self.function_name

        if self.block_ids is not None:
            data_dict["block_ids"] = self.block_ids

        data_dict.update(
            {
                "count": self.count,
                "branches": [
                    branchcov.serialize(get_data_sources)
                    for branchcov in self.branches(sort=True)
                ],
            }
        )
        if self.__conditions:
            data_dict["conditions"] = [
                conditioncov.serialize(get_data_sources)
                for conditioncov in self.conditions(sort=True)
            ]
        if self.decision is not None:
            data_dict["gcovr/decision"] = self.decision.serialize(get_data_sources)
        if len(self.__calls) > 0:
            data_dict["calls"] = [
                callcov.serialize(get_data_sources) for callcov in self.calls(sort=True)
            ]
        if self.md5:
            data_dict["gcovr/md5"] = self.md5
        if self.excluded:
            data_dict[GCOVR_EXCLUDED] = True
        data_dict.update(get_data_sources(self))

        return data_dict

    @classmethod
    def deserialize(
        cls,
        filecov: FileCoverage,
        get_data_sources: Callable[[dict[str, Any]], set[tuple[str, ...]]],
        data_dict: dict[str, Any],
    ) -> LineCoverage:
        """Deserialize the object."""
        linecov = filecov.insert_line_coverage(
            get_data_sources(data_dict),
            lineno=data_dict["line_number"],
            count=data_dict["count"],
            function_name=data_dict.get("function_name"),
            block_ids=data_dict.get("block_ids"),
            md5=data_dict.get("gcovr/md5"),
            excluded=data_dict.get(GCOVR_EXCLUDED, False),
        )

        for data_dict_branch in data_dict["branches"]:
            BranchCoverage.deserialize(linecov, get_data_sources, data_dict_branch)

        if (conditions := data_dict.get("conditions")) is not None:
            for data_dict_condition in conditions:
                ConditionCoverage.deserialize(
                    linecov, get_data_sources, data_dict_condition
                )

        if (data_dict_decision := data_dict.get("gcovr/decision")) is not None:
            decision_type = data_dict_decision["type"]
            if decision_type == "uncheckable":
                DecisionCoverageUncheckable.deserialize(
                    linecov, get_data_sources, data_dict_decision
                )
            elif decision_type == "conditional":
                DecisionCoverageConditional.deserialize(
                    linecov, get_data_sources, data_dict_decision
                )
            elif decision_type == "switch":
                DecisionCoverageSwitch.deserialize(
                    linecov, get_data_sources, data_dict_decision
                )
            else:  # pragma: no cover
                raise AssertionError(f"Unknown decision type: {decision_type!r}")

        if (calls := data_dict.get("calls")) is not None:
            for data_dict_call in calls:
                CallCoverage.deserialize(linecov, get_data_sources, data_dict_call)

        return linecov

    def merge(
        self,
        other: LineCoverage,
        options: MergeOptions,
    ) -> None:
        """
        Merge LineCoverage information.

        Do not use 'left' or 'right' objects afterwards!

        Precondition: both objects must have same lineno.
        """
        if self.function_name != other.function_name:
            self.raise_merge_error("Function name must be equal.", other)
        # Merge the block_ids if present
        if self.block_ids is None:
            self.block_ids = other.block_ids
        elif other.block_ids is not None:
            self.block_ids = sorted(set(self.block_ids) | set(other.block_ids))

        self.count += other.count
        self.excluded |= other.excluded
        # pylint: disable=protected-access
        self.__branches.merge(other.__branches, options)
        self.__conditions.merge(other.__conditions, options)
        self.__merge_decision(other.decision)
        self.__calls.merge(other.__calls, options)
        self.merge_base_data(other)

    def __merge_decision(  # pylint: disable=too-many-return-statements
        self,
        decisioncov: DecisionCoverage | None,
    ) -> None:
        """Merge DecisionCoverage information.

        The DecisionCoverage has different states:

        - None (no known decision)
        - Uncheckable (there was a decision, but it can't be analyzed properly)
        - Conditional
        - Switch

        If there is a conflict between different types, Uncheckable will be returned.
        """
        # If decision coverage is not known for one side, return the other.
        if self.decision is not None and decisioncov is not None:
            # If the type is different the result is Uncheckable.
            if type(self.decision) is type(decisioncov):
                self.decision.merge(decisioncov)  # type: ignore [arg-type]
            else:
                self.decision = DecisionCoverageUncheckable(
                    self,
                    set[tuple[str, ...]](
                        [*self.decision.data_sources, *decisioncov.data_sources]
                    ),
                )
        elif self.decision is None:
            self.decision = decisioncov

    def insert_branch_coverage(
        self,
        data_sources: str | set[tuple[str, ...]],
        *,
        branchno: int | None,
        count: int,
        fallthrough: bool = False,
        throw: bool = False,
        source_block_id: int | None = None,
        destination_block_id: int | None = None,
        excluded: bool = False,
    ) -> BranchCoverage:
        """Add a branch coverage item, merge if needed."""
        branchcov = BranchCoverage(
            self,
            data_sources,
            branchno=branchno,
            count=count,
            fallthrough=fallthrough,
            throw=throw,
            source_block_id=source_block_id,
            destination_block_id=destination_block_id,
            excluded=excluded,
        )
        key = branchcov.key
        if key in self.__branches:
            self.__branches[key].merge(branchcov, DEFAULT_MERGE_OPTIONS)
        else:
            self.__branches[key] = branchcov
            self.__branches[key].parent = self

        return branchcov

    def clear_branches(self) -> None:
        """Remove all the branches."""
        self.__branches.clear()

    def remove_branch(self, branchcov: BranchCoverage) -> None:
        """Remove the given branch."""
        del self.__branches[branchcov.key]

    def insert_condition_coverage(
        self,
        data_sources: str | set[tuple[str, ...]],
        *,
        conditionno: int,
        count: int,
        covered: int,
        not_covered_true: list[int],
        not_covered_false: list[int],
        excluded: bool = False,
    ) -> ConditionCoverage:
        """Add a condition coverage item, merge if needed."""
        conditioncov = ConditionCoverage(
            self,
            data_sources=data_sources,
            conditionno=conditionno,
            count=count,
            covered=covered,
            not_covered_true=not_covered_true,
            not_covered_false=not_covered_false,
            excluded=excluded,
        )
        key = conditioncov.key
        if key in self.__conditions:
            self.__conditions[key].merge(conditioncov, DEFAULT_MERGE_OPTIONS)
        else:
            self.__conditions[key] = conditioncov
            self.__conditions[key].parent = self

        return conditioncov

    def insert_decision_coverage(
        self,
        decisioncov: DecisionCoverage | None,
    ) -> None:
        """Add a condition coverage item, merge if needed."""
        self.__merge_decision(decisioncov)
        if self.decision is not None:
            self.decision.parent = self

    def insert_call_coverage(
        self,
        data_sources: str | set[tuple[str, ...]],
        *,
        callno: int | None,
        source_block_id: int,
        destination_block_id: int | None,
        returned: int,
        excluded: bool = False,
    ) -> CallCoverage:
        """Add a branch coverage item, merge if needed."""
        callcov = CallCoverage(
            self,
            data_sources=data_sources,
            callno=callno,
            source_block_id=source_block_id,
            destination_block_id=destination_block_id,
            returned=returned,
            excluded=excluded,
        )
        key = callcov.key
        if key in self.__calls:
            self.__calls[key].merge(callcov, DEFAULT_MERGE_OPTIONS)
        else:
            self.__calls[key] = callcov
            self.__calls[key].parent = self

        return callcov

    @property
    def key(self) -> LinecovKeyType:
        """Get the key used for the dictionary to unique identify the coverage object."""
        return "" if self.function_name is None else self.function_name

    @property
    def report_function_name(self) -> str:
        """Get the function name for this line to be shown in reports and logs."""
        return str(self.demangled_function_name or self.function_name)

    @property
    def location(self) -> str | None:
        """Get the source location of the coverage data."""
        return self.parent.location

    @property
    def lineno(self) -> int:
        """Get the source location of the coverage data."""
        return self.parent.lineno

    @property
    def md5(self) -> str | None:
        """Get the md5of the source line."""
        return self.parent.md5

    @property
    def is_excluded(self) -> bool:
        """Return True if the line is excluded."""
        return self.excluded

    @property
    def is_reportable(self) -> bool:
        """Return True if the line is reportable."""
        return not self.excluded

    @property
    def is_covered(self) -> bool:
        """Return True if the line is covered."""
        return self.is_reportable and self.count > 0

    @property
    def is_uncovered(self) -> bool:
        """Return True if the line is uncovered."""
        return self.is_reportable and self.count == 0

    @property
    def has_reportable_branches(self) -> bool:
        """Test if there are reportable branches."""
        return any(branchcov.is_reportable for branchcov in self.branches())

    @property
    def has_uncovered_branch(self) -> bool:
        """Test if the line has uncovered branches."""
        return not all(
            branchcov.is_covered or branchcov.is_excluded
            for branchcov in self.branches()
        )

    def branches(self, *, sort: bool = False) -> Iterable[BranchCoverage]:
        """Iterate over the branches."""
        if sort:
            yield from [v for _, v in sorted(self.__branches.items())]

        else:
            yield from self.__branches.values()

    @property
    def has_reportable_conditions(self) -> bool:
        """Test if there are reportable conditions."""
        return any(conditioncov.is_reportable for conditioncov in self.conditions())

    @property
    def has_uncovered_conditions(self) -> bool:
        """Test if the line has uncovered conditions."""
        return not all(
            conditioncov.is_covered or conditioncov.is_excluded
            for conditioncov in self.conditions()
        )

    def conditions(self, *, sort: bool = False) -> Iterable[ConditionCoverage]:
        """Iterate over the conditions."""
        if sort:
            yield from [v for _, v in sorted(self.__conditions.items())]
        else:
            yield from self.__conditions.values()

    @property
    def has_uncovered_decision(self) -> bool:
        """Test if the line has an uncovered decision."""
        if self.decision is None:
            return False

        return not self.decision.is_covered

    @property
    def has_reportable_calls(self) -> bool:
        """Test if there are reportable calls."""
        return any(callcov.is_reportable for callcov in self.calls())

    def calls(self, *, sort: bool = False) -> Iterable[CallCoverage]:
        """Iterate over the calls."""
        if sort:
            yield from [v for _, v in sorted(self.__calls.items())]
        else:
            yield from self.__calls.values()

    def exclude(self) -> None:
        """Exclude line from coverage statistic."""
        self.excluded = True
        for callcov in self.calls():
            callcov.excluded = True
        self.exclude_branches()

    def exclude_branches(self) -> None:
        """Exclude branches and conditions/decisions of line from coverage statistic."""
        for branchcov in self.branches():
            branchcov.excluded = True
        for conditioncov in self.conditions():
            conditioncov.excluded = True
        self.decision = None

    def branch_coverage(self) -> CoverageStat:
        """Return the branch coverage statistic of the line."""
        total_with_excluded = 0
        covered = 0
        excluded = 0
        for branchcov in self.__branches.values():
            total_with_excluded += 1
            if branchcov.is_reportable and branchcov.is_covered:
                covered += 1
            if branchcov.is_excluded:
                excluded += 1
        return CoverageStat(
            covered=covered, excluded=excluded, total_with_excluded=total_with_excluded
        )

    def condition_coverage(self) -> CoverageStat:
        """Return the condition coverage statistic of the line."""
        total_with_excluded = 0
        covered = 0
        excluded = 0
        for condition in self.__conditions.values():
            total_with_excluded += condition.count
            if condition.is_reportable:
                covered += condition.covered
            if condition.is_excluded:
                excluded += condition.count
        return CoverageStat(
            covered=covered, excluded=excluded, total_with_excluded=total_with_excluded
        )

    def decision_coverage(self) -> DecisionCoverageStat:
        """Return the decision coverage statistic of the line."""
        if self.decision is None:
            return DecisionCoverageStat(0, 0, 0)

        return self.decision.coverage()

    def call_coverage(self) -> CoverageStat:
        """Return the call coverage statistic of the line."""
        total_with_excluded = 0
        covered = 0
        excluded = 0
        for callcov in self.__calls.values():
            total_with_excluded += 1
            if callcov.is_reportable and callcov.is_covered:
                covered += 1
            if callcov.is_excluded:
                excluded += 1
        return CoverageStat(
            covered=covered, excluded=excluded, total_with_excluded=total_with_excluded
        )


class LineCoverageCollection(CoverageBase):
    r"""Represent coverage information about a line.

    Each line is either *excluded* or *reportable*.

    A *reportable* line is either *covered* or *uncovered*.

    The default state of a line is *coverable*/*reportable*/*uncovered*.

    Args:
        lineno (int):
            The line number.
        md5 (str, optional):
            The md5 checksum of the source code line.

    >>> filecov = FileCoverage("file.gcov", filename="file.c")
    >>> linecov_list = LineCoverageCollection(filecov, "line.gcov", lineno=0)
    Traceback (most recent call last):
        ...
    gcovr.exceptions.GcovrDataAssertionError: file.c:0 lineno must be a positive value.
    GCOV data file is:
       line.gcov
    """

    __slots__ = ("parent", "lineno", "md5", "__linecov", "__raw_linecov")

    def __init__(
        self,
        parent: FileCoverage,
        data_sources: str | set[tuple[str, ...]],
        *,
        lineno: int,
        md5: str | None = None,
    ) -> None:
        super().__init__(data_sources)
        self.parent = parent
        self.lineno = lineno
        self.md5 = md5
        self.__linecov = CoverageDict[LinecovKeyType, LineCoverage]()
        self.__raw_linecov = CoverageDict[LinecovKeyType, LineCoverage]()

        if lineno <= 0:
            self.raise_data_error("lineno must be a positive value.")

    def __setitem__(self, key: LinecovKeyType, item: LineCoverage) -> None:
        self.__linecov[key] = item

    def __getitem__(self, key: LinecovKeyType) -> LineCoverage:
        return self.__linecov[key]

    def __len__(self) -> int:
        return len(self.__linecov)

    def linecov(self, *, sort: bool = False) -> Iterable[LineCoverage]:
        """Iterate over the lines."""
        if sort:
            yield from [linecov for _, linecov in sorted(self.__linecov.items())]
        else:
            yield from self.__linecov.values()

    def raw_linecov(self, *, sort: bool = False) -> Iterable[LineCoverage]:
        """Iterate over the lines."""
        if self.__raw_linecov:
            if sort:
                yield from [
                    linecov for _, linecov in sorted(self.__raw_linecov.items())
                ]
            else:
                yield from self.__raw_linecov.values()
        else:
            yield from self.linecov(sort=sort)

    def merge_lines(self, *, replace: bool = False) -> LineCoverageCollection:
        """Merge line coverage if there are several items for same line."""
        if len(self) == 1:
            return self

        # Merge the information, needed for line coverage
        data_sources = set[tuple[str, ...]]()
        block_ids = set[int]()
        for linecov in self.linecov():
            data_sources.update(linecov.data_sources)
            if linecov.block_ids is not None:
                block_ids.update(linecov.block_ids)
        merged_linecov_collection = LineCoverageCollection(
            self.parent,
            data_sources,
            lineno=self.lineno,
            md5=self.md5,
        )
        merged_linecov = merged_linecov_collection.insert_line_coverage(
            data_sources,
            count=sum(
                linecov.count for linecov in self.linecov() if linecov.is_reportable
            ),
            function_name=None,
            block_ids=list(sorted(block_ids)) if block_ids else None,
            excluded=all(linecov.is_excluded for linecov in self.linecov()),
        )
        # ...and add the child objects
        for linecov in self.linecov():
            merged_linecov_collection.__raw_linecov[linecov.key] = linecov  # pylint: disable=protected-access
            for branchcov in linecov.branches():
                merged_linecov.insert_branch_coverage(
                    branchcov.data_sources,
                    branchno=branchcov.branchno,
                    count=branchcov.count,
                    fallthrough=branchcov.fallthrough,
                    throw=branchcov.throw,
                    source_block_id=branchcov.source_block_id,
                    destination_block_id=branchcov.destination_block_id,
                    excluded=branchcov.excluded,
                )
            for conditioncov in linecov.conditions():
                merged_linecov.insert_condition_coverage(
                    conditioncov.data_sources,
                    conditionno=conditioncov.conditionno,
                    count=conditioncov.count,
                    covered=conditioncov.covered,
                    not_covered_true=list(conditioncov.not_covered_true),
                    not_covered_false=list(conditioncov.not_covered_false),
                    excluded=conditioncov.excluded,
                )
            if (decisioncov := linecov.decision) is not None:
                if isinstance(decisioncov, DecisionCoverageUncheckable):
                    decisioncov = DecisionCoverageUncheckable(
                        merged_linecov,
                        decisioncov.data_sources,
                    )
                elif isinstance(decisioncov, DecisionCoverageConditional):
                    decisioncov = DecisionCoverageConditional(
                        merged_linecov,
                        decisioncov.data_sources,
                        count_true=decisioncov.count_true,
                        count_false=decisioncov.count_false,
                    )
                elif isinstance(decisioncov, DecisionCoverageSwitch):
                    decisioncov = DecisionCoverageSwitch(
                        merged_linecov,
                        decisioncov.data_sources,
                        count=decisioncov.count,
                    )
                else:  # pragma: no cover
                    raise AssertionError("Unknown decision type.")
                merged_linecov.insert_decision_coverage(decisioncov)
            for callcov in linecov.calls():
                merged_linecov.insert_call_coverage(
                    callcov.data_sources,
                    callno=callcov.callno,
                    source_block_id=callcov.source_block_id,
                    destination_block_id=callcov.destination_block_id,
                    returned=callcov.returned,
                    excluded=callcov.excluded,
                )

        if replace:
            self.__linecov = merged_linecov_collection.__linecov  # pylint: disable=protected-access
            self.__raw_linecov = merged_linecov_collection.__raw_linecov  # pylint: disable=protected-access

        return merged_linecov_collection

    @property
    def key(self) -> LinecovCollectionKeyType:
        """Get the key for the dict."""
        return self.lineno

    @property
    def location(self) -> str | None:
        """Get the source location of the coverage data."""
        return f"{self.parent.location}:{self.lineno}"

    @property
    def count(self) -> int:
        """Get the overall count of the line."""
        return sum(linecov.count for linecov in self.linecov())

    @property
    def is_excluded(self) -> bool:
        """Return True if the line is excluded."""
        return all(linecov.excluded for linecov in self.linecov())

    @property
    def is_reportable(self) -> bool:
        """Return True if the line is reportable."""
        return not self.is_excluded

    @property
    def is_covered(self) -> bool:
        """Return True if the line is covered."""
        return self.is_reportable and self.count > 0

    @property
    def is_uncovered(self) -> bool:
        """Return True if the line is uncovered."""
        return self.is_reportable and self.count == 0

    def exclude(self) -> None:
        """Exclude line from coverage statistic."""
        for linecov in self.linecov():
            linecov.exclude()

    def exclude_branches(self) -> None:
        """Exclude line from coverage statistic."""
        for linecov in self.linecov():
            linecov.exclude_branches()

    def decision_coverage(self) -> DecisionCoverageStat:
        """Return the decision coverage statistic of the line."""
        stat = DecisionCoverageStat.new_empty()
        for linecov in self.linecov():
            stat += linecov.decision_coverage()
        return stat

    def insert_line_coverage(
        self,
        data_sources: str | set[tuple[str, ...]],
        options: MergeOptions = DEFAULT_MERGE_OPTIONS,
        *,
        count: int,
        function_name: str | None,
        block_ids: list[int] | None = None,
        excluded: bool = False,
    ) -> LineCoverage:
        """Add a line coverage item, merge if needed."""
        linecov = LineCoverage(
            self,
            data_sources,
            count=count,
            function_name=function_name,
            block_ids=block_ids,
            excluded=excluded,
        )
        key = linecov.key
        if key in self.__linecov:
            self.__linecov[key].merge(linecov, options)
        else:
            self.__linecov[key] = linecov
            self.__linecov[key].parent = self

        return self.__linecov[key]

    def remove_line_coverage(self, linecov: LineCoverage) -> None:
        """Remove line coverage object from line coverage collection."""
        del self.__linecov[linecov.key]
        # Remove the line coverage collection if no data is available anymore for the line.
        if not self.__linecov:
            self.parent.remove_line(linecov.lineno)

    def merge(
        self,
        other: LineCoverageCollection,
        options: MergeOptions,
    ) -> None:
        """Merge the data of the line coverage object."""
        if self.lineno != other.lineno:
            self.raise_merge_error("Line number must be equal.", other)
        self.md5 = self._merge_property(other, "MD5 checksum", lambda x: x.md5)
        # pylint: disable=protected-access
        self.__linecov.merge(other.__linecov, options)
        self.merge_base_data(other)


class FunctionCoverage(CoverageBase):
    r"""Represent coverage information about a function.

    The counter is stored as dictionary with the line as key to be able
    to merge function coverage in different ways

    Args:
        mangled_name (str):
            The mangled name of the function. If demangled_name is None and
            the name contains a brace it's used as demangled_name. This is needed
            to support existing GCOV text output where we do not know if the
            option --demanglednames was used for generation. If it contains a brace
            the demangled name must be None.
        demangled_name (str):
            The demangled name of the functions.
        lineno (int):
            The line number.
        count (int):
            How often this function was executed.
        blocks (float):
            Block coverage of function.
        start ((int, int)), optional):
            Tuple with function start line and column.
        end ((int, int)), optional):
            Tuple with function end line and column.
        excluded (bool, optional):
            Whether this line is excluded by a marker.

        >>> filecov = FileCoverage("file.c", filename="file.c")
        >>> FunctionCoverage(filecov, "func.gcov", mangled_name="foo()", demangled_name="bar()", lineno=5, count=3, blocks=0.5)
        Traceback (most recent call last):
          ...
        gcovr.exceptions.GcovrDataAssertionError: file.c:5 Got foo() as 'mangled_name', in this case 'demangled_name' must be None.
        GCOV data file is:
           func.gcov
        >>> FunctionCoverage(filecov, "func.gcov", mangled_name="foo()", demangled_name=None, lineno=-1, count=1, blocks=0.5)
        Traceback (most recent call last):
          ...
        gcovr.exceptions.GcovrDataAssertionError: file.c:-1 lineno must not be a negative value.
        GCOV data file is:
           func.gcov
        >>> FunctionCoverage(filecov, "func.gcov", mangled_name="foo()", demangled_name=None, lineno=1, count=-1, blocks=0.5)
        Traceback (most recent call last):
          ...
        gcovr.exceptions.GcovrDataAssertionError: file.c:1 count must not be a negative value.
        GCOV data file is:
           func.gcov
    """

    __slots__ = (
        "parent",
        "mangled_name",
        "demangled_name",
        "count",
        "blocks",
        "start",
        "end",
        "excluded",
    )

    def __init__(
        self,
        parent: FileCoverage,
        data_sources: str | set[tuple[str, ...]],
        *,
        mangled_name: str | None,
        demangled_name: str | None,
        lineno: int,
        count: int | None,
        blocks: float | None,
        start: tuple[int, int] | None = None,
        end: tuple[int, int] | None = None,
        excluded: bool = False,
    ) -> None:
        super().__init__(data_sources)
        self.parent = parent
        self.count = CoverageDict[int, int | None]({lineno: count})
        self.blocks = CoverageDict[int, float | None]({lineno: blocks})
        self.excluded = CoverageDict[int, bool]({lineno: excluded})
        self.start: CoverageDict[int, tuple[int, int]] | None = (
            None
            if start is None
            else CoverageDict[int, tuple[int, int]]({lineno: start})
        )
        self.end: CoverageDict[int, tuple[int, int]] | None = (
            None if end is None else CoverageDict[int, tuple[int, int]]({lineno: end})
        )

        if mangled_name is not None:
            # We have a demangled name as name -> demangled_name must be None and we need to change the values
            if "(" in mangled_name:
                # Set the value to have the correct error message.
                self.demangled_name = demangled_name
                if demangled_name is not None:
                    self.raise_data_error(
                        f"Got {mangled_name} as 'mangled_name', in this case 'demangled_name' must be None."
                    )
                # Change the attribute values
                mangled_name, demangled_name = (None, mangled_name)
        self.mangled_name = mangled_name
        self.demangled_name = demangled_name

        if lineno < 0:
            self.raise_data_error("lineno must not be a negative value.")
        if count is not None and count < 0:
            self.raise_data_error("count must not be a negative value.")

    def serialize(
        self,
        get_data_sources: Callable[[CoverageBase], dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Serialize the object."""
        data_dict_list = list[dict[str, Any]]()
        for lineno, count in self.count.items():
            data_dict = dict[str, Any]()
            if self.mangled_name is not None:
                data_dict["name"] = self.mangled_name
            if self.demangled_name is not None:
                data_dict["demangled_name"] = self.demangled_name
            data_dict["lineno"] = lineno
            if count is not None:
                data_dict["execution_count"] = count
            if self.blocks[lineno] is not None:
                data_dict["blocks_percent"] = self.blocks[lineno]
            if self.start is not None and self.end is not None:
                data_dict["pos"] = (
                    ":".join([str(e) for e in self.start[lineno]]),
                    ":".join([str(e) for e in self.end[lineno]]),
                )
            if self.excluded[lineno]:
                data_dict[GCOVR_EXCLUDED] = True
            data_dict.update(get_data_sources(self))
            data_dict_list.append(data_dict)

        return data_dict_list

    @classmethod
    def deserialize(
        cls,
        filecov: FileCoverage,
        merge_options: MergeOptions,
        get_data_sources: Callable[[dict[str, Any]], set[tuple[str, ...]]],
        data_dict: dict[str, Any],
    ) -> FunctionCoverage:
        """Deserialize the object."""
        start: tuple[int, int] | None = None
        end: tuple[int, int] | None = None
        if "pos" in data_dict:
            start_l_c = data_dict["pos"][0].split(":", maxsplit=1)
            start = (int(start_l_c[0]), int(start_l_c[1]))
            end_l_c = data_dict["pos"][1].split(":", maxsplit=1)
            end = (int(end_l_c[0]), int(end_l_c[1]))

        return filecov.insert_function_coverage(
            get_data_sources(data_dict),
            merge_options,
            mangled_name=data_dict.get("name"),
            demangled_name=data_dict.get("demangled_name"),
            lineno=data_dict["lineno"],
            count=data_dict.get("execution_count"),
            blocks=data_dict.get("blocks_percent"),
            start=start,
            end=end,
            excluded=data_dict.get(GCOVR_EXCLUDED, False),
        )

    def merge(
        self,
        other: FunctionCoverage,
        options: MergeOptions,
    ) -> None:
        """
        Merge FunctionCoverage information.

        Do not use 'left' or 'right' objects afterwards!

        Precondition: both objects must have same name and lineno.

        If ``options.func_opts.ignore_function_lineno`` is set,
        the two function coverage objects can have differing line numbers.
        With following flags the merge mode can be defined:
        - ``options.func_opts.merge_function_use_line_zero``
        - ``options.func_opts.merge_function_use_line_min``
        - ``options.func_opts.merge_function_use_line_max``
        - ``options.func_opts.separate_function``
        """
        self.demangled_name = self._merge_property(
            other, "Function demangled name", lambda x: x.demangled_name
        )
        # If we have a demangled name use the first mangled name
        # For virtual constructors/destructors several mangled functions map to the same demangled name,
        # see https://itanium-cxx-abi.github.io/cxx-abi/abi.html#mangling-special-ctor-dtor:
        # <ctor-dtor-name> ::= C1                     # complete object constructor
        #                  ::= C2                     # base object constructor
        #                  ::= C3                     # complete object allocating constructor
        #                  ::= CI1 <base class type>  # complete object inheriting constructor
        #                  ::= CI2 <base class type>  # base object inheriting constructor
        #                  ::= D0                     # deleting destructor
        #                  ::= D1                     # complete object destructor
        #                  ::= D2                     # base object destructor
        if self.demangled_name is not None:
            if self.mangled_name is None:
                self.mangled_name = other.mangled_name
            elif (
                other.mangled_name is not None
                and other.mangled_name < self.mangled_name
            ):
                self.mangled_name = other.mangled_name
        # If we do not have mangled names the mangled name must be the same.
        else:
            self.mangled_name = self._merge_property(
                other, "Function mangled name", lambda x: x.mangled_name
            )

        if not options.func_opts.ignore_function_lineno:
            if self.count.keys() != other.count.keys():
                lines = sorted(set([*self.count.keys(), *other.count.keys()]))
                self.raise_merge_error(
                    f"Got function {self.name} on multiple lines: {', '.join([str(line) for line in lines])}.\n"
                    "\tYou can run gcovr with --merge-mode-functions=MERGE_MODE.\n"
                    "\tThe available values for MERGE_MODE are described in the documentation.",
                    other,
                )

        # Keep distinct counts for each line number
        if options.func_opts.separate_function:
            for lineno, count in other.count.items():
                try:
                    if self.count[lineno] is None:
                        self.count[lineno] = count
                    elif count is not None:
                        self.count[lineno] += count  # type: ignore [operator]
                except KeyError:
                    self.count[lineno] = count
            for lineno, blocks in other.blocks.items():
                try:
                    # Take the maximum value for this line
                    if self.blocks[lineno] is None:
                        self.blocks[lineno] = blocks
                    elif blocks is not None and self.blocks[lineno] < blocks:  # type: ignore [operator]
                        self.blocks[lineno] = blocks
                except KeyError:
                    self.blocks[lineno] = blocks
            for lineno, excluded in other.excluded.items():
                try:
                    self.excluded[lineno] |= excluded
                except KeyError:
                    self.excluded[lineno] = excluded
            if other.start is not None:
                if self.start is None:
                    self.start = CoverageDict[int, tuple[int, int]]()
                for lineno, start in other.start.items():
                    self.start[lineno] = start
            if other.end is not None:
                if self.end is None:
                    self.end = CoverageDict[int, tuple[int, int]]()
                for lineno, end in other.end.items():
                    self.end[lineno] = end
        # Merge the counters into a single line number
        else:
            right_lineno = list(other.count.keys())[0]
            # merge all counts into an entry for a single line number
            if right_lineno in self.count:
                lineno = right_lineno
            elif options.func_opts.merge_function_use_line_zero:
                lineno = 0
            elif options.func_opts.merge_function_use_line_min:
                lineno = min(*self.count.keys(), *other.count.keys())
            elif options.func_opts.merge_function_use_line_max:
                lineno = max(*self.count.keys(), *other.count.keys())
            else:  # pragma: no cover
                raise AssertionError("Unknown merge mode")

            # Overwrite data with the sum at the desired line
            self.count = CoverageDict[int, int | None](
                {
                    lineno: None
                    if None in self.count.values() or None in other.count.values()
                    else sum(
                        int(value or 0)
                        for value in [*self.count.values(), *other.count.values()]
                    )
                }
            )
            # or the max value at the desired line
            self.blocks = CoverageDict[int, float | None](
                {
                    lineno: None
                    if None in self.blocks.values() or None in other.blocks.values()
                    else max(
                        float(value or 0)
                        for value in [*self.blocks.values(), *other.blocks.values()]
                    )
                }
            )
            # or the logical or of all values
            self.excluded = CoverageDict[int, bool](
                {lineno: any(self.excluded.values()) or any(other.excluded.values())}
            )

            if self.start is not None and other.start is not None:
                # or the minimum start
                self.start = CoverageDict[int, tuple[int, int]](
                    {lineno: min(*self.start.values(), *other.start.values())}
                )
            if self.end is not None and other.end is not None:
                # or the maximum end
                self.end = CoverageDict[int, tuple[int, int]](
                    {lineno: max(*self.end.values(), *other.end.values())}
                )

        self.merge_base_data(other)

    @property
    def key(self) -> FunctioncovKeyType:
        """Get the key for the dict."""
        return self.name

    @property
    def location(self) -> str | None:
        """Get the source location of the coverage data."""
        lines = sorted(self.count.keys())
        lines_as_string = str(lines.pop(0))
        if lines:
            lines_as_string += f" ({', '.join(str(line) for line in lines)})"
        return f"{self.parent.location}:{lines_as_string}"

    def exclude(self, lineno: int) -> None:
        """Exclude line from coverage statistic."""
        if lineno not in self.excluded:  # pragma: no cover
            raise SanityCheckError("Unknown lineno to exclude.")
        self.excluded[lineno] = True

    @property
    def linenos(self) -> list[int]:
        """Get the list of line numbers for this function which are not excluded."""
        return list(sorted(self.excluded.keys()))

    @property
    def reportable_linenos(self) -> list[int]:
        """Get the list of line numbers for this function which are not excluded."""
        return list(
            sorted(lineno for lineno, excluded in self.excluded.items() if not excluded)
        )

    @property
    def name(self) -> str:
        """Get the function name. This is the demangled name if present, else the mangled name."""
        return str(self.demangled_name or self.mangled_name)

    def is_function(self, name: str | None) -> bool:
        """Is the name one of the function."""
        return name is not None and (name in (self.mangled_name, self.demangled_name))

    @property
    def name_and_signature(self) -> tuple[str, str]:
        """Get a tuple with function name and signature, if signature is un."""
        if self.demangled_name is None:
            return (str(self.name), "")

        if "(" not in self.demangled_name:
            return (str(self.demangled_name), "")

        open_brackets, close_brackets = (0, 0)
        signature = ""
        for part in reversed(self.demangled_name.split("(")):
            signature = f"({part}{signature}"
            open_brackets += 1
            close_brackets += len(re.findall(r"(\))", part))
            if open_brackets == close_brackets:
                break
        else:
            self.raise_data_error(
                f"Can't split function {self.demangled_name!r} into name and signature."
            )

        return (self.demangled_name[: -len(signature)], signature)


class FileCoverage(CoverageBase):
    """Represent coverage information about a file."""

    __slots__ = "filename", "__functions", "__lines", "__linecov_by_function"

    def __init__(
        self,
        data_sources: str | set[tuple[str, ...]],
        *,
        filename: str,
    ) -> None:
        super().__init__(data_sources)
        self.filename: str = filename
        self.__functions = CoverageDict[FunctioncovKeyType, FunctionCoverage]()
        self.__lines = CoverageDict[LinecovCollectionKeyType, LineCoverageCollection]()
        self.__linecov_by_function = CoverageDict[str, list[LineCoverage]]()

    def serialize(self, options: Options) -> dict[str, Any]:
        """Serialize the object."""
        # Only write data source files if requested
        if options.json_trace_data_source:

            def get_data_sources(cov: CoverageBase) -> dict[str, Any]:
                """Return the printable data sources."""
                return {
                    GCOVR_DATA_SOURCES: [
                        [
                            _presentable_filename(filename, options.root_filter)
                            for filename in data_sources
                        ]
                        for data_sources in sorted(cov.data_sources)
                    ]
                }
        else:

            def get_data_sources(cov: CoverageBase) -> dict[str, Any]:  # pylint: disable=unused-argument
                """Stub if not running in verbose mode."""
                return {}

        filename = self.presentable_filename(options.root_filter)
        if options.json_base:
            filename = "/".join([options.json_base, filename])
        data_dict = {
            "file": filename,
            "lines": [
                linecov.serialize(get_data_sources)
                for linecov in self.raw_linecov(sort=True)
            ],
            "functions": [
                f
                for functioncov in self.functioncov(sort=True)
                for f in functioncov.serialize(get_data_sources)
            ],
        }
        data_dict.update(get_data_sources(self))

        return data_dict

    @classmethod
    def deserialize(
        cls,
        data_sources: str,
        data_dict: dict[str, Any],
        merge_options: MergeOptions,
        options: Options,
    ) -> FileCoverage | None:
        """Deserialize the object."""
        filename = os.path.join(
            os.path.abspath(options.root), os.path.normpath(data_dict["file"])
        )

        if is_file_excluded(
            "source file", filename, options.include_filter, options.exclude_filter
        ):
            return None

        def get_data_sources(data_dict: dict[str, Any]) -> set[tuple[str, ...]]:
            """Return the set for data sources."""
            return set(
                (*e,) for e in data_dict.get(GCOVR_DATA_SOURCES, [[data_sources]])
            )

        filecov = FileCoverage(
            get_data_sources(data_dict),
            filename=filename,
        )
        for data_dict_line in data_dict["lines"]:
            LineCoverage.deserialize(filecov, get_data_sources, data_dict_line)
        for data_dict_function in data_dict["functions"]:
            FunctionCoverage.deserialize(
                filecov, merge_options, get_data_sources, data_dict_function
            )
        return filecov

    def merge(
        self,
        other: FileCoverage,
        options: MergeOptions,
    ) -> None:
        """
        Merge FileCoverage information.

        Do not use 'other' objects afterwards!

        Precondition: both objects have same filename.
        """

        if self.filename != other.filename:
            self.raise_data_error("Filename must be equal")

        # pylint: disable=protected-access
        self.__lines.merge(other.__lines, options)
        self.__functions.merge(other.__functions, options)
        self.merge_base_data(other)

    @property
    def key(self) -> NoReturn:
        """Get the key used for the dictionary to unique identify the coverage object."""
        raise NotImplementedError(
            "Function not implemented for file coverage object, use property 'filename' instead."
        )

    @property
    def location(self) -> str | None:
        """Get the source location of the coverage data."""
        return self.filename

    def presentable_filename(self, root_filter: re.Pattern[str]) -> str:
        """Mangle a filename so that it is suitable for a report."""
        return _presentable_filename(self.filename, root_filter)

    def functioncov(
        self,
        *,
        sort: bool = False,
        key: Callable[[FunctionCoverage], Any] | None = None,
    ) -> Iterable[FunctionCoverage]:
        """Iterate over the function coverage object."""
        if sort or key:
            if key is None:

                def key_func(functioncov: FunctionCoverage) -> Any:
                    return (
                        min(functioncov.count.keys()),
                        functioncov.name,
                    )
            else:

                def key_func(functioncov: FunctionCoverage) -> Any:
                    return key(functioncov)

            yield from sorted(self.__functions.values(), key=key_func)
        else:
            yield from self.__functions.values()

    def get_functioncov(self, name: str) -> FunctionCoverage:
        """Get the function coverage object of a function."""
        return self.__functions[name]

    def has_lines(self) -> bool:
        """Test if there are line coverage collections."""
        return bool(self.__lines)

    def lines(self, *, sort: bool = False) -> Iterable[LineCoverageCollection]:
        """Iterate over the line coverage collection objects."""
        if sort:
            yield from [
                linecov_collection
                for _, linecov_collection in sorted(self.__lines.items())
            ]
        else:
            yield from self.__lines.values()

    def get_line(self, lineno: int) -> LineCoverageCollection | None:
        """Get the line coverage collection of the given line."""
        return self.__lines.get(lineno)

    def remove_line(self, lineno: int) -> None:
        """Remove the line coverage collection for the given line."""
        if lineno not in self.__lines:  # pragma: no cover
            raise SanityCheckError("Unknown line to remove.")
        del self.__lines[lineno]

    def merge_lines(self, activate_trace_logging: bool) -> None:
        """Merge line coverage if there are several items for same line."""
        merged_lines = []
        for linecov_collection in self.lines(sort=True):
            merged_linecov = linecov_collection.merge_lines(replace=True)
            if merged_linecov is not linecov_collection:
                merged_lines.append(linecov_collection.lineno)
        if activate_trace_logging and merged_lines:
            LOGGER.trace(
                "%s: Merged line coverage objects for lines: %s",
                self.location,
                ", ".join(str(lineno) for lineno in merged_lines),
            )

    def has_linecov(self) -> bool:
        """Test if there are line coverage objects available."""
        return any(linecov_collection for linecov_collection in self.lines())

    def linecov(self, *, sort: bool = False) -> Iterable[LineCoverage]:
        """Iterate over the line coverage objects."""
        for linecov_collection in self.lines(sort=sort):
            yield from linecov_collection.linecov(sort=sort)

    def raw_linecov(self, *, sort: bool = False) -> Iterable[LineCoverage]:
        """Iterate over the line coverage objects."""
        for linecov_collection in self.lines(sort=sort):
            yield from linecov_collection.raw_linecov(sort=sort)

    @property
    def stats(self) -> SummarizedStats:
        """Create a coverage statistic of a file coverage object."""
        return SummarizedStats(
            line=self.line_coverage(),
            branch=self.branch_coverage(),
            condition=self.condition_coverage(),
            decision=self.decision_coverage(),
            function=self.function_coverage(),
            call=self.call_coverage(),
        )

    def insert_line_coverage(
        self,
        data_sources: str | set[tuple[str, ...]],
        options: MergeOptions = DEFAULT_MERGE_OPTIONS,
        *,
        lineno: int,
        count: int,
        function_name: str | None,
        block_ids: list[int] | None = None,
        md5: str | None = None,
        excluded: bool = False,
    ) -> LineCoverage:
        """Add a line coverage item, merge if needed."""
        linecov_collection = LineCoverageCollection(
            self, data_sources, lineno=lineno, md5=md5
        )
        key = linecov_collection.key
        if key in self.__lines:
            self.__lines[key].merge(linecov_collection, options)
        else:
            self.__lines[key] = linecov_collection
            self.__lines[key].parent = self

        linecov = self.__lines[key].insert_line_coverage(
            data_sources,
            options,
            count=count,
            function_name=function_name,
            block_ids=block_ids,
            excluded=excluded,
        )
        if function_name is not None:
            if function_name not in self.__linecov_by_function:
                self.__linecov_by_function[function_name] = list[LineCoverage]()
            self.__linecov_by_function[function_name].append(linecov)

        return linecov

    def remove_line_coverage(self, linecov: LineCoverage) -> None:
        """Remove line coverage objects."""
        self.__lines[linecov.parent.key].remove_line_coverage(linecov)
        if linecov.function_name is not None:
            self.__linecov_by_function[linecov.function_name] = [
                current_linecov
                for current_linecov in self.__linecov_by_function[linecov.function_name]
                if current_linecov != linecov
            ]

    def insert_function_coverage(
        self,
        data_sources: str | set[tuple[str, ...]],
        options: MergeOptions = DEFAULT_MERGE_OPTIONS,
        *,
        mangled_name: str | None,
        demangled_name: str | None,
        lineno: int,
        count: int | None,
        blocks: float | None,
        start: tuple[int, int] | None = None,
        end: tuple[int, int] | None = None,
        excluded: bool = False,
    ) -> FunctionCoverage:
        """Add a function coverage item, merge if needed."""
        functioncov = FunctionCoverage(
            self,
            data_sources,
            mangled_name=mangled_name,
            demangled_name=demangled_name,
            lineno=lineno,
            count=count,
            blocks=blocks,
            start=start,
            end=end,
            excluded=excluded,
        )
        key = functioncov.key
        if key in self.__functions:
            self.__functions[key].merge(functioncov, options)
        else:
            self.__functions[key] = functioncov
            self.__functions[key].parent = self
        if (
            functioncov.demangled_name is not None
            and functioncov.mangled_name is not None
            and functioncov.mangled_name in self.__linecov_by_function
        ):
            for linecov in self.__linecov_by_function[functioncov.mangled_name]:
                linecov.demangled_function_name = functioncov.demangled_name

        return functioncov

    def remove_function_coverage(self, functioncov: FunctionCoverage) -> None:
        """Remove line coverage objects."""
        # Remove function and exclude the related lines
        del self.__functions[functioncov.key]
        # Iterate over a shallow copy
        for linecov in list(self.linecov()):
            if functioncov.is_function(linecov.function_name):
                self.remove_line_coverage(linecov)

    def filter_for_function(self, functioncov: FunctionCoverage) -> FileCoverage:
        """Get a file coverage object reduced to a single function"""
        if functioncov.key not in self.__functions:
            self.raise_data_error(
                f"Function {functioncov.key} must be in filtered file coverage object."
            )
        filecov = FileCoverage(self.data_sources, filename=self.filename)
        # pylint: disable=protected-access
        filecov.__functions[functioncov.key] = functioncov

        def add_linecov_to_collection(linecov: LineCoverage) -> None:
            """Add a new linecov item."""
            lineno = linecov.lineno
            filecov.__lines[lineno] = LineCoverageCollection(
                filecov, linecov.data_sources, lineno=lineno
            )
            filecov.__lines[lineno][linecov.key] = linecov

        for linecov in self.raw_linecov():
            if functioncov.is_function(linecov.function_name):
                add_linecov_to_collection(linecov)

        return filecov

    def function_coverage(self) -> CoverageStat:
        """Return the function coverage statistic of the file."""
        total_with_excluded = 0
        covered = 0
        excluded = 0

        for functioncov in self.functioncov():
            for lineno, excluded_function in functioncov.excluded.items():
                total_with_excluded += 1
                if excluded_function:
                    excluded += 1
                else:
                    if (functioncov.count[lineno] or 0) > 0:
                        covered += 1

        return CoverageStat(
            covered=covered, excluded=excluded, total_with_excluded=total_with_excluded
        )

    def line_coverage(self) -> CoverageStat:
        """Return the line coverage statistic of the file."""
        total_with_excluded = 0
        covered = 0
        excluded = 0

        for linecov in self.linecov():
            total_with_excluded += 1
            if linecov.is_reportable and linecov.is_covered:
                covered += 1
            if linecov.is_excluded:
                excluded += 1

        return CoverageStat(
            covered=covered, excluded=excluded, total_with_excluded=total_with_excluded
        )

    def branch_coverage(self) -> CoverageStat:
        """Return the branch coverage statistic of the file."""
        stat = CoverageStat.new_empty()

        for linecov in self.linecov():
            stat += linecov.branch_coverage()

        return stat

    def condition_coverage(self) -> CoverageStat:
        """Return the condition coverage statistic of the file."""
        stat = CoverageStat.new_empty()

        for linecov in self.linecov():
            stat += linecov.condition_coverage()

        return stat

    def decision_coverage(self) -> DecisionCoverageStat:
        """Return the decision coverage statistic of the file."""
        stat = DecisionCoverageStat.new_empty()

        for linecov in self.linecov():
            stat += linecov.decision_coverage()

        return stat

    def call_coverage(self) -> CoverageStat:
        """Return the call coverage statistic of the file."""
        stat = CoverageStat.new_empty()

        for linecov in self.linecov():
            stat += linecov.call_coverage()

        return stat
