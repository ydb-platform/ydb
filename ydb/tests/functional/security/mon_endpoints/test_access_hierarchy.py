# -*- coding: utf-8 -*-
import pytest

from .helpers import assert_monotonic_access_pattern, case_ids
from .inventory import get_inventory_cases


HIERARCHY_CASES = get_inventory_cases(
    participates_in_access_hierarchy=True,
    access_verified_here=True,
)


@pytest.mark.parametrize('case', HIERARCHY_CASES, ids=case_ids(HIERARCHY_CASES))
def test_case_access_pattern_is_monotonic(case):
    """Each hierarchy case must become no more restrictive for stronger roles."""
    assert_monotonic_access_pattern(case.full_path, case.expected_statuses)


def test_hierarchy_cases_are_grouped_by_rank():
    """Hierarchy cases should be ordered from weaker access groups to stronger ones."""
    previous_rank = -1

    for case in HIERARCHY_CASES:
        assert case.group_rank is not None, f'Hierarchy case {case.case_id} must define group_rank'
        assert case.group_rank >= previous_rank, (
            f'Hierarchy case order is invalid: {case.case_id} with rank {case.group_rank} '
            f'appears after rank {previous_rank}'
        )
        previous_rank = case.group_rank
