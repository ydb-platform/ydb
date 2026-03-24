# -*- coding: utf-8 -*-
"""Canonical inventory of monitoring endpoint cases.

Each entry describes one access-relevant request shape. A case may differ from
another case with the same path by query parameters, config mode, redirect
behavior, or expected auth semantics.
"""

from dataclasses import dataclass, field
from typing import Dict, Iterable, Optional, Tuple
from urllib.parse import urlencode

from .helpers import RUNTIME_TOKENS_ORDER

# Allowed values are centralized here so validation errors stay explicit and new
# values can be added in one place.
ALLOWED_CONFIG_MODES = {
    'default',
    'require_counters_auth',
    'require_healthcheck_auth',
}

ALLOWED_CASE_TYPES = {
    'normal',
    'redirect',
    'unresolved',
    'requires_params',
    'special_config',
    'bug',
}

ALLOWED_ACCESS_GROUPS = {
    'public',
    'database',
    'viewer',
    'monitoring',
    'admin',
    'special',
}

ALLOWED_RANDOM_INVALID_TOKEN_POLICIES = {
    'derive',
    'explicit',
}

# Rank is stored in data because hierarchy checks operate on inventory entries,
# not on hardcoded endpoint lists in tests.
GROUP_RANK_BY_ACCESS_GROUP = {
    'public': 0,
    'database': 1,
    'viewer': 2,
    'monitoring': 3,
    'admin': 4,
}

# `minimum_role` is kept as explicit data because it is useful for export and
# for future machine-readable representations.
MINIMUM_ROLE_BY_ACCESS_GROUP = {
    'public': 'public',
    'database': 'database',
    'viewer': 'viewer',
    'monitoring': 'monitoring',
    'admin': 'admin',
    'special': 'special',
}


@dataclass(frozen=True)
class MonitoringEndpointCase:
    """Canonical description of one monitoring endpoint case."""

    # Stable identifier used in validation, exports, and pytest parametrization.
    case_id: str

    # Request identity.
    path: str
    query_params: Tuple[Tuple[str, str], ...] = ()
    method: str = 'GET'
    config_mode: str = 'default'

    # Classification.
    case_type: str = 'normal'
    access_group: str = 'public'
    group_rank: Optional[int] = None
    participates_in_access_hierarchy: bool = False

    # Auth and expected behavior.
    random_invalid_token_policy: str = 'explicit'
    expected_statuses: Dict[Optional[str], int] = field(default_factory=dict)
    redirect_target: Optional[str] = None
    auth_scheme: str = 'monitoring'
    minimum_role: str = 'public'

    # Bug and export metadata.
    is_bug: bool = False
    bug_reason: str = ''
    export_to_all_endpoints: bool = True
    export_to_bug_endpoints: bool = False

    # Coverage metadata.
    access_verified_here: bool = True
    covered_elsewhere: bool = False
    covered_elsewhere_by: str = ''
    coverage_notes: str = ''
    audit_expected: bool = True

    @property
    def full_path(self):
        """Return the request path with canonical query serialization."""
        if not self.query_params:
            return self.path
        return f'{self.path}?{urlencode(self.query_params, doseq=True)}'

    @property
    def canonical_case_key(self):
        """Return the uniqueness key used to detect duplicate cases."""
        return (self.path, self.query_params, self.config_mode)


# The initial inventory is intentionally small. It is enough to exercise the
# model, validation, hierarchy checks, and readable case ids without migrating
# the whole legacy suite at once.
CANONICAL_INVENTORY = (
    MonitoringEndpointCase(
        case_id='public_ping',
        path='/ping',
        config_mode='default',
        case_type='normal',
        access_group='public',
        group_rank=GROUP_RANK_BY_ACCESS_GROUP['public'],
        participates_in_access_hierarchy=True,
        random_invalid_token_policy='derive',
        expected_statuses={
            None: 200,
            'user@builtin': 200,
            'database@builtin': 200,
            'viewer@builtin': 200,
            'monitoring@builtin': 200,
            'root@builtin': 200,
        },
        auth_scheme='monitoring',
        minimum_role='public',
        export_to_all_endpoints=True,
        export_to_bug_endpoints=False,
        access_verified_here=True,
        covered_elsewhere=False,
        coverage_notes='Simple public endpoint used as a baseline case.',
        audit_expected=True,
    ),
    MonitoringEndpointCase(
        case_id='database_viewer_cluster',
        path='/viewer/cluster',
        config_mode='default',
        case_type='normal',
        access_group='database',
        group_rank=GROUP_RANK_BY_ACCESS_GROUP['database'],
        participates_in_access_hierarchy=True,
        random_invalid_token_policy='derive',
        expected_statuses={
            None: 401,
            'user@builtin': 403,
            'database@builtin': 200,
            'viewer@builtin': 200,
            'monitoring@builtin': 200,
            'root@builtin': 200,
        },
        auth_scheme='monitoring',
        minimum_role='database',
        export_to_all_endpoints=True,
        export_to_bug_endpoints=False,
        access_verified_here=True,
        covered_elsewhere=False,
        coverage_notes='Protected endpoint used to verify hierarchy-aware access checks.',
        audit_expected=True,
    ),
)


def iter_inventory() -> Iterable[MonitoringEndpointCase]:
    """Return all registered cases."""
    return CANONICAL_INVENTORY


def get_inventory_cases(*, access_group=None, access_verified_here=None, participates_in_access_hierarchy=None):
    """Select cases by the same attributes that test files use for grouping."""
    selected_cases = []
    for case in CANONICAL_INVENTORY:
        if access_group is not None and case.access_group != access_group:
            continue
        if access_verified_here is not None and case.access_verified_here != access_verified_here:
            continue
        if (
            participates_in_access_hierarchy is not None
            and case.participates_in_access_hierarchy != participates_in_access_hierarchy
        ):
            continue
        selected_cases.append(case)
    return tuple(selected_cases)


def validate_inventory(cases=None):
    """Validate inventory structure and metadata consistency."""
    cases = tuple(CANONICAL_INVENTORY if cases is None else cases)

    seen_case_ids = set()
    seen_case_keys = set()

    for case in cases:
        # `case_id` is the primary stable identifier for a case.
        assert case.case_id, 'case_id must be non-empty'
        assert case.case_id not in seen_case_ids, f'Duplicate case_id detected: {case.case_id}'
        seen_case_ids.add(case.case_id)

        # Paths are stored in request form and must stay absolute.
        assert case.path.startswith('/'), f'Path must start with / for case {case.case_id}: {case.path}'

        # Enum-like fields are validated explicitly to catch typos early.
        assert case.config_mode in ALLOWED_CONFIG_MODES, (
            f'Invalid config_mode for case {case.case_id}: {case.config_mode}'
        )
        assert case.case_type in ALLOWED_CASE_TYPES, f'Invalid case_type for case {case.case_id}: {case.case_type}'
        assert case.access_group in ALLOWED_ACCESS_GROUPS, (
            f'Invalid access_group for case {case.case_id}: {case.access_group}'
        )
        assert case.random_invalid_token_policy in ALLOWED_RANDOM_INVALID_TOKEN_POLICIES, (
            f'Invalid random_invalid_token_policy for case {case.case_id}: {case.random_invalid_token_policy}'
        )

        # Query params are stored as structured tuples so equivalent requests do
        # not depend on raw string formatting.
        assert isinstance(case.query_params, tuple), (
            f'query_params must be a tuple for case {case.case_id}'
        )
        for query_item in case.query_params:
            assert isinstance(query_item, tuple) and len(query_item) == 2, (
                f'Each query param must be a key/value tuple for case {case.case_id}: {query_item}'
            )

        # Duplicate detection uses the canonical case key instead of path only.
        canonical_case_key = case.canonical_case_key
        assert canonical_case_key not in seen_case_keys, (
            f'Duplicate canonical case key detected for case {case.case_id}: {canonical_case_key}'
        )
        seen_case_keys.add(canonical_case_key)

        # Hierarchy-participating cases must have a rank that matches their group.
        if case.participates_in_access_hierarchy:
            expected_group_rank = GROUP_RANK_BY_ACCESS_GROUP.get(case.access_group)
            assert expected_group_rank is not None, (
                f'Hierarchy-participating case {case.case_id} must use a ranked access_group, '
                f'got {case.access_group}'
            )
            assert case.group_rank == expected_group_rank, (
                f'Invalid group_rank for case {case.case_id}: expected {expected_group_rank}, got {case.group_rank}'
            )

        # Redirect cases must carry the redirect target explicitly.
        if case.case_type == 'redirect':
            assert case.redirect_target, f'Redirect case {case.case_id} must define redirect_target'
            assert case.export_to_all_endpoints, (
                f'Redirect case {case.case_id} must stay visible in all-endpoints export'
            )

        # Bug cases must explain why they are marked as bugs.
        if case.case_type == 'bug' or case.is_bug:
            assert case.bug_reason, f'Bug case {case.case_id} must define bug_reason'

        # Covered-elsewhere cases still keep full metadata in the inventory.
        if case.covered_elsewhere:
            assert case.covered_elsewhere_by, (
                f'Covered-elsewhere case {case.case_id} must define covered_elsewhere_by'
            )
            assert ':' in case.covered_elsewhere_by, (
                f'covered_elsewhere_by must use file:function format for case {case.case_id}'
            )
            assert case.expected_statuses, (
                f'Covered-elsewhere case {case.case_id} must still define expected_statuses'
            )

        # `minimum_role` should stay aligned with the normal access hierarchy.
        if case.access_group != 'special':
            expected_minimum_role = MINIMUM_ROLE_BY_ACCESS_GROUP[case.access_group]
            assert case.minimum_role == expected_minimum_role, (
                f'minimum_role must match access_group for case {case.case_id}: '
                f'expected {expected_minimum_role}, got {case.minimum_role}'
            )

        # Special cases are outside the normal hierarchy by default.
        if case.access_group == 'special':
            assert not case.participates_in_access_hierarchy, (
                f'Special case {case.case_id} must not participate in access hierarchy by default'
            )

        # Token coverage is validated according to the selected policy.
        if case.random_invalid_token_policy == 'derive':
            assert 'random-invalid-token' not in case.expected_statuses, (
                f'Case {case.case_id} uses derive policy and must not define random-invalid-token explicitly'
            )
        else:
            assert 'random-invalid-token' in case.expected_statuses, (
                f'Case {case.case_id} uses explicit policy and must define random-invalid-token status'
            )

        required_runtime_tokens = [token for token in RUNTIME_TOKENS_ORDER if token != 'random-invalid-token']
        if case.random_invalid_token_policy == 'explicit':
            required_runtime_tokens = list(RUNTIME_TOKENS_ORDER)

        missing_runtime_tokens = [token for token in required_runtime_tokens if token not in case.expected_statuses]
        assert not missing_runtime_tokens, (
            f'Case {case.case_id} is missing runtime token statuses for: {missing_runtime_tokens}'
        )

        # Bug export is only valid for cases that are actually marked as bugs.
        if case.export_to_bug_endpoints:
            assert case.is_bug, f'Case {case.case_id} exports to bug endpoints but is_bug is False'

    return cases


# Fail fast during test collection if inventory metadata becomes inconsistent.
validate_inventory()
