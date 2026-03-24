# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Iterable, Optional, Tuple

import requests

# Runtime execution order is shared by all access tests. The order matches the
# tokens used by the legacy suite so migration can stay incremental.
RUNTIME_TOKENS_ORDER = [
    None,
    'random-invalid-token',
    'user@builtin',
    'database@builtin',
    'viewer@builtin',
    'monitoring@builtin',
    'root@builtin',
]

# Hierarchy checks use only the normal access ladder. Invalid tokens are runtime
# auth scenarios, not hierarchy levels, so they must not participate here.
HIERARCHY_TOKENS_ORDER = [
    None,
    'database@builtin',
    'viewer@builtin',
    'monitoring@builtin',
    'root@builtin',
]

# Human-readable labels make hierarchy assertion messages easier to understand.
HIERARCHY_TOKEN_LABELS = {
    None: 'no_token',
    'database@builtin': 'database',
    'viewer@builtin': 'viewer',
    'monitoring@builtin': 'monitoring',
    'root@builtin': 'administration',
}

# Each tuple describes which hierarchy levels are allowed for one monotonic
# access group. The order is important because hierarchy tests also verify that
# cases are grouped from less restrictive to more restrictive access.
VALID_MONOTONIC_GROUPS_IN_ORDER = [
    (True, True, True, True, True),
    (False, True, True, True, True),
    (False, False, True, True, True),
    (False, False, False, True, True),
    (False, False, False, False, True),
    (False, False, False, False, False),
]

# Precomputed ranks keep hierarchy validation simple and make error messages
# deterministic.
VALID_MONOTONIC_GROUP_RANK = {
    access_pattern: rank for rank, access_pattern in enumerate(VALID_MONOTONIC_GROUPS_IN_ORDER)
}

REDIRECT_STATUSES = (301, 302, 303, 307, 308)


@dataclass(frozen=True)
class EndpointExecutionResult:
    """Result of one concrete request executed by the shared helper."""

    endpoint_path: str
    token: Optional[str]
    expected_status: int
    actual_status: int


def normalize_expected_statuses(endpoint_path, expected_statuses, derive_random_invalid_token):
    """Validate one expected-status mapping before execution or hierarchy checks."""
    normalized_statuses = dict(expected_statuses)

    assert None in normalized_statuses, f'Endpoint {endpoint_path} is missing None status'

    if derive_random_invalid_token:
        assert 'random-invalid-token' not in normalized_statuses, (
            f'Endpoint {endpoint_path} must not define explicit random-invalid-token status '
            f'when derive_random_invalid_token=True'
        )
        # PR0 foundation rule: derived random-invalid-token behavior follows the
        # anonymous request expectation unless explicitly overridden by policy.
        normalized_statuses['random-invalid-token'] = normalized_statuses[None]
    else:
        assert 'random-invalid-token' in normalized_statuses, (
            f'Endpoint {endpoint_path} must define explicit random-invalid-token status'
        )

    missing_runtime_tokens = [token for token in RUNTIME_TOKENS_ORDER if token not in normalized_statuses]
    assert not missing_runtime_tokens, (
        f'Endpoint {endpoint_path} is missing runtime token statuses for: {missing_runtime_tokens}'
    )

    return normalized_statuses


def normalize_expected_results(expected_results, derive_random_invalid_token):
    """Normalize and validate a whole endpoint mapping."""
    return {
        endpoint_path: normalize_expected_statuses(
            endpoint_path,
            expected_statuses,
            derive_random_invalid_token=derive_random_invalid_token,
        )
        for endpoint_path, expected_statuses in expected_results.items()
    }


def assert_same_endpoint_keys(left_name, left_results, right_name, right_results):
    """Ensure two endpoint maps describe the same set of request shapes."""
    left_keys = set(left_results.keys())
    right_keys = set(right_results.keys())

    missing_in_right = sorted(left_keys - right_keys)
    missing_in_left = sorted(right_keys - left_keys)

    assert left_keys == right_keys, (
        f'Endpoint key sets differ:\n'
        f'missing in {right_name}: {missing_in_right}\n'
        f'missing in {left_name}: {missing_in_left}'
    )


def is_accessible(status_code):
    """Treat any non-auth status as accessible for hierarchy purposes."""
    return status_code not in (401, 403)


def access_pattern_from_statuses(expected_statuses):
    """Convert expected statuses into a boolean access pattern."""
    missing_hierarchy_tokens = [token for token in HIERARCHY_TOKENS_ORDER if token not in expected_statuses]
    assert not missing_hierarchy_tokens, (
        f'Missing hierarchy token statuses: {missing_hierarchy_tokens}; statuses={expected_statuses}'
    )
    return tuple(is_accessible(expected_statuses[token]) for token in HIERARCHY_TOKENS_ORDER)


def assert_monotonic_access_pattern(endpoint_path, expected_statuses):
    """Verify that access only widens when moving to stronger roles."""
    access_pattern = access_pattern_from_statuses(expected_statuses)

    for left_token, right_token, left_access, right_access in zip(
        HIERARCHY_TOKENS_ORDER,
        HIERARCHY_TOKENS_ORDER[1:],
        access_pattern,
        access_pattern[1:],
    ):
        left_name = HIERARCHY_TOKEN_LABELS[left_token]
        right_name = HIERARCHY_TOKEN_LABELS[right_token]
        assert not left_access or right_access, (
            f'Access hierarchy violation for {endpoint_path}: '
            f'{left_name} is allowed while {right_name} is denied; '
            f'statuses={expected_statuses}'
        )

    return access_pattern


def assert_access_order(expected_results):
    """Verify that endpoint groups are ordered from weaker to stronger access."""
    normalized_results = normalize_expected_results(
        expected_results,
        derive_random_invalid_token=False,
    )

    encountered_groups = []
    previous_group = None
    previous_rank = -1

    for endpoint_path, expected_statuses in normalized_results.items():
        access_pattern = assert_monotonic_access_pattern(endpoint_path, expected_statuses)

        assert access_pattern in VALID_MONOTONIC_GROUP_RANK, (
            f'Endpoint {endpoint_path} produced unexpected non-monotonic group {access_pattern}; '
            f'statuses={expected_statuses}'
        )

        current_rank = VALID_MONOTONIC_GROUP_RANK[access_pattern]

        assert current_rank >= previous_rank, (
            f'Unexpected access groups order for endpoint {endpoint_path}: '
            f'group {access_pattern} appears after a stricter group. '
            f'Encountered groups so far: {encountered_groups}'
        )

        if access_pattern != previous_group:
            encountered_groups.append(access_pattern)
            previous_group = access_pattern
            previous_rank = current_rank

    assert encountered_groups, 'No access groups were detected'


def assert_redirect_headers(endpoint_path, token, response, redirect_location_expectations):
    """Validate redirect target for endpoints that are expected to redirect."""
    expected_location = redirect_location_expectations[endpoint_path][token]
    actual_location = response.headers.get('Location')
    token_description = token if token is not None else 'null'
    assert (
        actual_location == expected_location
    ), f'Expected {endpoint_path} with token={token_description} to redirect to {expected_location}, got {actual_location}'


def assert_non_redirect_headers(endpoint_path, token, response):
    """Ensure auth failures do not accidentally return redirect headers."""
    token_description = token if token is not None else 'null'
    assert (
        response.headers.get('Location') is None
    ), f'Expected {endpoint_path} with token={token_description} to have no Location header, got {response.headers.get("Location")}'


def execute_endpoint(endpoint_url, endpoint_path, token, expected_status, redirect_location_expectations):
    """Execute one request and validate its status and redirect headers."""
    headers = {}
    if token is not None:
        headers['Authorization'] = token

    response = requests.get(endpoint_url, headers=headers, verify=False, allow_redirects=False)
    token_description = token if token is not None else 'null'

    assert (
        response.status_code == expected_status
    ), f'Expected {endpoint_path} with token={token_description} to return {expected_status}, got {response.status_code}'

    if expected_status in REDIRECT_STATUSES:
        assert endpoint_path in redirect_location_expectations, (
            f'Missing redirect location expectations for {endpoint_path}'
        )
        assert token in redirect_location_expectations[endpoint_path], (
            f'Missing redirect location expectation for {endpoint_path} with token={token_description}'
        )
        assert_redirect_headers(endpoint_path, token, response, redirect_location_expectations)
    elif expected_status in (401, 403):
        assert_non_redirect_headers(endpoint_path, token, response)

    return EndpointExecutionResult(
        endpoint_path=endpoint_path,
        token=token,
        expected_status=expected_status,
        actual_status=response.status_code,
    )


def execute_endpoints(cluster, expected_results, derive_random_invalid_token, redirect_location_expectations):
    """Execute all requests from one expected-results mapping."""
    normalized_results = normalize_expected_results(
        expected_results,
        derive_random_invalid_token=derive_random_invalid_token,
    )

    host = cluster.nodes[1].host
    monitoring_port = cluster.nodes[1].mon_port
    base_url = f'https://{host}:{monitoring_port}'

    execution_results = []
    for endpoint_path, expected_statuses in normalized_results.items():
        endpoint_url = f'{base_url}{endpoint_path}'
        for token in RUNTIME_TOKENS_ORDER:
            expected_status = expected_statuses[token]
            execution_results.append(
                execute_endpoint(
                    endpoint_url,
                    endpoint_path,
                    token,
                    expected_status,
                    redirect_location_expectations,
                )
            )

    return execution_results


def case_ids(cases: Iterable[object]) -> Tuple[str, ...]:
    """Build readable pytest ids from inventory cases."""
    return tuple(case.case_id for case in cases)
