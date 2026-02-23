"""Tests for mute.rules."""
import os
import tempfile

import pytest

from mute.rules import (
    get_delete_rule,
    get_mute_rule,
    get_quarantine_graduation_rule,
    get_rule_params,
    get_rules_for_build,
    get_unmute_rule,
    load_rules,
)


@pytest.fixture
def rules_yaml():
    return """
rules:
  - id: mute1
    pattern: test_flaky
    scope: regression
    build_types: [relwithdebinfo]
    params:
      window_days: 4
      min_failures_high: 3
    reaction: mute

  - id: unmute1
    pattern: test_stable
    scope: regression
    build_types: [relwithdebinfo]
    params:
      window_days: 7
      min_runs: 4
    reaction: unmute

  - id: delete1
    pattern: test_no_runs
    scope: regression
    build_types: [relwithdebinfo]
    params:
      window_days: 7
    reaction: delete

  - id: graduation1
    pattern: quarantine_graduation
    scope: regression
    build_types: [relwithdebinfo]
    params:
      window_days: 1
      min_runs: 4
      min_passes: 1
    reaction: remove_from_quarantine

  - id: disabled_rule
    pattern: test_flaky
    scope: regression
    build_types: [relwithdebinfo]
    reaction: mute
    enabled: false

  - id: asan_only
    pattern: test_flaky
    scope: regression
    build_types: [release-asan]
    params:
      window_days: 4
    reaction: mute
"""


def test_load_rules_from_path(rules_yaml):
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(rules_yaml)
        path = f.name
    try:
        rules = load_rules(path)
        assert len(rules) == 6
        assert any(r['id'] == 'mute1' for r in rules)
    finally:
        os.unlink(path)


def test_get_mute_rule(rules_yaml):
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(rules_yaml)
        path = f.name
    try:
        rules = load_rules(path)
        r = get_mute_rule(rules, 'relwithdebinfo')
        assert r is not None
        assert r['id'] == 'mute1'
    finally:
        os.unlink(path)


def test_get_rule_params():
    rule = {'params': {'window_days': 4, 'min_failures_high': 3}}
    p = get_rule_params(rule)
    assert p['window_days'] == 4
    p2 = get_rule_params(rule, {'min_failures_low': 2})
    assert p2['min_failures_low'] == 2
