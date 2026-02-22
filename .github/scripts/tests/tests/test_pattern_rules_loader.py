"""Tests for pattern_rules_loader."""
import os
import tempfile

import pytest

from pattern_rules_loader import (
    load_rules,
    get_rules_for_build,
    get_mute_rule,
    get_unmute_rule,
    get_delete_rule,
    get_quarantine_graduation_rule,
    get_rule_params,
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
        ids = [r['id'] for r in rules]
        assert 'mute1' in ids
        assert 'unmute1' in ids
        assert 'disabled_rule' in ids
    finally:
        os.unlink(path)


def test_load_rules_default():
    rules = load_rules()
    assert isinstance(rules, list)
    # Default file may or may not exist
    if rules:
        assert all('id' in r for r in rules)


def test_get_rules_for_build_filters_build_type(rules_yaml):
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(rules_yaml)
        path = f.name
    try:
        rules = load_rules(path)
        rel = get_rules_for_build(rules, 'relwithdebinfo')
        assert len(rel) >= 4  # disabled_rule excluded
        asan = get_rules_for_build(rules, 'release-asan')
        assert any(r['id'] == 'asan_only' for r in asan)
        assert not any(r['id'] == 'mute1' for r in asan)  # mute1 is relwithdebinfo only
    finally:
        os.unlink(path)


def test_get_rules_for_build_skips_disabled(rules_yaml):
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(rules_yaml)
        path = f.name
    try:
        rules = load_rules(path)
        mute_rules = get_rules_for_build(rules, 'relwithdebinfo', reaction='mute')
        assert not any(r['id'] == 'disabled_rule' for r in mute_rules)
        assert any(r['id'] == 'mute1' for r in mute_rules)
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
        assert r['reaction'] == 'mute'
        r_asan = get_mute_rule(rules, 'release-asan')
        assert r_asan is not None
        assert r_asan['id'] == 'asan_only'
    finally:
        os.unlink(path)


def test_get_unmute_rule(rules_yaml):
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(rules_yaml)
        path = f.name
    try:
        rules = load_rules(path)
        r = get_unmute_rule(rules, 'relwithdebinfo')
        assert r is not None
        assert r['id'] == 'unmute1'
    finally:
        os.unlink(path)


def test_get_delete_rule(rules_yaml):
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(rules_yaml)
        path = f.name
    try:
        rules = load_rules(path)
        r = get_delete_rule(rules, 'relwithdebinfo')
        assert r is not None
        assert r['id'] == 'delete1'
    finally:
        os.unlink(path)


def test_get_quarantine_graduation_rule(rules_yaml):
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(rules_yaml)
        path = f.name
    try:
        rules = load_rules(path)
        r = get_quarantine_graduation_rule(rules, 'relwithdebinfo')
        assert r is not None
        assert r['id'] == 'graduation1'
        assert r['params']['min_runs'] == 4
        assert r['params']['min_passes'] == 1
    finally:
        os.unlink(path)


def test_get_rule_params():
    rule = {'params': {'window_days': 4, 'min_failures_high': 3}}
    p = get_rule_params(rule)
    assert p['window_days'] == 4
    assert p['min_failures_high'] == 3

    p2 = get_rule_params(rule, {'min_failures_low': 2})
    assert p2['min_failures_low'] == 2
    assert p2['window_days'] == 4


def test_get_rule_params_empty_rule():
    p = get_rule_params({})
    assert p == {}
    p = get_rule_params(None)
    assert p == {}
