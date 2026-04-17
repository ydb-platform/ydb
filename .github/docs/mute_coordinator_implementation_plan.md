# Mute Coordinator Implementation Plan

## Goal

Build a production-ready mute coordination layer with explicit rule visibility in YDB:

- `default_unmute`
- `quarantine_new_test`
- `quarantine_user_fixed`
- `quarantine_manual_unmute`

The rule currently applied to each test must be queryable directly from DB.

## Phase 1: Foundation (start now)

1. Add dedicated YDB namespace under `mute_coordinator/`.
2. Create and maintain core tables:
   - `mute_coordinator/control_state`
   - `mute_coordinator/control_events`
   - `mute_coordinator/effective_rule`
3. Implement exporter script in `.github/scripts/analytics/` that:
   - creates tables,
   - builds `effective_rule` rows for currently muted tests using `default_unmute`,
   - preserves future compatibility for quarantine policy types.
4. Wire exporter into workflows:
   - `update_muted_ya.yml` before `create_new_muted_ya.py update_muted_ya`,
   - `collect_analytics_fast.yml` for analytics freshness.

## Phase 2: Quarantine Triggers

1. Add `quarantine_new_test` trigger:
   - detect tests newly appearing in monitoring stream.
2. Add `quarantine_user_fixed` trigger:
   - human closes issue for muted tests,
   - bot reopens issue and sets quarantine state.
3. Add `quarantine_manual_unmute` trigger:
   - test removed manually from `muted_ya.txt` by merged PR diff.

## Phase 3: Resolver and Decisions

1. Add resolver logic for expired quarantines:
   - evaluate stability only on quarantine window,
   - transition to coarse `unmuted` or back to `muted` (`policy_type` stays `default_unmute` for rule-engine rows),
   - write explicit `decision_reason`.
2. Add idempotency guards to avoid duplicate transitions/comments.

## Phase 4: Analytics and Dashboards

1. Join `mute_coordinator/effective_rule` in marts:
   - `test_muted_monitor_mart_with_issue.sql`,
   - `muted_tests_with_issue_and_area.sql`.
2. Expose columns:
   - `effective_rule_type`,
   - `effective_window_days`,
   - `quarantine_active`,
   - `quarantine_until`,
   - `decision_reason`.

## Definition of Done

- For any `(branch, build_type, full_name)`, DB returns exactly one active effective rule.
- Rule source is explicit (`default` vs quarantine type).
- Quarantine transitions are auditable via `control_events`.
- Workflow execution remains backward-compatible with current mute/unmute generation.
