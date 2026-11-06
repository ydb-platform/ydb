#pragma once

#include <util/generic/strbuf.h>

namespace NBridgeSkipper::NFixtures {

// The complete /api/state document for the Full() snapshot at 100 s.
constexpr TStringBuf State = R"json({
  "schema": 2, "cluster_id": "cluster", "mode": "manual",
  "now": "1970-01-01T00:01:40.000000Z", "loop_age_ms": 250,
  "view": {"generation": "9007199254740993", "piles": [
    {"name": "", "state": "PRIMARY"}, {"name": "b", "state": "SYNCHRONIZED"}],
    "pending_operation": {"kind": "ADD", "pile": "", "error": "waiting"},
    "conversion_in_progress": true, "selected_from": "grpc://a:2135", "age_ms": 1500},
  "generation_high_water": "9007199254740994",
  "endpoints": [{"uri": "grpc://a:2135", "pile": "", "identity": "VERIFIED", "ignored": false,
    "state_generation": "9007199254740993", "state_age_ms": 1500,
    "selfcheck_mode": "BSC", "selfcheck_age_ms": 2000, "truncated": true, "size_warning": false,
    "self_check_result": "DEGRADED", "notes": ["PILE_BOARD RED pile=b: <redacted>"],
    "omitted_notes": 3, "last_error": null},
    {"uri": "grpc://ignored:2135", "pile": "old", "identity": "MISMATCH", "ignored": true,
    "state_generation": null, "state_age_ms": null, "selfcheck_mode": null,
    "selfcheck_age_ms": null, "truncated": null, "size_warning": null,
    "self_check_result": null, "notes": null, "omitted_notes": null,
    "last_error": "ENDPOINT_PILE_MISMATCH"}],
  "piles": [{"name": "", "verdict": "HEALTHY", "reasons": ["BSC_CLEAN"],
    "fallback_red": false, "self_quorum": true, "connectivity": [{
      "vantage_pile": "", "reporter_node_id": 7, "has_node_quorum": true,
      "in_config_quorum": true, "binding_in_progress": false}],
    "down_since": null, "confirmed": false},
    {"name": "b", "verdict": "LOST", "reasons": ["NO_NODE_QUORUM"],
    "fallback_red": true, "self_quorum": false, "connectivity": [],
    "down_since": "1970-01-01T00:01:10.000000Z", "confirmed": true}],
  "decision": {"kind": "READY", "reason": "confirmed failure", "blocker": null,
    "plan": {"expected_generation": "9007199254740993",
      "update": {"b": "DISCONNECTED"}, "endpoint": "grpc://a:2135"},
    "cli_command": "guarded CLI command"},
  "pending_mutation": {"expected_generation": "9007199254740993",
    "update": {"b": "DISCONNECTED"}},
  "state_write_error": "fsync failed",
  "last_mutation": {"sent_at": "1970-01-01T00:01:30.000000Z", "endpoint": "grpc://a:2135",
    "expected_generation": "9007199254740993", "update": {"b": "DISCONNECTED"},
    "status": "CLIENT_DEADLINE_EXCEEDED", "outcome": "AMBIGUOUS", "ambiguous_resends": 0}
})json";

} // namespace NBridgeSkipper::NFixtures
