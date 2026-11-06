# Monitoring

Skipper serves only `/api/state` and `/metrics`, to GET and HEAD; HEAD gets the
same status and content type without a body. The query string is ignored. Any
other target returns 404; every other method returns 405 with
`Allow: GET, HEAD`. Every response has `Cache-Control: no-store`.
The default bind is `127.0.0.1:8080`, without HTTP authentication or TLS; a
wider bind exposes cluster information. Both routes read an immutable snapshot,
so scraping does not advance counters or controller state. HTTP and the
controller share an actor system, so HTTP success alone does not prove
control-loop progress. Until the first controller tick the snapshot shows no
view and a `NO_VIEW` blocker.

## State document, schema 2

`/api/state` has numeric `schema: 2`. Generations are decimal **strings**;
preserve them exactly rather than converting through JavaScript Number or
spreadsheet numeric cells. Node IDs and ages are JSON numbers. Times are UTC
ISO-8601 strings, or null when unavailable. Missing ages are null, not zero.
Ages are measured at request time from the original RPC send time, so delayed
replies do not become fresh on arrival. Diagnostic strings are null when empty.

| Field | Contents |
| --- | --- |
| `cluster_id`, `mode`, `now`, `loop_age_ms` | Process identity, configured `manual`/`auto`, `now` as the wall clock at response time, and loop progress |
| `view` | Selected view object or null: `generation`, `piles[{name,state}]`, `pending_operation`, `conversion_in_progress`, `selected_from`, `age_ms` |
| `view.pending_operation` | Null or `{kind,pile,error}`; kind is normally ADD/REMOVE (UNKNOWN for an unrecognized SDK kind), error null when empty |
| `generation_high_water` | Effective generation floor; during a write failure it can be above the persisted one |
| `endpoints[]` | Sorted by `uri`: `uri`, `pile`, `identity`, `ignored`, `state_generation`, `state_age_ms`, `selfcheck_mode`, `selfcheck_age_ms`, `truncated`, `size_warning`, `self_check_result`, `notes`, `omitted_notes`, `last_error` |
| `piles[]` | Assessed piles of the selected view, empty without one: `name`, `verdict`, `reasons[]`, `fallback_red`, `self_quorum`, `connectivity[]`, `down_since`, `confirmed`; `down_since` is derived from a monotonic stamp (`now - age`); `fallback_red` means a fresh fallback-mode reply attributes a RED or GREY no-data group part to the pile |
| `piles[].connectivity[]` | `vantage_pile`, `reporter_node_id`, `has_node_quorum`, `in_config_quorum`, `binding_in_progress` |
| `decision` | `kind`, `reason`, nullable `blocker` (also the view-selection blocker), nullable `plan{expected_generation,update,endpoint}`, nullable `cli_command` |
| `pending_mutation` | Null or persisted `{expected_generation,update}`; not a YDB lifecycle operation |
| `state_write_error` | Persistence error or null |
| `last_mutation` | Null or the latest Skipper invocation: `sent_at`, `endpoint`, `expected_generation`, `update`, `status`, `outcome`, `ambiguous_resends`; `sent_at` is derived from a monotonic stamp (`now - age`); status/outcome null while unresolved. Earlier invocations are in the `mutation_sent` and `mutation_outcome` logs |

The primary and promoted piles are the `view.piles` entries in those states.

`view` is nested and can be null, including at startup. Empty pile name `""`
is valid and distinct from null/missing.

Endpoint fields:
- `identity` is `UNKNOWN` until a SelfCheck reply names a pile, then
  `VERIFIED` or `MISMATCH` against the configured pile. A reply without a pile
  keeps the earlier identity. `ignored` means the configured pile is absent
  from the last selected view. Neither is proof that the named pile failed.
- The seven SelfCheck fields (`selfcheck_mode` through `omitted_notes`) come
  from the latest successful pile-named reply and are all null before one.
  A successful pile-less reply keeps the earlier cache and its original send
  time/TTL, and reports `ENDPOINT_PILE_UNVERIFIED`. Named replies keep updating
  while no view is selected. A stale reply stays displayed; check
  `selfcheck_age_ms`. `selfcheck_mode` is `BSC` or `FALLBACK`. `truncated` and
  `size_warning` reflect a RED and a YELLOW/ORANGE `HEALTHCHECK_STATUS` issue.
  `notes` holds at most 16 redacted `STORAGE`, `BRIDGE_PILE` and `PILE_*`
  issues as `<TYPE> <STATUS> pile=<p>: <message>` (the pile part only when the
  issue names one); `omitted_notes` counts the rest.
- `last_error` joins the current per-method errors with `; `: `<STATUS>: <issues>`
  for a failed call, a decode error, or one of `ENDPOINT_PILE_MISMATCH`,
  `ENDPOINT_PILE_UNVERIFIED` (SelfCheck named no pile),
  `CONNECTIVITY_REPORT_ABSENT` and `CONNECTIVITY_GENERATION_MISMATCH`. An
  unusable connectivity reply never replaces the endpoint's still-valid
  report; the endpoint is re-polled at the next state poll.

`piles[].connectivity[]` lists the valid reports that mention the pile, in
evaluation order; a binding report counts for positive claims only. While an
endpoint's latest report is binding, its latest settled report is listed too
until its TTL. `vantage_pile` is the configured pile of the queried endpoint.
The report itself describes the component that the queried node forwards to.

Verdicts are `HEALTHY`, `FAILED`, `LOST`, `REPLICATING`, `UNKNOWN`. Participating
piles are PRIMARY, PROMOTED, SYNCHRONIZED and NOT_SYNCHRONIZED piles in config
quorum or that a fresh BSC-mode reply attributes a part to. A NOT_SYNCHRONIZED
pile that no valid connectivity report mentions and no fresh BSC-mode reply
attributes a part to is participating with `UNKNOWN` and reason
`participation unknown`. SUSPENDED is
assessed only for Lost and otherwise remains UNKNOWN by design; it does not
make the decision UNKNOWN, and the Down alert still covers Lost. Other piles have no
verdict series. Closed listeners for DISCONNECTED, INACTIVE or stage-1
NOT_SYNCHRONIZED piles are expected, not failure evidence.

Decision kinds are `STABLE`, `UNKNOWN`, `CONFIRMING`, `BLOCKED`, `READY`.
`STABLE` permits replication-only verdicts; it is not an all-Healthy assertion.
`PILE_*`, `BRIDGE_PILE` and overall SelfCheck status are
observations, not storage verdicts. The rules behind verdicts and decisions are
in the [implementation guide](../IMPLEMENTATION-index.md#observation-and-policy-invariants).

## Metrics

All counters reset on process restart. Gauges project the snapshot at scrape
time; removed pile series disappear. Label vocabularies are bounded enums,
except `pile`, which comes from the current cluster view. Endpoint URIs, raw
issues, arbitrary reasons and credentials never become labels. Each metric
family is emitted contiguously.

| Gauge | Labels and meaning |
| --- | --- |
| `skipper_loop_age_seconds` | Time since last controller tick; absent until known |
| `skipper_cluster_state_age_seconds` | Send-time age of selected view; absent without a selected view |
| `skipper_pile_verdict` | One-hot `pile,verdict` for assessed piles |
| `skipper_decision` | One-hot `kind` from the five decision kinds above |
| `skipper_blocked` | One-hot bounded blocker `reason` while BLOCKED, all zero otherwise |
| `skipper_pending_mutation` | 1 while a durable pending update exists |

Generations, pile states and the mode are not metrics: read the exact decimal
strings and states from `/api/state`.

| Counter | Labels and meaning |
| --- | --- |
| `skipper_rpc_total` | `method,status`, counted on RPC result; only pairs that occurred are emitted |
| `skipper_mutation_total` | `outcome`: SUCCESS, GENERATION_MOVED, NOT_APPLIED_HERE, REJECTED, AMBIGUOUS |
| `skipper_unexplained_state_change_total` | Observed transitions not explained by the controller's own update or by YDB follow-ups; investigate external changes |

The four `method` labels are `get_cluster_state`,
`get_cluster_state_connectivity`, `self_check`, `update_cluster_state`.
`status` is the SDK's own status name, `SDK_START_ERROR`, `SDK_RESULT_ERROR`,
`INVALID_RPC_REQUEST` or `OTHER` for a value the SDK does not name; JSON and
logs use the same names.

Blocker labels form this closed set:

```text
NO_VIEW VIEW_CONFLICT STALE_HISTORY PENDING_MUTATION NO_CANDIDATE
SUSPENDED_PILE_LOST NO_MUTATION_ENDPOINT STATE_WRITE_FAILED
```

## Alerts and investigation

[alerts.yaml](alerts.yaml) provides example Prometheus rules for a scrape job
named `bridge_skipper`. Adapt the job selector and time thresholds to the
deployed timing configuration and measured workload. The examples use the
30 s evidence TTL and do not establish production timing qualification.
Missing evidence is never zero-age healthy evidence.

Investigate persistent Unknown, Failed/Lost, blocked decisions, pending records,
rejected/ambiguous mutations and unexplained changes. Manual mode intentionally
does not send Ready plans; a Ready plan is reviewable work, not an error.
Expected planned Add/Remove or initial BSC warm-up can explain temporary alerts;
correlate with change records. No alert measures a runtime qualification gate.

Use `/api/state` for the exact plan, blockers, endpoint errors and freshness.
`last_mutation` is process-local diagnostic state, not an audit log. Retain operator commands and service logs externally for incident
review.

## Structured stderr logs

Normal Skipper logs use native NActors JSON records on stderr, one record per
line. Structured fields are scalar **strings**, including numeric IDs and
booleans. The `plan`, `states` and `update` fields contain JSON text
inside those strings; decode that text separately when querying its contents.
An absent plan is the string `"null"`. This differs from `/api/state`, where
plans and updates are native JSON objects and an absent plan is JSON null.

| Event | Useful structured fields |
| --- | --- |
| `startup` | `cluster_id`, `mode`, `build`, `endpoints`, `http_listen` |
| `plaintext_token` | Warning at startup: a token is configured with `grpc://` endpoints |
| `token_reloaded` | Info with `category` `credentials`, written to stderr as a raw JSON line: a re-read token file changed; no token bytes or path |
| `view_change` | `generation`, `states`, `explanation`: Initial for the first selected view (not counted), otherwise Explained or Unexplained; Unexplained warns |
| `configured_endpoint_ignored` | Warning: `endpoint`, `pile`; the configured pile is absent from the selected view |
| `decision` | `kind`, `reason`, `plan`; one info record per change of any of them |
| `mutation_sent` | `invocation_id`, `expected_generation`, `endpoint`, `update` |
| `mutation_outcome` | `invocation_id`, `expected_generation`, `endpoint`, exact `status`, `outcome`, redacted `issues` |
| `pending_mutation` | `action` (set/cleared), `expected_generation`, `update` |
| `rpc_result` | Debug only: `category=sdk`, `method`, `endpoint`, `sequence`, exact `status`, redacted `issues` |
| `state_write_failed`, `mutation_rejected` | Errors with `category` `state_write` and `sdk` |
| `runtime_failed` | Error with `category` `startup`, `listen` (HTTP bind failure, before any YDB call) or `runtime` |

Info logging tracks transitions rather than every tick. Set
`logging.level: debug` and restart for individual RPC results. Exact loaded
secrets are replaced in RPC issue text; credential material is never a log
field.

Startup errors emit a separate raw JSON line with `event=startup_failed`,
`level=error`, `category` and `message`, including state-lock/open errors before
the actor logger starts. For `arguments`, `config`, `credentials` and `state`
the message is the validation or state-file error, at most 1024 bytes; it is
built from literals and key paths, never from credential or file bytes. For
`runtime` it is a fixed generic text. The explicit startup clear override uses
raw `pending_mutation_override` JSON. A stop that the controller actor does not
acknowledge within 5 s emits raw `shutdown_failed` JSON with `level=error` and
`category=stop_ack`, and the process exits with status 1. These exceptional
records do not have the normal NActors envelope; log ingestion must accept both
forms.
