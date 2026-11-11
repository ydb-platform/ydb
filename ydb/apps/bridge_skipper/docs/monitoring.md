# Monitoring

Skipper serves an embedded web UI at `/`, canonical `/api/state`, `/metrics`,
operation records, transition history and retained logs. GET/HEAD are read-only;
HEAD preserves status/content type with no body. Commands use JSON POST through
the controller actor; see the [command contract](operations.md#command-api).
Every response is `Cache-Control: no-store`, with no CORS headers. The default
bind is `127.0.0.1:8080`; everyone reaching the host/port can administer it.

HTTP success alone does not prove controller-loop progress or healthy storage.
Before the private readiness handshake commands return 503; seed reads remain
available with no view and `NO_VIEW`. Scraping never advances counters.

## State document, schema 3

`/api/state` has numeric `schema: 3`. Generations and mode revisions are decimal
**strings**; preserve them exactly rather than converting through JavaScript
Number or spreadsheet numeric cells. Node IDs and ages are JSON numbers. Times
are ISO-8601 UTC strings or null. Missing ages are null, never zero. Freshness
uses monotonic send-time ages, so delayed replies do not become fresh on arrival.
Absolute event UTC is recorded once; wall-clock changes do not rewrite history.
The server-rendered page labels UTC; JavaScript formats those same timestamps in
the browser's IANA zone with date-specific DST offsets.

| Field | Contents |
| --- | --- |
| `cluster_id`, `mode`, `now`, `loop_age_ms` | Cluster, acknowledged Observe/Manual/Auto, current UTC and controller progress. |
| `controller_boot_id`, `controller_ready`, `mode_revision`, `snapshot_revision` | Current-boot concurrency/eligibility tokens. A semantic revision changes for relevant state or availability changes. |
| `view`, `last_observed_view` | Fresh selected view, and last observed roles retained through gaps. Each includes decimal generation, exact pile names/states, lifecycle pending, conversion, source and send age. |
| `generation_high_water` | Effective generation floor; can exceed acknowledged persistence during a failed save. |
| `cards[]` | Canonical JSON/HTML projection in exact-name alphabetical order: state, compact current/last-known assessments, incident, evidence TTL/expiry, endpoint URI references and per-action availability/reasons. |
| `mode_text`, `decision_text`, `state_age_text`, `loop_age_text` | Shared initial HTML/live-page wording. Policy consumers use the typed mode, decision and numeric ages. |
| `dispatch_refusal`, `auto_activation_text` | A refusal message (or null) when rejected failover blocks sends at the current or last observed generation, and the canonical Auto activation explanation. Observation expiry does not clear the refusal. The headline and Auto dialog explain that enabling Auto cannot bypass this latch; lifecycle-only rejection does not set this global warning. |
| `operation_details` | Shared pending record, latest attempt and next-retry object displayed by both initial HTML and the enhanced page. |
| `endpoints[]` | Exact URI/pile identity, state/evidence ages, per-reply SelfCheck mode, truncation, redacted notes/errors. |
| `piles[]` | Controller assessments/reasons, confirmation and per-reporter connectivity. A missing assessment does not imply health or non-participation. |
| `last_known_piles[]` | Retained raw assessments/connectivity used by cards whose current assessment is absent or Unknown. Current raw records remain in `piles[]`; cards do not clone either raw record. |
| `decision` | Kind/reason/blocker and nullable exact guarded failover plan/CLI command. |
| `pending_mutation` | Durable expected generation/update/kind/origin plus current-boot uncertainty. Requested states never replace observed card roles. |
| `state_write_error`, `save_warning` | Persistence failure and explicit save/restart warning; dispatch stays blocked until persistence succeeds. |
| `last_mutation`, `retry_at`, `retry_message` | Latest current-boot send/outcome and guarded Auto retry scheduling. Manual never automatically retries. |
| `history`, `logs` | Bounded excerpts with cursor, coverage and whole-stream retention metadata; full retained reads are available through their respective routes. The default state/HTML log excerpt is Info+. |

Pending attempt counts and first/last send times remain available until the
pending record is durably cleared. Counts cover the current-boot pending chain,
including attempts for compatible predecessor overlays. Its latest status/outcome
identifies an attempt with the same generation, exact overlay, operation kind and dispatch
origin. Older attempts remain available separately in `last_mutation`, operation
records and history, including while a larger pending overlay awaits its send.

`cards[].evidence` supplies sample age, TTL, expiry and `expired_for_ms`.
Card `health_text`, `evidence_age_text` and `context_action` supply the same
presentation to HTML and JavaScript; elapsed browser time advances ages and
expiry between reads. Card `current_assessment` and `last_known_assessment`
contain verdict, reasons, fallback/self-quorum flags, evidence, assessment
availability and confirmation metadata. `fallback_red` is a candidate veto
for any attributed RED/GREY or incomplete fallback part, not sufficient proof
of failure. A fallback Failed verdict has a reason naming decisive RED static
group parts; the generation, role warm-up, no-BSC and no-self-quorum checks
still apply. The evidence drawer resolves raw pile
records by exact name and `endpoint_uris` through root `endpoints[]`, keeping
current and historical evidence distinct. Captured log rows provide a canonical `summary` alongside
their original redacted `message`.
History rows likewise provide canonical `summary` and `origin_label` fields for
initial HTML and live updates. A missed generation range appears in
`coverage_gap_text` and in that summary; controller mode and decision events
are labeled Operator and Skipper.
Stream `coverage_gap` reports an unavailable requested cursor (including a
previous boot) or actual pruning on an initial read. A bounded excerpt sets
`tail_clipped` separately when matching retained rows were omitted; clipping
alone does not mean history or logs were lost. Counts always describe the
complete retained stream, including on filtered or excerpt reads.
A sample age of 630 seconds with a 30-second TTL means expired 10 minutes ago,
not 10 minutes 30 seconds. Exact boundary means expired just now. The page
advances relative ages using elapsed monotonic browser time and rebases on each
read. It never guesses expiry from the last page refresh.

DISCONNECTED/INACTIVE can be explicitly not assessed. Known non-participating
NOT_SYNCHRONIZED is excluded only with backend participation evidence. An
expired Healthy/Failed sample becomes Unknown with its last-known verdict and
sample/expiry ages; a known unresolved service incident remains visible.
Seeing a pile DISCONNECTED or otherwise excluded does not prove that its storage
recovered. Exclusion hides the current incident while retaining the last-known
failure. After rejoin, unknown or stage-two participation exposes that retained
incident until fresh health or replication evidence resolves it; warm-up alone
does not resolve it.
Lifecycle checks, dispatch routing and cards share the evaluator's resolved
participation, including fresh BSC evidence of stage two. The card's
`participation_evidence` records its send time and TTL; expiry makes participation
unknown even before another controller tick. A negative claim in a binding
report cannot prove non-participation.
Each connectivity reporter retains generation, age, binding, static/connected
node counts and separate node/config/data quorum flags. These reports describe
a distconf component; they are not independent storage votes.

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

Log/history reads report current-boot `retained_count`, `total_count`,
`dropped_count`, `oldest_at` and `newest_at` before filtering. These describe the
whole captured ring, including Debug records when capture is enabled, rather
than the state document's short Info+ excerpt. `/api/logs?level=debug` reads
captured Debug records; choosing a filter cannot collect records below the
configured capture level. Pile filtering searches the redacted message text;
general text filtering searches the serialized record. Filtered export uses
the same predicates as the displayed log tail. Browser retention preserves
selected/focused rows within a 5,000-row bound and reports local gaps or a pause
when all capacity is pinned; Jump to latest refreshes the retained tail.
The state excerpt seeds the initial page only. `/api/logs` supplies the live
tail in server order with server filtering; normal polls request deltas, while
filter changes and unavailable cursors reconcile a full retained response.

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
| `skipper_mode` | One-hot `mode` = observe/manual/auto |

Generations and pile states remain exact values in `/api/state` rather than
metric labels.

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
8 s evidence TTL and do not establish production timing qualification.
The stale-view rule warns above 4 s before the selected-view series disappears
at expiry. Use 1 s scrape and rule-evaluation intervals for that short window;
slower monitoring requires coordinated threshold and TTL adjustments.
Missing evidence is never zero-age healthy evidence.

Investigate persistent Unknown, Failed/Lost, blocked decisions, pending records,
rejected/ambiguous mutations and unexplained changes. Observe sends nothing;
Manual sends only reviewed plans. A Ready plan awaiting review is not an error.
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


## Retained history and log coverage

The controller appends typed baseline, transition, mode, pending, send and outcome
history next to its transitions. Inferred/external observations are labeled; a
generation jump does not fabricate unseen intermediate changes. History retains
1000 events. The redacted structured log tee retains 5000 records at the configured
capture level while continuing stderr output. Debug filtering cannot recover
Debug records not captured at startup; change `logging.level` and restart to
capture future Debug records. Logs can be filtered and exported from the retained
ring; exports retain raw UTC.

Cursor responses report boot/eviction coverage gaps. Restart discards diagnostics,
plans, receipts and process-local rejection latches, while mode/high-water/pending
restore from schema 3. Operation lookup retains 1000 completed attempts plus
endpoint-bounded in-flight attempts; 404 means unavailable, never cancelled or
proven unsent. Retain service logs externally for a complete incident record.
