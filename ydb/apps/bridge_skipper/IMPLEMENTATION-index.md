# Skipper implementation guide

Skipper is a single external failover controller for one YDB bridge cluster.
Operator procedures, the state file and mutation outcomes are in the
[runbook](docs/operations.md); `/api/state`, metrics and logs are in
[monitoring](docs/monitoring.md); release gates are in the
[README checklist](README.md#required-builds-and-release-checklist).

## Layout

```text
main.cpp
  config/                 strict YAML, endpoint parsing, credentials, CLI
  runtime/skipper.*       one actor owns mutable controller state
  runtime/ydb_clients.*   endpoint-pinned SDK clients, decoding, redaction, outcomes
  runtime/state_file.*    local flock and atomic state.json
  domain/types.*          shared view, connectivity, verdict and plan types
  domain/view.*           fresh highest view and transition explanation
  domain/health.*         SelfCheck classification and per-pile verdicts
  domain/policy.*         confirmation, failover overlay and CLI rendering
  domain/mutation.*       resend rules, survivor visibility and endpoint selection
  http/actor.cpp          read routes and asynchronous controller commands
  http/state_json.cpp     schema-3 canonical JSON/HTML projection
  http/page.cpp           embedded server-rendered HTML, plain JS and CSS
  runtime/diagnostics.*   bounded history, redacted logs and attempt records
  http/metrics.cpp        Prometheus rendering
  config/ut, domain/ut, runtime/ut, http/ut
  domain/ut/fixtures      synthetic source-derived SelfCheck fixtures
  acceptance/             process/gRPC end-to-end tests with ydbd_mock
  ydbd_mock/              synthetic server behavior and contract tests
```

## Actors and threads

`main.cpp` validates arguments, configuration and credentials, opens the
locked state directory and starts NActors with a basic pool for HTTP, proxy and
logging plus a dedicated one-thread IO executor for the controller only. The token file is re-read before each UpdateClusterState and after an
UNAUTHORIZED reply; CA, certificate and key are read once. `main.cpp`
registers the HTTP proxy and actor, waits for the listen
confirmation and only then registers the Skipper actor, followed by a private
controller-ready handshake, so a bind failure exits before any YDB call and
commands return 503 before readiness. The Skipper actor owns scheduling, latest replies,
health and confirmation state, the mutation protocol, persistence and
counters. Domain functions are pure: callers supply `now`; domain code does no
I/O and uses no SDK clients.

Every 250 ms the actor starts due calls, selects and persists a view, evaluates
verdicts and confirmations, decides, sends a Ready plan in Auto and publishes an
immutable snapshot. HTTP parses command syntax and forwards typed requests to
the controller; per-request actors retain the original HTTP request/cookie,
track controller replies, handle nondelivery and return 504 after 15 seconds.
A timeout never cancels a controller command. There is at
most one outstanding call per endpoint and method, with separate slots for
plain state, connectivity state, SelfCheck and update. A deadline does not free
a slot before the SDK future settles, so a result always belongs to the call
that holds its slot. SDK callbacks own their results and only send actor events.
State-file writes are small synchronous atomic replacements on the dedicated
IO executor. HTTP and logging stay responsive while an SDK call is outstanding;
Off cannot preempt the controller during an ongoing fsync.

SIGINT/SIGTERM stops scheduling first: the main thread waits for one stop
acknowledgement, so the actor starts no new call. It then drains
`TDriver::Stop(true)` while the actor system is alive and stops the actors.
Processing and persisting terminal outcomes during the drain is best-effort;
on restart a newer view, the identical resend, or a plan that keeps its primary
and adds DISCONNECTED targets reconciles any remaining pending record. Foreign
callbacks never access an actor object.

## Observation and policy invariants

1. **Inventory.** Endpoints come from configuration, keyed by pile; there is
   no discovery. Each endpoint must address exactly one node, because identity
   is verified only from its own SelfCheck reply. A reply without a pile keeps
   the earlier identity and last pile-named observation until its original TTL;
   a mismatch excludes the endpoint's evidence until a
   matching reply. Views are cluster-wide and count from every endpoint. Configured piles absent from the view are ignored;
   view piles without endpoints stay visible.
   This verifies pile attribution, not remote cluster identity. All endpoints
   must be independently verified as nodes of the intended cluster; the
   configured `cluster_id` only labels the local state directory. Unknown
   endpoints' views can influence the high-water mark before SelfCheck returns.
2. **View.** Plain `GetClusterState` polls run every state poll. The highest
   fresh generation among all endpoints is selected; same-generation
   pile-state disagreement is
   `VIEW_CONFLICT`, and a view below the persisted high-water mark is
   `STALE_HISTORY`. A numeric maximum is not a quorum read or proof against
   divergent histories.
3. **Sampling.** Each round rotates SelfCheck and connectivity independently
   through each configured pile's endpoints, up to `health_endpoints_per_pile`
   each, skipping piles absent from the last selected view; mismatched
   endpoints get separate SelfCheck recovery probes. A new generation starts a
   round at once. A SelfCheck or connectivity slot that is busy at the round
   boundary is sampled as soon as the call settles, within the per-pile budget.
   A connectivity reply that is absent or from another generation never
   replaces a usable report; a binding reply replaces the latest report but
   not the latest settled one; both cases re-poll at the next state poll.
   SelfCheck uses an empty database,
   verbose false, NoMerge, NoCache and both server and client bounds.
4. **Evidence.** A reply counts only if it was sent
   within the TTL, from a VERIFIED endpoint whose fresh view is at G. A
   connectivity report must be at G; a report sent while binding counts for
   self quorum, participation and survivor visibility, and the Lost verdict
   uses settled reports only. A report's vantage is the endpoint's configured pile; its contents come from
   the component that the queried node forwards to.
   Launch authorization expires at the earliest send-time-plus-TTL of the
   admitted view, health and connectivity observations. The actor checks this
   bound after persistence; the SDK adapter checks it again after token reload,
   immediately before entering the SDK. Local expiry keeps the pending overlay
   and publishes `EVIDENCE_EXPIRED` without an invocation, completion, occupied
   update slot or `mutation_sent` log. Fresh evidence may authorize it again.
5. **Classification.** `ClassifySelfCheck` pins exact message texts and
   distinguishes BSC from fallback mode. In fallback mode every RED group part
   is fallback RED, and a GREY `Group has disks with no data` with a pile name
   is an incomplete (unknown) part; without a pile name it is ignored. In BSC
   mode, any reason other than a neutral (GREEN, YELLOW,
   ORANGE) or BLUE `Replication in progress` VDisk makes the part RED;
   otherwise a reason id missing from the reply, as after truncation, or
   present more than once, makes it incomplete; otherwise at least one
   replicating VDisk makes it replication-only, and a part without one is RED.
   BSC is one shared source; agreement between BSC replies is not independent
   voting.
6. **Verdicts.** Participating piles are PRIMARY, PROMOTED, SYNCHRONIZED and
   NOT_SYNCHRONIZED piles that a valid report shows in config quorum, or that
   a fresh BSC-mode reply attributes a part to; when neither exists, a
   NOT_SYNCHRONIZED pile is participating and Unknown. In order: Failed when
   every BSC reply has a RED part for the pile; Lost when at least
   one outside report exists, every outside report shows the pile in config
   quorum without node quorum, and no inside report shows its node quorum;
   Replicating when every BSC reply has a RED or replication-only part and one
   is replication-only; Healthy after the warm-up; otherwise Unknown, which an
   incomplete part also gives. SUSPENDED gets only the Lost rule;
   DISCONNECTED, INACTIVE and stage-1 piles get no verdict.
7. **Warm-up.** Healthy requires clean BSC evaluations, with no part and no
   truncation, whose reply send times span `bsc_warmup_seconds`. Startup, a
   PRIMARY or PROMOTED change, a fresh non-clean or fallback-only evaluation,
   a gap of more than one TTL between consecutive clean sends, a fresh
   fallback reply from an endpoint of the BSC-host pile (PROMOTED, else
   PRIMARY) while it is fresh, and an endpoint returning from fallback to BSC
   mode reset it. After that switch the window restarts at the first BSC
   reply, which can be clean by omission (Ov 3.2.5). A fallback reply from
   another pile is only that side's observation. An evaluation with no fresh
   SelfCheck reply keeps the window. A generation
   change alone does not reset it: BSC moves only on a role change.
   A role change also records its observation time: only clean evidence sent
   at or after that boundary can start or advance the replacement window;
   cached pre-change replies cannot reseed it. An unavailable or conflicting
   view clears clean spans and confirmations but retains the last roles and
   their boundary, so recovery cannot be mistaken for initial startup. Fallback
   and GREY never prove health. The warm-up can complete on cached clean
   evidence that is still fresh; this is an acknowledged policy limitation.
8. **Confirmation.** Down is Failed or Lost. It is confirmed at one
   generation when the supporting evidence was sent over at least
   `confirm_seconds`, so cached replies kept by failed attempts cannot complete
   the window; Unknown or non-Down resets it. The span is not elapsed time since
   the first Down observation (`down_since`, display only), so the mutation can
   follow it by less than `confirm_seconds`. Failed to Lost does not reset it.
   A generation change keeps a pile's confirmation when its state and the
   PRIMARY and PROMOTED roles are unchanged (a no-op bump, CMS or a stage flap
   elsewhere); any other change restarts it. Every Down pile must confirm
   before acting.
9. **Clock.** Windows, schedules and evidence send times use the monotonic
   clock, read when a request is launched; a wall-clock step changes nothing.
   The HTTP layer reads the wall clock once per response for `now`. Event UTC is
   fixed when recorded (with the boot monotonic/UTC mapping for observation
   stamps), and freshness always uses monotonic time. A wall-clock step can
   change `now`, but never rewrites an event timestamp or extends evidence TTL.
10. **Decision.** A lost SUSPENDED pile blocks. With no Down pile, Healthy or
    Replicating everywhere is Stable, otherwise Unknown. Replicating is never
    a promotion candidate. A primary that is not Down stays primary whatever
    its verdict, and the confirmed Down non-primaries are disconnected; a Down
    primary is replaced by the eligible PROMOTED pile, else the first eligible
    SYNCHRONIZED pile by name (`NO_CANDIDATE` otherwise).
    Eligible means Healthy, or, without a fresh BSC-mode reply, self quorum
    with no fallback part on the candidate. In a partition, the BSC side
    wins when its BSC replies show the other side RED. When the BSC-host side
    cannot be observed for a TTL and the other side reports it without node
    quorum, it is Lost and the reachable side wins through the no-BSC rule.
    Known limits, pending real-cluster recordings: if survivors'
    connectivity reports stay `binding_in_progress`, no Lost verdict forms and
    a silent primary loss is not handled automatically; act manually from
    the published state. With fresh BSC-mode replies a candidate's fallback
    RED is not a veto (D3); a stale clean BSC reply within the TTL can
    therefore qualify a pile whose own fallback reply shows its groups RED.
11. **Overlay.** One update disconnects every Down pile; if the primary is
    down, it also assigns the survivor PRIMARY and resets any other PROMOTED
    pile to SYNCHRONIZED. The mutation endpoint must be a VERIFIED current
    endpoint in a pile that stays in config quorum after the update, with a
    valid report queried through that exact endpoint that shows every such pile
    with node quorum
    (`NO_MUTATION_ENDPOINT` otherwise). An endpoint that answered
    NotAppliedHere is excluded until the next state poll that finds no
    selectable endpoint (the previous evaluation was `NO_MUTATION_ENDPOINT`);
    excluded endpoints accumulate, so each is retried about once per state
    poll. Success, a resolved record or a new record clears the exclusions.
    Candidates are ranked by their pile's state after the update: PRIMARY, then
    PROMOTED or SYNCHRONIZED, then participating stage 2, then SUSPENDED, then by URI.
12. **Mutation.** Manual and Auto compute the same decision. Every update
    carries `expected_generation=G` and empty `quorum_piles`, with operation
    and client deadlines. The pending record (`expected_generation` and
    `update`) is persisted before the first send, and only the identical
    update, or a plan that keeps its primary and adds DISCONNECTED targets, is
    sent at G; such a plan replaces the record before it is sent. A Rejected
    outcome keeps the record because earlier attempts may still commit.
    Rejected failover stops all dispatch at that generation until a restart or
    a new generation (reason `REJECTED_BY_SERVER`); rejected lifecycle stops
    lifecycle dispatch only, and its retained record blocks incompatible work.
    Manual requires a new review for every resend; Auto ambiguous retries back
    off exponentially up to five minutes. The state file and
    outcome rules are in the
    [runbook](docs/operations.md#pending-mutations-and-state-overrides).
13. **Explanation.** A new generation is explained when its piles equal the
    previous ones, or the previous ones overlaid with the pending or last own
    update, up to YDB follow-ups: PROMOTED to PRIMARY with the old primary to
    SYNCHRONIZED, NOT_SYNCHRONIZED to SYNCHRONIZED and SUSPENDED to
    DISCONNECTED. Anything else is unexplained, logged and counted, but never
    blocks policy.

## Validation map

| Area | Relevant targets and purpose |
| --- | --- |
| `config/ut` | Schema/defaults, endpoint binding, timing inequalities, unknown-key rejection and credential/CLI validation |
| `domain/ut` | View conflicts/high-water, fixture classifier, connectivity, warm-up, confirmation, overlay/CLI, transition explanation, endpoint selection and pending rules |
| `runtime/ut` | Pinned SDK settings and deadlines, owned decoding, actor scheduling, persistence failures, restart, high-water and shutdown |
| `http/ut` | One state golden with read-time ages, method/path handling, no-store, scrape invariance, contiguous families and bounded one-hot metrics |
| `acceptance/tests` | Actual C++ binary and SDK against mock processes; guarded update and empty quorum_piles checked on wire |
| `ydbd_mock/tests` | Connectivity, fallback, suspend follow-ups and server guard contracts |

This guide records no test pass. Synthetic fixtures and mock behavior are not
real-cluster qualification; the real recordings and loaded measurements in the
README checklist remain external release work.


## Web administration contracts

Fresh state starts Observe; persisted schema 3 stores mode and pending
`kind`/`origin` with decimal-string generations. Unversioned state is rejected;
there is no migration. External clear-pending atomically stores Observe and
preserves high-water.

Commands execute in one controller turn. A review binds boot ID, mode revision,
action, target, exact generation/vector/endpoint and a 120-second single-use
plan ID; execute freshly evaluates, consumes, persists pending and hands off.
The controller retains 256 plans and 1024 process-local receipts. Receipt
eviction cannot revive a consumed token. Manual authorizes one handoff only;
Auto owns guarded failover retries and never lifecycle. Rejected failover
latches all dispatch at G; lifecycle rejection latches lifecycle only. Restored,
ambiguous and overlapping work carries volatile uncertainty through compatible
failover supersets, preventing a later rejection from erasing potentially live
work. Old replies only update their own diagnostic attempt.

Diagnostic stores stay outside the per-tick snapshot: 1000 history rows, 5000
log rows, 1000 completed attempts plus endpoint-bounded in-flight attempts.
Cursors report restart/eviction gaps; capacity never blocks commands. JSON and
server-rendered HTML share one presentation projection; last observed roles and
unresolved incidents survive evidence expiry. No request overlay becomes an
observed card state. The browser polls once per second with one read outstanding,
reconciles POST responses and never automatically repeats uncertain commands.

Browser resource registration and actual browser execution remain a separate
validation prerequisite; see [browser acceptance](acceptance/browser/README.md).
