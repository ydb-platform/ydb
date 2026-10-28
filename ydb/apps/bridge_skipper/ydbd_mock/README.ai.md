# ydbd_mock behavior and conformance

Contract version **1**, reviewed against YDB revision `7d5365d4b7c8`. Run instructions are in [README.md](README.md). The mock serves public generated YDB protobufs; it does not implement YDB storage, tablets, Hive, interconnect, or distconf.

## Processes, inventory, and persistent state

One process represents one physical pile and exposes one gRPC listener per node plus one HTTP control/peer listener. The topology `piles` and nodes are a fixed physical inventory, with stable node IDs, labels, and dial addresses. Provision every pile needed by a test before starting the run. Adding a name absent from this inventory requires a new topology/run; it is not live provisioning. The `cluster up --piles` CSV shorthand accepts only nonempty segments and rejects leading/trailing/repeated commas or an empty argument before any run-directory or process changes. To provision an explicit empty name, use `--topology FILE` with YAML `piles: [named, ""]`; subsequent quoted CLI and JSON selectors still accept it.

`initial: {configured_piles: [pile1], bridged: false, self_management: true}` starts a non-bridged scenario with only pile1 configured. Defaults are all inventory piles configured and synchronized, first pile primary, bridged and self-managed. Configured inventory is independent of physical processes. `POST /topology` changes the deployed configuration prerequisites; only inactive piles may be dropped. Configuration reorder preserves registry slots, holes are reused, and trailing holes are trimmed. A new configured pile starts INACTIVE. Exact empty string pile names are valid inventory entries and remain distinct from absent selectors. YAML, JSON controls, CLI quoted arguments, and public protobufs preserve them; registry holes use null rather than an empty string. The mock emits an explicit empty `bridge_pile_name` in FetchConfig; it does not derive ImplicitPileId from omitted host labels. Public GetClusterState and discovery enumerate registered names in registry order, while FetchConfig includes configured hosts and configuration order. FetchConfig version changes with the mock storage revision.

The model stores per-physical-pile states and a separate persistent registry; public name/slot behavior matches the registry. `/state` exposes the physical `index` separately from `registry_slot`. This internal representation is intentionally simpler than real YDB's slot-indexed vector.

Each process persists its snapshot in `replica.json` using a temporary file, fsync, and rename. Snapshots include generation, storage revision, configuration, registry, lifecycle metadata, and generation high-water mark. Invalid persistence stops startup. Fault settings, followup holds, and response overrides are process-local and are cleared by restart. Supervisor partition/keeper-split macros are reapplied on revival.

## Public bridge state and validation

| Stored state | Data quorum | Config quorum | Allows connections |
| --- | --- | --- | --- |
| SYNCHRONIZED | yes | yes | yes |
| NOT_SYNCHRONIZED_2 | yes | yes | yes |
| SUSPENDED | no | yes | yes |
| NOT_SYNCHRONIZED_1 | no | no | yes |
| DISCONNECTED | no | no | no |
| INACTIVE | no | no | no |

Primary and promoted roles take precedence over the stored public mapping; both underlying states must be SYNCHRONIZED. `_1` and `_2` map to public NOT_SYNCHRONIZED. Omitted updates preserve the exact stored state, including `_2` and INACTIVE. Explicit NOT_SYNCHRONIZED on `_2` still resets it to `_1`. Updating an inactive pile fails: use AddPile first. Setting INACTIVE with UpdateClusterState fails: use RemovePile.

UpdateClusterState rejects empty/duplicate/unknown updates, multiple primary/promoted roles, suspended targets, invalid transitions, and missing primary with the checked-in server's errors. Multiple changed stored slots are allowed only when every changed slot becomes DISCONNECTED. A successor role can be included in that same batch. An already-promoted pile remains promoted unless explicitly replaced. Repeated no-op admin updates still advance bridge generation. `quorum_piles` is validated and recorded but does not narrow the required commit set.

GetClusterState returns UNSUPPORTED when self-management is disabled and NOT_FOUND before conversion. UpdateClusterState before conversion returns PRECONDITION_FAILED; it does not independently enforce the self-management check that GetClusterState uses.

## Lifecycle RPCs

All lifecycle requests use ready public Operation envelopes. They are administrator operations.

| RPC | Preconditions | Immediate state | Completion |
| --- | --- | --- | --- |
| ConvertToBridge | Unbridged, self-managed, exactly one configured pile | Bridge state appears; conversion_in_progress=true | RPC acknowledges conversion acceptance; background conversion clears the flag without a bridge-generation change |
| AddPile | Registered INACTIVE pile, no other pending pile operation or conversion | DISCONNECTED; pending_pile_operation ADD | RPC waits for simulated BSC completion, which clears pending metadata without changing bridge generation |
| RemovePile | Registered DISCONNECTED pile, no pending operation or conversion | Remains DISCONNECTED; pending REMOVE | RPC waits for BSC completion, then pile becomes INACTIVE and bridge generation advances |

Pending metadata includes pile_name, kind, and error_reason on the public wire. The mock additionally stores an operation_id and physical pile_index internally. A BSC error updates error_reason at the same bridge generation and returns GENERIC_ERROR to a waiting Add/Remove call. Work remains pending and retries. Clearing the injected error permits completion; clients must reconcile rather than repeat the mutation. The runtime `/topology` option `bridged:false` is a lab baseline-reset mechanism, not a public unconvert operation.

Followup kinds are `promote`, `suspend`, `connect`, `sync_step`, `bsc_done`, `synchronized`, `convert`, `add`, and `remove`. Promotion changes the primary without simulating tablet drain. Suspend leads to disconnected. Rejoin progresses `_1` -> `_2` -> synchronized through local sync stages. `lifecycle_delay` controls conversion/add/remove work. Hold a kind or `kind:pile` using `/followups`; release it explicitly. Inject `error_reason` for BSC add/remove failure. Broadcast holds to every physical pile before dispatching the operation; a hold on one process does not hold competing processes.

## Distributed revisions and transport completion

The mock uses two-phase prepare/commit with every new-state config-quorum pile required, plus observers to serialize competing proposals. It has no real distconf leader election or durability quorum. A strict majority of simulated nodes must be up for their pile to prepare. `unavailable` is client-facing and does not remove peer readiness; `down` does.

Every generated proposal advances storage revision by one. Bridge generation either advances by one for a bridge-vector/role change, or stays unchanged for lifecycle metadata/configuration changes. Appending/trimming registry slots changes the equivalent bridge vector and advances bridge generation; reorder and reuse of an inactive slot can remain metadata-only. Prepare checks both revisions so an old generation-only proposal cannot overwrite new BSC details. Anti-entropy compares both versions. Configuration changes are pushed to inventory processes; a disconnected pile can otherwise retain stale bridge state until a rejoin.

A client deadline/cancellation does not cancel an accepted mutation pipeline. `operation_params.operation_timeout` returns an application TIMEOUT envelope; gRPC deadlines return transport DEADLINE_EXCEEDED. Add/Remove background work survives either timeout. Process kill loses in-memory work but persisted pending state re-arms followups on restart. A process whose pile lacks the config-quorum trait (DISCONNECTED, NOT_SYNCHRONIZED_1, or INACTIVE) has closed gRPC listeners, including before AddPile. A disconnecting mutation reply is delivered before closure; the control/peer listener remains available. Set `close_grpc_when_disconnected:false` only for explicit lab scenarios.

## Read services and authentication

Discovery returns the answering node and public pile states. Direct SDK callers use the manifest's actual dial endpoints with discovery disabled. The demo still uses `scripts/ydb_shim.py` to translate its `grpc://logical-label:2135` requests into local endpoints; the shim rejects external endpoints.

FetchConfig returns one YAML config entry, configured hosts, and bridge configuration. SelfCheck supplies stable location.id, host, port, and pile; synthetic verbose database storage/pool/group status; and attributed RED BRIDGE_GROUP issues for storage/connectivity blame.

- Only internally SYNCHRONIZED pile copies are evaluated (PRIMARY/PROMOTED are roles on that stored state). NOT_SYNCHRONIZED_2 is invisible even though it requires data/config quorum. Both automatic and explicit blame controls follow this filter; raw response overrides can deliberately bypass it.
- With no blamed synchronized copies the representative group is GREEN and SelfCheck is GOOD. Some failed copies produce ORANGE/MAINTENANCE_REQUIRED; all failed copies produce RED/EMERGENCY. `issue_style:realistic` adds the corresponding parent STORAGE_GROUP; `minimal` omits that parent without changing aggregates. A self_check fault explicitly overrides the overall verdict only.
- Diagnostic request fields `minimum_status`, `maximum_level`, `return_verbose_status`, merge, and cache controls are ignored. Generic operation timeout behavior still applies. The mock always emits its synthetic verbose coverage.
- There is no tablet/whiteboard collection, BSC-unavailable or missing-data diagnostic generation, response-size truncation/HEALTHCHECK_STATUS generation, or realistic fan-out/load model. Per-node overrides can inject captured or synthetic responses; they do not establish real-YDB conformance. Primary-loss/static-group fallback, failed resync, truncation, and implicit-pile fixtures remain required before accepting an autonomous controller.

The following policies default to true; disabling one is an explicit lab deviation. `auto_blame_dead_piles` adds connectivity blame for other unreachable SYNCHRONIZED piles; false leaves explicit faults only. `enforce_config_quorum` requires every proposed config-quorum pile to prepare; false bypasses that requirement. `anti_entropy` enables periodic snapshot pulling; false does not disable proposal pushes. Pulls normally reject a snapshot that disallows the local pile's connection, but registry or configured_piles changes permit a pull even into DISCONNECTED/INACTIVE. See [README.md](README.md#health-and-replication-policies) for configuration examples.

`policies.admin_token` requires a matching x-ydb-auth-ticket on all Bridge mutation RPCs. `policies.read_token` independently protects the implemented read RPCs. With neither configured, corresponding RPCs are open. `tls` takes cert_file, key_file, optional root_cert_file, and require_client_auth. Requiring client authentication validates the certificate chain in gRPC. Optional `policies.admin_certificate_cn:[name,...]` authorizes mutation by an exact verified certificate common-name match, with admin_token as an alternative. A trusted certificate outside that list cannot mutate. This explicit lab CN mapping does not model YDB SIDs, SAN matching, or real permission configuration.

## Per-pile HTTP controls

The control API is local and unauthenticated. Default binding is loopback. It must not be exposed to an untrusted network. It also carries the internal peer protocol.

| Route | Body or query | Effect |
| --- | --- | --- |
| GET /state | none | Snapshot, registry, lifecycle details, storage_revision, pending followups, nodes, faults, readiness, and PID; tokens are omitted |
| GET /events | since, limit, after_ts, kind, grep | Local event stream |
| POST /control | node ID, label, or all:true plus op | storage/connectivity blame, down, unavailable, silence, delay, self_check, clear |
| POST /link | block/unblock/set lists | Peer partitions |
| POST /replica | states by name, optional generation/detach; or clear:true | Replace actual pile replica; omitted exact stored states and lifecycle metadata are retained |
| POST /topology | configured_piles, bridged, self_management | Commit simulated deployed configuration prerequisites |
| POST /followups | hold/release lists; error_reason string or null | Local gates and BSC failure injection |
| POST /timings | timing names to seconds | Local scheduling settings |
| POST /policies | policy fields | Local policy/auth settings |
| POST /failpoints | target, full method, stage, action, optional once | Hold or lose a response at a deterministic boundary |
| POST /response-override | target, short method, result object or null | Replace a read result independently of the actual replica |
| POST /rpc-override | target, short method, response object or null, optional once/raw_result | Replace an entire response envelope before the handler; never commits a mutation |
| POST /reset-prepare, /reset-cancel | empty object | Enter/leave reset barrier |
| POST /reset-local | generation | Install configured initial baseline at that generation |
| POST /scenario | arbitrary event fields | Annotate event log |

Here `target` means node, label, or all:true. Failpoint method is the exact full RPC path, such as `/Ydb.Bridge.V1.BridgeService/UpdateClusterState`; stages are after_snapshot, after_acceptance, and after_commit. Read handlers reach after_snapshot after capturing an immutable result. Mutations reach after_acceptance after validation, inside the cancellation-shielded pipeline; after_commit is after initial commit and before the response/BSC wait. Actions are hold, unavailable, and release. once:true affects only the first hit; inspect failpoint.hit/rpc_id then release a held hit. clear releases gates and removes overrides. `unavailable` at after_acceptance may reject before commit; after_commit reliably models a committed mutation with a lost response.

Read-result overrides support GetClusterState, SelfCheck, FetchConfig, and ListEndpoints, in protobuf JSON format. Envelope overrides additionally support UpdateClusterState, ConvertToBridge, AddPile, and RemovePile. A raw_result object with type_url and value_base64 permits unknown/wrong/corrupt Any payloads; response itself remains protobuf JSON. Authorization is checked before consuming envelope overrides. Use null to clear an override. These controls can produce impossible observations deliberately; they do not claim normal server fidelity.

Events include rpc.start/end/cancelled, rpc.override, failpoint.hit/released, commit, lifecycle and followup activity. RPC and failpoint events carry rpc_id for correlation. Per-pile sequences survive process restart in the JSONL event log. Controls/fault and commit events are separate from RPC correlation.

## Supervisor HTTP API and SDK manifest

`cluster serve --host 127.0.0.1 --port 21999` serves the existing run in the foreground. It serializes mutations and rejects cross-origin browser requests and invalid Host values. It does not add user authentication. A non-loopback `--host` requires `--allow-remote`; topology `allow_remote` is independent and controls pile listeners only. All routes have `/api/v1` prefix:

- GET /manifest returns schema_version=1, kind=ydbd_mock, cluster_id, supervisor_id, topology_sha256, cluster_name, logical node IDs/labels/piles and dial_endpoint/TLS, pile control endpoints, and endpoint_map. Every successful `cluster up` creates a new cluster_id, with or without --fresh. Restarting only `cluster serve` preserves cluster_id and changes supervisor_id; a pinned Skipper Lab binding needs a new Skipper state directory after down/up. `cluster manifest` and endpoint_manifest.json provide the same inventory without a running supervisor identity.
- GET /state returns every physical pile, including dead/unresponsive errors, plus current cluster and supervisor identities.
- GET /events takes cursors (JSON object of per-pile sequence positions), limit (default 1000, range 1–10000), and optional kind; returns schema_version, cluster_id, supervisor_id, events, cursors, has_more, and reset_piles. reset_piles lists piles for which the submitted cursor exceeded the current log end; it does not identify cluster reset operations. `cluster reset` preserves logs/sequences; `cluster up --fresh` clears them, while plain `cluster up` retains them.
- POST /process {pile,action:kill|revive|stop|cont}; /fault {target,op,...}; /partition {spec}; /heal {}; /replica {pile,...}; /reset {}.
- POST /timings or /policies {values:{...}} and /followups {hold,release,error_reason} broadcast to every physical pile. HTTP 207 reports partial errors; success must never be assumed for an unreachable pile.
- POST /topology {pile?:source,configured_piles?,bridged?,self_management?} submits one distributed configuration change through the chosen live source.
- POST /failpoints, /response-override, and /rpc-override replace per-pile node selectors with target (a pile name or node label) and proxy only their corresponding allowlisted route.

Partition, heal, and reset failures after mutation begins return HTTP 207 with
`{ok:false,action,stage,piles,attempted,results,errors,uncertain:true,error,generation?}`.
Attempted targets are listed per stage; results/errors map stage to target and
response/message. Reset records revival, preparation, sampling, installation,
resumption and bookkeeping, and attempts to resume every pile whose prepare was
attempted even if its reply was lost. Partition/heal preserve macro records on a
partial failure. Invalid requests return 400 before starting work. A 207 response
requires reconciliation of aggregate state before any retry.

Reset revives/resumes all physical processes, freezes proposals everywhere, chooses a generation above every persisted high-water mark, installs the configured initial baseline, clears faults/gates/overrides/macros, and leaves the reset barrier. There is no public YDB reset RPC.

## Conformance matrix and acceptance boundary

| Contract | Coverage / limit |
| --- | --- |
| Omitted _2/INACTIVE, batched disconnects, roles and public statuses | Validator and real generated gRPC tests |
| Conversion/add/remove, pending/error metadata, registry order/slots | Deterministic model and real gRPC tests; storage allocation/capability checks simulated |
| Same-generation metadata replication, stale proposal race, restart persistence | Separate storage revision, CAS, persistence and multi-reporter tests |
| Synchronized-only BRIDGE_GROUP visibility; partial/all-copy aggregate severity | Unit and real-gRPC tests; synthetic representative group only |
| Failed NOT_SYNCHRONIZED_2 pile | Regression verifies no pile issue despite quorum membership; real-YDB fixture required for detection policy |
| Primary-pile loss / unavailable BSC; static-group fallback; missing data and truncation | Not generated; real-YDB fixtures must be captured and replayed through overrides |
| SelfCheck filtering, merging, cache behavior, response size and load | Diagnostic request fields ignored; acceptance requires real-YDB tests |
| Empty pile name on reads, mutations, and controls | Topology/control/public-wire regression; ImplicitPileId and absent host labels require real-YDB fixtures |
| Health confidence and malformed responses | Synthetic verbose group coverage plus per-node result/envelope overrides |
| Deadlines, accepted cancellation, commit-before-lost-reply | Real gRPC transport tests plus deterministic service-boundary failpoints |
| TLS/mTLS and administrator decisions | Actual certificate handshakes and CN/token tests; YDB SID/ACL mapping excluded |
| Supervisor Lab controls and identity | HTTP tests and versioned manifest; real process macros reuse existing supervisor implementation |
| Data durability, true quorum/partition safety, group migration, tablet drain, application readiness | Not simulated; real-YDB acceptance is required |

Tests are registered in tests/ya.make. Use the checkout’s build/test instructions. Existing CLI/demo e2e scenarios remain a separate compatibility tier. The mock cannot by itself establish that autonomous Skipper policy is safe against real storage failures.
