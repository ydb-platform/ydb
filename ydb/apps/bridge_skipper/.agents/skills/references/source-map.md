# Skipper source map

Paths are relative to the YDB repository root; a basename following a full path in the same row uses that directory. Verify they still exist before citing. Controller rows were checked at `skipper_initial_wip` `c33440067d60ba18b814dd92df9f1be37ed50df4`; server rows originally at `87481912996dadc6aae8e904e0a07c767036cd11` and rechecked at the same worktree. Line-level controller facts are in `controller-facts.md`.

## Controller (`ydb/apps/bridge_skipper/`)

| Topic | Where |
| --- | --- |
| Historical Python demo (not spec) | `ydb/apps/bridge_skipper_demo/` (`health.py`, `bridge.py` incl. `get_pile_with_right_view`/`_decide`, `cli_wrapper.py`) |
| Policy summary, required server commits, release checklist (recordings, M1–M4) | `README.md` |
| Source map, actor/thread model, observation and policy invariants, validation map, web-administration contracts | `IMPLEMENTATION-index.md` |
| Runbook, command API, modes, lifecycle/membership, pending and state overrides, recovery constraints | `docs/operations.md` |
| `/api/state` schema 3, metrics, alerts, log events, retention | `docs/monitoring.md`, `docs/alerts.yaml` |
| Entry point, startup/shutdown, actor system, HTTP listener | `main.cpp` |
| Config parsing, validation invariants, CLI flags, credentials | `config/config.cpp`, `config.h`, `example.yaml`; tests `config/ut/config_ut.cpp` |
| Enums (pile state, verdict, decision kind, blockers, outcomes), cluster view validation, state record | `domain/types.h`, `types.cpp` |
| SelfCheck classification (`ClassifySelfCheck`), pile verdicts and participation (`EvaluatePiles`, `EvaluatedParticipation`, `IsConnectivityValid`) | `domain/health.cpp`, `health.h` |
| Decision (`Decide`), failover update (`BuildFailoverUpdate`), superset rule (`IsSupersetReplacement`), confirmations (`TConfirmations`), Manual lifecycle (`PlanLifecycle`) | `domain/policy.cpp`, `policy.h` |
| Outcome application (`ApplyOutcome`), resend policy (`MayResend`), receiver selection (`SelectMutationEndpoint`, `SeesSurvivors`) | `domain/mutation.cpp`, `mutation.h` |
| View selection (`SelectView`), transition explanation (`ExplainTransition`), `ValidUntil` | `domain/view.cpp`, `view.h` |
| Domain tests and synthetic SelfCheck fixtures | `domain/ut/policy_ut.cpp`, `health_ut.cpp`, `admin_ut.cpp`, `fixtures/*.pbtxt`, `fixtures/README.md` |
| Actor: scheduling, evaluation, persistence, dispatch, launch authorization, HTTP command handling (`Evaluate`, `MaybeSend`, `Dispatch`, `CreateReview`, `ExecuteReview`) | `runtime/skipper.cpp`, `skipper.h`, `events.h`, `commands.h` |
| SDK calls, deadlines, request settings, outcome classification (`ClassifyUpdateOutcome`), token reload | `runtime/ydb_clients.cpp`, `ydb_clients.h` |
| `state.json`, `skipper.lock`, atomic write, `ClearPending` | `runtime/state_file.cpp`, `state_file.h` |
| History/log stores, `boot_id` | `runtime/diagnostics.cpp` |
| Runtime tests (actor, wire, state file, diagnostics) | `runtime/ut/skipper_ut.cpp`, `ydb_clients_ut.cpp`, `state_file_ut.cpp`, `diagnostics_ut.cpp` |
| HTTP routes, Host/Origin guard, command parsing | `http/actor.cpp`, `http.h` |
| `/api/state` JSON, HTML page, Prometheus metrics | `http/state_json.cpp`, `page.cpp`, `metrics.cpp`; tests `http/ut/http_ut.cpp`, `commands_ut.cpp` |
| Mock server (Bridge, Monitoring, Discovery, Config), control API, faults, cluster lab | `ydbd_mock/lib/services.py`, `control_api.py`, `faults.py`, `supervisor.py`, `scripts/lab.sh`, `README.md`; tests `ydbd_mock/tests/` |
| Acceptance harness driving the real binary against the mock | `acceptance/lib/harness.py`, `acceptance/tests/test_e2e.py`, `test_standalone.py`, `test_ports.py`, `acceptance/README.md` |

## Server and SDK

| Topic | Where |
| --- | --- |
| Bridge concepts and split-brain recovery docs | `ydb/docs/en/core/concepts/bridge.md` (force mode is not exposed by the Bridge RPC) |
| Bridge RPCs and validation; status mapping | `ydb/core/grpc_services/rpc_bridge.cpp`, `rpc_bridge_base.h` |
| Public API definitions | `ydb/public/api/grpc/draft/ydb_bridge_v1.proto`, `ydb/public/api/protos/draft/ydb_bridge.proto`, `ydb/public/api/protos/ydb_bridge_common.proto`, `ydb_monitoring.proto`, `ydb_config.proto`, `ydb_discovery.proto` |
| C++ SDK client / update serialization | `ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_bridge.h`, `ydb/public/sdk/cpp/src/client/draft/ydb_bridge.cpp` (`UpdatesToProto`; `TPileStateUpdate::Generation` is not sent; use `TUpdateClusterStateSettings::ExpectedGeneration`) |
| Cluster-state commits, generation, pile list, Add/Remove, Convert | `ydb/core/blobstorage/nodewarden/distconf_invoke_bridge.cpp`, `distconf_bridge.cpp` (`UpdateClusterStateGeneration`, `ConvertStorageConfigToBridge`) |
| Generation guard; node-quorum check before every proposition; unguarded-only synthesized success | `ydb/core/grpc_services/rpc_bridge.cpp`; `ydb/core/blobstorage/nodewarden/distconf_invoke_bridge.cpp`, `distconf_invoke_common.cpp` (`StartPropositionImpl`/`HasConnectedNodeQuorum`, `IsClusterStateReached`, `TEvNodeDisconnected` handler) |
| Automatic promotion; stage 1 → stage 2 | `ydb/core/blobstorage/nodewarden/distconf_fsm.cpp` (`UpdateQuorums`, `CheckForConfigUpdate`), `distconf_bridge.cpp` |
| Connectivity report | `ydb/core/blobstorage/nodewarden/distconf_fsm.cpp` (`FillPileConnectivity`), `distconf_invoke_common.cpp`; `ydb/core/grpc_services/rpc_bridge.cpp`; `ydb/public/api/protos/draft/ydb_bridge.proto` |
| Which pile states join which quorum | `ydb/core/base/bridge.h` (`PileStateTraits`) |
| Config/static-storage quorums | `ydb/core/blobstorage/nodewarden/distconf_quorum.cpp` (`HasConfigQuorum`) |
| SUSPENDED → DISCONNECTED, root authority loss | `ydb/core/cms/cms.cpp` and `ydb/core/blobstorage/nodewarden/distconf_invoke_bridge.cpp` (`NotifyBridgeSuspended`), `distconf_invoke_common.cpp` (`RunCommonChecks`), `distconf_fsm.cpp` (`ReconcileNodeRole`, `StopRootActivities`) |
| Pile resolution, implicit pile, state comparison | `ydb/core/base/bridge.h`, `bridge.cpp` (`ResolvePile`, `IsSameClusterState`); internal states in `ydb/core/protos/bridge.proto` |
| gRPC listener start/stop by pile state | `ydb/core/driver_lib/run/run.cpp` (`TGRpcServersManager`, `TEvNodeWardenStorageConfig` handler) |
| SelfCheck bridge issues, fallback, group aggregation, responder pile (`location.pile.name`) | `ydb/core/health_check/health_check.cpp` (`RequestNodeWhiteboard`, `AddVDiskStatus`, `FillVDiskStatus`, `GetWhiteboardReplyState`, `FillPDiskStatusWithWhiteboard`, `FillVDiskStatusWithWhiteboard`, `TGroupChecker`); tests `health_check_ut.cpp` (`Whiteboard*`, `CheckStaticPool`), `ydb/core/blobstorage/ut_blobstorage/bridge_rejoin.cpp` |
| SelfCheck ring checks, truncation, timeouts | `ydb/core/health_check/health_check.cpp` (`FillStateStorage`, `TruncateIssuesIfBeyondLimit`, `ReplyAndPassAway`, `Bootstrap`, `TimeoutBSC`, `TimeoutFinal`); `ydb/core/protos/config.proto` (`THealthCheckConfig.Timeout`) |
| VDisk `ERROR` on BSC node loss; 15 s status expiry | `ydb/core/mind/bscontroller/register_node.cpp`; `sys_view.cpp` (`UpdateSystemViews`), `load_everything.cpp` |
| Tablet placement during promotion | `ydb/core/mind/hive/hive_impl.cpp` (`IsPrimaryPile`) |
| Blob write/read completion | `ydb/core/blobstorage/bridge/proxy/bridge_proxy.cpp` (`ProcessFullQuorumResponse`, `DataIsTrustedInPile`, `ProcessResponse`) |
| StateStorage / SchemeBoard / Board rules | `ydb/core/blobstorage/nodewarden/distconf_bridge.cpp` (`ExtractStateStorageConfig`), `ydb/core/base/statestorage.cpp` (`GetRingGroupState`), `statestorage_proxy.cpp`, `ydb/core/tx/scheme_board/populator.cpp`, `subscriber.cpp`, `ydb/core/base/board_lookup.cpp`, `board_publish.cpp` |
| Auth and admin checks | `ydb/core/base/auth.cpp` (`IsTokenAllowedImpl`), `ydb/core/grpc_services/rpc_login.cpp`, `ydb/core/security/ticket_parser_impl.h` |
| Discovery pile-state filtering | `ydb/core/discovery/discovery.cpp` |
