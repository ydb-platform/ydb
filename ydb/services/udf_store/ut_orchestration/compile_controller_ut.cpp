#include "snapshot_helpers.h"

#include <ydb/services/udf_store/compile_controller/compile_controller.h>
#include <ydb/services/udf_store/compile_controller/events.h>
#include <ydb/services/udf_store/compile_controller/private_events.h>
#include <ydb/services/udf_store/metadata_subscription/snapshot.h>
#include <ydb/services/udf_store/metadata_subscription/wasm_artifact.h>
#include <ydb/services/udf_store/service.h>

#include <ydb/core/base/counters.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/services/metadata/abstract/common.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NUdfStore {

namespace {

constexpr ui64 ControllerTabletId = TTestTxConfig::TxTablet0;
const TDuration SimTimeout = TDuration::Seconds(10);
const TDuration SettleTime = TDuration::Seconds(2);

const TString CpuSpecX86 = "x86_64-linux-gnu-haswell";
const TString CpuSpecArm = "aarch64-linux-gnu-neoversen1";

const TString ModuleKind = WasmArtifactKindToString(EWasmArtifactKind::Module);
const TString LibraryKind = WasmArtifactKindToString(EWasmArtifactKind::Library);

//! An assignment as the test observed it on the wire, together with the node it
//! was handed to. The controller addresses workers by node-local service id, so
//! the recipient is the only place the target node shows up.
struct TSeenAssignment {
    ui32 NodeId = 0;
    NKikimrUdfStore::TEvAssignCompile Record;

    TString Name() const {
        return Record.GetKey().GetName();
    }

    TString Uid() const {
        return Record.GetKey().GetUid();
    }

    bool IsLibrary() const {
        return Record.GetKey().GetKind() == NKikimrUdfStore::ARTIFACT_KIND_LIBRARY;
    }
};

class TTestEnv {
public:
    explicit TTestEnv(ui32 nodeCount = 1)
        : Runtime(nodeCount, false)
    {
        Runtime.SetLogPriority(NKikimrServices::METADATA_PROVIDER, NActors::NLog::PRI_DEBUG);

        TAppPrepare app;
        app.AddDomain(TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds(
            "dc-1", /*domainUid=*/0, /*schemeRoot=*/0, /*planResolution=*/100500,
            TVector<ui64>{}, TVector<ui64>{}, TVector<ui64>{}, DefaultPoolKinds(2)).Release());
        SetupChannelProfiles(app);
        SetupTabletServices(Runtime, &app, true);

        Runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& ev) {
            Observe(ev);
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        const TActorId bootstrapper = CreateTestBootstrapper(Runtime,
            CreateTestTabletInfo(ControllerTabletId, TTabletTypes::WasmCompileController),
            &CreateWasmCompileController);
        // Anything an actor schedules for itself is dropped unless it is on this
        // whitelist, and the tablet inherits it from the bootstrapper that starts
        // it. Without this the controller's periodic tick never fires here, so
        // neither the heartbeat timeout nor any housekeeping can be tested.
        Runtime.EnableScheduleForActor(bootstrapper, true);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvTablet::EvBoot);
        Runtime.DispatchEvents(options, SimTimeout);

        Sender = Runtime.AllocateEdgeActor();

        // The controller addresses dinodes by node-local service id. Without a
        // registered service the events would be dropped before the observer
        // ever sees them, so every node gets a sink standing in for the store.
        for (ui32 nodeIndex = 0; nodeIndex < nodeCount; ++nodeIndex) {
            Runtime.RegisterService(
                MakeServiceId(Runtime.GetNodeId(nodeIndex)),
                Runtime.AllocateEdgeActor(nodeIndex),
                nodeIndex);
        }
    }

    TTestActorRuntime& GetRuntime() {
        return Runtime;
    }

    //! Registers a dinode worker over a real tablet pipe: the controller keys
    //! its liveness on the pipe server, so a shortcut through ForwardToTablet
    //! would make the disconnect path untestable.
    void RegisterWorker(ui32 nodeIndex, const TString& cpuSpec, ui32 capacity = 1) {
        const TActorId sender = Runtime.AllocateEdgeActor(nodeIndex);
        const TActorId pipe = Runtime.ConnectToPipe(
            ControllerTabletId, sender, nodeIndex, NTabletPipe::TClientConfig());

        auto ev = std::make_unique<TEvCompileController::TEvRegister>();
        ev->Record.SetCpuSpec(cpuSpec);
        ev->Record.SetNodeId(Runtime.GetNodeId(nodeIndex));
        ev->Record.SetCapacity(capacity);
        Runtime.SendToPipe(pipe, sender, ev.release(), nodeIndex);

        auto result = Runtime.GrabEdgeEventRethrow<TEvCompileController::TEvRegisterResult>(
            sender, SimTimeout);
        UNIT_ASSERT(result);

        WorkerPipes[nodeIndex] = std::make_pair(pipe, sender);
    }

    void DisconnectWorker(ui32 nodeIndex) {
        const auto it = WorkerPipes.find(nodeIndex);
        UNIT_ASSERT_C(it != WorkerPipes.end(), "node " << nodeIndex << " has no pipe");
        Runtime.ClosePipe(it->second.first, it->second.second, nodeIndex);
        WorkerPipes.erase(it);
    }

    void SendHeartbeat(ui32 nodeIndex, const TVector<ui64>& activeAssignmentIds = {}) {
        const auto it = WorkerPipes.find(nodeIndex);
        UNIT_ASSERT_C(it != WorkerPipes.end(), "node " << nodeIndex << " has no pipe");

        auto ev = std::make_unique<TEvCompileController::TEvHeartbeat>();
        ev->Record.SetCpuSpec("");
        ev->Record.SetNodeId(Runtime.GetNodeId(nodeIndex));
        ev->Record.SetCapacity(0);
        for (const ui64 assignmentId : activeAssignmentIds) {
            ev->Record.AddActiveAssignmentIds(assignmentId);
        }
        Runtime.SendToPipe(it->second.first, it->second.second, ev.release(), nodeIndex);
    }

    //! Replaces what the controller believes `artifacts/{cpu_spec}` contains.
    //! The real reconcile reads that table over YQL, which no bare tablet
    //! runtime can serve, so the tests inject its outcome instead.
    void SetArtifacts(const TString& cpuSpec, const TVector<std::tuple<TString, TString, TString>>& keys) {
        auto ev = std::make_unique<TEvControllerPrivate::TEvReconcileResult>();
        ev->CpuSpec = cpuSpec;
        ev->Success = true;
        for (const auto& [name, kind, uid] : keys) {
            ev->ArtifactKeys.insert(MakeArtifactKey(name, kind, uid));
        }
        ForwardToTablet(Runtime, ControllerTabletId, Sender, ev.release());
    }

    void SetSnapshot(const TVector<TModuleDesc>& modules) {
        ForwardToTablet(Runtime, ControllerTabletId, Sender,
            new NMetadata::NProvider::TEvRefreshSubscriberData(MakeSnapshot(modules)));
    }

    //! One config notification carries the whole TUdfStoreConfig, so everything
    //! a test wants to change has to travel together: a second notification
    //! would reset whatever the first one set. Zero means "keep the default".
    void SetBudgets(
        ui32 maxPerCpuSpec,
        ui32 maxPerDinode,
        ui32 maxAttempts = 3,
        ui32 heartbeatTimeoutSeconds = 0,
        ui32 assignGraceSeconds = 0)
    {
        auto ev = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationRequest>();
        auto& config = *ev->Record.MutableConfig()->MutableUdfStoreConfig();
        config.SetWasmCompileMaxPerCpuSpec(maxPerCpuSpec);
        config.SetWasmCompileMaxPerDinode(maxPerDinode);
        config.SetWasmMaxCompileAttempts(maxAttempts);
        config.SetWasmCompileHeartbeatTimeoutSeconds(heartbeatTimeoutSeconds);
        config.SetWasmCompileAssignGraceSeconds(assignGraceSeconds);
        ForwardToTablet(Runtime, ControllerTabletId, Sender, ev.release());
    }

    //! Index of the node an assignment went to, so that a test can tell the
    //! assignee and its peers apart without assuming which one was picked.
    ui32 NodeIndexOf(const TSeenAssignment& assignment, ui32 nodeCount) {
        for (ui32 nodeIndex = 0; nodeIndex < nodeCount; ++nodeIndex) {
            if (Runtime.GetNodeId(nodeIndex) == assignment.NodeId) {
                return nodeIndex;
            }
        }
        UNIT_FAIL("assignment went to a node outside the runtime");
        return 0;
    }

    void ReportDone(const TSeenAssignment& assignment) {
        auto ev = std::make_unique<TEvCompileController::TEvCompileDone>();
        ev->Record.SetAssignmentId(assignment.Record.GetAssignmentId());
        *ev->Record.MutableKey() = assignment.Record.GetKey();
        ForwardToTablet(Runtime, ControllerTabletId, Sender, ev.release());
    }

    void ReportFailed(const TSeenAssignment& assignment, bool stale, const TString& error = "boom") {
        auto ev = std::make_unique<TEvCompileController::TEvCompileFailed>();
        ev->Record.SetAssignmentId(assignment.Record.GetAssignmentId());
        *ev->Record.MutableKey() = assignment.Record.GetKey();
        ev->Record.SetError(error);
        ev->Record.SetStale(stale);
        ForwardToTablet(Runtime, ControllerTabletId, Sender, ev.release());
    }

    //! Lets the controller finish whatever the last stimulus started. Every
    //! interesting outcome is a message the observer already recorded, so the
    //! tests only need the scheduling round to drain.
    void Settle(TDuration duration = SettleTime) {
        Runtime.SimulateSleep(duration);
    }

    TVector<TSeenAssignment> Assignments() const {
        TVector<TSeenAssignment> result;
        result.reserve(SeenAssignments.size());
        for (const auto& [assignmentId, assignment] : SeenAssignments) {
            Y_UNUSED(assignmentId);
            result.push_back(assignment);
        }
        SortBy(result, [](const TSeenAssignment& item) {
            return item.Record.GetAssignmentId();
        });
        return result;
    }

    const THashSet<TString>& ReadyBroadcasts() const {
        return SeenReady;
    }

    //! Reads a per-platform gauge the way monitoring would: by the `cpu_spec`
    //! label rather than by reaching into the tablet.
    i64 PlatformCounter(const TString& cpuSpec, const TString& name) {
        auto counter = GetServiceCounters(Runtime.GetAppData(0).Counters, "udf_store")
            ->GetSubgroup("subsystem", "wasm_compile")
            ->GetSubgroup("cpu_spec", cpuSpec)
            ->FindCounter(name);
        UNIT_ASSERT_C(counter, "no counter " << name << " for " << cpuSpec);
        return counter->Val();
    }

private:
    void Observe(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            case TEvCompileController::TEvAssignCompile::EventType: {
                const auto& record = ev->Get<TEvCompileController::TEvAssignCompile>()->Record;
                // A cross-node event can be seen both before and after the
                // interconnect, so the assignment id keeps the record unique.
                SeenAssignments[record.GetAssignmentId()] = TSeenAssignment{
                    .NodeId = ev->Recipient.NodeId(),
                    .Record = record,
                };
                break;
            }
            case TEvCompileController::TEvArtifactReady::EventType: {
                const auto& key = ev->Get<TEvCompileController::TEvArtifactReady>()->Record.GetKey();
                SeenReady.insert(TStringBuilder()
                    << ev->Recipient.NodeId() << '/' << key.GetName() << '/' << key.GetUid());
                break;
            }
            default:
                break;
        }
    }

    TTestBasicRuntime Runtime;
    TActorId Sender;
    THashMap<ui32, std::pair<TActorId, TActorId>> WorkerPipes;
    THashMap<ui64, TSeenAssignment> SeenAssignments;
    THashSet<TString> SeenReady;
};

} // namespace

Y_UNIT_TEST_SUITE(WasmCompileController) {

    Y_UNIT_TEST(OnePlatformCompilesOnce) {
        TTestEnv env(3);
        for (ui32 nodeIndex = 0; nodeIndex < 3; ++nodeIndex) {
            env.RegisterWorker(nodeIndex, CpuSpecX86);
        }
        env.SetArtifacts(CpuSpecX86, {});
        env.SetSnapshot({{.Name = "m1", .Uid = "u1"}});
        env.Settle();

        const auto assignments = env.Assignments();
        UNIT_ASSERT_VALUES_EQUAL_C(assignments.size(), 1, "thundering herd across peers of one platform");
        UNIT_ASSERT_VALUES_EQUAL(assignments[0].Name(), "m1");
        UNIT_ASSERT_VALUES_EQUAL(assignments[0].Uid(), "u1");
    }

    Y_UNIT_TEST(TwoPlatformsCompileInParallel) {
        TTestEnv env(2);
        env.RegisterWorker(0, CpuSpecX86);
        env.RegisterWorker(1, CpuSpecArm);
        env.SetArtifacts(CpuSpecX86, {});
        env.SetArtifacts(CpuSpecArm, {});
        env.SetSnapshot({{.Name = "m1", .Uid = "u1"}});
        env.Settle();

        const auto assignments = env.Assignments();
        UNIT_ASSERT_VALUES_EQUAL(assignments.size(), 2);
        THashSet<TString> cpuSpecs;
        for (const auto& assignment : assignments) {
            cpuSpecs.insert(assignment.Record.GetKey().GetCpuSpec());
        }
        UNIT_ASSERT(cpuSpecs.contains(CpuSpecX86));
        UNIT_ASSERT(cpuSpecs.contains(CpuSpecArm));
        UNIT_ASSERT_VALUES_UNEQUAL(assignments[0].NodeId, assignments[1].NodeId);
    }

    Y_UNIT_TEST(DisconnectedWorkerIsReassigned) {
        TTestEnv env(2);
        env.RegisterWorker(0, CpuSpecX86);
        env.RegisterWorker(1, CpuSpecX86);
        env.SetArtifacts(CpuSpecX86, {});
        env.SetSnapshot({{.Name = "m1", .Uid = "u1"}});
        env.Settle();

        auto assignments = env.Assignments();
        UNIT_ASSERT_VALUES_EQUAL(assignments.size(), 1);
        const ui32 firstNodeId = assignments[0].NodeId;

        const ui32 deadNodeIndex = firstNodeId == env.GetRuntime().GetNodeId(0) ? 0 : 1;
        env.DisconnectWorker(deadNodeIndex);
        env.Settle();

        assignments = env.Assignments();
        UNIT_ASSERT_VALUES_EQUAL_C(assignments.size(), 2, "the surviving peer never picked the gap up");
        UNIT_ASSERT_VALUES_UNEQUAL(assignments[1].NodeId, firstNodeId);
        UNIT_ASSERT_VALUES_EQUAL(assignments[1].Name(), "m1");
    }

    Y_UNIT_TEST(FreshAssignmentSurvivesAnInFlightHeartbeat) {
        TTestEnv env(2);
        env.RegisterWorker(0, CpuSpecX86);
        env.RegisterWorker(1, CpuSpecX86);
        env.SetArtifacts(CpuSpecX86, {});
        env.SetSnapshot({{.Name = "m1", .Uid = "u1"}});
        env.Settle();

        auto assignments = env.Assignments();
        UNIT_ASSERT_VALUES_EQUAL(assignments.size(), 1);
        const ui32 assigneeIndex = env.NodeIndexOf(assignments[0], 2);

        // A heartbeat composed before the assignment reached the node cannot
        // possibly mention it. Reading that as a refusal frees the gap while
        // the assignee is starting on it, and the peer picks up the same work.
        env.SendHeartbeat(assigneeIndex);
        env.Settle();

        assignments = env.Assignments();
        UNIT_ASSERT_VALUES_EQUAL_C(assignments.size(), 1,
            "the gap was handed out again after a heartbeat crossed the assignment");
    }

    Y_UNIT_TEST(UnclaimedAssignmentIsReleasedAfterGrace) {
        TTestEnv env(2);
        env.SetBudgets(/*maxPerCpuSpec=*/1, /*maxPerDinode=*/1, /*maxAttempts=*/3,
            /*heartbeatTimeoutSeconds=*/0, /*assignGraceSeconds=*/5);
        env.RegisterWorker(0, CpuSpecX86);
        env.RegisterWorker(1, CpuSpecX86);
        env.SetArtifacts(CpuSpecX86, {});
        env.SetSnapshot({{.Name = "m1", .Uid = "u1"}});
        env.Settle();
        UNIT_ASSERT_VALUES_EQUAL(env.Assignments().size(), 1);

        const ui32 assigneeIndex = env.NodeIndexOf(env.Assignments()[0], 2);

        // Past the grace the same silence is real information: the node has had
        // every chance to declare the assignment and does not.
        env.Settle(TDuration::Seconds(8));
        env.SendHeartbeat(assigneeIndex);
        env.Settle();

        UNIT_ASSERT_VALUES_EQUAL_C(env.Assignments().size(), 2,
            "an assignment the worker keeps disowning is never handed to anybody else");
    }

    Y_UNIT_TEST(ReRegisterKeepsPerDinodeBudget) {
        TTestEnv env(1);
        env.SetBudgets(/*maxPerCpuSpec=*/4, /*maxPerDinode=*/1);
        env.RegisterWorker(0, CpuSpecX86, /*capacity=*/4);
        env.SetArtifacts(CpuSpecX86, {});
        env.SetSnapshot({
            {.Name = "m1", .Uid = "u1"},
            {.Name = "m2", .Uid = "u2"},
        });
        env.Settle();
        UNIT_ASSERT_VALUES_EQUAL(env.Assignments().size(), 1);

        // Reconnecting does not finish the AOT the node is running, so the
        // budget it is already spending has to survive the new registration.
        env.RegisterWorker(0, CpuSpecX86, /*capacity=*/4);
        env.Settle(TDuration::Seconds(7));

        UNIT_ASSERT_VALUES_EQUAL_C(env.Assignments().size(), 1,
            "a second AOT was started on one dinode with MaxPerDinode = 1");
    }

    Y_UNIT_TEST(HeartbeatTimeoutReassignsGap) {
        TTestEnv env(2);
        env.SetBudgets(/*maxPerCpuSpec=*/1, /*maxPerDinode=*/1, /*maxAttempts=*/3,
            /*heartbeatTimeoutSeconds=*/20);
        env.RegisterWorker(0, CpuSpecX86);
        env.RegisterWorker(1, CpuSpecX86);
        env.SetArtifacts(CpuSpecX86, {});
        env.SetSnapshot({{.Name = "m1", .Uid = "u1"}});
        env.Settle();

        auto assignments = env.Assignments();
        UNIT_ASSERT_VALUES_EQUAL(assignments.size(), 1);
        const ui32 firstNodeId = assignments[0].NodeId;
        const ui32 peerIndex = env.NodeIndexOf(assignments[0], 2) == 0 ? 1 : 0;

        // The assignee goes quiet mid-compile while holding its pipe open, so
        // only the heartbeat timeout can notice. The peer keeps reporting in,
        // otherwise the platform would simply lose both of its workers.
        for (size_t round = 0; round < 4; ++round) {
            env.SendHeartbeat(peerIndex);
            env.Settle(TDuration::Seconds(8));
        }

        assignments = env.Assignments();
        UNIT_ASSERT_VALUES_EQUAL_C(assignments.size(), 2,
            "a worker that stopped heartbeating kept the gap forever");
        UNIT_ASSERT_VALUES_UNEQUAL(assignments[1].NodeId, firstNodeId);
    }

    Y_UNIT_TEST(MissingArtifactSurvivesWorkerLoss) {
        TTestEnv env(1);
        env.RegisterWorker(0, CpuSpecX86);
        env.SetArtifacts(CpuSpecX86, {});
        env.SetSnapshot({
            {.Name = "m1", .Uid = "u1"},
            {.Name = "m2", .Uid = "u2"},
        });
        env.Settle();
        UNIT_ASSERT_VALUES_EQUAL(env.PlatformCounter(CpuSpecX86, "WasmModulesMissingArtifact"), 2);

        env.DisconnectWorker(0);
        env.Settle();

        // Losing the last worker of a platform is exactly what this gauge is
        // for; reading zero here would silence the alarm at the moment it fires.
        UNIT_ASSERT_VALUES_EQUAL_C(env.PlatformCounter(CpuSpecX86, "WasmModulesMissingArtifact"), 2,
            "an uncovered platform reports nothing missing");
        UNIT_ASSERT_VALUES_EQUAL(env.PlatformCounter(CpuSpecX86, "WasmCompileWorkersRegistered"), 0);
    }

    Y_UNIT_TEST(AttemptsOfADroppedModuleAreForgotten) {
        TTestEnv env(1);
        env.RegisterWorker(0, CpuSpecX86);
        env.SetArtifacts(CpuSpecX86, {});
        env.SetSnapshot({{.Name = "m1", .Uid = "u1"}});
        env.Settle();

        for (size_t attempt = 0; attempt < 3; ++attempt) {
            const auto assignments = env.Assignments();
            UNIT_ASSERT_VALUES_EQUAL(assignments.size(), attempt + 1);
            env.ReportFailed(assignments.back(), /*stale=*/false);
            env.Settle();
        }
        UNIT_ASSERT_VALUES_EQUAL(env.Assignments().size(), 3);

        // The module is gone, so its failure count refers to nothing. Keeping
        // the row would leave it in local DB for the lifetime of the tenant.
        env.SetSnapshot({});
        env.Settle(TDuration::Seconds(7));

        env.SetSnapshot({{.Name = "m1", .Uid = "u1"}});
        env.Settle();

        UNIT_ASSERT_VALUES_EQUAL_C(env.Assignments().size(), 4,
            "the poison pill of a module that no longer exists was kept");
    }

    Y_UNIT_TEST(StaleFailureDoesNotPoison) {
        TTestEnv env(1);
        env.RegisterWorker(0, CpuSpecX86);
        env.SetArtifacts(CpuSpecX86, {});
        env.SetSnapshot({{.Name = "m1", .Uid = "u1"}});
        env.Settle();

        // Three losses to a re-upload, i.e. one more than the poison pill would
        // tolerate if they were counted as real failures.
        for (size_t attempt = 0; attempt < 3; ++attempt) {
            const auto assignments = env.Assignments();
            UNIT_ASSERT_VALUES_EQUAL(assignments.size(), attempt + 1);
            env.ReportFailed(assignments.back(), /*stale=*/true);
            env.Settle();
        }

        UNIT_ASSERT_VALUES_EQUAL_C(env.Assignments().size(), 4,
            "a stale report was counted against the retry budget");
    }

    Y_UNIT_TEST(PoisonPillStopsRetrying) {
        TTestEnv env(1);
        env.RegisterWorker(0, CpuSpecX86);
        env.SetArtifacts(CpuSpecX86, {});
        env.SetSnapshot({{.Name = "m1", .Uid = "u1"}});
        env.Settle();

        for (size_t attempt = 0; attempt < 3; ++attempt) {
            const auto assignments = env.Assignments();
            UNIT_ASSERT_VALUES_EQUAL(assignments.size(), attempt + 1);
            env.ReportFailed(assignments.back(), /*stale=*/false);
            env.Settle();
        }

        UNIT_ASSERT_VALUES_EQUAL_C(env.Assignments().size(), 3,
            "the module is still being handed out after the poison pill");
    }

    Y_UNIT_TEST(LibraryIsCompiledBeforeItsModule) {
        TTestEnv env(1);
        // Budget wide enough for both, so that the ordering is the only thing
        // holding the module back.
        env.SetBudgets(/*maxPerCpuSpec=*/4, /*maxPerDinode=*/4);
        env.RegisterWorker(0, CpuSpecX86, /*capacity=*/4);
        env.SetArtifacts(CpuSpecX86, {});
        env.SetSnapshot({
            {.Name = "lib1", .Uid = "lu1", .Type = EUdfType::LIBRARY},
            {.Name = "m1", .Uid = "u1", .Manifest = MakeManifest("m1", {"lib1"})},
        });
        env.Settle();

        auto assignments = env.Assignments();
        UNIT_ASSERT_VALUES_EQUAL_C(assignments.size(), 1, "the module was assigned before its library");
        UNIT_ASSERT(assignments[0].IsLibrary());
        UNIT_ASSERT_VALUES_EQUAL(assignments[0].Name(), "lib1");

        env.ReportDone(assignments[0]);
        env.Settle();

        assignments = env.Assignments();
        UNIT_ASSERT_VALUES_EQUAL(assignments.size(), 2);
        UNIT_ASSERT(!assignments[1].IsLibrary());
        UNIT_ASSERT_VALUES_EQUAL(assignments[1].Name(), "m1");
        UNIT_ASSERT_VALUES_EQUAL(assignments[1].Record.LibraryUidsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(assignments[1].Record.GetLibraryUids(0).GetUid(), "lu1");

        const TString expectedReady = TStringBuilder()
            << env.GetRuntime().GetNodeId(0) << "/lib1/lu1";
        UNIT_ASSERT_C(env.ReadyBroadcasts().contains(expectedReady),
            "the platform was not told its library artifact is ready");
    }

    Y_UNIT_TEST(NothingIsAssignedWithoutWorkers) {
        TTestEnv env(1);
        env.SetArtifacts(CpuSpecX86, {});
        env.SetSnapshot({{.Name = "m1", .Uid = "u1"}});
        env.Settle();

        UNIT_ASSERT_VALUES_EQUAL_C(env.Assignments().size(), 0,
            "a gap was handed out with no platform registered");

        env.RegisterWorker(0, CpuSpecX86);
        env.SendHeartbeat(0);
        env.Settle();

        UNIT_ASSERT_VALUES_EQUAL(env.Assignments().size(), 1);
    }

    Y_UNIT_TEST(MaxPerDinodeCapsOneWorker) {
        TTestEnv env(1);
        // The per-platform budget is wide, so only the per-dinode cap can keep
        // the second module waiting.
        env.SetBudgets(/*maxPerCpuSpec=*/4, /*maxPerDinode=*/1);
        env.RegisterWorker(0, CpuSpecX86, /*capacity=*/4);
        env.SetArtifacts(CpuSpecX86, {});
        env.SetSnapshot({
            {.Name = "m1", .Uid = "u1"},
            {.Name = "m2", .Uid = "u2"},
        });
        env.Settle();

        auto assignments = env.Assignments();
        UNIT_ASSERT_VALUES_EQUAL_C(assignments.size(), 1, "two AOT runs were started on one dinode");

        env.ReportDone(assignments[0]);
        env.Settle();

        assignments = env.Assignments();
        UNIT_ASSERT_VALUES_EQUAL(assignments.size(), 2);
        UNIT_ASSERT_VALUES_UNEQUAL(assignments[0].Name(), assignments[1].Name());
    }

    Y_UNIT_TEST(ExistingArtifactIsNotRecompiled) {
        TTestEnv env(1);
        env.RegisterWorker(0, CpuSpecX86);
        env.SetArtifacts(CpuSpecX86, {{"m1", ModuleKind, "u1"}});
        env.SetSnapshot({{.Name = "m1", .Uid = "u1"}});
        env.Settle();

        UNIT_ASSERT_VALUES_EQUAL_C(env.Assignments().size(), 0,
            "a module already present in artifacts/{cpu_spec} was compiled again");

        // A re-upload changes the uid, so the same name becomes a fresh gap.
        env.SetSnapshot({{.Name = "m1", .Uid = "u2"}});
        env.Settle();

        const auto assignments = env.Assignments();
        UNIT_ASSERT_VALUES_EQUAL(assignments.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(assignments[0].Uid(), "u2");
    }

    Y_UNIT_TEST(CountersFollowTheQueue) {
        TTestEnv env(1);
        env.RegisterWorker(0, CpuSpecX86);
        env.SetArtifacts(CpuSpecX86, {});
        env.SetSnapshot({
            {.Name = "m1", .Uid = "u1"},
            {.Name = "m2", .Uid = "u2"},
        });
        env.Settle();

        // One module in flight under the default per-platform budget, the other
        // waiting, and both still uncovered on this platform.
        UNIT_ASSERT_VALUES_EQUAL(env.PlatformCounter(CpuSpecX86, "WasmCompileWorkersRegistered"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.PlatformCounter(CpuSpecX86, "WasmCompileAssignmentsActive"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.PlatformCounter(CpuSpecX86, "WasmCompileQueueLength"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.PlatformCounter(CpuSpecX86, "WasmModulesMissingArtifact"), 2);

        auto assignments = env.Assignments();
        UNIT_ASSERT_VALUES_EQUAL(assignments.size(), 1);
        env.ReportDone(assignments[0]);
        env.Settle();

        UNIT_ASSERT_VALUES_EQUAL(env.PlatformCounter(CpuSpecX86, "WasmModulesMissingArtifact"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.PlatformCounter(CpuSpecX86, "WasmCompileAssignmentsActive"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.PlatformCounter(CpuSpecX86, "WasmCompileQueueLength"), 0);

        assignments = env.Assignments();
        UNIT_ASSERT_VALUES_EQUAL(assignments.size(), 2);
        env.ReportDone(assignments[1]);
        env.Settle();

        UNIT_ASSERT_VALUES_EQUAL_C(env.PlatformCounter(CpuSpecX86, "WasmModulesMissingArtifact"), 0,
            "the platform still reads as uncovered after both artifacts appeared");
        UNIT_ASSERT_VALUES_EQUAL(env.PlatformCounter(CpuSpecX86, "WasmCompileAssignmentsActive"), 0);
    }
}

} // namespace NKikimr::NUdfStore
