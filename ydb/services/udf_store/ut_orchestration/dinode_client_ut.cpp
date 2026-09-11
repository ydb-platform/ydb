#include "snapshot_helpers.h"

#include <ydb/services/udf_store/compile_controller/compile_controller.h>
#include <ydb/services/udf_store/compile_controller/events.h>
#include <ydb/services/udf_store/events.h>
#include <ydb/services/udf_store/service.h>

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <yql/essentials/minikql/mkql_function_registry.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NUdfStore {

namespace {

constexpr ui64 ControllerTabletId = TTestTxConfig::TxTablet0;
const TDuration SimTimeout = TDuration::Seconds(10);
const TDuration SettleTime = TDuration::Seconds(2);
// Longer than the 10s controller tick, so that one heartbeat is guaranteed.
const TDuration TickTime = TDuration::Seconds(11);

const TString TestCpuSpec = "test-cpu-spec";
const TString TestDatabase = "/dc-1";
const TString ForeignCpuSpec = "some-other-platform";

//! Stands in for the scheme cache. Discovery only reads the domain's processing
//! params out of a navigate, so that is all this has to produce; going through
//! a real scheme cache would drag in a whole schemeshard.
class TFakeSchemeCache : public NActors::TActor<TFakeSchemeCache> {
public:
    explicit TFakeSchemeCache(ui64 controllerTabletId)
        : TActor(&TThis::StateWork)
        , ControllerTabletId(controllerTabletId)
    {}

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySet, Handle);
            default:
                break;
        }
    }

private:
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr& ev) {
        TAutoPtr<NSchemeCache::TSchemeCacheNavigate> navigate(ev->Get()->Request.Release());
        for (auto& entry : navigate->ResultSet) {
            entry.Status = NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
            // Equal keys mean a non-serverless database, so discovery stops at
            // the first hop instead of asking for the resource domain.
            const TPathId domainKey(TTestTxConfig::SchemeShard, 1);
            entry.DomainInfo = MakeIntrusive<NSchemeCache::TDomainInfo>(domainKey, domainKey);
            if (ControllerTabletId) {
                entry.DomainInfo->Params.SetWasmCompileController(ControllerTabletId);
            }
        }
        Send(ev->Sender, new TEvTxProxySchemeCache::TEvNavigateKeySetResult(navigate), 0, ev->Cookie);
    }

    const ui64 ControllerTabletId;
};

struct TEnvOptions {
    bool EnableController = true;
    //! Zero means the database has no controller yet, i.e. the state an
    //! unmigrated tenant is in.
    ui64 ControllerTabletId = ::NKikimr::NUdfStore::ControllerTabletId;
};

//! The real controller tablet with the real dinode service next to it, and
//! nothing else. Everything the service needs from a database (KQP, metadata
//! provider, scheme cache) is either faked or simply absent, which is enough
//! for the orchestration wire: discovery, pipe, assignment routing, reporting.
class TDinodeEnv {
public:
    explicit TDinodeEnv(const TEnvOptions& options = {})
        : Runtime(1, false)
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

        CreateTestBootstrapper(Runtime,
            CreateTestTabletInfo(::NKikimr::NUdfStore::ControllerTabletId, TTabletTypes::WasmCompileController),
            &CreateWasmCompileController);

        TDispatchOptions bootOptions;
        bootOptions.FinalEvents.emplace_back(TEvTablet::EvBoot);
        Runtime.DispatchEvents(bootOptions, SimTimeout);

        Sender = Runtime.AllocateEdgeActor();

        // Discovery refuses to start without a database name, since it has
        // nothing to navigate to.
        Runtime.GetAppData(0).TenantName = TestDatabase;

        Runtime.RegisterService(MakeSchemeCacheID(),
            Runtime.Register(new TFakeSchemeCache(options.ControllerTabletId), 0),
            0);

        NKikimrConfig::TUdfStoreConfig config;
        config.SetEnabled(true);
        config.SetEnableWasmUdf(true);
        config.SetEnableWasmCompileController(options.EnableController);
        // Object code is host-specific, so the service normally detects the
        // platform; pinning it keeps the assertions independent of the machine.
        config.SetWasmCpuSpecOverride(TestCpuSpec);

        auto functionRegistry = Runtime.GetAppData(0).FunctionRegistry->Clone();
        ServiceId = Runtime.Register(
            CreateService(config, functionRegistry), 0, Runtime.GetAppData(0).UserPoolId);
        Runtime.RegisterService(MakeServiceId(Runtime.GetNodeId(0)), ServiceId, 0);
        // Tablets get onto this whitelist through their bootstrapper; an actor
        // registered by hand does not, and everything it schedules for itself
        // would be dropped without a word, including the heartbeat tick.
        Runtime.EnableScheduleForActor(ServiceId);
    }

    TTestActorRuntime& GetRuntime() {
        return Runtime;
    }

    ui32 NodeId() const {
        return Runtime.GetNodeId(0);
    }

    //! The service reaches the controller only once its artifact table exists,
    //! and the initializer that reports this runs YQL, which no bare runtime
    //! can serve. Delivering its result directly is the seam that lets the
    //! orchestration code run without a database behind it.
    void InitializeService() {
        SendToService(new TEvArtifactTableInitialized(
            TStringBuilder() << TestDatabase << "/.metadata/udf_store/artifacts/" << TestCpuSpec));
        Settle();
    }

    void SendToService(NActors::IEventBase* event) {
        Runtime.Send(new IEventHandle(ServiceId, Sender, event), 0, true);
    }

    //! Hands the service an assignment the way the controller would, without
    //! having to drive the controller into producing exactly this one.
    void SendAssignment(
        const TString& name,
        const TString& uid,
        ui64 assignmentId,
        const TString& cpuSpec = TestCpuSpec,
        bool isLibrary = false,
        TMaybe<ui64> generation = Nothing())
    {
        auto ev = std::make_unique<TEvCompileController::TEvAssignCompile>();
        auto& record = ev->Record;
        auto& key = *record.MutableKey();
        key.SetName(name);
        key.SetKind(isLibrary
            ? NKikimrUdfStore::ARTIFACT_KIND_LIBRARY
            : NKikimrUdfStore::ARTIFACT_KIND_MODULE);
        key.SetUid(uid);
        key.SetCpuSpec(cpuSpec);
        record.SetAssignmentId(assignmentId);
        record.SetManifest("{}");
        // By default speak for the leader the service actually registered with,
        // so that only the tests about generations have to think about them.
        record.SetControllerGeneration(generation.GetOrElse(SeenGeneration));
        SendToService(ev.release());
        Settle();
    }

    //! Delivers a snapshot the way the metadata provider would, which is what
    //! makes the service decide whether a module needs compiling at all.
    void SendSnapshot(const TVector<TModuleDesc>& modules) {
        SendToService(new NMetadata::NProvider::TEvRefreshSubscriberData(MakeSnapshot(modules)));
        Settle();
    }

    //! Stands in for the compile actor's local reply. The actor itself cannot
    //! run here: every step of it goes through YQL.
    void ReportCompileResult(const TString& name, bool success, bool deferred = false) {
        SendToService(new TEvWasmCompileResponse(
            success, name, success ? TString() : "boom", deferred));
        Settle();
    }

    void Settle(TDuration duration = SettleTime) {
        Runtime.SimulateSleep(duration);
    }

    ui32 NavigateCount() const {
        return SeenNavigates;
    }

    const TVector<NKikimrUdfStore::TEvRegister>& Registers() const {
        return SeenRegisters;
    }

    //! The generation the real controller announced when the service registered.
    ui64 Generation() const {
        return SeenGeneration;
    }

    const TVector<NKikimrUdfStore::TEvHeartbeat>& Heartbeats() const {
        return SeenHeartbeats;
    }

    const TVector<NKikimrUdfStore::TEvCompileDone>& Done() const {
        return SeenDone;
    }

    const TVector<NKikimrUdfStore::TEvCompileFailed>& Failed() const {
        return SeenFailed;
    }

    const TVector<NKikimrUdfStore::TEvNeedArtifact>& NeedArtifacts() const {
        return SeenNeedArtifacts;
    }

private:
    void Observe(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            case TEvTxProxySchemeCache::TEvNavigateKeySet::EventType: {
                // The store initializer navigates too, for the metadata tables
                // it creates. Only a lookup of the database itself, which is a
                // single-component path, is the discovery under test.
                const auto* request = ev->Get<TEvTxProxySchemeCache::TEvNavigateKeySet>()->Request.Get();
                if (request && request->ResultSet.size() == 1
                    && request->ResultSet.front().Path.size() == 1)
                {
                    ++SeenNavigates;
                }
                break;
            }
            case TEvCompileController::TEvRegister::EventType:
                SeenRegisters.push_back(ev->Get<TEvCompileController::TEvRegister>()->Record);
                break;
            case TEvCompileController::TEvRegisterResult::EventType:
                SeenGeneration = ev->Get<TEvCompileController::TEvRegisterResult>()
                    ->Record.GetControllerGeneration();
                break;
            case TEvCompileController::TEvHeartbeat::EventType:
                SeenHeartbeats.push_back(ev->Get<TEvCompileController::TEvHeartbeat>()->Record);
                break;
            case TEvCompileController::TEvCompileDone::EventType:
                SeenDone.push_back(ev->Get<TEvCompileController::TEvCompileDone>()->Record);
                break;
            case TEvCompileController::TEvCompileFailed::EventType:
                SeenFailed.push_back(ev->Get<TEvCompileController::TEvCompileFailed>()->Record);
                break;
            case TEvCompileController::TEvNeedArtifact::EventType:
                SeenNeedArtifacts.push_back(ev->Get<TEvCompileController::TEvNeedArtifact>()->Record);
                break;
            default:
                break;
        }
    }

    TTestBasicRuntime Runtime;
    TActorId Sender;
    TActorId ServiceId;

    ui32 SeenNavigates = 0;
    ui64 SeenGeneration = 0;
    TVector<NKikimrUdfStore::TEvRegister> SeenRegisters;
    TVector<NKikimrUdfStore::TEvHeartbeat> SeenHeartbeats;
    TVector<NKikimrUdfStore::TEvCompileDone> SeenDone;
    TVector<NKikimrUdfStore::TEvCompileFailed> SeenFailed;
    TVector<NKikimrUdfStore::TEvNeedArtifact> SeenNeedArtifacts;
};

} // namespace

Y_UNIT_TEST_SUITE(WasmCompileControllerClient) {

    Y_UNIT_TEST(DiscoveryRegistersWithController) {
        TDinodeEnv env;
        env.InitializeService();

        UNIT_ASSERT_C(env.NavigateCount() > 0, "discovery never asked the scheme cache");
        UNIT_ASSERT_C(!env.Registers().empty(), "the node never registered with the controller");

        const auto& record = env.Registers().front();
        UNIT_ASSERT_VALUES_EQUAL(record.GetCpuSpec(), TestCpuSpec);
        UNIT_ASSERT_VALUES_EQUAL(record.GetNodeId(), env.NodeId());
    }

    Y_UNIT_TEST(FlagOffNeverContactsController) {
        TDinodeEnv env({.EnableController = false});
        env.InitializeService();
        env.Settle(TickTime);

        UNIT_ASSERT_VALUES_EQUAL_C(env.NavigateCount(), 0,
            "the compile controller was resolved with the feature flag off");
        UNIT_ASSERT_VALUES_EQUAL_C(env.Registers().size(), 0,
            "the node registered with the controller with the feature flag off");
        UNIT_ASSERT_VALUES_EQUAL_C(env.Heartbeats().size(), 0,
            "the controller tick is running with the feature flag off");
    }

    Y_UNIT_TEST(DatabaseWithoutControllerIsTolerated) {
        // An unmigrated tenant: the params carry no controller id at all.
        TDinodeEnv env({.ControllerTabletId = 0});
        env.InitializeService();
        env.Settle(TickTime);

        UNIT_ASSERT_C(env.NavigateCount() > 0, "discovery never asked the scheme cache");
        UNIT_ASSERT_VALUES_EQUAL_C(env.Registers().size(), 0,
            "the node registered with a controller the database does not have");
    }

    Y_UNIT_TEST(SnapshotAsksTheControllerInsteadOfCompiling) {
        TDinodeEnv env;
        env.InitializeService();

        env.SendSnapshot({{.Name = "m1", .Uid = "u1"}});

        UNIT_ASSERT_VALUES_EQUAL_C(env.NeedArtifacts().size(), 1,
            "the node did not report the gap to the controller");
        const auto& key = env.NeedArtifacts().front().GetKey();
        UNIT_ASSERT_VALUES_EQUAL(key.GetName(), "m1");
        UNIT_ASSERT_VALUES_EQUAL(key.GetUid(), "u1");
        UNIT_ASSERT_VALUES_EQUAL(key.GetCpuSpec(), TestCpuSpec);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(key.GetKind()), static_cast<int>(NKikimrUdfStore::ARTIFACT_KIND_MODULE));
    }

    Y_UNIT_TEST(FlagOffKeepsTheSnapshotPath) {
        TDinodeEnv env({.EnableController = false});
        env.InitializeService();

        env.SendSnapshot({{.Name = "m1", .Uid = "u1"}});

        UNIT_ASSERT_VALUES_EQUAL_C(env.NeedArtifacts().size(), 0,
            "the node reported a gap to the controller with the feature flag off");
    }

    Y_UNIT_TEST(AssignmentIsClaimedInHeartbeat) {
        TDinodeEnv env;
        env.InitializeService();
        const size_t beforeAssign = env.Heartbeats().size();
        UNIT_ASSERT_VALUES_EQUAL_C(beforeAssign, 1, "registering did not produce the first heartbeat");

        env.SendAssignment("m1", "u1", /*assignmentId=*/42);
        env.Settle(TickTime);

        UNIT_ASSERT_C(env.Heartbeats().size() > beforeAssign, "no heartbeat after the assignment");
        const auto& record = env.Heartbeats().back();
        UNIT_ASSERT_VALUES_EQUAL_C(record.ActiveAssignmentIdsSize(), 1,
            "the node did not claim the assignment it was given");
        UNIT_ASSERT_VALUES_EQUAL(record.GetActiveAssignmentIds(0), 42);
    }

    Y_UNIT_TEST(ForeignCpuSpecAssignmentIsRejected) {
        TDinodeEnv env;
        env.InitializeService();

        env.SendAssignment("m1", "u1", /*assignmentId=*/42, ForeignCpuSpec);
        env.Settle(TickTime);

        const auto& record = env.Heartbeats().back();
        UNIT_ASSERT_VALUES_EQUAL_C(record.ActiveAssignmentIdsSize(), 0,
            "the node accepted an assignment for object code it cannot produce");
    }

    Y_UNIT_TEST(AssignmentFromAPreviousLeaderIsRejected) {
        TDinodeEnv env;
        env.InitializeService();
        UNIT_ASSERT_C(env.Generation() >= 1, "the controller announced no generation to compare against");

        env.SendAssignment("m1", "u1", /*assignmentId=*/42, TestCpuSpec, /*isLibrary=*/false,
            env.Generation() - 1);
        env.Settle(TickTime);

        UNIT_ASSERT_VALUES_EQUAL_C(env.Heartbeats().back().ActiveAssignmentIdsSize(), 0,
            "the node took an exclusive right from a leader that no longer holds it");
    }

    Y_UNIT_TEST(AssignmentFromANewerLeaderIsAccepted) {
        TDinodeEnv env;
        env.InitializeService();

        // A tablet move the node has not been told about yet: the registration
        // reply of the new leader is still in flight, but its assignment is
        // already the current truth.
        env.SendAssignment("m1", "u1", /*assignmentId=*/42, TestCpuSpec, /*isLibrary=*/false,
            env.Generation() + 1);
        env.Settle(TickTime);

        UNIT_ASSERT_VALUES_EQUAL_C(env.Heartbeats().back().ActiveAssignmentIdsSize(), 1,
            "the node ignored the leader that had actually taken over");
        UNIT_ASSERT_VALUES_EQUAL(env.Heartbeats().back().GetActiveAssignmentIds(0), 42);
    }

    Y_UNIT_TEST(SuccessIsReportedAsDone) {
        TDinodeEnv env;
        env.InitializeService();
        env.SendAssignment("m1", "u1", /*assignmentId=*/42);

        env.ReportCompileResult("m1", /*success=*/true);

        UNIT_ASSERT_VALUES_EQUAL_C(env.Done().size(), 1, "the controller was never told the compile finished");
        UNIT_ASSERT_VALUES_EQUAL(env.Done().front().GetAssignmentId(), 42);
        UNIT_ASSERT_VALUES_EQUAL(env.Done().front().GetKey().GetName(), "m1");
        UNIT_ASSERT_VALUES_EQUAL(env.Done().front().GetKey().GetUid(), "u1");
        UNIT_ASSERT_VALUES_EQUAL(env.Failed().size(), 0);

        // The slot has to be given back, otherwise the node would keep claiming
        // an assignment it has already finished.
        env.Settle(TickTime);
        UNIT_ASSERT_VALUES_EQUAL(env.Heartbeats().back().ActiveAssignmentIdsSize(), 0);
    }

    Y_UNIT_TEST(FailureIsReportedAsFailed) {
        TDinodeEnv env;
        env.InitializeService();
        env.SendAssignment("m1", "u1", /*assignmentId=*/42);

        env.ReportCompileResult("m1", /*success=*/false);

        UNIT_ASSERT_VALUES_EQUAL(env.Failed().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Failed().front().GetAssignmentId(), 42);
        UNIT_ASSERT_C(!env.Failed().front().GetStale(),
            "a real compile failure was reported as a lost uid race");
        UNIT_ASSERT_VALUES_EQUAL(env.Done().size(), 0);
    }

    Y_UNIT_TEST(DeferredResultIsReportedAsStale) {
        TDinodeEnv env;
        env.InitializeService();
        env.SendAssignment("m1", "u1", /*assignmentId=*/42);

        env.ReportCompileResult("m1", /*success=*/false, /*deferred=*/true);

        UNIT_ASSERT_VALUES_EQUAL(env.Failed().size(), 1);
        UNIT_ASSERT_C(env.Failed().front().GetStale(),
            "losing to a re-upload was charged to the module's retry budget");
    }
}

} // namespace NKikimr::NUdfStore
