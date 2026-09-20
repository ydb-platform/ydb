#include <ydb/core/mind/bscontroller/blob_checker_actors.h>
#include <ydb/core/mind/bscontroller/blob_checker_events.h>

#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NBsController {
namespace {

using namespace NActors;

constexpr ui32 GroupRawId = 0x80000001;
const TGroupId BlobCheckerGroupId = TGroupId::FromValue(GroupRawId);

TLogoBlobID MakeBlobId(ui32 step) {
    return TLogoBlobID(1, 1, step, 0, 100, 0);
}

class TActorTestEnv : public TTestCtxBase {
public:
    TActorTestEnv()
        : TTestCtxBase(TEnvironmentSetup::TSettings{
            .NodeCount = 1,
        })
    {
        AllocateEdgeActorOnSpecificNode(1);
        ProxyId = Edge;
        Env->Runtime->RegisterService(MakeBlobStorageProxyID(BlobCheckerGroupId), ProxyId);
    }

    TActorId RegisterWorker(TActorId orchestratorId, TLogoBlobID maxCheckedBlob = {}) {
        return Env->Runtime->Register(CreateBlobCheckerWorkerActor(
                BlobCheckerGroupId, orchestratorId, maxCheckedBlob), TActorId(), 0, std::nullopt, 1);
    }

    TActorId RegisterOrchestrator(const TBlobCheckerGroupStatus& status, TActorId bscActorId) {
        std::unordered_map<TGroupId, TString> groups{
            {BlobCheckerGroupId, status.SerializeProto()},
        };
        auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        return Env->Runtime->Register(CreateBlobCheckerOrchestratorActor(
                bscActorId, std::move(groups), TDuration::Zero(), counters),
                TActorId(), 0, std::nullopt, 1);
    }

    template<typename TEvent>
    void Send(TActorId recipient, TActorId sender, TEvent* event) {
        Env->Runtime->Send(new IEventHandle(recipient, sender, event), 1);
    }

    template<typename TEvent>
    std::unique_ptr<IEventHandle> Wait(TActorId edgeActorId) {
        std::unique_ptr<IEventHandle> ev = Env->Runtime->WaitForEdgeActorEvent({edgeActorId});
        UNIT_ASSERT_VALUES_EQUAL(ev->GetTypeRewrite(), TEvent::EventType);
        return ev;
    }

public:
    TActorId ProxyId;
};

void SendCheckResult(TActorTestEnv& env, TActorId workerId, const TLogoBlobID& blobId) {
    auto result = new TEvBlobStorage::TEvCheckIntegrityResult(NKikimrProto::OK);
    result->Id = blobId;
    env.Send(workerId, env.ProxyId, result);
}

std::unique_ptr<IEventHandle> StartOrchestratorWorker(TActorTestEnv& env,
        const TBlobCheckerGroupStatus& status, TActorId bscActorId,
        TActorId* orchestratorIdOut = nullptr) {
    const TActorId orchestratorId = env.RegisterOrchestrator(status, bscActorId);
    if (orchestratorIdOut) {
        *orchestratorIdOut = orchestratorId;
    }
    auto plan = env.Wait<TEvBlobCheckerPlanCheck>(bscActorId);
    UNIT_ASSERT_VALUES_EQUAL(plan->Get<TEvBlobCheckerPlanCheck>()->GroupId, BlobCheckerGroupId);
    env.Send(orchestratorId, bscActorId,
            new TEvBlobCheckerDecision(BlobCheckerGroupId, NKikimrProto::OK));
    return env.Wait<TEvBlobStorage::TEvAssimilate>(env.ProxyId);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(BlobCheckerActors) {
    Y_UNIT_TEST(WorkerPaginatesAssimilation) {
        TActorTestEnv env;
        const TActorId orchestratorId = env.Env->Runtime->AllocateEdgeActor(1);
        const TActorId workerId = env.RegisterWorker(orchestratorId);

        auto firstRequest = env.Wait<TEvBlobStorage::TEvAssimilate>(env.ProxyId);
        const auto* request = firstRequest->Get<TEvBlobStorage::TEvAssimilate>();
        UNIT_ASSERT_VALUES_EQUAL(*request->SkipBlocksUpTo, Max<ui64>());
        UNIT_ASSERT_VALUES_EQUAL(*request->SkipBarriersUpTo,
                std::make_tuple(Max<ui64>(), Max<ui8>()));
        UNIT_ASSERT(!request->SkipBlobsUpTo);

        const TLogoBlobID firstBlob = MakeBlobId(1);
        const TLogoBlobID secondBlob = MakeBlobId(2);
        auto page = new TEvBlobStorage::TEvAssimilateResult(NKikimrProto::OK);
        page->Blobs.emplace_back().Id = secondBlob;
        page->Blobs.emplace_back().Id = firstBlob;
        env.Send(workerId, env.ProxyId, page);

        auto firstCheck = env.Wait<TEvBlobStorage::TEvCheckIntegrity>(env.ProxyId);
        UNIT_ASSERT_VALUES_EQUAL(firstCheck->Get<TEvBlobStorage::TEvCheckIntegrity>()->Id, firstBlob);
        SendCheckResult(env, workerId, firstBlob);

        auto secondCheck = env.Wait<TEvBlobStorage::TEvCheckIntegrity>(env.ProxyId);
        UNIT_ASSERT_VALUES_EQUAL(secondCheck->Get<TEvBlobStorage::TEvCheckIntegrity>()->Id, secondBlob);
        SendCheckResult(env, workerId, secondBlob);

        auto secondRequest = env.Wait<TEvBlobStorage::TEvAssimilate>(env.ProxyId);
        request = secondRequest->Get<TEvBlobStorage::TEvAssimilate>();
        UNIT_ASSERT(request->SkipBlobsUpTo);
        UNIT_ASSERT_VALUES_EQUAL(*request->SkipBlobsUpTo, secondBlob);

        env.Send(workerId, env.ProxyId,
                new TEvBlobStorage::TEvAssimilateResult(NKikimrProto::OK));
        auto finish = env.Wait<TEvBlobCheckerFinishQuantum>(orchestratorId);
        const auto* result = finish->Get<TEvBlobCheckerFinishQuantum>();
        UNIT_ASSERT(result->QuantumStatus == EBlobCheckerWorkerQuantumStatus::FinishOk);
        UNIT_ASSERT_VALUES_EQUAL(result->MaxCheckedBlob, secondBlob);
    }

    Y_UNIT_TEST(WorkerContinuesAfterQuantumCheckpoint) {
        TActorTestEnv env;
        const TActorId orchestratorId = env.Env->Runtime->AllocateEdgeActor(1);
        const TActorId workerId = env.RegisterWorker(orchestratorId);

        env.Wait<TEvBlobStorage::TEvAssimilate>(env.ProxyId);
        const TLogoBlobID firstBlob = MakeBlobId(1);
        const TLogoBlobID secondBlob = MakeBlobId(2);
        auto page = new TEvBlobStorage::TEvAssimilateResult(NKikimrProto::OK);
        page->Blobs.emplace_back().Id = firstBlob;
        page->Blobs.emplace_back().Id = secondBlob;
        env.Send(workerId, env.ProxyId, page);

        env.Wait<TEvBlobStorage::TEvCheckIntegrity>(env.ProxyId);
        auto result = new TEvBlobStorage::TEvCheckIntegrityResult(NKikimrProto::OK);
        result->Id = firstBlob;
        env.Env->Runtime->Schedule(TDuration::Minutes(31),
                new IEventHandle(workerId, env.ProxyId, result), nullptr, 1);

        auto checkpoint = env.Wait<TEvBlobCheckerFinishQuantum>(orchestratorId);
        UNIT_ASSERT(checkpoint->Get<TEvBlobCheckerFinishQuantum>()->QuantumStatus ==
                EBlobCheckerWorkerQuantumStatus::IntermediateOk);

        auto nextCheck = env.Wait<TEvBlobStorage::TEvCheckIntegrity>(env.ProxyId);
        UNIT_ASSERT_VALUES_EQUAL(nextCheck->Get<TEvBlobStorage::TEvCheckIntegrity>()->Id, secondBlob);
    }

    Y_UNIT_TEST(OrchestratorResumesIncompleteScan) {
        TActorTestEnv env;
        const TActorId bscActorId = env.Env->Runtime->AllocateEdgeActor(1);
        const TLogoBlobID cursor = MakeBlobId(10);
        TBlobCheckerGroupStatus status(EBlobCheckerResultStatusFlags::DataIssues,
                TMonotonic::Zero(), cursor);

        auto assimilation = StartOrchestratorWorker(env, status, bscActorId);
        const auto* request = assimilation->Get<TEvBlobStorage::TEvAssimilate>();
        UNIT_ASSERT(request->SkipBlobsUpTo);
        UNIT_ASSERT_VALUES_EQUAL(*request->SkipBlobsUpTo, cursor);
    }

    Y_UNIT_TEST(OrchestratorResetsCompletedScan) {
        TActorTestEnv env;
        const TActorId bscActorId = env.Env->Runtime->AllocateEdgeActor(1);
        TBlobCheckerGroupStatus status(EBlobCheckerResultStatusFlags::ScanFinished,
                TMonotonic::Zero(), MakeBlobId(10));

        auto assimilation = StartOrchestratorWorker(env, status, bscActorId);
        UNIT_ASSERT(!assimilation->Get<TEvBlobStorage::TEvAssimilate>()->SkipBlobsUpTo);
    }

    Y_UNIT_TEST(OrchestratorIgnoresStaleWorkerResult) {
        TActorTestEnv env;
        const TActorId bscActorId = env.Env->Runtime->AllocateEdgeActor(1);
        const TActorId staleWorkerId = env.Env->Runtime->AllocateEdgeActor(1);
        TBlobCheckerGroupStatus status(0, TMonotonic::Zero(), {});

        TActorId orchestratorId;
        auto assimilation = StartOrchestratorWorker(env, status, bscActorId, &orchestratorId);
        const TActorId workerId = assimilation->Sender;
        const TLogoBlobID staleCursor = MakeBlobId(100);
        env.Send(orchestratorId, staleWorkerId,
                new TEvBlobCheckerFinishQuantum(BlobCheckerGroupId,
                        EBlobCheckerWorkerQuantumStatus::FinishOk, staleCursor,
                        0, 0, {}, 0));

        env.Send(workerId, env.ProxyId,
                new TEvBlobStorage::TEvAssimilateResult(NKikimrProto::OK));

        auto update = env.Wait<TEvBlobCheckerUpdateGroupStatus>(bscActorId);
        const auto restored = TBlobCheckerGroupStatus::Deserialize(
                update->Get<TEvBlobCheckerUpdateGroupStatus>()->SerializedState);
        UNIT_ASSERT_VALUES_EQUAL(restored.MaxCheckedBlob, TLogoBlobID{});
    }
}

} // namespace NBsController
} // namespace NKikimr
