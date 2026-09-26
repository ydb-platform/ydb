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

    TActorId RegisterOrchestrator(const TBlobCheckerGroupStatus& status, TActorId bscActorId,
            TDuration periodicity = TDuration::Seconds(1)) {
        std::unordered_map<TGroupId, TString> groups{
            {BlobCheckerGroupId, status.SerializeProto()},
        };
        if (!Counters) {
            Counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        }
        return Env->Runtime->Register(CreateBlobCheckerOrchestratorActor(
                bscActorId, std::move(groups), periodicity, Counters),
                TActorId(), 0, std::nullopt, 1);
    }

    ui64 GetBlobCheckerCounter(const TString& name) const {
        return Counters->GetSubgroup("subsystem", "blob_checker")
                ->GetCounter(name, false)->Val();
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
    ::NMonitoring::TDynamicCounterPtr Counters;
};

void SendCheckResult(TActorTestEnv& env, TActorId workerId, const TLogoBlobID& blobId,
        TEvBlobStorage::TEvCheckIntegrityResult::EPlacementStatus placementStatus =
                TEvBlobStorage::TEvCheckIntegrityResult::PS_OK,
        TEvBlobStorage::TEvCheckIntegrityResult::EDataStatus dataStatus =
                TEvBlobStorage::TEvCheckIntegrityResult::DS_OK,
        NKikimrProto::EReplyStatus status = NKikimrProto::OK) {
    auto result = new TEvBlobStorage::TEvCheckIntegrityResult(status);
    result->Id = blobId;
    result->PlacementStatus = placementStatus;
    result->DataStatus = dataStatus;
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

    Y_UNIT_TEST(WorkerReportsAssimilationError) {
        TActorTestEnv env;
        const TActorId orchestratorId = env.Env->Runtime->AllocateEdgeActor(1);
        const TActorId workerId = env.RegisterWorker(orchestratorId);

        env.Wait<TEvBlobStorage::TEvAssimilate>(env.ProxyId);
        env.Send(workerId, env.ProxyId,
                new TEvBlobStorage::TEvAssimilateResult(NKikimrProto::ERROR));

        auto finish = env.Wait<TEvBlobCheckerFinishQuantum>(orchestratorId);
        const auto* result = finish->Get<TEvBlobCheckerFinishQuantum>();
        UNIT_ASSERT_VALUES_EQUAL(result->GroupId, BlobCheckerGroupId);
        UNIT_ASSERT(result->QuantumStatus == EBlobCheckerWorkerQuantumStatus::Error);
        UNIT_ASSERT_VALUES_EQUAL(result->MaxCheckedBlob, TLogoBlobID{});
        UNIT_ASSERT_VALUES_EQUAL(result->UnknownDataStatusCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->UnknownPlacementStatusCount, 0);
        UNIT_ASSERT(result->BlobsWithDataIssues.empty());
        UNIT_ASSERT_VALUES_EQUAL(result->PlacementIssuesCount, 0);
    }

    Y_UNIT_TEST(WorkerReportsIntegrityErrorWithoutAdvancingCursor) {
        TActorTestEnv env;
        const TActorId orchestratorId = env.Env->Runtime->AllocateEdgeActor(1);
        const TLogoBlobID cursor = MakeBlobId(10);
        const TLogoBlobID blob = MakeBlobId(11);
        const TActorId workerId = env.RegisterWorker(orchestratorId, cursor);

        env.Wait<TEvBlobStorage::TEvAssimilate>(env.ProxyId);
        auto page = new TEvBlobStorage::TEvAssimilateResult(NKikimrProto::OK);
        page->Blobs.emplace_back().Id = blob;
        env.Send(workerId, env.ProxyId, page);

        auto check = env.Wait<TEvBlobStorage::TEvCheckIntegrity>(env.ProxyId);
        UNIT_ASSERT_VALUES_EQUAL(check->Get<TEvBlobStorage::TEvCheckIntegrity>()->Id, blob);
        SendCheckResult(env, workerId, blob,
                TEvBlobStorage::TEvCheckIntegrityResult::PS_OK,
                TEvBlobStorage::TEvCheckIntegrityResult::DS_OK,
                NKikimrProto::ERROR);

        auto finish = env.Wait<TEvBlobCheckerFinishQuantum>(orchestratorId);
        const auto* result = finish->Get<TEvBlobCheckerFinishQuantum>();
        UNIT_ASSERT(result->QuantumStatus == EBlobCheckerWorkerQuantumStatus::Error);
        UNIT_ASSERT_VALUES_EQUAL(result->MaxCheckedBlob, cursor);
        UNIT_ASSERT_VALUES_EQUAL(result->UnknownDataStatusCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->UnknownPlacementStatusCount, 0);
        UNIT_ASSERT(result->BlobsWithDataIssues.empty());
        UNIT_ASSERT_VALUES_EQUAL(result->PlacementIssuesCount, 0);
    }

    Y_UNIT_TEST(WorkerAggregatesIntegrityStatuses) {
        TActorTestEnv env;
        const TActorId orchestratorId = env.Env->Runtime->AllocateEdgeActor(1);
        const TActorId workerId = env.RegisterWorker(orchestratorId);

        env.Wait<TEvBlobStorage::TEvAssimilate>(env.ProxyId);
        const TLogoBlobID unknownBlob = MakeBlobId(1);
        const TLogoBlobID replicatingBlob = MakeBlobId(2);
        const TLogoBlobID lostBlob = MakeBlobId(3);
        const TLogoBlobID recoverableBlob = MakeBlobId(4);
        auto page = new TEvBlobStorage::TEvAssimilateResult(NKikimrProto::OK);
        page->Blobs.emplace_back().Id = unknownBlob;
        page->Blobs.emplace_back().Id = replicatingBlob;
        page->Blobs.emplace_back().Id = lostBlob;
        page->Blobs.emplace_back().Id = recoverableBlob;
        env.Send(workerId, env.ProxyId, page);

        env.Wait<TEvBlobStorage::TEvCheckIntegrity>(env.ProxyId);
        SendCheckResult(env, workerId, unknownBlob,
                TEvBlobStorage::TEvCheckIntegrityResult::PS_UNKNOWN,
                TEvBlobStorage::TEvCheckIntegrityResult::DS_UNKNOWN);
        env.Wait<TEvBlobStorage::TEvCheckIntegrity>(env.ProxyId);
        SendCheckResult(env, workerId, replicatingBlob,
                TEvBlobStorage::TEvCheckIntegrityResult::PS_REPLICATION_IN_PROGRESS,
                TEvBlobStorage::TEvCheckIntegrityResult::DS_OK);
        env.Wait<TEvBlobStorage::TEvCheckIntegrity>(env.ProxyId);
        SendCheckResult(env, workerId, lostBlob,
                TEvBlobStorage::TEvCheckIntegrityResult::PS_BLOB_IS_LOST,
                TEvBlobStorage::TEvCheckIntegrityResult::DS_ERROR);
        env.Wait<TEvBlobStorage::TEvCheckIntegrity>(env.ProxyId);
        SendCheckResult(env, workerId, recoverableBlob,
                TEvBlobStorage::TEvCheckIntegrityResult::PS_BLOB_IS_RECOVERABLE,
                TEvBlobStorage::TEvCheckIntegrityResult::DS_ERROR);

        env.Wait<TEvBlobStorage::TEvAssimilate>(env.ProxyId);
        env.Send(workerId, env.ProxyId,
                new TEvBlobStorage::TEvAssimilateResult(NKikimrProto::OK));

        auto finish = env.Wait<TEvBlobCheckerFinishQuantum>(orchestratorId);
        const auto* result = finish->Get<TEvBlobCheckerFinishQuantum>();
        UNIT_ASSERT(result->QuantumStatus == EBlobCheckerWorkerQuantumStatus::FinishOk);
        UNIT_ASSERT_VALUES_EQUAL(result->MaxCheckedBlob, recoverableBlob);
        UNIT_ASSERT_VALUES_EQUAL(result->UnknownDataStatusCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->UnknownPlacementStatusCount, 2);
        UNIT_ASSERT_VALUES_EQUAL(result->BlobsWithDataIssues.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(result->BlobsWithDataIssues[0], lostBlob);
        UNIT_ASSERT_VALUES_EQUAL(result->BlobsWithDataIssues[1], recoverableBlob);
        UNIT_ASSERT_VALUES_EQUAL(result->PlacementIssuesCount, 2);
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
        result->PlacementStatus = TEvBlobStorage::TEvCheckIntegrityResult::PS_UNKNOWN;
        result->DataStatus = TEvBlobStorage::TEvCheckIntegrityResult::DS_ERROR;
        env.Env->Runtime->Schedule(TDuration::Minutes(31),
                new IEventHandle(workerId, env.ProxyId, result), nullptr, 1);

        auto checkpoint = env.Wait<TEvBlobCheckerFinishQuantum>(orchestratorId);
        const auto* checkpointResult = checkpoint->Get<TEvBlobCheckerFinishQuantum>();
        UNIT_ASSERT(checkpointResult->QuantumStatus ==
                EBlobCheckerWorkerQuantumStatus::IntermediateOk);
        UNIT_ASSERT_VALUES_EQUAL(checkpointResult->MaxCheckedBlob, firstBlob);
        UNIT_ASSERT_VALUES_EQUAL(checkpointResult->UnknownDataStatusCount, 0);
        UNIT_ASSERT_VALUES_EQUAL(checkpointResult->UnknownPlacementStatusCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(checkpointResult->BlobsWithDataIssues.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(checkpointResult->BlobsWithDataIssues[0], firstBlob);
        UNIT_ASSERT_VALUES_EQUAL(checkpointResult->PlacementIssuesCount, 0);

        auto nextCheck = env.Wait<TEvBlobStorage::TEvCheckIntegrity>(env.ProxyId);
        UNIT_ASSERT_VALUES_EQUAL(nextCheck->Get<TEvBlobStorage::TEvCheckIntegrity>()->Id, secondBlob);
        SendCheckResult(env, workerId, secondBlob,
                TEvBlobStorage::TEvCheckIntegrityResult::PS_BLOB_IS_RECOVERABLE,
                TEvBlobStorage::TEvCheckIntegrityResult::DS_UNKNOWN);

        env.Wait<TEvBlobStorage::TEvAssimilate>(env.ProxyId);
        env.Send(workerId, env.ProxyId,
                new TEvBlobStorage::TEvAssimilateResult(NKikimrProto::OK));
        auto finish = env.Wait<TEvBlobCheckerFinishQuantum>(orchestratorId);
        const auto* finishResult = finish->Get<TEvBlobCheckerFinishQuantum>();
        UNIT_ASSERT(finishResult->QuantumStatus == EBlobCheckerWorkerQuantumStatus::FinishOk);
        UNIT_ASSERT_VALUES_EQUAL(finishResult->MaxCheckedBlob, secondBlob);
        UNIT_ASSERT_VALUES_EQUAL(finishResult->UnknownDataStatusCount, 1);
        UNIT_ASSERT_VALUES_EQUAL(finishResult->UnknownPlacementStatusCount, 0);
        UNIT_ASSERT(finishResult->BlobsWithDataIssues.empty());
        UNIT_ASSERT_VALUES_EQUAL(finishResult->PlacementIssuesCount, 1);
    }

    Y_UNIT_TEST(OrchestratorResumesIncompleteScan) {
        TActorTestEnv env;
        const TActorId bscActorId = env.Env->Runtime->AllocateEdgeActor(1);
        const TLogoBlobID cursor = MakeBlobId(10);
        TBlobCheckerGroupStatus status(EBlobCheckerResultStatusFlags::DataIssues,
                TInstant::Zero(), cursor);

        auto assimilation = StartOrchestratorWorker(env, status, bscActorId);
        const auto* request = assimilation->Get<TEvBlobStorage::TEvAssimilate>();
        UNIT_ASSERT(request->SkipBlobsUpTo);
        UNIT_ASSERT_VALUES_EQUAL(*request->SkipBlobsUpTo, cursor);
    }

    Y_UNIT_TEST(OrchestratorResetsCompletedScan) {
        TActorTestEnv env;
        const TActorId bscActorId = env.Env->Runtime->AllocateEdgeActor(1);
        TBlobCheckerGroupStatus status(EBlobCheckerResultStatusFlags::ScanFinished,
                TInstant::Zero(), MakeBlobId(10));

        auto assimilation = StartOrchestratorWorker(env, status, bscActorId);
        UNIT_ASSERT(!assimilation->Get<TEvBlobStorage::TEvAssimilate>()->SkipBlobsUpTo);
    }

    Y_UNIT_TEST(OrchestratorIgnoresStaleWorkerResult) {
        TActorTestEnv env;
        const TActorId bscActorId = env.Env->Runtime->AllocateEdgeActor(1);
        const TActorId staleWorkerId = env.Env->Runtime->AllocateEdgeActor(1);
        TBlobCheckerGroupStatus status(0, TInstant::Zero(), {});

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

    Y_UNIT_TEST(OrchestratorPersistsIntermediateResultAndUpdatesCounters) {
        TActorTestEnv env;
        const TActorId bscActorId = env.Env->Runtime->AllocateEdgeActor(1);
        const TInstant lastFinished = TInstant::Zero();
        TBlobCheckerGroupStatus status(0, lastFinished, {});

        TActorId orchestratorId;
        auto assimilation = StartOrchestratorWorker(env, status, bscActorId, &orchestratorId);
        const TActorId workerId = assimilation->Sender;
        const TLogoBlobID cursor = MakeBlobId(20);
        const TLogoBlobID dataIssue = MakeBlobId(19);
        env.Send(orchestratorId, workerId,
                new TEvBlobCheckerFinishQuantum(BlobCheckerGroupId,
                        EBlobCheckerWorkerQuantumStatus::IntermediateOk, cursor,
                        3, 4, {dataIssue}, 2));

        auto update = env.Wait<TEvBlobCheckerUpdateGroupStatus>(bscActorId);
        const auto* result = update->Get<TEvBlobCheckerUpdateGroupStatus>();
        UNIT_ASSERT_VALUES_EQUAL(result->GroupId, BlobCheckerGroupId);
        UNIT_ASSERT(!result->FinishScan);
        const auto restored = TBlobCheckerGroupStatus::Deserialize(result->SerializedState);
        UNIT_ASSERT_VALUES_EQUAL(restored.ShortStatus,
                EBlobCheckerResultStatusFlags::DataIssues |
                EBlobCheckerResultStatusFlags::PlacementIssues);
        UNIT_ASSERT_VALUES_EQUAL(restored.LastScanFinishedTimestamp, lastFinished);
        UNIT_ASSERT_VALUES_EQUAL(restored.MaxCheckedBlob, cursor);

        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("DataIssues"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("PlacementIssues"), 2);
        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("WorkersCreated"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("WorkersTerminated"), 0);
        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("ChecksCompleted"), 0);
    }

    Y_UNIT_TEST(OrchestratorPersistsSuccessfulResultAndUpdatesCounters) {
        TActorTestEnv env;
        const TActorId bscActorId = env.Env->Runtime->AllocateEdgeActor(1);
        TBlobCheckerGroupStatus status(0, TInstant::Zero(), {});

        TActorId orchestratorId;
        auto assimilation = StartOrchestratorWorker(env, status, bscActorId, &orchestratorId);
        const TActorId workerId = assimilation->Sender;
        const TInstant finishTime = env.Env->Runtime->GetClock();
        const TLogoBlobID cursor = MakeBlobId(30);
        const TLogoBlobID dataIssue = MakeBlobId(29);
        env.Send(orchestratorId, workerId,
                new TEvBlobCheckerFinishQuantum(BlobCheckerGroupId,
                        EBlobCheckerWorkerQuantumStatus::FinishOk, cursor,
                        0, 0, {dataIssue}, 3));

        auto update = env.Wait<TEvBlobCheckerUpdateGroupStatus>(bscActorId);
        const auto* result = update->Get<TEvBlobCheckerUpdateGroupStatus>();
        UNIT_ASSERT(result->FinishScan);
        const auto restored = TBlobCheckerGroupStatus::Deserialize(result->SerializedState);
        UNIT_ASSERT_VALUES_EQUAL(restored.ShortStatus,
                EBlobCheckerResultStatusFlags::ScanFinished |
                EBlobCheckerResultStatusFlags::DataIssues |
                EBlobCheckerResultStatusFlags::PlacementIssues);
        UNIT_ASSERT_VALUES_EQUAL(restored.LastScanFinishedTimestamp, finishTime);
        UNIT_ASSERT_VALUES_EQUAL(restored.MaxCheckedBlob, cursor);

        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("DataIssues"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("PlacementIssues"), 3);
        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("WorkersCreated"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("WorkersTerminated"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("ChecksCompleted"), 1);
    }

    Y_UNIT_TEST(OrchestratorErrorKeepsProgressWithoutMarkingScanFinished) {
        TActorTestEnv env;
        const TActorId bscActorId = env.Env->Runtime->AllocateEdgeActor(1);
        const TInstant lastFinished = TInstant::Zero();
        const TLogoBlobID cursor = MakeBlobId(10);
        TBlobCheckerGroupStatus status(EBlobCheckerResultStatusFlags::DataIssues,
                lastFinished, cursor);

        TActorId orchestratorId;
        auto assimilation = StartOrchestratorWorker(env, status, bscActorId, &orchestratorId);
        const TActorId workerId = assimilation->Sender;
        const TInstant errorReportedAt = env.Env->Runtime->GetClock();
        env.Send(orchestratorId, workerId,
                new TEvBlobCheckerFinishQuantum(BlobCheckerGroupId,
                        EBlobCheckerWorkerQuantumStatus::Error, MakeBlobId(100),
                        0, 0, {}, 2));

        auto update = env.Wait<TEvBlobCheckerUpdateGroupStatus>(bscActorId);
        const auto* result = update->Get<TEvBlobCheckerUpdateGroupStatus>();
        UNIT_ASSERT(result->FinishScan);
        const auto restored = TBlobCheckerGroupStatus::Deserialize(result->SerializedState);
        UNIT_ASSERT_VALUES_EQUAL(restored.ShortStatus,
                EBlobCheckerResultStatusFlags::DataIssues |
                EBlobCheckerResultStatusFlags::PlacementIssues);
        UNIT_ASSERT_VALUES_EQUAL(restored.LastScanFinishedTimestamp, lastFinished);
        UNIT_ASSERT_VALUES_EQUAL(restored.MaxCheckedBlob, cursor);

        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("DataIssues"), 0);
        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("PlacementIssues"), 2);
        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("WorkersCreated"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("WorkersTerminated"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("ChecksCompleted"), 0);

        auto retry = env.Wait<TEvBlobCheckerPlanCheck>(bscActorId);
        UNIT_ASSERT_VALUES_EQUAL(retry->Get<TEvBlobCheckerPlanCheck>()->GroupId,
                BlobCheckerGroupId);
        UNIT_ASSERT(env.Env->Runtime->GetClock() >= errorReportedAt + TDuration::Minutes(1));
    }

    Y_UNIT_TEST(OrchestratorRetriesRejectedPlanAfterInitialDelay) {
        TActorTestEnv env;
        const TActorId bscActorId = env.Env->Runtime->AllocateEdgeActor(1);
        TBlobCheckerGroupStatus status(0, TInstant::Zero(), {});

        const TActorId orchestratorId = env.RegisterOrchestrator(status, bscActorId);
        env.Wait<TEvBlobCheckerPlanCheck>(bscActorId);
        const TInstant retryScheduledAt = env.Env->Runtime->GetClock();
        env.Send(orchestratorId, bscActorId,
                new TEvBlobCheckerDecision(BlobCheckerGroupId, NKikimrProto::ERROR));

        auto retry = env.Wait<TEvBlobCheckerPlanCheck>(bscActorId);
        UNIT_ASSERT_VALUES_EQUAL(retry->Get<TEvBlobCheckerPlanCheck>()->GroupId,
                BlobCheckerGroupId);
        UNIT_ASSERT(env.Env->Runtime->GetClock() >=
                retryScheduledAt + TDuration::Minutes(1));
        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("WorkersCreated"), 0);
        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("WorkersTerminated"), 0);
        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("ChecksCompleted"), 0);
    }

    Y_UNIT_TEST(OrchestratorDisableAcknowledgesPendingRequestAndCanReenable) {
        TActorTestEnv env;
        const TActorId bscActorId = env.Env->Runtime->AllocateEdgeActor(1);
        TBlobCheckerGroupStatus status(0, TInstant::Zero(), {});

        const TActorId orchestratorId = env.RegisterOrchestrator(status, bscActorId);
        env.Wait<TEvBlobCheckerPlanCheck>(bscActorId);
        env.Send(orchestratorId, bscActorId,
                new TEvBlobCheckerUpdateSettings(TDuration::Zero()));

        auto cancelled = env.Wait<TEvBlobCheckerUpdateGroupStatus>(bscActorId);
        UNIT_ASSERT(cancelled->Get<TEvBlobCheckerUpdateGroupStatus>()->FinishScan);
        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("WorkersCreated"), 0);

        env.Send(orchestratorId, bscActorId,
                new TEvBlobCheckerUpdateSettings(TDuration::Seconds(1)));
        auto nextPlan = env.Wait<TEvBlobCheckerPlanCheck>(bscActorId);
        UNIT_ASSERT_VALUES_EQUAL(nextPlan->Get<TEvBlobCheckerPlanCheck>()->GroupId,
                BlobCheckerGroupId);
    }

    Y_UNIT_TEST(OrchestratorActiveDeleteWaitsForWorkerTerminalAck) {
        TActorTestEnv env;
        const TActorId bscActorId = env.Env->Runtime->AllocateEdgeActor(1);
        TBlobCheckerGroupStatus status(0, TInstant::Zero(), {});

        TActorId orchestratorId;
        auto assimilation = StartOrchestratorWorker(env, status, bscActorId, &orchestratorId);
        const TActorId workerId = assimilation->Sender;

        bool allowWorkerPoison = false;
        env.Env->Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (!allowWorkerPoison && ev->GetTypeRewrite() == TEvents::TEvPoisonPill::EventType &&
                    ev->Recipient == workerId) {
                return false;
            }
            return true;
        };

        env.Send(orchestratorId, bscActorId,
                new TEvBlobCheckerUpdateGroupSet({}, {BlobCheckerGroupId}));
        env.Env->Sim(TDuration::Zero());
        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("WorkersCreated"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("WorkersTerminated"), 0);

        allowWorkerPoison = true;
        env.Send(workerId, orchestratorId, new TEvents::TEvPoisonPill);
        auto cancelled = env.Wait<TEvBlobCheckerUpdateGroupStatus>(bscActorId);
        UNIT_ASSERT(cancelled->Get<TEvBlobCheckerUpdateGroupStatus>()->FinishScan);
        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("WorkersTerminated"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("ChecksCompleted"), 0);
    }

    Y_UNIT_TEST(OrchestratorActiveCancellationWaitsForAckThenRetries) {
        TActorTestEnv env;
        const TActorId bscActorId = env.Env->Runtime->AllocateEdgeActor(1);
        TBlobCheckerGroupStatus status(0, TInstant::Zero(), {});

        TActorId orchestratorId;
        auto assimilation = StartOrchestratorWorker(env, status, bscActorId, &orchestratorId);
        const TActorId workerId = assimilation->Sender;

        bool allowWorkerPoison = false;
        env.Env->Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (!allowWorkerPoison && ev->GetTypeRewrite() == TEvents::TEvPoisonPill::EventType &&
                    ev->Recipient == workerId) {
                return false;
            }
            return true;
        };

        const TInstant cancellationStartedAt = env.Env->Runtime->GetClock();
        env.Send(orchestratorId, bscActorId,
                new TEvBlobCheckerDecision(BlobCheckerGroupId, NKikimrProto::ERROR));
        env.Env->Sim(TDuration::Zero());
        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("WorkersTerminated"), 0);

        allowWorkerPoison = true;
        env.Send(workerId, orchestratorId, new TEvents::TEvPoisonPill);
        auto cancelled = env.Wait<TEvBlobCheckerUpdateGroupStatus>(bscActorId);
        const auto* result = cancelled->Get<TEvBlobCheckerUpdateGroupStatus>();
        UNIT_ASSERT(result->FinishScan);
        const auto restored = TBlobCheckerGroupStatus::Deserialize(result->SerializedState);
        UNIT_ASSERT(!(restored.ShortStatus & EBlobCheckerResultStatusFlags::ScanFinished));
        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("WorkersTerminated"), 1);

        auto retry = env.Wait<TEvBlobCheckerPlanCheck>(bscActorId);
        UNIT_ASSERT_VALUES_EQUAL(retry->Get<TEvBlobCheckerPlanCheck>()->GroupId,
                BlobCheckerGroupId);
        UNIT_ASSERT(env.Env->Runtime->GetClock() >=
                cancellationStartedAt + TDuration::Minutes(1));
    }

    Y_UNIT_TEST(OrchestratorStaleAcceptedDecisionReleasesPlannerLock) {
        TActorTestEnv env;
        const TActorId bscActorId = env.Env->Runtime->AllocateEdgeActor(1);
        TBlobCheckerGroupStatus status(0, TInstant::Zero(), {});

        const TActorId orchestratorId = env.RegisterOrchestrator(status, bscActorId);
        env.Wait<TEvBlobCheckerPlanCheck>(bscActorId);
        env.Send(orchestratorId, bscActorId,
                new TEvBlobCheckerUpdateSettings(TDuration::Zero()));
        env.Wait<TEvBlobCheckerUpdateGroupStatus>(bscActorId);

        env.Send(orchestratorId, bscActorId,
                new TEvBlobCheckerDecision(BlobCheckerGroupId, NKikimrProto::OK));
        auto staleAck = env.Wait<TEvBlobCheckerUpdateGroupStatus>(bscActorId);
        UNIT_ASSERT(staleAck->Get<TEvBlobCheckerUpdateGroupStatus>()->FinishScan);
        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("WorkersCreated"), 0);
        UNIT_ASSERT_VALUES_EQUAL(env.GetBlobCheckerCounter("WorkersTerminated"), 0);
    }
}

} // namespace NBsController
} // namespace NKikimr
