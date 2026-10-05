#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/tx/columnshard/test_helper/shard_reader.h>
#include <ydb/core/tx/columnshard/test_helper/test_combinator.h>

namespace NKikimr {
using namespace NColumnShard;
using namespace Tests;
using namespace NTxUT;
using TCommitTestController = NYDBTest::NColumnShard::TController;

namespace {
const TColumnShard* WaitForShard(TCommitTestController& controller, TTestBasicRuntime& runtime) {
    for (ui32 i = 0; controller.GetShardActualsCount() == 0 && i < 100; ++i) {
        runtime.SimulateSleep(TDuration::MilliSeconds(50));
    }
    UNIT_ASSERT_VALUES_EQUAL(controller.GetShardActualsCount(), 1);
    return controller.GetTheOnlyShard();
}

enum class ECommitMode {
    Simple,
    Primary,
    Secondary
};

enum class EProposalScenario {
    Retry,
    Interval,
    DifferentLock,
    DifferentKind,
    FreshBrokenLock,
    Deadline
};

void CheckPreparedCommitRetry(ECommitMode mode, bool reboot, EProposalScenario scenario = EProposalScenario::Retry) {
    const bool sync = mode != ECommitMode::Simple;
    const bool secondary = mode == ECommitMode::Secondary;
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);
    auto controller = NYDBTest::TControllers::RegisterCSControllerGuard<TCommitTestController>();
    TActorId sender = runtime.AllocateEdgeActor();
    TestTableDescription table{};
    Y_UNUSED(PrepareTablet(runtime, 1, table.Schema));
    constexpr ui64 lockId = 103;
    constexpr ui64 commitTxId = 20;
    constexpr ui64 peer = TTestTxConfig::TxTablet0 + 100;
    std::vector<ui64> ids;
    UNIT_ASSERT(WriteData(
        runtime, sender, 10, 1, MakeTestBlob({ 0, 100 }, table.Schema), table.Schema, true, &ids, NEvWrite::EModificationType::Upsert, lockId));
    const auto* lock = WaitForShard(*controller.operator->(), runtime)->GetOperationsManager().GetLockOptional(lockId);
    UNIT_ASSERT(lock && !lock->IsBroken());
    NKikimrDataEvents::TEvWrite request;
    request.SetTxId(commitTxId);
    request.SetTxMode(NKikimrDataEvents::TEvWrite::MODE_PREPARE);
    auto* locks = request.MutableLocks();
    locks->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
    auto* protoLock = locks->AddLocks();
    protoLock->SetLockId(lockId);
    protoLock->SetGeneration(lock->GetGeneration());
    protoLock->SetCounter(lock->GetInternalGenerationCounter());
    if (sync) {
        locks->SetArbiterColumnShard(secondary ? peer : TTestTxConfig::TxTablet0);
        locks->AddSendingShards(TTestTxConfig::TxTablet0);
        locks->AddReceivingShards(TTestTxConfig::TxTablet0);
        if (secondary) {
            locks->AddSendingShards(peer);
            locks->AddReceivingShards(peer);
        }
    }
    const auto propose = [&](const NKikimrDataEvents::TEvWrite& record, NKikimrDataEvents::TEvWriteResult::EStatus expected) {
        auto ev = std::make_unique<NEvents::TDataEvents::TEvWrite>();
        ev->Record.CopyFrom(record);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, ev.release());
        auto result = runtime.GrabEdgeEvent<NEvents::TDataEvents::TEvWriteResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetTxId(), record.GetTxId());
        UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Record.GetStatus(), expected, result->Get()->Record.DebugString());
        return result->Get()->Record;
    };
    const auto prepared = propose(request, NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
    if (reboot) {
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        const auto* recovered = WaitForShard(*controller.operator->(), runtime)->GetOperationsManager().GetLockOptional(lockId);
        UNIT_ASSERT(recovered && recovered->IsBroken());
    }
    if (scenario == EProposalScenario::FreshBrokenLock) {
        UNIT_ASSERT(reboot);
        auto fresh = request;
        fresh.SetTxId(commitTxId + 1);
        propose(fresh, NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);
    }
    if (scenario == EProposalScenario::DifferentLock) {
        auto differentLock = request;
        differentLock.MutableLocks()->MutableLocks(0)->SetLockId(lockId + 1);
        propose(differentLock, NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
    }
    if (scenario == EProposalScenario::DifferentKind) {
        auto differentKind = request;
        if (sync) {
            differentKind.MutableLocks()->ClearSendingShards();
            differentKind.MutableLocks()->ClearReceivingShards();
            differentKind.MutableLocks()->ClearArbiterColumnShard();
        } else {
            differentKind.MutableLocks()->SetArbiterColumnShard(TTestTxConfig::TxTablet0);
            differentKind.MutableLocks()->AddSendingShards(TTestTxConfig::TxTablet0);
            differentKind.MutableLocks()->AddReceivingShards(TTestTxConfig::TxTablet0);
        }
        propose(differentKind, NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
    }
    if (scenario == EProposalScenario::Retry || scenario == EProposalScenario::Interval || scenario == EProposalScenario::Deadline) {
        runtime.SimulateSleep(TDuration::Seconds(1));
        for (ui32 retry = 0; retry < 2; ++retry) {
            const auto result = propose(request, NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
            if (scenario == EProposalScenario::Interval) {
                UNIT_ASSERT_VALUES_EQUAL(result.GetMinStep(), prepared.GetMinStep());
                UNIT_ASSERT_VALUES_EQUAL(result.GetMaxStep(), prepared.GetMaxStep());
            }
        }
    }
    if (scenario == EProposalScenario::Deadline) {
        const auto expiredStep = prepared.GetMaxStep() + 1;
        const auto aux =
            ProposeSchemaTx(runtime, sender, TTestSchema::CreateTableTxBody(99, table.Standalone, table.Schema, table.Pk, {}, 1), 100);
        PlanSchemaTx(runtime, sender, { Max(aux.Val(), expiredStep), 100 });
        for (ui32 i = 0; i < 100; ++i) {
            if (!WaitForShard(*controller.operator->(), runtime)->GetProgressTxController().GetTxInfo(commitTxId, ETxOperatorStatus::Any)) {
                break;
            }
            runtime.SimulateSleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT(!WaitForShard(*controller.operator->(), runtime)->GetProgressTxController().GetTxInfo(commitTxId, ETxOperatorStatus::Any));
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        UNIT_ASSERT(!WaitForShard(*controller.operator->(), runtime)->GetProgressTxController().GetTxInfo(commitTxId, ETxOperatorStatus::Any));
        TShardReader reader(runtime, TTestTxConfig::TxTablet0, 1, { Max(aux.Val(), expiredStep), 100 });
        reader.SetReplyColumnIds(TTestSchema::ExtractIds(table.Schema));
        auto rows = reader.ReadAll();
        UNIT_ASSERT(!reader.IsError());
        UNIT_ASSERT_VALUES_EQUAL(rows ? rows->num_rows() : 0, 0);
        return;
    }
    const bool aborted = sync && reboot;
    bool voted = false;
    runtime.SetEventFilter([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
        if (secondary && ev->GetTypeRewrite() == TEvPipeCache::TEvForward::EventType) {
            auto* forward = ev->Get<TEvPipeCache::TEvForward>();
            if (forward->TabletId == peer) {
                if (auto* readSet = dynamic_cast<TEvTxProcessing::TEvReadSet*>(forward->Ev.Get())) {
                    NKikimrTx::TReadSetData data;
                    UNIT_ASSERT(data.ParseFromString(readSet->Record.GetReadSet()));
                    UNIT_ASSERT(
                        data.GetDecision() == (aborted ? NKikimrTx::TReadSetData::DECISION_ABORT : NKikimrTx::TReadSetData::DECISION_COMMIT));
                    voted = true;
                }
                return true;
            }
        }
        return false;
    });
    const TPlanStep step{ Max(prepared.GetMinStep(), runtime.GetTimeProvider()->Now().MilliSeconds()) + 1 };
    UNIT_ASSERT(step.Val() <= prepared.GetMaxStep());
    PlanWriteTx(runtime, sender, { step, commitTxId }, false);
    if (secondary) {
        for (ui32 i = 0; i < 100 && !voted; ++i) {
            runtime.SimulateSleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT(voted);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender,
            new TEvTxProcessing::TEvReadSetAck(0, commitTxId, TTestTxConfig::TxTablet0, peer, peer, 0));
        NKikimrTx::TReadSetData decision;
        decision.SetDecision(aborted ? NKikimrTx::TReadSetData::DECISION_ABORT : NKikimrTx::TReadSetData::DECISION_COMMIT);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender,
            new TEvTxProcessing::TEvReadSet(step.Val(), commitTxId, peer, TTestTxConfig::TxTablet0, peer, decision.SerializeAsString()));
    }
    const auto result = runtime.GrabEdgeEvent<NEvents::TDataEvents::TEvWriteResult>(sender);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetTxId(), commitTxId);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(),
        aborted ? NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN : NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    UNIT_ASSERT(!WaitForShard(*controller.operator->(), runtime)->GetProgressTxController().GetTxInfo(commitTxId, ETxOperatorStatus::Any));
    TShardReader reader(runtime, TTestTxConfig::TxTablet0, 1, { step, commitTxId });
    reader.SetReplyColumnIds(TTestSchema::ExtractIds(table.Schema));
    auto rows = reader.ReadAll();
    UNIT_ASSERT(!reader.IsError());
    UNIT_ASSERT_VALUES_EQUAL(rows ? rows->num_rows() : 0, aborted ? 0 : 100);
}
}   // namespace

Y_UNIT_TEST_SUITE(ColumnShardCommitProposal) {
    Y_UNIT_TEST_DUO(PreparedSimpleCommitRetry, Reboot) {
        CheckPreparedCommitRetry(ECommitMode::Simple, Reboot);
    }
    Y_UNIT_TEST_DUO(PreparedPrimaryCommitRetry, Reboot) {
        CheckPreparedCommitRetry(ECommitMode::Primary, Reboot);
    }
    Y_UNIT_TEST_DUO(PreparedSecondaryCommitRetry, Reboot) {
        CheckPreparedCommitRetry(ECommitMode::Secondary, Reboot);
    }

    Y_UNIT_TEST(SimpleCommitPreservesPlanningInterval) {
        CheckPreparedCommitRetry(ECommitMode::Simple, false, EProposalScenario::Interval);
    }
    Y_UNIT_TEST(PrimaryCommitPreservesPlanningInterval) {
        CheckPreparedCommitRetry(ECommitMode::Primary, false, EProposalScenario::Interval);
    }
    Y_UNIT_TEST(SecondaryCommitPreservesPlanningInterval) {
        CheckPreparedCommitRetry(ECommitMode::Secondary, false, EProposalScenario::Interval);
    }
    Y_UNIT_TEST(SimpleCommitRejectsDifferentLock) {
        CheckPreparedCommitRetry(ECommitMode::Simple, false, EProposalScenario::DifferentLock);
    }
    Y_UNIT_TEST(PrimaryCommitRejectsDifferentLock) {
        CheckPreparedCommitRetry(ECommitMode::Primary, false, EProposalScenario::DifferentLock);
    }
    Y_UNIT_TEST(SecondaryCommitRejectsDifferentLock) {
        CheckPreparedCommitRetry(ECommitMode::Secondary, false, EProposalScenario::DifferentLock);
    }
    Y_UNIT_TEST(SimpleCommitRejectsDifferentKind) {
        CheckPreparedCommitRetry(ECommitMode::Simple, false, EProposalScenario::DifferentKind);
    }
    Y_UNIT_TEST(PrimaryCommitRejectsDifferentKind) {
        CheckPreparedCommitRetry(ECommitMode::Primary, false, EProposalScenario::DifferentKind);
    }
    Y_UNIT_TEST(SecondaryCommitRejectsDifferentKind) {
        CheckPreparedCommitRetry(ECommitMode::Secondary, false, EProposalScenario::DifferentKind);
    }
    Y_UNIT_TEST(SimpleCommitExpiresAfterRetry) {
        CheckPreparedCommitRetry(ECommitMode::Simple, false, EProposalScenario::Deadline);
    }
    Y_UNIT_TEST(PrimaryCommitExpiresAfterRetry) {
        CheckPreparedCommitRetry(ECommitMode::Primary, false, EProposalScenario::Deadline);
    }
    Y_UNIT_TEST(SecondaryCommitExpiresAfterRetry) {
        CheckPreparedCommitRetry(ECommitMode::Secondary, false, EProposalScenario::Deadline);
    }
    Y_UNIT_TEST(NewCommitRejectsRecoveredBrokenLock) {
        CheckPreparedCommitRetry(ECommitMode::Simple, true, EProposalScenario::FreshBrokenLock);
    }

    Y_UNIT_TEST(CommitIsRejectedAfterAbortWasQueued) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TCommitTestController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        const ui64 lockId = 3;
        const ui64 commitTxId = 13;
        TestTableDescription testTable{};
        Y_UNUSED(PrepareTablet(runtime, pathId, testTable.Schema));

        int writeId = 10;
        std::vector<ui64> lockedWriteIds;
        UNIT_ASSERT(WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true,
            &lockedWriteIds, NEvWrite::EModificationType::Upsert, lockId));

        const auto createTableTxBody = [&](const ui64 auxPathId, const ui32 round) {
            NKikimrTxColumnShard::TSchemaTxBody auxTx;
            UNIT_ASSERT(auxTx.ParseFromString(TTestSchema::CreateTableTxBody(auxPathId, testTable.Standalone, testTable.Schema, testTable.Pk)));
            auxTx.MutableSeqNo()->SetRound(round);
            TString body;
            Y_PROTOBUF_SUPPRESS_NODISCARD auxTx.SerializeToString(&body);
            return body;
        };
        // The response to a schema proposal is sent after the earlier write finishes in the executor.
        const auto barrierPlanStep = ProposeSchemaTx(runtime, sender, createTableTxBody(98, 2), 11);
        PlanSchemaTx(runtime, sender, { barrierPlanStep, 11 });

        auto* shard = WaitForShard(*csControllerGuard.operator->(), runtime);
        auto* lockInfo = shard->GetOperationsManager().GetLockOptional(lockId);
        UNIT_ASSERT(lockInfo);
        // Reproduce the state after rollback has queued TAbortWriteTransaction, but before it
        // executes. The commit proposal must not assign a TxId to this lock.
        lockInfo->SetNeedsAborting();
        UNIT_ASSERT(lockInfo->ReadyForAborting());
        lockInfo->SetAborting();

        auto commit = std::make_unique<NEvents::TDataEvents::TEvWrite>(commitTxId, NKikimrDataEvents::TEvWrite::MODE_PREPARE);
        commit->Record.MutableLocks()->AddLocks()->SetLockId(lockId);
        commit->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, commit.release());
        auto commitResult = runtime.GrabEdgeEvent<NEvents::TDataEvents::TEvWriteResult>(sender);
        UNIT_ASSERT(commitResult);
        UNIT_ASSERT_VALUES_EQUAL(commitResult->Get()->Record.GetTxId(), commitTxId);
        UNIT_ASSERT_VALUES_EQUAL(commitResult->Get()->Record.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);
        UNIT_ASSERT(!lockInfo->IsTxIdAssigned());

        std::vector<ui64> nextWriteIds;
        UNIT_ASSERT(
            WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 100, 150 }, testTable.Schema), testTable.Schema, true, &nextWriteIds));
    }

    Y_UNIT_TEST(CommitIsRejectedWhileRollbackIsInFlight) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto controller = NYDBTest::TControllers::RegisterCSControllerGuard<TCommitTestController>();
        TActorId sender = runtime.AllocateEdgeActor();
        TestTableDescription table{};
        const auto schemaStep = PrepareTablet(runtime, 1, table.Schema);
        constexpr ui64 lockId = 103;
        constexpr ui64 commitTxId = 20;
        std::vector<ui64> ids;
        UNIT_ASSERT(WriteData(runtime, sender, 10, 1, MakeTestBlob({ 0, 100 }, table.Schema), table.Schema, true, &ids,
            NEvWrite::EModificationType::Upsert, lockId));
        std::vector<TAutoPtr<IEventHandle>> heldPuts;
        bool hold = true;
        runtime.SetEventFilter([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (hold && ev->GetTypeRewrite() == TEvBlobStorage::TEvPut::EventType) {
                heldPuts.emplace_back(ev.Release());
                return true;
            }
            return false;
        });
        auto rollback = std::make_unique<NEvents::TDataEvents::TEvWrite>();
        rollback->Record.SetTxMode(NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
        rollback->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Rollback);
        rollback->Record.MutableLocks()->AddLocks()->SetLockId(lockId);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, rollback.release());
        const auto ack = runtime.GrabEdgeEvent<NEvents::TDataEvents::TEvWriteResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL(ack->Get()->Record.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        for (ui32 i = 0; heldPuts.empty() && i < 100; ++i) {
            runtime.SimulateSleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT(!heldPuts.empty());
        auto* lock = WaitForShard(*controller.operator->(), runtime)->GetOperationsManager().GetLockOptional(lockId);
        UNIT_ASSERT(lock && lock->IsAborting());
        UNIT_ASSERT(!lock->IsTxIdAssigned());
        auto commit = std::make_unique<NEvents::TDataEvents::TEvWrite>(commitTxId, NKikimrDataEvents::TEvWrite::MODE_PREPARE);
        commit->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
        commit->Record.MutableLocks()->AddLocks()->SetLockId(lockId);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, commit.release());
        runtime.SimulateSleep(TDuration::MilliSeconds(50));
        UNIT_ASSERT(!lock->IsTxIdAssigned());
        hold = false;
        for (auto& ev : heldPuts) {
            runtime.Send(ev.Release());
        }
        const auto result = runtime.GrabEdgeEvent<NEvents::TDataEvents::TEvWriteResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetTxId(), commitTxId);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        WaitForShard(*controller.operator->(), runtime);
        TShardReader reader(runtime, TTestTxConfig::TxTablet0, 1, NOlap::TSnapshot(schemaStep, Max<ui64>()));
        reader.SetReplyColumnIds(TTestSchema::ExtractIds(table.Schema));
        const auto rows = reader.ReadAll();
        UNIT_ASSERT(!reader.IsError());
        UNIT_ASSERT(!rows || rows->num_rows() == 0);
    }
}
}   // namespace NKikimr
