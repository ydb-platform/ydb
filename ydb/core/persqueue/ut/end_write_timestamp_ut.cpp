#include "kafka_batch.h"
#include "pqtablet_fixture.h"

#include <ydb/core/persqueue/common/key.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>

namespace NKikimr::NPQ {

namespace {

using TDeferredPublicationApi = NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi;

constexpr ui32 LifetimeSeconds = 600;
// Not a whole second: a commit time rounded to seconds must differ from the write time of the committed messages
constexpr TDuration TickDuration = TDuration::MilliSeconds(1'500);

enum class EAction {
    Direct,
    KafkaTx,
    Restart,
    CleanUpAll,
};

TStringBuf ActionName(EAction action) {
    switch (action) {
        case EAction::Direct: return "Direct";
        case EAction::KafkaTx: return "KafkaTx";
        case EAction::Restart: return "Restart";
        case EAction::CleanUpAll: return "CleanUpAll";
    }
    Y_UNREACHABLE();
}

bool HasDataKeys(const NKikimrClient::TKeyValueRequest& record) {
    return record.CmdRenameSize() > 0 || AnyOf(record.GetCmdWrite(), [](const auto& write) {
        const TStringBuf key = write.GetKey();
        return !key.empty() && (key[0] == TKeyPrefix::TypeData || key[0] == TKeyPrefix::ServiceTypeData);
    });
}

// EndWriteTimestamp must be the newest write time of the messages that are persisted in the main partition.
// It never decreases: it survives the retention of all blobs and the restarts.
class TEndWriteTimestampFixture : public TPQTabletFixture {
protected:
    struct TKafkaTx {
        NKafka::TProducerInstanceId Producer;
        TString OwnerCookie;
    };

    struct TTopicTx {
        TWriteId WriteId;
        TString OwnerCookie;
        ui32 SupportivePartitionId = 0;
        ui64 MessageNo = 0;
    };

    void SetUp(NUnitTest::TTestContext& context) override {
        TPQTabletFixture::SetUp(context);
        InitCompleteObserver = Ctx->Runtime->AddObserver<TEvPQ::TEvInitComplete>([this](TEvPQ::TEvInitComplete::TPtr& ev) {
            const auto& partition = ev->Get()->Partition;
            if (partition.IsSupportivePartition()) {
                LastSupportivePartitionId = partition.InternalPartitionId;
            } else if (partition.OriginalPartitionId == 0) {
                MainPartitionActor = ev->Sender;
            }
        });
        KvRequestObserver = Ctx->Runtime->AddObserver<TEvKeyValue::TEvRequest>([this](TEvKeyValue::TEvRequest::TPtr& ev) {
            if (ev->Sender != MainPartitionActor) {
                return;
            }
            MainPartitionRenames += ev->Get()->Record.CmdRenameSize();
            if (HoldDataWriteArmed && HasDataKeys(ev->Get()->Record)) {
                HoldDataWriteArmed = false;
                HeldDataWrite.Reset(ev.Release());
            }
        });
    }

    void Prepare(bool deleteLastBlob = true) {
        if (deleteLastBlob) {
            SetEnableTopicRetentionDeleteLastBlob(*Ctx);
        }
        PQTabletPrepare({.deleteTime = LifetimeSeconds, .partitions = 1}, {}, *Ctx);
        WaitMainPartitionInit();
    }

    void WaitMainPartitionInit() {
        if (MainPartitionActor != TActorId()) {
            return;
        }
        TDispatchOptions options;
        options.CustomFinalCondition = [this] { return MainPartitionActor != TActorId(); };
        UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    }

    TInstant PartitionEndWriteTimestamp() const {
        auto* partition = dynamic_cast<TPartition*>(Ctx->Runtime->FindActor(MainPartitionActor));
        UNIT_ASSERT(partition);
        return partition->GetEndWriteTimestamp();
    }

    std::pair<ui64, ui64> PartitionOffsets() {
        Ctx->Runtime->SendToPipe(Ctx->TabletId, Ctx->Edge, new TEvPersQueue::TEvOffsets, 0, GetPipeConfigWithRetries());
        const auto response = Ctx->Runtime->GrabEdgeEvent<TEvPersQueue::TEvOffsetsResponse>();
        UNIT_ASSERT_VALUES_EQUAL(response->Record.PartResultSize(), 1);
        const auto& result = response->Record.GetPartResult(0);
        return {result.GetStartOffset(), result.GetEndOffset()};
    }

    TInstant MaxStoredWriteTimestamp() {
        auto [offset, endOffset] = PartitionOffsets();
        TInstant result;
        while (offset < endOffset) {
            const ui64 readOffset = offset;
            const auto read = CmdReadAndGetResult(TPQCmdReadSettings("", 0, offset, endOffset - offset, 32_MB, 0), *Ctx);
            for (const auto& message : read.GetResult()) {
                result = Max(result, TInstant::MilliSeconds(message.GetWriteTimestampMS()));
                offset = message.GetOffset() + Max<ui64>(message.GetLogicalMessageCount(), 1);
            }
            UNIT_ASSERT_GT(offset, readOffset);
        }
        return result;
    }

    void UpdateExpected() {
        Expected = Max(Expected, MaxStoredWriteTimestamp());
    }

    void CheckEndWriteTimestamp(TStringBuf step) {
        UNIT_ASSERT_VALUES_EQUAL_C(PartitionEndWriteTimestamp(), Expected, step);
    }

    void CheckSurvivesRestart(TStringBuf step) {
        CheckEndWriteTimestamp(step);
        Tick();
        Restart();
        CheckEndWriteTimestamp(TStringBuilder() << step << " and a restart");
    }

    void Tick() {
        Ctx->Runtime->AdvanceCurrentTime(TickDuration);
    }

    void DirectWrite(size_t size = 100) {
        CmdWrite(0, "direct-src", {{++DirectSeqNo, TString(size, 'd')}}, *Ctx);
        UpdateExpected();
    }

    TKafkaTx BeginKafkaTx() {
        TKafkaTx tx{.Producer = {.Id = NextProducerId++, .Epoch = 0}};
        tx.OwnerCookie = CreateSupportivePartitionForKafka(tx.Producer);
        return tx;
    }

    void KafkaWrite(const TKafkaTx& tx, const TString& data = "kafka-payload") {
        SendKafkaTxnWriteRequest(tx.Producer, tx.OwnerCookie, 0, 0, data);
    }

    void CommitKafka(const TKafkaTx& tx) {
        CommitKafkaTransaction(tx.Producer, NextTxId++, {0}, NextPlanStep++);
        UpdateExpected();
    }

    void KafkaTx(const TString& data = "kafka-payload") {
        const auto tx = BeginKafkaTx();
        KafkaWrite(tx, data);
        Tick();
        CommitKafka(tx);
    }

    TTopicTx BeginTopicTx() {
        LastSupportivePartitionId.Clear();
        TTopicTx tx{.WriteId = TWriteId(0, NextWriteKeyId++)};
        tx.OwnerCookie = CreateSupportivePartitionForDeferredPublication(tx.WriteId);
        UNIT_ASSERT(LastSupportivePartitionId.Defined());
        tx.SupportivePartitionId = *LastSupportivePartitionId;
        return tx;
    }

    void TopicWrite(TTopicTx& tx) {
        SendSupportivePartitionWrite(tx.WriteId, tx.OwnerCookie, ++TopicSeqNo, tx.MessageNo++, "topic-payload", 300);
    }

    void CommitTopic(const TTopicTx& tx) {
        CommitTopicTransaction(tx.WriteId, tx.SupportivePartitionId, NextTxId++, {0}, NextPlanStep++);
        UpdateExpected();
    }

    void TopicTx() {
        auto tx = BeginTopicTx();
        TopicWrite(tx);
        Tick();
        CommitTopic(tx);
    }

    TWriteId DeferredWrite() {
        const ui64 publicationId = NextPublicationId++;
        const TWriteId writeId = NHelpers::MakeDeferredWriteId(publicationId, TStringBuilder() << "ext-" << publicationId);
        const TString ownerCookie = CreateSupportivePartitionForDeferredPublication(writeId);
        SendDeferredPublicationWriteRequest(writeId, ownerCookie);
        // The response was only observed: it stays on the edge actor and breaks later GrabEdgeEvent calls
        Ctx->Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>();
        return writeId;
    }

    void FinalizeDeferred(const TWriteId& writeId, TDeferredPublicationApi::EOp op) {
        CommitDeferredPublicationFinalize(writeId, NextTxId++, op, {0}, NextPlanStep++);
        UpdateExpected();
    }

    void AbortDeferredStaging(const TWriteId& writeId) {
        SendAbortDeferredStagingRequest(writeId);
        WaitAbortDeferredStagingResponse();
        Ctx->Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>();
    }

    void Restart() {
        MainPartitionActor = {};
        PQTabletRestart(*Ctx);
        ResetPipe();
        WaitMainPartitionInit();
    }

    void CleanUpAll() {
        const ui64 endOffset = PartitionOffsets().second;
        WaitRetentionCleanup(*Ctx, endOffset, endOffset, LifetimeSeconds);
    }

    void WaitStartOffsetAbove(ui64 offset) {
        Ctx->Runtime->AdvanceCurrentTime(TDuration::Seconds(LifetimeSeconds + 1));
        for (ui32 attempt = 0; attempt < 10; ++attempt) {
            DispatchUntilWakeup(*Ctx);
            if (PartitionOffsets().first > offset) {
                return;
            }
            Ctx->Runtime->AdvanceCurrentTime(TDuration::Seconds(5));
        }
        UNIT_FAIL("StartOffset did not move above " << offset);
    }

    void HoldNextDataWrite() {
        UNIT_ASSERT(!HeldDataWrite);
        HoldDataWriteArmed = true;
    }

    void WaitHeldDataWrite() {
        TDispatchOptions options;
        options.CustomFinalCondition = [this] { return bool(HeldDataWrite); };
        UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    }

    void ReleaseHeldDataWrite() {
        Ctx->Runtime->Send(HeldDataWrite.Release(), 0, true);
    }

    void Do(EAction action) {
        switch (action) {
            case EAction::Direct: return DirectWrite();
            case EAction::KafkaTx: return KafkaTx();
            case EAction::Restart: return Restart();
            case EAction::CleanUpAll: return CleanUpAll();
        }
    }

    void RunCheckingEach(std::initializer_list<EAction> actions) {
        Prepare();
        TStringBuilder done;
        for (const EAction action : actions) {
            Do(action);
            done << ' ' << ActionName(action);
            CheckEndWriteTimestamp(TStringBuilder() << "after" << done);
            Tick();
        }
    }

    TInstant Expected;
    TActorId MainPartitionActor;
    TMaybe<ui32> LastSupportivePartitionId;
    ui64 MainPartitionRenames = 0;
    bool HoldDataWriteArmed = false;
    TAutoPtr<IEventHandle> HeldDataWrite;

    ui64 DirectSeqNo = 0;
    ui64 TopicSeqNo = 0;
    i64 NextProducerId = 1;
    ui64 NextWriteKeyId = 1;
    ui64 NextPublicationId = 1;
    ui64 NextTxId = 10'000;
    ui64 NextPlanStep = 100;

    TTestActorRuntimeBase::TEventObserverHolder InitCompleteObserver;
    TTestActorRuntimeBase::TEventObserverHolder KvRequestObserver;
};

} // namespace

Y_UNIT_TEST_SUITE(TEndWriteTimestampTests) {

//
// Restart
//

Y_UNIT_TEST_F(Restart_EmptyPartition, TEndWriteTimestampFixture) {
    Prepare();
    Restart();
    CheckEndWriteTimestamp("after a restart");
}

Y_UNIT_TEST_F(Restart_AfterDirectWrite, TEndWriteTimestampFixture) {
    Prepare();
    DirectWrite();
    Tick();
    Restart();
    CheckEndWriteTimestamp("after a restart");
}

Y_UNIT_TEST_F(Restart_AfterKafkaTx, TEndWriteTimestampFixture) {
    Prepare();
    KafkaTx();
    Tick();
    Restart();
    CheckEndWriteTimestamp("after a restart");
}

Y_UNIT_TEST_F(Restart_AfterTopicTx, TEndWriteTimestampFixture) {
    Prepare();
    TopicTx();
    Tick();
    Restart();
    CheckEndWriteTimestamp("after a restart");
}

Y_UNIT_TEST_F(Restart_SupportiveBeforeCommit, TEndWriteTimestampFixture) {
    Prepare();
    const auto tx = BeginKafkaTx();
    KafkaWrite(tx);
    Tick();
    Restart();
    CheckEndWriteTimestamp("after a restart with an uncommitted transaction");
    Tick();
    CommitKafka(tx);
    CheckEndWriteTimestamp("after the commit");
    Tick();
    Restart();
    CheckEndWriteTimestamp("after the commit and a restart");
}

//
// Direct write
//

Y_UNIT_TEST_F(Direct_Single, TEndWriteTimestampFixture) {
    Prepare();
    DirectWrite();
    CheckEndWriteTimestamp("after a write");
}

Y_UNIT_TEST_F(Direct_SeveralWithTimeGaps, TEndWriteTimestampFixture) {
    Prepare();
    for (ui32 i = 0; i < 3; ++i) {
        DirectWrite();
        CheckEndWriteTimestamp(TStringBuilder() << "after write #" << i);
        Tick();
    }
}

Y_UNIT_TEST_F(Direct_LargeMessageInOneBlob, TEndWriteTimestampFixture) {
    Prepare();
    DirectWrite();
    Tick();
    DirectWrite(1_MB);
    CheckSurvivesRestart("after a large write");
}

Y_UNIT_TEST_F(Direct_LargeMessageAcrossBlobs, TEndWriteTimestampFixture) {
    Prepare();
    DirectWrite();
    Tick();
    DirectWrite(10_MB);
    UNIT_ASSERT_GT(MainPartitionRenames, 0);
    CheckSurvivesRestart("after a large write");
}

Y_UNIT_TEST_F(Direct_NativeBatch, TEndWriteTimestampFixture) {
    SetEnableTopicMessagesBatching(*Ctx);
    Prepare();
    CmdWriteBatched(0, "batch-src", 1, TString(16, 'b'), 3, *Ctx);
    UpdateExpected();
    CheckSurvivesRestart("after a batch write");
}

Y_UNIT_TEST_F(Direct_KafkaBatch, TEndWriteTimestampFixture) {
    SetEnableTopicMessagesBatching(*Ctx);
    Prepare();
    CmdWriteKafkaBatch(0, "kafka-batch-src", 1, {"value0", "value1", "value2"}, *Ctx);
    UpdateExpected();
    CheckSurvivesRestart("after a Kafka batch write");
}

Y_UNIT_TEST_F(Direct_WithOffsetGap, TEndWriteTimestampFixture) {
    Prepare();
    DirectWrite();
    Tick();
    CmdWrite(0, "direct-src", {{++DirectSeqNo, "after-gap"}}, *Ctx, false, {}, false, "", -1, 10);
    UNIT_ASSERT_VALUES_EQUAL(PartitionOffsets().second, 11u);
    UpdateExpected();
    CheckEndWriteTimestamp("after a write after an offset gap");
    Tick();
    KafkaTx();
    CheckSurvivesRestart("after a commit after an offset gap");
}

Y_UNIT_TEST_F(Direct_NotPersistedUntilKvResponse, TEndWriteTimestampFixture) {
    Prepare();
    DirectWrite();
    Tick();
    const TString ownerCookie = CmdSetOwner(0, *Ctx).first;
    HoldNextDataWrite();
    WriteData(0, "direct-src", {{++DirectSeqNo, "not-persisted-yet"}}, *Ctx, ownerCookie, 0, -1);
    WaitHeldDataWrite();
    CheckEndWriteTimestamp("while the write is not persisted");
    ReleaseHeldDataWrite();
    TAutoPtr<IEventHandle> handle;
    Ctx->Runtime->GrabEdgeEventIf<TEvPersQueue::TEvResponse>(handle, [](const TEvPersQueue::TEvResponse& ev) { return ev.Record.GetPartitionResponse().CmdWriteResultSize() > 0; });
    UpdateExpected();
    CheckEndWriteTimestamp("after the write is persisted");
}

//
// Transactions
//

Y_UNIT_TEST_F(KafkaTx_EmptyPartition, TEndWriteTimestampFixture) {
    Prepare();
    KafkaTx();
    CheckEndWriteTimestamp("after the commit");
}

Y_UNIT_TEST_F(TopicTx_EmptyPartition, TEndWriteTimestampFixture) {
    Prepare();
    TopicTx();
    CheckEndWriteTimestamp("after the commit");
}

Y_UNIT_TEST_F(DeferredPublish_EmptyPartition, TEndWriteTimestampFixture) {
    Prepare();
    const TWriteId writeId = DeferredWrite();
    Tick();
    FinalizeDeferred(writeId, TDeferredPublicationApi::Publish);
    CheckEndWriteTimestamp("after the publication");
}

Y_UNIT_TEST_F(KafkaTx_LargeMessageInOneBlob, TEndWriteTimestampFixture) {
    Prepare();
    KafkaTx(TString(1_MB, 'k'));
    CheckSurvivesRestart("after the commit");
}

Y_UNIT_TEST_F(KafkaTx_LargeMessageAcrossBlobs, TEndWriteTimestampFixture) {
    Prepare();
    KafkaTx(TString(10_MB, 'k'));
    CheckSurvivesRestart("after the commit");
}

Y_UNIT_TEST_F(KafkaTx_KafkaBatch, TEndWriteTimestampFixture) {
    SetEnableTopicMessagesBatching(*Ctx);
    Prepare();
    const auto tx = BeginKafkaTx();
    const TVector<TString> values = {"value0", "value1", "value2"};
    SendKafkaTxnWriteRequest(tx.Producer, tx.OwnerCookie, 0, 1, MakeKafkaBatchData(values, 1), 123, true, values.size());
    Tick();
    CommitKafka(tx);
    CheckSurvivesRestart("after the commit");
}

Y_UNIT_TEST_F(TopicTx_SeveralWritesInOneTx, TEndWriteTimestampFixture) {
    Prepare();
    auto tx = BeginTopicTx();
    for (ui32 i = 0; i < 3; ++i) {
        TopicWrite(tx);
        Tick();
    }
    CommitTopic(tx);
    CheckEndWriteTimestamp("after the commit");
}

Y_UNIT_TEST_F(Tx_UncommittedDoesNotChange, TEndWriteTimestampFixture) {
    Prepare();
    DirectWrite();
    Tick();
    const auto tx = BeginKafkaTx();
    KafkaWrite(tx);
    CheckEndWriteTimestamp("after a write into the supportive partition");
    Tick();
    Restart();
    CheckEndWriteTimestamp("after a restart");
}

Y_UNIT_TEST_F(DeferredCancel_DoesNotChange, TEndWriteTimestampFixture) {
    Prepare();
    DirectWrite();
    Tick();
    const TWriteId writeId = DeferredWrite();
    Tick();
    FinalizeDeferred(writeId, TDeferredPublicationApi::Cancel);
    CheckEndWriteTimestamp("after the cancel");
    Tick();
    Restart();
    CheckEndWriteTimestamp("after the cancel and a restart");
}

Y_UNIT_TEST_F(AbortDeferredStaging_DoesNotChange, TEndWriteTimestampFixture) {
    Prepare();
    DirectWrite();
    Tick();
    const TWriteId writeId = DeferredWrite();
    Tick();
    AbortDeferredStaging(writeId);
    CheckEndWriteTimestamp("after the abort of the staging");
}

Y_UNIT_TEST_F(Tx_CommitNotPersistedUntilKvResponse, TEndWriteTimestampFixture) {
    Prepare();
    DirectWrite();
    Tick();
    const auto tx = BeginKafkaTx();
    KafkaWrite(tx);
    Tick();
    const ui64 txId = NextTxId++;
    const ui64 planStep = NextPlanStep++;
    ProposeKafkaTransaction(tx.Producer, txId);
    HoldNextDataWrite();
    SendPlanStep({.Step = planStep, .TxIds = {txId}});
    WaitHeldDataWrite();
    CheckEndWriteTimestamp("while the commit is not persisted");
    ReleaseHeldDataWrite();
    WaitTransactionCompleted(txId, planStep);
    UpdateExpected();
    CheckEndWriteTimestamp("after the commit is persisted");
}

Y_UNIT_TEST_F(TwoTx_CommittedInReverseOrder, TEndWriteTimestampFixture) {
    Prepare();
    const auto older = BeginKafkaTx();
    KafkaWrite(older);
    Tick();
    const auto newer = BeginKafkaTx();
    KafkaWrite(newer);
    Tick();
    CommitKafka(newer);
    CheckEndWriteTimestamp("after the commit of the newer transaction");
    Tick();
    CommitKafka(older);
    CheckEndWriteTimestamp("after the commit of the older transaction");
}

//
// Retention
//

Y_UNIT_TEST_F(CleanUpAll_AfterDirect, TEndWriteTimestampFixture) {
    Prepare();
    DirectWrite();
    CleanUpAll();
    CheckEndWriteTimestamp("after the cleanup");
}

Y_UNIT_TEST_F(CleanUpAll_AfterDirect_Restart, TEndWriteTimestampFixture) {
    Prepare();
    DirectWrite();
    CleanUpAll();
    Restart();
    CheckEndWriteTimestamp("after the cleanup and a restart");
}

Y_UNIT_TEST_F(CleanUpAll_AfterKafkaTx_Restart, TEndWriteTimestampFixture) {
    Prepare();
    KafkaTx();
    CleanUpAll();
    Restart();
    CheckEndWriteTimestamp("after the cleanup and a restart");
}

Y_UNIT_TEST_F(CleanUpAll_ThenDirect, TEndWriteTimestampFixture) {
    Prepare();
    DirectWrite();
    CleanUpAll();
    Tick();
    DirectWrite();
    CheckEndWriteTimestamp("after a write after the cleanup");
}

Y_UNIT_TEST_F(CleanUpAll_ThenKafkaTx, TEndWriteTimestampFixture) {
    Prepare();
    DirectWrite();
    CleanUpAll();
    Tick();
    KafkaTx();
    CheckEndWriteTimestamp("after a commit after the cleanup");
}

Y_UNIT_TEST_F(CleanUpPartial_LegacyKeepsLastBlob, TEndWriteTimestampFixture) {
    Ctx->Runtime->GetAppData(0).PQConfig.MutableCompactionConfig()->SetBlobsCount(0);
    Prepare(false);
    DirectWrite(7_MB);
    Tick();
    DirectWrite(7_MB);
    CmdRunCompaction(0, *Ctx);
    WaitStartOffsetAbove(0);
    CheckEndWriteTimestamp("after the cleanup");
    Restart();
    CheckEndWriteTimestamp("after the cleanup and a restart");
}

//
// Combinations
//

Y_UNIT_TEST_F(Direct_ThenKafkaTx, TEndWriteTimestampFixture) {
    Prepare();
    DirectWrite();
    CheckEndWriteTimestamp("after a write");
    Tick();
    KafkaTx();
    CheckEndWriteTimestamp("after the commit");
}

Y_UNIT_TEST_F(TxWrite_Direct_Commit, TEndWriteTimestampFixture) {
    Prepare();
    const auto tx = BeginKafkaTx();
    KafkaWrite(tx);
    Tick();
    DirectWrite();
    CheckEndWriteTimestamp("after a direct write");
    Tick();
    CommitKafka(tx);
    CheckEndWriteTimestamp("after the commit of the older messages");
}

Y_UNIT_TEST_F(KafkaTx_ThenDirect, TEndWriteTimestampFixture) {
    Prepare();
    KafkaTx();
    Tick();
    DirectWrite();
    CheckEndWriteTimestamp("after a write");
}

Y_UNIT_TEST_F(KafkaTx_Restart_Direct_Restart, TEndWriteTimestampFixture) {
    Prepare();
    KafkaTx();
    Tick();
    Restart();
    CheckEndWriteTimestamp("after a restart");
    Tick();
    DirectWrite();
    CheckEndWriteTimestamp("after a write");
    Tick();
    Restart();
    CheckEndWriteTimestamp("after the second restart");
}

Y_UNIT_TEST_F(Direct_KafkaTx_CleanUpAll_Restart_Direct, TEndWriteTimestampFixture) {
    Prepare();
    DirectWrite();
    Tick();
    KafkaTx();
    CleanUpAll();
    Restart();
    CheckEndWriteTimestamp("after the cleanup and a restart");
    Tick();
    DirectWrite();
    CheckEndWriteTimestamp("after a write");
}

#define EWT_ORDER_TEST(A, B, C, D) Y_UNIT_TEST_F(Order_##A##_##B##_##C##_##D, TEndWriteTimestampFixture) { RunCheckingEach({EAction::A, EAction::B, EAction::C, EAction::D}); }

EWT_ORDER_TEST(Direct, KafkaTx, Restart, CleanUpAll)
EWT_ORDER_TEST(Direct, KafkaTx, CleanUpAll, Restart)
EWT_ORDER_TEST(Direct, Restart, KafkaTx, CleanUpAll)
EWT_ORDER_TEST(Direct, Restart, CleanUpAll, KafkaTx)
EWT_ORDER_TEST(Direct, CleanUpAll, KafkaTx, Restart)
EWT_ORDER_TEST(Direct, CleanUpAll, Restart, KafkaTx)
EWT_ORDER_TEST(KafkaTx, Direct, Restart, CleanUpAll)
EWT_ORDER_TEST(KafkaTx, Direct, CleanUpAll, Restart)
EWT_ORDER_TEST(KafkaTx, Restart, Direct, CleanUpAll)
EWT_ORDER_TEST(KafkaTx, Restart, CleanUpAll, Direct)
EWT_ORDER_TEST(KafkaTx, CleanUpAll, Direct, Restart)
EWT_ORDER_TEST(KafkaTx, CleanUpAll, Restart, Direct)
EWT_ORDER_TEST(Restart, Direct, KafkaTx, CleanUpAll)
EWT_ORDER_TEST(Restart, Direct, CleanUpAll, KafkaTx)
EWT_ORDER_TEST(Restart, KafkaTx, Direct, CleanUpAll)
EWT_ORDER_TEST(Restart, KafkaTx, CleanUpAll, Direct)
EWT_ORDER_TEST(Restart, CleanUpAll, Direct, KafkaTx)
EWT_ORDER_TEST(Restart, CleanUpAll, KafkaTx, Direct)
EWT_ORDER_TEST(CleanUpAll, Direct, KafkaTx, Restart)
EWT_ORDER_TEST(CleanUpAll, Direct, Restart, KafkaTx)
EWT_ORDER_TEST(CleanUpAll, KafkaTx, Direct, Restart)
EWT_ORDER_TEST(CleanUpAll, KafkaTx, Restart, Direct)
EWT_ORDER_TEST(CleanUpAll, Restart, Direct, KafkaTx)
EWT_ORDER_TEST(CleanUpAll, Restart, KafkaTx, Direct)

#undef EWT_ORDER_TEST

}

} // namespace NKikimr::NPQ
