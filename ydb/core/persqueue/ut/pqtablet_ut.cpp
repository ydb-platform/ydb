#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/pqtablet/common/constants.h>
#include <ydb/core/persqueue/pqtablet/partition/partition.h>
#include <ydb/core/persqueue/pqtablet/quota/read_quoter.h>
#include <ydb/core/persqueue/pqtablet/fix_transaction_states.h>
#include <memory>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/protos/counters_keyvalue.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h>
#include <ydb/public/lib/base/msgbus_status.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/protos/actors.pb.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/json/json_reader.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/system/types.h>

#include "make_config.h"
#include "pqtablet_mock.h"
#include "pqtablet_fixture.h"

namespace NKikimr::NPQ {

namespace NDeferredWriterTest {

class TClientActor : public TActorBootstrapped<TClientActor> {
public:
    TClientActor(ui64 tabletId, ui32 partitionId, ui64 writeCookie, const std::shared_ptr<bool>& writeDone)
        : TabletId(tabletId)
        , PartitionId(partitionId)
        , WriteCookie(writeCookie)
        , WriteDone(writeDone)
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        TPartitionWriterOpts opts;
        opts.WithDeduplication(false)
            .WithSourceId("deferred-writer-source")
            .WithTopicPath("/topic")
            .WithDatabase("/Root")
            .WithDeferredPublish(52, "ext-52");

        WriterId = ctx.Register(CreatePartitionWriter(SelfId(), TabletId, PartitionId, opts));
        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPartitionWriter::TEvInitResult, HandleInit);
            hFunc(TEvPartitionWriter::TEvWriteResponse, HandleWrite);
            hFunc(TEvPartitionWriter::TEvDisconnected, HandleDisconnected);
            hFunc(TEvPartitionWriter::TEvRequestDeferredDestinationUpsert, HandleDeferredDestinationUpsert);
        default:
            break;
        }
    }

private:
    void HandleDeferredDestinationUpsert(TEvPartitionWriter::TEvRequestDeferredDestinationUpsert::TPtr&) {
        auto* result = new TEvPartitionWriter::TEvDeferredDestinationUpsertResult;
        result->Success = true;
        Send(WriterId, result);
    }

    void HandleInit(TEvPartitionWriter::TEvInitResult::TPtr& ev) {
        if (!ev->Get()->IsSuccess()) {
            *WriteDone = false;
            return;
        }

        auto writeEv = MakeHolder<TEvPartitionWriter::TEvWriteRequest>(WriteCookie);
        auto* request = writeEv->Record.MutablePartitionRequest();
        request->SetOwnerCookie(ev->Get()->GetResult().OwnerCookie);
        auto* cmdWrite = request->AddCmdWrite();
        cmdWrite->SetSourceId("deferred-writer-source");
        cmdWrite->SetSeqNo(0);
        const TString data = "deferred-writer-payload";
        cmdWrite->SetData(data);
        cmdWrite->SetCreateTimeMS(TInstant::Now().MilliSeconds());
        cmdWrite->SetDisableDeduplication(true);
        cmdWrite->SetUncompressedSize(data.size());
        cmdWrite->SetIgnoreQuotaDeadline(true);
        cmdWrite->SetExternalOperation(true);
        Send(WriterId, writeEv.Release());
    }

    void HandleWrite(TEvPartitionWriter::TEvWriteResponse::TPtr& ev) {
        *WriteDone = ev->Get()->IsSuccess();
    }

    void HandleDisconnected(TEvPartitionWriter::TEvDisconnected::TPtr&) {
        *WriteDone = false;
    }

    const ui64 TabletId;
    const ui32 PartitionId;
    const ui64 WriteCookie;
    const std::shared_ptr<bool> WriteDone;
    TActorId WriterId;
};

} // namespace NDeferredWriterTest

Y_UNIT_TEST_SUITE(TPQTabletTests) {

Y_UNIT_TEST_F(Multiple_PQTablets_1, TPQTabletFixture)
{
    TestMultiplePQTablets("consumer", "consumer");
}

Y_UNIT_TEST_F(Multiple_PQTablets_2, TPQTabletFixture)
{
    TestMultiplePQTablets("consumer-1", "consumer-2");
}

Y_UNIT_TEST_F(Parallel_Transactions_1, TPQTabletFixture)
{
    TestParallelTransactions("consumer", "consumer");
}

Y_UNIT_TEST_F(Parallel_Transactions_2, TPQTabletFixture)
{
    TestParallelTransactions("consumer-1", "consumer-2");
}

Y_UNIT_TEST_F(Single_PQTablet_And_Multiple_Partitions, TPQTabletFixture)
{
    PQTabletPrepare({.partitions=2}, {}, *Ctx);

    const ui64 txId = 67890;

    SendProposeTransactionRequest({.TxId=txId,
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  {.Partition=1, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});

    //
    // TODO(abcdef): проверить, что в команде CmdWrite есть информация о транзакции
    //

    WaitPlanStepAck({.Step=100, .TxIds={txId}}); // TEvPlanStepAck для координатора
    WaitPlanStepAccepted({.Step=100});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    //
    // TODO(abcdef): проверить, что удалена информация о транзакции
    //
}

Y_UNIT_TEST_F(PQTablet_Send_RS_With_Abort, TPQTabletFixture)
{
    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(22222);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    const ui64 txId = 67890;

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={22222}, .Receivers={22222},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});

    WaitReadSet(*tablet, {.Step=100, .TxId=txId, .Source=Ctx->TabletId, .Target=22222, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});
    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId, .Target=Ctx->TabletId, .Decision=NKikimrTx::TReadSetData::DECISION_ABORT});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});

    WaitPlanStepAck({.Step=100, .TxIds={txId}}); // TEvPlanStepAck для координатора
    WaitPlanStepAccepted({.Step=100});

    tablet->SendReadSetAck(*Ctx->Runtime, {.Step=100, .TxId=txId, .Source=Ctx->TabletId});
    WaitReadSetAck(*tablet, {.Step=100, .TxId=txId, .Source=22222, .Target=Ctx->TabletId, .Consumer=Ctx->TabletId});
}

Y_UNIT_TEST_F(PlanStep_Ack_All_Senders_After_Mediator_Restart, TPQTabletFixture)
{
    // The mediator tablet may restart: a TEvPlanStep from a stale mediator leader
    // can arrive after the one from the current leader. The PQ tablet must keep
    // all senders and ack each of them on transaction completion, not only the
    // last one (otherwise the real mediator never gets the ack for its PlanStep).
    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(22222);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    const ui64 txId = 67890;
    const ui64 mockTabletId = 22222;

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={mockTabletId}, .Receivers={mockTabletId},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    // The current mediator leader plans the step.
    const TActorId realLeader = Ctx->Edge;
    SendPlanStep({.Step=100, .TxIds={txId}});

    // The tx is now in WAIT_RS (a readset was sent to the mock tablet and not
    // yet answered), so it is not yet EXECUTED — a window for a stale leader.
    WaitReadSet(*tablet, {.Step=100, .TxId=txId, .Source=Ctx->TabletId, .Target=mockTabletId,
                          .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});

    // A stale mediator leader (a different ActorId) replays the same PlanStep.
    // It arrives after the real leader's message but before the tx completes.
    const TActorId staleLeader = Ctx->Runtime->AllocateEdgeActor();
    SendPlanStep({.Step=100, .TxIds={txId}, .Sender=staleLeader});

    // The mock tablet answers the readset, completing the transaction.
    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId, .Target=Ctx->TabletId,
                                         .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    // Both leaders must receive TEvPlanStepAccepted; the coordinator (AckTo ==
    // Ctx->Edge) must receive one TEvPlanStepAck per stored PlanStep event.
    auto accepted1 = Ctx->Runtime->GrabEdgeEvent<TEvTxProcessing::TEvPlanStepAccepted>(realLeader);
    UNIT_ASSERT(accepted1);
    auto accepted2 = Ctx->Runtime->GrabEdgeEvent<TEvTxProcessing::TEvPlanStepAccepted>(staleLeader);
    UNIT_ASSERT(accepted2);

    auto ack1 = Ctx->Runtime->GrabEdgeEvent<TEvTxProcessing::TEvPlanStepAck>(realLeader);
    UNIT_ASSERT(ack1);
    UNIT_ASSERT_VALUES_EQUAL(100, ack1->Get()->Record.GetStep());
    UNIT_ASSERT_VALUES_EQUAL(1, ack1->Get()->Record.TxIdSize());
    UNIT_ASSERT_VALUES_EQUAL(txId, ack1->Get()->Record.GetTxId(0));

    auto ack2 = Ctx->Runtime->GrabEdgeEvent<TEvTxProcessing::TEvPlanStepAck>(realLeader);
    UNIT_ASSERT(ack2);
    UNIT_ASSERT_VALUES_EQUAL(100, ack2->Get()->Record.GetStep());
    UNIT_ASSERT_VALUES_EQUAL(1, ack2->Get()->Record.TxIdSize());
    UNIT_ASSERT_VALUES_EQUAL(txId, ack2->Get()->Record.GetTxId(0));

    tablet->SendReadSetAck(*Ctx->Runtime, {.Step=100, .TxId=txId, .Source=Ctx->TabletId});
    WaitReadSetAck(*tablet, {.Step=100, .TxId=txId, .Source=mockTabletId, .Target=Ctx->TabletId, .Consumer=Ctx->TabletId});
}

Y_UNIT_TEST_F(Partition_Send_Predicate_With_False, TPQTabletFixture)
{
    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(22222);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    const ui64 txId = 67890;

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={22222}, .Receivers={22222},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=2, .Path="/topic"},
                                  }});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});

    WaitReadSet(*tablet, {.Step=100, .TxId=txId, .Source=Ctx->TabletId, .Target=22222, .Decision=NKikimrTx::TReadSetData::DECISION_ABORT, .Producer=Ctx->TabletId});
    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId, .Target=Ctx->TabletId, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});

    tablet->SendReadSetAck(*Ctx->Runtime, {.Step=100, .TxId=txId, .Source=Ctx->TabletId});
    WaitReadSetAck(*tablet, {.Step=100, .TxId=txId, .Source=22222, .Target=Ctx->TabletId, .Consumer=Ctx->TabletId});

    WaitPlanStepAck({.Step=100, .TxIds={txId}}); // TEvPlanStepAck для координатора
    WaitPlanStepAccepted({.Step=100});
}

Y_UNIT_TEST_F(DropTablet_And_Tx, TPQTabletFixture)
{
    PQTabletPrepare({.partitions=2}, {}, *Ctx);

    const ui64 txId_1 = 67890;
    const ui64 txId_2 = 67891;

    StartPQWriteStateObserver();

    SendProposeTransactionRequest({.TxId=txId_1,
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  {.Partition=1, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    SendDropTablet({.TxId=12345});

    //
    // транзакция TxId_1 будет обработана
    //
    WaitProposeTransactionResponse({.TxId=txId_1,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    WaitForPQWriteState();

    //
    // по транзакции TxId_2 получим отказ
    //
    SendProposeTransactionRequest({.TxId=txId_2,
                                  .TxOps={
                                  {.Partition=1, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId_2,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});

    SendPlanStep({.Step=100, .TxIds={txId_1}});

    SendDropTablet({.TxId=67890});                 // TEvDropTable когда выполняется транзакция

    WaitProposeTransactionResponse({.TxId=txId_1,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    WaitPlanStepAck({.Step=100, .TxIds={txId_1}}); // TEvPlanStepAck для координатора
    WaitPlanStepAccepted({.Step=100});

    //
    // ответы на TEvDropTablet будут после транзакции
    //
    WaitDropTabletReply({.Status=NKikimrProto::EReplyStatus::OK, .TxId=12345, .TabletId=Ctx->TabletId, .State=NKikimrPQ::EDropped});
    WaitDropTabletReply({.Status=NKikimrProto::EReplyStatus::OK, .TxId=67890, .TabletId=Ctx->TabletId, .State=NKikimrPQ::EDropped});
}

Y_UNIT_TEST_F(DropTablet, TPQTabletFixture)
{
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    //
    // транзакций нет, ответ будет сразу
    //
    SendDropTablet({.TxId=99999});
    WaitDropTabletReply({.Status=NKikimrProto::EReplyStatus::OK, .TxId=99999, .TabletId=Ctx->TabletId, .State=NKikimrPQ::EDropped});
}

Y_UNIT_TEST_F(DropTablet_Before_Write, TPQTabletFixture)
{
    PQTabletPrepare({.partitions=2}, {}, *Ctx);

    const ui64 txId_1 = 67890;
    const ui64 txId_2 = 67891;
    const ui64 txId_3 = 67892;

    StartPQWriteStateObserver();

    //
    // TEvDropTablet между транзакциями
    //
    SendProposeTransactionRequest({.TxId=txId_1,
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  {.Partition=1, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    SendDropTablet({.TxId=12345});
    SendProposeTransactionRequest({.TxId=txId_2,
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  {.Partition=1, .Consumer="user", .Begin=0, .End=0, .Path="/topic"}
                                  }});

    WaitProposeTransactionResponse({.TxId=txId_1,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    WaitForPQWriteState();

    SendProposeTransactionRequest({.TxId=txId_3,
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  {.Partition=1, .Consumer="user", .Begin=0, .End=0, .Path="/topic"}
                                  }});

    //
    // транзакция пришла до того как состояние было записано на диск. будет обработана
    //
    WaitProposeTransactionResponse({.TxId=txId_2,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    //
    // транзакция пришла после того как состояние было записано на диск. не будет обработана
    //
    WaitProposeTransactionResponse({.TxId=txId_3,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});
}

Y_UNIT_TEST_F(DropTablet_And_UnplannedConfigTransaction, TPQTabletFixture)
{
    PQTabletPrepare({.partitions=2}, {}, *Ctx);

    const ui64 txId = 67890;

    auto tabletConfig =
        NHelpers::MakeConfig(2, {
                             {.Consumer="client-1", .Generation=0},
                             {.Consumer="client-3", .Generation=7}},
                             2);

    SendProposeTransactionRequest({.TxId=txId,
                                  .Configs=NHelpers::TConfigParams{
                                  .Tablet=tabletConfig,
                                  .Bootstrap=NHelpers::MakeBootstrapConfig(),
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    // The 'TEvDropTablet` message arrives when the transaction has not yet received a PlanStep. We know that SS
    // performs no more than one operation at a time. Therefore, we believe that no one is waiting for this
    // transaction anymore.
    SendDropTablet({.TxId=12345});
    WaitDropTabletReply({.Status=NKikimrProto::EReplyStatus::OK, .TxId=12345, .TabletId=Ctx->TabletId, .State=NKikimrPQ::EDropped});
}

Y_UNIT_TEST_F(DropTablet_And_PlannedConfigTransaction, TPQTabletFixture)
{
    PQTabletPrepare({.partitions=2}, {}, *Ctx);

    const ui64 txId = 67890;

    auto tabletConfig =
        NHelpers::MakeConfig(2, {
                             {.Consumer="client-1", .Generation=0},
                             {.Consumer="client-3", .Generation=7}},
                             2);

    SendProposeTransactionRequest({.TxId=txId,
                                  .Configs=NHelpers::TConfigParams{
                                  .Tablet=tabletConfig,
                                  .Bootstrap=NHelpers::MakeBootstrapConfig(),
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});
    WaitPlanStepAck({.Step=100, .TxIds={txId}});

    // The 'TEvDropTablet` message arrives when the transaction has already received a PlanStep.
    // We will receive the response when the transaction is executed.
    SendDropTablet({.TxId=12345});

    WaitPlanStepAccepted({.Step=100});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    WaitDropTabletReply({.Status=NKikimrProto::EReplyStatus::OK, .TxId=12345, .TabletId=Ctx->TabletId, .State=NKikimrPQ::EDropped});
}

Y_UNIT_TEST_F(UpdateConfig_1, TPQTabletFixture)
{
    PQTabletPrepare({.partitions=2}, {}, *Ctx);

    const ui64 txId = 67890;

    auto tabletConfig =
        NHelpers::MakeConfig(2, {
                             {.Consumer="client-1", .Generation=0},
                             {.Consumer="client-3", .Generation=7}},
                             2);

    SendProposeTransactionRequest({.TxId=txId,
                                  .Configs=NHelpers::TConfigParams{
                                  .Tablet=tabletConfig,
                                  .Bootstrap=NHelpers::MakeBootstrapConfig(),
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});

    WaitPlanStepAck({.Step=100, .TxIds={txId}});
    WaitPlanStepAccepted({.Step=100});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});
}

Y_UNIT_TEST_F(UpdateConfig_2, TPQTabletFixture)
{
    PQTabletPrepare({.partitions=2}, {}, *Ctx);

    const ui64 txId_2 = 67891;
    const ui64 txId_3 = 67892;

    auto tabletConfig =
        NHelpers::MakeConfig(2, {
                             {.Consumer="client-1", .Generation=1},
                             {.Consumer="client-2", .Generation=1}
                             },
                             3);

    SendProposeTransactionRequest({.TxId=txId_2,
                                  .Configs=NHelpers::TConfigParams{
                                  .Tablet=tabletConfig,
                                  .Bootstrap=NHelpers::MakeBootstrapConfig(),
                                  }});
    SendProposeTransactionRequest({.TxId=txId_3,
                                  .TxOps={
                                  {.Partition=1, .Consumer="client-2", .Begin=0, .End=0, .Path="/topic"},
                                  {.Partition=2, .Consumer="client-1", .Begin=0, .End=0, .Path="/topic"}
                                  }});

    WaitProposeTransactionResponse({.TxId=txId_2,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});
    WaitProposeTransactionResponse({.TxId=txId_3,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId_2, txId_3}});

    WaitPlanStepAck({.Step=100, .TxIds={txId_2, txId_3}});
    WaitPlanStepAccepted({.Step=100});

    WaitProposeTransactionResponse({.TxId=txId_2,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});
    WaitProposeTransactionResponse({.TxId=txId_3,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});
}

Y_UNIT_TEST_F(Test_Waiting_For_TEvReadSet_When_There_Are_More_Senders_Than_Recipients, TPQTabletFixture)
{
    TestWaitingForTEvReadSet(4, 2);
}

Y_UNIT_TEST_F(Test_Waiting_For_TEvReadSet_When_There_Are_Fewer_Senders_Than_Recipients, TPQTabletFixture)
{
    TestWaitingForTEvReadSet(2, 4);
}

Y_UNIT_TEST_F(Test_Waiting_For_TEvReadSet_When_The_Number_Of_Senders_And_Recipients_Match, TPQTabletFixture)
{
    TestWaitingForTEvReadSet(2, 2);
}

Y_UNIT_TEST_F(Test_Waiting_For_TEvReadSet_Without_Recipients, TPQTabletFixture)
{
    TestWaitingForTEvReadSet(2, 0);
}

Y_UNIT_TEST_F(Test_Waiting_For_TEvReadSet_Without_Senders, TPQTabletFixture)
{
    TestWaitingForTEvReadSet(0, 2);
}

Y_UNIT_TEST_F(TEvReadSet_comes_before_TEvPlanStep, TPQTabletFixture)
{
    const ui64 mockTabletId = 22222;

    CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    const ui64 txId = 67890;

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={mockTabletId}, .Receivers={mockTabletId},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=1, .Path="/topic"}
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendReadSet({.Step=100, .TxId=txId, .Source=mockTabletId, .Target=Ctx->TabletId, .Predicate=true});

    SendPlanStep({.Step=100, .TxIds={txId}});

    //WaitPlanStepAck({.Step=100, .TxIds={txId}}); // TEvPlanStepAck для координатора
    //WaitPlanStepAccepted({.Step=100});
}

Y_UNIT_TEST_F(Cancel_Tx, TPQTabletFixture)
{
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    const ui64 txId = 67890;

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={22222}, .Receivers={22222},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    StartPQWriteTxsObserver();

    // запись о транзакции не удаляется сразу
    SendCancelTransactionProposal({.TxId=txId});
    SendProposeTransactionRequest({.TxId=txId + 1,
                                  .Senders={22222}, .Receivers={22222},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});

    WaitForPQWriteTxs();
}

Y_UNIT_TEST_F(ProposeTx_Missing_Operations, TPQTabletFixture)
{
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    const ui64 txId = 2;

    SendProposeTransactionRequest({.TxId=txId,
                                  });
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});
}

Y_UNIT_TEST_F(ProposeTx_Unknown_Partition_1, TPQTabletFixture)
{
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    const ui64 txId = 2;
    const ui32 unknownPartitionId = 3;

    SendProposeTransactionRequest({.TxId=txId,
                                  .TxOps={{.Partition=unknownPartitionId, .Path="/topic"}}
                                  });
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});
}

Y_UNIT_TEST_F(Ignore_Late_TransactionCompleted_For_Unknown_WriteId, TPQTabletFixture)
{
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    SendToPipe(Ctx->Edge, new TEvPQ::TEvTransactionCompleted(TWriteId(0, 3)));

    AssertTabletIsAlive();
}

Y_UNIT_TEST_F(ProposeTx_Unknown_WriteId, TPQTabletFixture)
{
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    const ui64 txId = 2;
    const TWriteId writeId(0, 3);

    SendProposeTransactionRequest({.TxId=txId,
                                  .TxOps={{.Partition=0, .Path="/topic"}},
                                  .WriteId=writeId});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});
}

Y_UNIT_TEST_F(ProposeTx_Unknown_Partition_2, TPQTabletFixture)
{
    PQTabletPrepare({.partitions=2}, {}, *Ctx);

    const ui64 txId = 2;
    const TWriteId writeId(0, 3);
    const ui64 cookie = 4;

    SendGetOwnershipRequest({.Partition=0,
                            .WriteId=writeId,
                            .Owner=DEFAULT_OWNER,
                            .Cookie=cookie});
    WaitGetOwnershipResponse({.Cookie=cookie});

    SendProposeTransactionRequest({.TxId=txId,
                                  .TxOps={{.Partition=1, .Path="/topic"}},
                                  .WriteId=writeId});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});
}

Y_UNIT_TEST_F(Ignore_MLPConsumerStatus_Without_ReadBalancer, TPQTabletFixture)
{
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    SendToPipe(Ctx->Edge, new TEvPQ::TEvMLPConsumerStatus("user", 0, true));

    AssertTabletIsAlive();
}

Y_UNIT_TEST_F(ProposeTx_Command_After_Propose, TPQTabletFixture)
{
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    const ui32 partitionId = 0;
    const ui64 txId = 2;
    const TWriteId writeId(0, 3);

    SyncGetOwnership({.Partition=partitionId,
                     .WriteId=writeId,
                     .NeedSupportivePartition=true,
                     .Owner=DEFAULT_OWNER,
                     .Cookie=4},
                     {.Cookie=4,
                     .Status=NMsgBusProxy::MSTATUS_OK});

    SendProposeTransactionRequest({.TxId=txId,
                                  .TxOps={{.Partition=partitionId, .Path="/topic", .SupportivePartition=100'000}},
                                  .WriteId=writeId});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SyncGetOwnership({.Partition=partitionId,
                     .WriteId=writeId,
                     .Owner=DEFAULT_OWNER,
                     .Cookie=5},
                     {.Cookie=5,
                     .Status=NMsgBusProxy::MSTATUS_ERROR});
}

Y_UNIT_TEST_F(Read_TEvTxCommit_After_Restart, TPQTabletFixture)
{
    const ui64 txId = 67890;
    const ui64 mockTabletId = 22222;

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={mockTabletId}, .Receivers={mockTabletId},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});

    WaitForCalcPredicateResult();

    // the transaction is now in the WAIT_RS state in memory and PLANNED state in disk

    PQTabletRestart(*Ctx);
    ResetPipe();

    // Tablet PQ has not confirmed that she received TEvPlanStep. Therefore, the coordinator will send it again
    SendPlanStep({.Step=100, .TxIds={txId}});

    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId, .Target=Ctx->TabletId, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    tablet->SendReadSetAck(*Ctx->Runtime, {.Step=100, .TxId=txId, .Source=Ctx->TabletId});
    WaitReadSetAck(*tablet, {.Step=100, .TxId=txId, .Source=mockTabletId, .Target=Ctx->TabletId, .Consumer=Ctx->TabletId});
}

Y_UNIT_TEST_F(Config_TEvTxCommit_After_Restart, TPQTabletFixture)
{
    const ui64 txId = 67890;
    const ui64 mockTabletId = 22222;

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    auto tabletConfig = NHelpers::MakeConfig({.Version=2,
                                             .Consumers={
                                             {.Consumer="client-1", .Generation=0},
                                             {.Consumer="client-3", .Generation=7}
                                             },
                                             .Partitions={
                                             {.Id=0}
                                             },
                                             .AllPartitions={
                                             {.Id=0, .TabletId=Ctx->TabletId, .Children={},  .Parents={1}},
                                             {.Id=1, .TabletId=mockTabletId,  .Children={0}, .Parents={}}
                                             }});

    SendProposeTransactionRequest({.TxId=txId,
                                  .Configs=NHelpers::TConfigParams{
                                  .Tablet=tabletConfig,
                                  .Bootstrap=NHelpers::MakeBootstrapConfig(),
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});

    WaitForProposePartitionConfigResult();

    // the transaction is now in the WAIT_RS state in memory and PLANNED state in disk

    PQTabletRestart(*Ctx);
    ResetPipe();

    // Tablet PQ has not confirmed that she received TEvPlanStep. Therefore, the coordinator will send it again
    SendPlanStep({.Step=100, .TxIds={txId}});

    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId, .Target=Ctx->TabletId, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    tablet->SendReadSetAck(*Ctx->Runtime, {.Step=100, .TxId=txId, .Source=Ctx->TabletId});
    WaitReadSetAck(*tablet, {.Step=100, .TxId=txId, .Source=mockTabletId, .Target=Ctx->TabletId, .Consumer=Ctx->TabletId});
}

Y_UNIT_TEST_F(One_Tablet_For_All_Partitions, TPQTabletFixture)
{
    const ui64 txId = 67890;

    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    auto tabletConfig = NHelpers::MakeConfig({.Version=2,
                                             .Consumers={
                                             {.Consumer="client-1", .Generation=0},
                                             {.Consumer="client-3", .Generation=7}
                                             },
                                             .Partitions={
                                             {.Id=0},
                                             {.Id=1},
                                             {.Id=2}
                                             },
                                             .AllPartitions={
                                             {.Id=0, .TabletId=Ctx->TabletId, .Children={1, 2},  .Parents={}},
                                             {.Id=1, .TabletId=Ctx->TabletId, .Children={}, .Parents={0}},
                                             {.Id=2, .TabletId=Ctx->TabletId, .Children={}, .Parents={0}}
                                             }});

    SendProposeTransactionRequest({.TxId=txId,
                                  .Configs=NHelpers::TConfigParams{
                                  .Tablet=tabletConfig,
                                  .Bootstrap=NHelpers::MakeBootstrapConfig(),
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});

    WaitForProposePartitionConfigResult(2);

    // the transaction is now in the WAIT_RS state in memory and PLANNED state in disk

    PQTabletRestart(*Ctx);
    ResetPipe();

    // Tablet PQ has not confirmed that she received TEvPlanStep. Therefore, the coordinator will send it again
    SendPlanStep({.Step=100, .TxIds={txId}});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});
}

Y_UNIT_TEST_F(One_New_Partition_In_Another_Tablet, TPQTabletFixture)
{
    const ui64 txId = 67890;
    const ui64 mockTabletId = 22222;

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    auto tabletConfig = NHelpers::MakeConfig({.Version=2,
                                             .Consumers={
                                             {.Consumer="client-1", .Generation=0},
                                             {.Consumer="client-3", .Generation=7}
                                             },
                                             .Partitions={
                                             {.Id=0},
                                             {.Id=1},
                                             },
                                             .AllPartitions={
                                             {.Id=0, .TabletId=Ctx->TabletId, .Children={1, 2}, .Parents={}},
                                             {.Id=1, .TabletId=Ctx->TabletId, .Children={}, .Parents={0}},
                                             {.Id=2, .TabletId=mockTabletId,  .Children={}, .Parents={0}}
                                             }});

    SendProposeTransactionRequest({.TxId=txId,
                                  .Configs=NHelpers::TConfigParams{
                                  .Tablet=tabletConfig,
                                  .Bootstrap=NHelpers::MakeBootstrapConfig(),
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});

    WaitForProposePartitionConfigResult(2);

    // the transaction is now in the WAIT_RS state in memory and PLANNED state in disk

    PQTabletRestart(*Ctx);
    ResetPipe();

    // Tablet PQ has not confirmed that she received TEvPlanStep. Therefore, the coordinator will send it again
    SendPlanStep({.Step=100, .TxIds={txId}});

    // TEvReadSet от владельца партиции 2
    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId, .Target=Ctx->TabletId, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    tablet->SendReadSetAck(*Ctx->Runtime, {.Step=100, .TxId=txId, .Source=Ctx->TabletId});

    // Deferred TEvReadSetAck is flushed by the WRITE_TX cycle without a follow-up ProposeTransaction.
    WaitReadSetAck(*tablet, {.Step=100, .TxId=txId, .Source=mockTabletId, .Target=Ctx->TabletId, .Consumer=Ctx->TabletId});
}

Y_UNIT_TEST_F(All_New_Partitions_In_Another_Tablet, TPQTabletFixture)
{
    const ui64 txId = 67890;
    const ui64 mockTabletId = 22222;

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    auto tabletConfig = NHelpers::MakeConfig({.Version=2,
                                             .Consumers={
                                             {.Consumer="client-1", .Generation=0},
                                             {.Consumer="client-3", .Generation=7}
                                             },
                                             .Partitions={
                                             {.Id=0},
                                             {.Id=1},
                                             },
                                             .AllPartitions={
                                             {.Id=0, .TabletId=Ctx->TabletId, .Children={}, .Parents={2}},
                                             {.Id=1, .TabletId=Ctx->TabletId, .Children={}, .Parents={2}},
                                             {.Id=2, .TabletId=mockTabletId,  .Children={0, 1}, .Parents={}}
                                             }});

    SendProposeTransactionRequest({.TxId=txId,
                                  .Configs=NHelpers::TConfigParams{
                                  .Tablet=tabletConfig,
                                  .Bootstrap=NHelpers::MakeBootstrapConfig(),
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});

    WaitForProposePartitionConfigResult(2);

    // the transaction is now in the WAIT_RS state in memory and PLANNED state in disk

    PQTabletRestart(*Ctx);
    ResetPipe();

    // Tablet PQ has not confirmed that she received TEvPlanStep. Therefore, the coordinator will send it again
    SendPlanStep({.Step=100, .TxIds={txId}});

    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId, .Target=Ctx->TabletId, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    tablet->SendReadSetAck(*Ctx->Runtime, {.Step=100, .TxId=txId, .Source=Ctx->TabletId});
    WaitReadSetAck(*tablet, {.Step=100, .TxId=txId, .Source=mockTabletId, .Target=Ctx->TabletId, .Consumer=Ctx->TabletId});
}

Y_UNIT_TEST_F(Huge_ProposeTransacton, TPQTabletFixture)
{
    const ui64 mockTabletId = 22222;

    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    auto tabletConfig = NHelpers::MakeConfig({.Version=2,
                                             .Consumers={
                                             {.Consumer="client-1", .Generation=0},
                                             {.Consumer="client-3", .Generation=7},
                                             },
                                             .Partitions={
                                             {.Id=0},
                                             {.Id=1},
                                             },
                                             .AllPartitions={
                                             {.Id=0, .TabletId=Ctx->TabletId, .Children={}, .Parents={2}},
                                             {.Id=1, .TabletId=Ctx->TabletId, .Children={}, .Parents={2}},
                                             {.Id=2, .TabletId=mockTabletId,  .Children={0, 1}, .Parents={}}
                                             },
                                             .HugeConfig = true});

    const ui64 txId_1 = 67890;
    SendProposeTransactionRequest({.TxId=txId_1,
                                  .Configs=NHelpers::TConfigParams{
                                  .Tablet=tabletConfig,
                                  .Bootstrap=NHelpers::MakeBootstrapConfig(),
                                  }});
    WaitProposeTransactionResponse({.TxId=txId_1,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    const ui64 txId_2 = 67891;
    SendProposeTransactionRequest({.TxId=txId_2,
                                  .Configs=NHelpers::TConfigParams{
                                  .Tablet=tabletConfig,
                                  .Bootstrap=NHelpers::MakeBootstrapConfig(),
                                  }});
    WaitProposeTransactionResponse({.TxId=txId_2,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    PQTabletRestart(*Ctx);
    ResetPipe();

    // Tablet PQ has not confirmed that she received TEvPlanStep. Therefore, the coordinator will send it again
    SendPlanStep({.Step=100, .TxIds={txId_1, txId_2}});

    //WaitPlanStepAck({.Step=100, .TxIds={txId_1, txId_2}});
    //WaitPlanStepAccepted({.Step=100});
}

Y_UNIT_TEST_F(Duplicate_ReadSetAck_From_Same_Recipient, TPQTabletFixture)
{
    const ui64 txId = 67890;
    const ui64 tabletB = 22222;
    const ui64 tabletC = 33333;

    NHelpers::TPQTabletMock* mockB = CreatePQTabletMock(tabletB);
    NHelpers::TPQTabletMock* mockC = CreatePQTabletMock(tabletC);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    SendProposeTransactionRequest({.TxId=txId,
                                  .Receivers={tabletB, tabletC},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});
    WaitForCalcPredicateResult();

    WaitReadSet(*mockB, {.Step=100, .TxId=txId, .Source=Ctx->TabletId, .Target=tabletB,
                         .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});
    WaitReadSet(*mockC, {.Step=100, .TxId=txId, .Source=Ctx->TabletId, .Target=tabletC,
                         .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    auto sendReadSetAck = [&](ui64 consumerTabletId) {
        auto event = std::make_unique<TEvTxProcessing::TEvReadSetAck>(
            100, txId, Ctx->TabletId, consumerTabletId, consumerTabletId, 0);
        SendToPipe(Ctx->Edge, event.release());
    };

    const TString deleteTxKeyFrom = GetTxKey(txId);
    const TString deleteTxKeyTo = GetTxKey(txId + 1);
    bool sawPrematureDelete = false;
    auto prev = Ctx->Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
        if (auto* msg = event->CastAsLocal<TEvKeyValue::TEvRequest>()) {
            if (msg->Record.HasCookie() && msg->Record.GetCookie() == WRITE_TX_COOKIE) {
                for (const auto& cmd : msg->Record.GetCmdDeleteRange()) {
                    if (cmd.HasRange() &&
                        cmd.GetRange().GetFrom() == deleteTxKeyFrom &&
                        cmd.GetRange().GetTo() == deleteTxKeyTo)
                    {
                        sawPrematureDelete = true;
                    }
                }
            }
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    });

    sendReadSetAck(tabletB);
    sendReadSetAck(tabletB);

    // Without dedup, two acks from B would satisfy HaveAllRecipientsReceive (2/2) even though C
    // has not acked. DeleteTx must not run; give WRITE_TX a chance to flush if it were queued.
    TDispatchOptions options;
    options.CustomFinalCondition = [&]() {
        return sawPrematureDelete;
    };
    Ctx->Runtime->DispatchEvents(options, TDuration::Seconds(2));
    UNIT_ASSERT(!sawPrematureDelete);
    AssertTransactionInKV(txId);
    Ctx->Runtime->SetObserverFunc(prev);

    sendReadSetAck(tabletC);

    // Delete is persisted by a WRITE_TX cycle without a follow-up ProposeTransaction.
    WaitForTheTransactionToBeDeleted(txId);
}

Y_UNIT_TEST_F(TEvReadSet_For_A_Non_Existent_Tablet, TPQTabletFixture)
{
    const ui64 txId = 67890;
    const ui64 mockTabletId = MakeTabletID(false, 22222);

    // We are simulating a situation where the recipient of TEvReadSet has already completed a transaction
    // and has been deleted.
    //
    // To do this, we "forget" the TEvReadSet from the PQ tablet and send TEvClientConnected with the Dead flag
    // instead of TEvReadSetAck.
    TTestActorRuntimeBase::TEventFilter prev;
    auto filter = [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) -> bool {
        if (auto* msg = event->CastAsLocal<TEvTxProcessing::TEvReadSet>()) {
            const auto& r = msg->Record;
            if (r.GetTabletSource() == Ctx->TabletId) {
                runtime.Send(event->Sender,
                             Ctx->Edge,
                             new TEvTabletPipe::TEvClientConnected(mockTabletId,
                                                                   NKikimrProto::ERROR,
                                                                   event->Sender,
                                                                   TActorId(),
                                                                   true,
                                                                   true, // Dead
                                                                   0));
                return true;
            }
        }
        return false;
    };
    prev = Ctx->Runtime->SetEventFilter(filter);

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={mockTabletId}, .Receivers={mockTabletId},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});

    // We are sending a TEvReadSet so that the PQ tablet can complete the transaction.
    tablet->SendReadSet(*Ctx->Runtime,
                        {.Step=100, .TxId=txId, .Target=Ctx->TabletId, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    WaitProposeTransactionResponse({.TxId=txId, .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    // Instead of TEvReadSetAck, the PQ tablet will receive TEvClientConnected with the Dead flag. The transaction
    // will switch from the WAIT_RS_ACKS state to the DELETING state and be deleted without a follow-up propose.
    WaitForTheTransactionToBeDeleted(txId);
}

Y_UNIT_TEST_F(Limit_On_The_Number_Of_Transactons, TPQTabletFixture)
{
    const ui64 mockTabletId = MakeTabletID(false, 22222);
    const ui64 txId = 67890;

    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    for (ui64 i = 0; i < 1002; ++i) {
        SendProposeTransactionRequest({.TxId=txId + i,
                                      .Senders={mockTabletId}, .Receivers={mockTabletId},
                                      .TxOps={
                                      {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                      }});
    }

    size_t preparedCount = 0;
    size_t overloadedCount = 0;

    for (ui64 i = 0; i < 1002; ++i) {
        auto event = Ctx->Runtime->GrabEdgeEvent<TEvPersQueue::TEvProposeTransactionResult>();
        UNIT_ASSERT(event != nullptr);

        UNIT_ASSERT(event->Record.HasStatus());

        const auto status = event->Record.GetStatus();
        switch (status) {
        case NKikimrPQ::TEvProposeTransactionResult::PREPARED:
            ++preparedCount;
            break;
        case NKikimrPQ::TEvProposeTransactionResult::OVERLOADED:
            ++overloadedCount;
            break;
        default:
            UNIT_FAIL("unexpected transaction status " << NKikimrPQ::TEvProposeTransactionResult_EStatus_Name(status));
        }
    }

    UNIT_ASSERT_EQUAL(preparedCount, 1000);
    UNIT_ASSERT_EQUAL(overloadedCount, 2);
}

Y_UNIT_TEST_F(DeleteTx_Without_FollowUp_Propose_Complete, TPQTabletFixture)
{
    // A1-N1: non-Kafka COMPLETE tx is deleted from KV without a follow-up ProposeTransaction.
    const ui64 txId = 67890;
    const ui64 mockTabletId = 22222;

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={mockTabletId}, .Receivers={mockTabletId},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});

    WaitReadSet(*tablet, {.Step=100, .TxId=txId, .Source=Ctx->TabletId, .Target=mockTabletId,
                          .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});
    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId, .Target=Ctx->TabletId,
                                        .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    tablet->SendReadSetAck(*Ctx->Runtime, {.Step=100, .TxId=txId, .Source=Ctx->TabletId});
    WaitForTheTransactionToBeDeleted(txId);
}

Y_UNIT_TEST_F(DeleteTx_Without_FollowUp_Propose_Abort, TPQTabletFixture)
{
    // A1-N2: ABORTED tx is deleted from KV without a follow-up ProposeTransaction.
    const ui64 txId = 67890;
    const ui64 mockTabletId = 22222;

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={mockTabletId}, .Receivers={mockTabletId},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});

    WaitReadSet(*tablet, {.Step=100, .TxId=txId, .Source=Ctx->TabletId, .Target=mockTabletId,
                          .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});
    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId, .Target=Ctx->TabletId,
                                        .Decision=NKikimrTx::TReadSetData::DECISION_ABORT});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});

    tablet->SendReadSetAck(*Ctx->Runtime, {.Step=100, .TxId=txId, .Source=Ctx->TabletId});
    WaitForTheTransactionToBeDeleted(txId);
}

Y_UNIT_TEST_F(DeleteTx_Without_FollowUp_Propose_Frees_Slot_For_New_Propose, TPQTabletFixture)
{
    // A1-N3: completed txs leave DELETING without a propose; slots free so a new propose is PREPARED.
    const ui64 mockTabletId = 22222;
    const ui64 baseTxId = 67890;

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    for (ui64 i = 0; i < 3; ++i) {
        const ui64 txId = baseTxId + i;
        const ui64 step = 100 + i;

        SendProposeTransactionRequest({.TxId=txId,
                                      .Senders={mockTabletId}, .Receivers={mockTabletId},
                                      .TxOps={
                                      {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                      }});
        WaitProposeTransactionResponse({.TxId=txId,
                                       .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

        SendPlanStep({.Step=step, .TxIds={txId}});

        WaitReadSet(*tablet, {.Step=step, .TxId=txId, .Source=Ctx->TabletId, .Target=mockTabletId,
                              .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});
        tablet->SendReadSet(*Ctx->Runtime, {.Step=step, .TxId=txId, .Target=Ctx->TabletId,
                                            .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

        WaitProposeTransactionResponse({.TxId=txId,
                                       .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

        tablet->ReadSetAck.Clear();
        tablet->SendReadSetAck(*Ctx->Runtime, {.Step=step, .TxId=txId, .Source=Ctx->TabletId});
        WaitForTheTransactionToBeDeleted(txId);
    }

    const ui64 nextTxId = baseTxId + 3;
    SendProposeTransactionRequest({.TxId=nextTxId,
                                  .Senders={mockTabletId}, .Receivers={mockTabletId},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=nextTxId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});
}

Y_UNIT_TEST_F(DeleteTx_Without_FollowUp_Propose_Batch, TPQTabletFixture)
{
    // A1-N4: several DeleteTxs flush without a follow-up ProposeTransaction.
    const ui64 mockTabletId = 22222;
    const ui64 baseTxId = 67890;
    constexpr ui64 txCount = 3;

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    for (ui64 i = 0; i < txCount; ++i) {
        const ui64 txId = baseTxId + i;
        const ui64 step = 100 + i;

        SendProposeTransactionRequest({.TxId=txId,
                                      .Senders={mockTabletId}, .Receivers={mockTabletId},
                                      .TxOps={
                                      {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                      }});
        WaitProposeTransactionResponse({.TxId=txId,
                                       .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

        SendPlanStep({.Step=step, .TxIds={txId}});

        WaitReadSet(*tablet, {.Step=step, .TxId=txId, .Source=Ctx->TabletId, .Target=mockTabletId,
                              .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});
        tablet->SendReadSet(*Ctx->Runtime, {.Step=step, .TxId=txId, .Target=Ctx->TabletId,
                                            .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

        WaitProposeTransactionResponse({.TxId=txId,
                                       .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});
    }

    for (ui64 i = 0; i < txCount; ++i) {
        const ui64 txId = baseTxId + i;
        const ui64 step = 100 + i;
        tablet->ReadSetAck.Clear();
        tablet->SendReadSetAck(*Ctx->Runtime, {.Step=step, .TxId=txId, .Source=Ctx->TabletId});
    }

    for (ui64 i = 0; i < txCount; ++i) {
        WaitForTheTransactionToBeDeleted(baseTxId + i);
    }
}

Y_UNIT_TEST_F(DeleteTx_Without_FollowUp_Propose_Kafka, TPQTabletFixture)
{
    // A1-N5: Kafka commit still deletes the tx without a follow-up ProposeTransaction.
    NKafka::TProducerInstanceId producerInstanceId = {1, 0};
    const ui64 txId = 67890;

    PQTabletPrepare({.partitions=1}, {}, *Ctx);
    EnsurePipeExist();

    TString ownerCookie = CreateSupportivePartitionForKafka(producerInstanceId);
    SendKafkaTxnWriteRequest(producerInstanceId, ownerCookie);
    CommitKafkaTransaction(producerInstanceId, txId);

    WaitForTheTransactionToBeDeleted(txId);
}

Y_UNIT_TEST_F(DeleteTx_With_Concurrent_Propose, TPQTabletFixture)
{
    // A1-N6: DeleteTxs and a new ProposeTransaction share one WRITE_TX persist.
    const ui64 txId = 67890;
    const ui64 nextTxId = txId + 1;
    const ui64 unknownTxId = 424299;
    const ui64 mockTabletId = 22222;
    const TString deleteTxKeyFrom = GetTxKey(txId);
    const TString deleteTxKeyTo = GetTxKey(txId + 1);
    const TString proposeTxKey = GetTxKey(nextTxId);

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={mockTabletId}, .Receivers={mockTabletId},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});

    WaitReadSet(*tablet, {.Step=100, .TxId=txId, .Source=Ctx->TabletId, .Target=mockTabletId,
                          .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});
    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId, .Target=Ctx->TabletId,
                                        .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    // Hold the next WRITE_TX so DeleteTx and the new propose both queue while WriteTxsInProgress.
    TVector<TAutoPtr<IEventHandle>> heldWriteTxRequests;
    bool holdWriteTx = true;
    bool foundCombinedPersist = false;
    auto prev = Ctx->Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
        if (auto* msg = event->CastAsLocal<TEvKeyValue::TEvRequest>()) {
            if (msg->Record.HasCookie() && msg->Record.GetCookie() == WRITE_TX_COOKIE) {
                if (holdWriteTx) {
                    heldWriteTxRequests.push_back(event);
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }

                bool hasDelete = false;
                bool hasProposeWrite = false;
                for (const auto& cmd : msg->Record.GetCmdDeleteRange()) {
                    if (cmd.HasRange() &&
                        cmd.GetRange().GetFrom() == deleteTxKeyFrom &&
                        cmd.GetRange().GetTo() == deleteTxKeyTo)
                    {
                        hasDelete = true;
                    }
                }
                for (const auto& cmd : msg->Record.GetCmdWrite()) {
                    if (cmd.GetKey() == proposeTxKey) {
                        hasProposeWrite = true;
                    }
                }
                if (hasDelete && hasProposeWrite) {
                    foundCombinedPersist = true;
                }
            }
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    });

    // Start a WRITE_TX cycle (deferred RS ack) and keep it in flight.
    tablet->SendReadSet(*Ctx->Runtime, {.Step=200, .TxId=unknownTxId, .Target=Ctx->TabletId,
                                        .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});
    {
        TDispatchOptions options;
        options.CustomFinalCondition = [&]() {
            return !heldWriteTxRequests.empty();
        };
        UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    }

    SendProposeTransactionRequest({.TxId=nextTxId,
                                  .Senders={mockTabletId}, .Receivers={mockTabletId},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    tablet->SendReadSetAck(*Ctx->Runtime, {.Step=100, .TxId=txId, .Source=Ctx->TabletId});

    holdWriteTx = false;
    for (auto& held : heldWriteTxRequests) {
        Ctx->Runtime->Send(held.Release());
    }
    heldWriteTxRequests.clear();

    WaitProposeTransactionResponse({.TxId=nextTxId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});
    WaitForTheTransactionToBeDeleted(txId);

    {
        TDispatchOptions options;
        options.CustomFinalCondition = [&]() {
            return foundCombinedPersist;
        };
        UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    }
    Ctx->Runtime->SetObserverFunc(prev);

    UNIT_ASSERT(foundCombinedPersist);
}

Y_UNIT_TEST_F(Deferred_ReadSetAck_For_Unknown_Tx_Without_Propose, TPQTabletFixture)
{
    // B1-N1: unknown TEvReadSet is acked after WRITE_TX without a follow-up ProposeTransaction.
    const ui64 unknownTxId = 424242;
    const ui64 mockTabletId = 22222;

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=unknownTxId, .Target=Ctx->TabletId,
                                        .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    WaitReadSetAck(*tablet, {.Step=100, .TxId=unknownTxId, .Source=mockTabletId,
                             .Target=Ctx->TabletId, .Consumer=Ctx->TabletId});
}

Y_UNIT_TEST_F(Deferred_ReadSetAck_Waits_For_Successful_WriteTx, TPQTabletFixture)
{
    // B1-N2: stale-leader gate — ack is not sent until WRITE_TX succeeds.
    const ui64 unknownTxId = 424243;
    const ui64 mockTabletId = 22222;

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    TVector<TAutoPtr<IEventHandle>> heldRequests;
    bool holdWriteTx = true;
    auto prev = Ctx->Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
        if (holdWriteTx) {
            if (auto* msg = event->CastAsLocal<TEvKeyValue::TEvRequest>()) {
                if (msg->Record.HasCookie() && msg->Record.GetCookie() == WRITE_TX_COOKIE) {
                    heldRequests.push_back(event);
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }
            }
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    });

    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=unknownTxId, .Target=Ctx->TabletId,
                                        .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    {
        TDispatchOptions options;
        options.CustomFinalCondition = [&]() {
            return !heldRequests.empty();
        };
        UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    }

    WaitForNoReadSetAck(*tablet);

    holdWriteTx = false;
    for (auto& held : heldRequests) {
        Ctx->Runtime->Send(held.Release());
    }
    heldRequests.clear();
    Ctx->Runtime->SetObserverFunc(prev);

    WaitReadSetAck(*tablet, {.Step=100, .TxId=unknownTxId, .Source=mockTabletId,
                             .Target=Ctx->TabletId, .Consumer=Ctx->TabletId});
}

Y_UNIT_TEST_F(Deferred_ReadSetAck_While_WriteTx_In_Progress, TPQTabletFixture)
{
    // B1-N3: unknown RS during an in-flight WRITE_TX is flushed after that cycle ends.
    const ui64 txId = 67890;
    const ui64 unknownTxId = 424244;
    const ui64 mockTabletId = 22222;

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={mockTabletId}, .Receivers={mockTabletId},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});

    // Catch the propose WRITE_TX and inject an unknown RS while it is in progress.
    TVector<TAutoPtr<IEventHandle>> heldResponses;
    bool holdWriteTxResponse = true;
    bool seenWriteTxRequest = false;
    auto prev = Ctx->Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
        if (auto* msg = event->CastAsLocal<TEvKeyValue::TEvRequest>()) {
            if (msg->Record.HasCookie() && msg->Record.GetCookie() == WRITE_TX_COOKIE) {
                seenWriteTxRequest = true;
            }
        }
        if (holdWriteTxResponse && seenWriteTxRequest) {
            if (auto* msg = event->CastAsLocal<TEvKeyValue::TEvResponse>()) {
                if (msg->Record.HasCookie() && msg->Record.GetCookie() == WRITE_TX_COOKIE) {
                    heldResponses.push_back(event);
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }
            }
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    });

    {
        TDispatchOptions options;
        options.CustomFinalCondition = [&]() {
            return seenWriteTxRequest;
        };
        UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    }

    tablet->SendReadSet(*Ctx->Runtime, {.Step=200, .TxId=unknownTxId, .Target=Ctx->TabletId,
                                        .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    {
        TDispatchOptions options;
        options.CustomFinalCondition = [&]() {
            return !heldResponses.empty();
        };
        UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    }

    WaitForNoReadSetAck(*tablet);

    holdWriteTxResponse = false;
    for (auto& held : heldResponses) {
        Ctx->Runtime->Send(held.Release());
    }
    heldResponses.clear();
    Ctx->Runtime->SetObserverFunc(prev);

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});
    WaitReadSetAck(*tablet, {.Step=200, .TxId=unknownTxId, .Source=mockTabletId,
                             .Target=Ctx->TabletId, .Consumer=Ctx->TabletId});
}

Y_UNIT_TEST_F(Deferred_ReadSetAck_Multiple_Unknown_Without_Propose, TPQTabletFixture)
{
    // B1-N4: several deferred unknown RS acks flush without ProposeTransaction.
    const ui64 mockTabletId = 22222;
    const ui64 baseTxId = 424250;

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    THashSet<ui64> ackedTxIds;
    auto prev = Ctx->Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
        if (auto* msg = event->CastAsLocal<TEvTxProcessing::TEvReadSetAck>()) {
            if (msg->Record.GetTabletDest() == Ctx->TabletId) {
                ackedTxIds.insert(msg->Record.GetTxId());
            }
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    });

    for (ui64 i = 0; i < 3; ++i) {
        tablet->SendReadSet(*Ctx->Runtime, {.Step=100 + i, .TxId=baseTxId + i, .Target=Ctx->TabletId,
                                            .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});
    }

    TDispatchOptions options;
    options.CustomFinalCondition = [&]() {
        return ackedTxIds.size() >= 3;
    };
    UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    Ctx->Runtime->SetObserverFunc(prev);

    for (ui64 i = 0; i < 3; ++i) {
        UNIT_ASSERT(ackedTxIds.contains(baseTxId + i));
    }
}

Y_UNIT_TEST_F(Known_ReadSet_Path_Unchanged, TPQTabletFixture)
{
    // B1-N5: known-tx TEvReadSet path still completes and deletes without relying on deferred-only flush.
    const ui64 txId = 67890;
    const ui64 mockTabletId = 22222;

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={mockTabletId}, .Receivers={mockTabletId},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});

    WaitReadSet(*tablet, {.Step=100, .TxId=txId, .Source=Ctx->TabletId, .Target=mockTabletId,
                          .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});
    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId, .Target=Ctx->TabletId,
                                        .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    WaitReadSetAck(*tablet, {.Step=100, .TxId=txId, .Source=mockTabletId,
                             .Target=Ctx->TabletId, .Consumer=Ctx->TabletId});

    tablet->SendReadSetAck(*Ctx->Runtime, {.Step=100, .TxId=txId, .Source=Ctx->TabletId});
    WaitForTheTransactionToBeDeleted(txId);
}

Y_UNIT_TEST_F(Deferred_ReadSetAck_From_Silent_Peer_Without_Propose, TPQTabletFixture)
{
    // B1-N6: simplified PQ↔PQ ring — peer RS for an absent local tx is acked without propose.
    const ui64 peerTxId = 1412647829058208ull;
    const ui64 mockTabletId = 22222;

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    tablet->SendReadSet(*Ctx->Runtime, {.Step=1779169393560ull, .TxId=peerTxId, .Target=Ctx->TabletId,
                                        .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    WaitReadSetAck(*tablet, {.Step=1779169393560ull, .TxId=peerTxId, .Source=mockTabletId,
                             .Target=Ctx->TabletId, .Consumer=Ctx->TabletId});
}

Y_UNIT_TEST_F(PlanStepAck_For_Unknown_Waits_For_WriteTx, TPQTabletFixture)
{
    // За шагом, все txId которого неизвестны, наших транзакций нет, и ждать нам нечего. Подтвердить
    // такой шаг можно только после успешно завершённого цикла записи: он доказывает, что таблетка
    // всё ещё лидер. Иначе зафенченное поколение подтверждало бы шаги за живое.
    const ui64 unknownTxId = 424301;

    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    TVector<TAutoPtr<IEventHandle>> heldRequests;
    size_t acceptedCount = 0;
    bool holdWriteTx = true;
    auto prev = Ctx->Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
        if (event->CastAsLocal<TEvTxProcessing::TEvPlanStepAccepted>()) {
            ++acceptedCount;
        }
        if (holdWriteTx) {
            if (auto* msg = event->CastAsLocal<TEvKeyValue::TEvRequest>()) {
                if (msg->Record.HasCookie() && msg->Record.GetCookie() == WRITE_TX_COOKIE) {
                    heldRequests.push_back(event);
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }
            }
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    });

    SendPlanStep({.Step=100, .TxIds={unknownTxId}});

    // Таблетка сама начинает цикл записи, потому что иначе шаг подтверждать нечем
    {
        TDispatchOptions options;
        options.CustomFinalCondition = [&]() {
            return !heldRequests.empty();
        };
        UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    }

    UNIT_ASSERT_VALUES_EQUAL(acceptedCount, 0u);

    holdWriteTx = false;
    for (auto& held : heldRequests) {
        Ctx->Runtime->Send(held.Release());
    }
    heldRequests.clear();
    Ctx->Runtime->SetObserverFunc(prev);

    WaitPlanStepAck({.Step=100, .TxIds={unknownTxId}});
    WaitPlanStepAccepted({.Step=100});
}

Y_UNIT_TEST_F(PlanStepAccepted_Order_Unknown_Before_Executed_Retransmit, TPQTabletFixture)
{
    // Медиатор ждёт подтверждений в возрастающем порядке шагов. Раньше подтверждение для шага с
    // неизвестными txId уходило сразу, а для шага с известной транзакцией - в конце транзакции,
    // из-за чего порядок переворачивался и голова очереди медиатора вставала.
    const ui64 txId = 67890;
    const ui64 unknownTxId = 424302;
    const ui64 mockTabletId = 22222;

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={mockTabletId}, .Receivers={mockTabletId},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=200, .TxIds={txId}});

    WaitReadSet(*tablet, {.Step=200, .TxId=txId, .Source=Ctx->TabletId, .Target=mockTabletId,
                          .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});
    tablet->SendReadSet(*Ctx->Runtime, {.Step=200, .TxId=txId, .Target=Ctx->TabletId,
                                        .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    // Drain the initial PlanStep ack/accepted for the known EXECUTED tx.
    WaitPlanStepAck({.Step=200, .TxIds={txId}});
    WaitPlanStepAccepted({.Step=200});

    // Транзакция остаётся в Txs (EXECUTED, ждёт подтверждений readset'ов), поэтому повторный шаг
    // попадает в ветку известной транзакции.
    TVector<TAutoPtr<IEventHandle>> heldRequests;
    size_t acceptedCount = 0;
    bool holdWriteTx = true;
    auto prev = Ctx->Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
        if (event->CastAsLocal<TEvTxProcessing::TEvPlanStepAccepted>()) {
            ++acceptedCount;
        }
        if (holdWriteTx) {
            if (auto* msg = event->CastAsLocal<TEvKeyValue::TEvRequest>()) {
                if (msg->Record.HasCookie() && msg->Record.GetCookie() == WRITE_TX_COOKIE) {
                    heldRequests.push_back(event);
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }
            }
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    });

    // Оба шага отправлены до ожидания: сначала младший с неизвестной транзакцией, потом повторный
    // шаг выполненной.
    SendPlanStep({.Step=100, .TxIds={unknownTxId}});
    SendPlanStep({.Step=200, .TxIds={txId}});

    // Шаг 200 уже ниже границы выполненного, но он стоит в очереди за шагом 100, поэтому пока не
    // завершится цикл записи, не уйдёт ни одно подтверждение
    {
        TDispatchOptions options;
        options.CustomFinalCondition = [&]() {
            return !heldRequests.empty();
        };
        UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    }

    UNIT_ASSERT_VALUES_EQUAL(acceptedCount, 0u);

    holdWriteTx = false;
    for (auto& held : heldRequests) {
        Ctx->Runtime->Send(held.Release());
    }
    heldRequests.clear();
    Ctx->Runtime->SetObserverFunc(prev);

    // GrabEdgeEvent отдаёт события в порядке доставки: 100 должен прийти раньше 200
    WaitPlanStepAccepted({.Step=100});
    WaitPlanStepAccepted({.Step=200});
    WaitPlanStepAck({.Step=100, .TxIds={unknownTxId}});
    WaitPlanStepAck({.Step=200, .TxIds={txId}});
}

Y_UNIT_TEST_F(PlanStep_Ack_Waits_For_Tx_Reusing_Step_And_TxId, TPQTabletFixture)
{
    // Транзакцию с тем же TxId могут пропоузить заново после того, как предыдущая выполнилась и была
    // удалена, и запланировать тем же шагом. Подтверждать шаг надо по выполнению новой транзакции.
    // Поэтому ждать нельзя по паре (ExecStep, ExecTxId): она уже стоит на этой паре.
    const ui64 txId = 67890;
    const ui64 mockTabletId = 22222;

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={mockTabletId}, .Receivers={mockTabletId},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});

    WaitReadSet(*tablet, {.Step=100, .TxId=txId, .Source=Ctx->TabletId, .Target=mockTabletId,
                          .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});
    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId, .Target=Ctx->TabletId,
                                        .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});
    WaitPlanStepAck({.Step=100, .TxIds={txId}});
    WaitPlanStepAccepted({.Step=100});

    WaitReadSetAck(*tablet, {.Step=100, .TxId=txId, .Source=mockTabletId,
                             .Target=Ctx->TabletId, .Consumer=Ctx->TabletId});
    tablet->SendReadSetAck(*Ctx->Runtime, {.Step=100, .TxId=txId, .Source=Ctx->TabletId});
    WaitForTheTransactionToBeDeleted(txId);

    size_t acceptedCount = 0;
    auto prev = Ctx->Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
        if (event->CastAsLocal<TEvTxProcessing::TEvPlanStepAccepted>()) {
            ++acceptedCount;
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    });

    // Та же транзакция и тот же шаг
    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={mockTabletId}, .Receivers={mockTabletId},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});

    // Транзакция дошла до ожидания readset'а, значит она ещё не выполнена и шаг подтверждать нельзя
    WaitReadSet(*tablet, {.Step=100, .TxId=txId, .Source=Ctx->TabletId, .Target=mockTabletId,
                          .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});

    UNIT_ASSERT_VALUES_EQUAL(acceptedCount, 0u);

    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId, .Target=Ctx->TabletId,
                                        .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});
    WaitPlanStepAck({.Step=100, .TxIds={txId}});
    WaitPlanStepAccepted({.Step=100});

    Ctx->Runtime->SetObserverFunc(prev);
}

Y_UNIT_TEST_F(PlanStepAccepted_Order_Pending_Tx_Before_Unknown, TPQTabletFixture)
{
    // Впереди в очереди шаг с нашей транзакцией, за ним шаг без наших транзакций. Второй отпускается
    // сразу после цикла записи, но отправить его подтверждение раньше первого нельзя.
    const ui64 txId = 67890;
    const ui64 unknownTxId = 424303;
    const ui64 mockTabletId = 22222;

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={mockTabletId}, .Receivers={mockTabletId},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    // Транзакция шага 100 встаёт в ожидание readset'а, поэтому шаг 100 ещё не подтверждён
    SendPlanStep({.Step=100, .TxIds={txId}});

    WaitReadSet(*tablet, {.Step=100, .TxId=txId, .Source=Ctx->TabletId, .Target=mockTabletId,
                          .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});

    SendPlanStep({.Step=200, .TxIds={unknownTxId}});

    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId, .Target=Ctx->TabletId,
                                        .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    WaitPlanStepAccepted({.Step=100});
    WaitPlanStepAccepted({.Step=200});
}

Y_UNIT_TEST_F(PlanStep_Unsorted_TxIds_With_Duplicates, TPQTabletFixture)
{
    // Медиатор склеивает в один шаг транзакции разных координаторов и сортирует их без дедупликации,
    // поэтому список txId может прийти в любом порядке и с дублями. Таблетка не должна от этого умирать.
    const ui64 txId_1 = 67890;
    const ui64 txId_2 = 67891;
    const ui64 mockTabletId = 22222;

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {{"consumer-1", true}, {"consumer-2", true}}, *Ctx);

    SendProposeTransactionRequest({.TxId=txId_1,
                                  .Senders={mockTabletId}, .Receivers={mockTabletId},
                                  .TxOps={
                                  {.Partition=0, .Consumer="consumer-1", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId_1,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendProposeTransactionRequest({.TxId=txId_2,
                                  .Senders={mockTabletId}, .Receivers={mockTabletId},
                                  .TxOps={
                                  {.Partition=0, .Consumer="consumer-2", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId_2,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId_2, txId_1, txId_2}});

    WaitReadSet(*tablet, {.Step=100, .TxId=txId_1, .Source=Ctx->TabletId, .Target=mockTabletId,
                          .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});
    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId_1, .Target=Ctx->TabletId,
                                        .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    WaitReadSet(*tablet, {.Step=100, .TxId=txId_2, .Source=Ctx->TabletId, .Target=mockTabletId,
                          .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});
    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId_2, .Target=Ctx->TabletId,
                                        .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    // Транзакции выполняются по возрастанию TxId, а не в порядке из сообщения
    WaitProposeTransactionResponse({.TxId=txId_1,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});
    WaitProposeTransactionResponse({.TxId=txId_2,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    // Подтверждение повторяет список из сообщения: координатор дедуплицирует по (txId, tabletId)
    WaitPlanStepAck({.Step=100, .TxIds={txId_2, txId_1, txId_2}});
    WaitPlanStepAccepted({.Step=100});
}

Y_UNIT_TEST_F(PlanStep_After_MaxStep_Is_Acked_Without_Planning, TPQTabletFixture)
{
    // Шаг больше MaxStep транзакции планировать нельзя: транзакция истечёт. Но шаг всё равно надо
    // подтвердить, иначе очередь медиатора встанет.
    const ui64 txId = 67890;
    const ui64 mockTabletId = 22222;
    const ui64 stepAfterMaxStep = Max<ui64>() - 1;

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={mockTabletId}, .Receivers={mockTabletId},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=stepAfterMaxStep, .TxIds={txId}});

    WaitPlanStepAck({.Step=stepAfterMaxStep, .TxIds={txId}});
    WaitPlanStepAccepted({.Step=stepAfterMaxStep});

    // Транзакция не запланирована: она выполняется по следующему шагу, попадающему в MaxStep
    SendPlanStep({.Step=100, .TxIds={txId}});

    WaitReadSet(*tablet, {.Step=100, .TxId=txId, .Source=Ctx->TabletId, .Target=mockTabletId,
                          .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});
    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId, .Target=Ctx->TabletId,
                                        .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    WaitPlanStepAck({.Step=100, .TxIds={txId}});
    WaitPlanStepAccepted({.Step=100});
}

Y_UNIT_TEST_F(PlanStep_Changed_While_WriteTx_Inflight_Is_Persisted, TPQTabletFixture)
{
    // Пока цикл WRITE_TX уже снят и лежит в полёте, медиатор присылает план-шаг, транзакция
    // доходит до выполнения и таблетка подтверждает шаг. Граница PlanStep/ExecStep должна
    // попасть в _txinfo: после рестарта она читается только оттуда, а медиатор шаг не повторит.
    //
    // Второй пропоуз — обычная параллельная транзакция, из-за неё и стартует цикл записи.
    // ReadSetAck второй таблетки не шлём: в проде он приходит позже, и до него удаления нет.
    const ui64 txId = 67890;
    const ui64 nextTxId = txId + 1;
    const ui64 mockTabletId = 22222;
    const ui64 step = 100;

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={mockTabletId}, .Receivers={mockTabletId},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    TVector<TAutoPtr<IEventHandle>> heldRequests;
    bool holdWriteTx = true;
    auto prev = Ctx->Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
        if (holdWriteTx) {
            if (auto* msg = event->CastAsLocal<TEvKeyValue::TEvRequest>()) {
                if (msg->Record.HasCookie() && msg->Record.GetCookie() == WRITE_TX_COOKIE) {
                    heldRequests.push_back(event);
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }
            }
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    });

    SendProposeTransactionRequest({.TxId=nextTxId,
                                  .Senders={mockTabletId}, .Receivers={mockTabletId},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    {
        TDispatchOptions options;
        options.CustomFinalCondition = [&]() {
            return !heldRequests.empty();
        };
        UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    }
    UNIT_ASSERT_VALUES_EQUAL(heldRequests.size(), 1u);

    // До план-шага в снимке граница подготовки таблетки, а не шаг этой транзакции.
    const NKikimrPQ::TTabletTxInfo snapshot = ParseTxWritesFromWriteTxRequest(
        heldRequests.front()->Get<TEvKeyValue::TEvRequest>()->Record);
    UNIT_ASSERT(snapshot.GetPlanStep() < step);
    UNIT_ASSERT(snapshot.GetExecStep() < step);

    SendPlanStep({.Step=step, .TxIds={txId}});

    WaitReadSet(*tablet, {.Step=step, .TxId=txId, .Source=Ctx->TabletId, .Target=mockTabletId,
                          .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});
    tablet->SendReadSet(*Ctx->Runtime, {.Step=step, .TxId=txId, .Target=Ctx->TabletId,
                                        .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});
    WaitPlanStepAck({.Step=step, .TxIds={txId}});
    WaitPlanStepAccepted({.Step=step});

    // Подтверждение ушло, пока снимок ещё в полёте. Второго цикла записи быть не должно:
    // удаление начнётся только после ReadSetAck.
    UNIT_ASSERT_VALUES_EQUAL(heldRequests.size(), 1u);

    holdWriteTx = false;
    for (auto& held : heldRequests) {
        Ctx->Runtime->Send(held.Release());
    }
    heldRequests.clear();
    Ctx->Runtime->SetObserverFunc(prev);

    WaitProposeTransactionResponse({.TxId=nextTxId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    {
        TDispatchOptions options;
        Ctx->Runtime->DispatchEvents(options, TDuration::MilliSeconds(500));
    }

    const NKikimrPQ::TTabletTxInfo info = GetTxWritesFromKV();
    UNIT_ASSERT_VALUES_EQUAL(info.GetPlanStep(), step);
    UNIT_ASSERT_VALUES_EQUAL(info.GetPlanTxId(), txId);
    UNIT_ASSERT_VALUES_EQUAL(info.GetExecStep(), step);
    UNIT_ASSERT_VALUES_EQUAL(info.GetExecTxId(), txId);
}

Y_UNIT_TEST_F(Kafka_Transaction_Supportive_Partitions_Should_Be_Deleted_After_Timeout, TPQTabletFixture)
{
    NKafka::TProducerInstanceId producerInstanceId = {1, 0};
    PQTabletPrepare({.partitions=1}, {}, *Ctx);
    EnsurePipeExist();
    TString ownerCookie = CreateSupportivePartitionForKafka(producerInstanceId);

    // send data to create blobs for supportive partitions
    SendKafkaTxnWriteRequest(producerInstanceId, ownerCookie);

    // validate supportive partition was created
    WaitForExactSupportivePartitionsCount(1);
    auto txInfo = GetTxWritesFromKV();
    UNIT_ASSERT_VALUES_EQUAL(txInfo.TxWritesSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(txInfo.GetTxWrites(0).GetKafkaTransaction(), true);

    // increment time till after kafka txn timeout
    ui64 kafkaTxnTimeoutMs = Ctx->Runtime->GetAppData(0).KafkaProxyConfig.GetTransactionTimeoutMs()
        + KAFKA_TRANSACTION_DELETE_DELAY_MS;
    Ctx->Runtime->AdvanceCurrentTime(TDuration::MilliSeconds(kafkaTxnTimeoutMs + 1));
    SendToPipe(Ctx->Edge, MakeHolder<TEvents::TEvWakeup>().Release());

    // wait till supportive partition for this kafka transaction is deleted
    WaitForExactSupportivePartitionsCount(0);
}

Y_UNIT_TEST_F(Kafka_Transaction_Supportive_Partitions_Should_Be_Deleted_With_Delete_Partition_Done_Event_Drop, TPQTabletFixture)
{
    NKafka::TProducerInstanceId producerInstanceId = {1, 0};
    PQTabletPrepare({.partitions=1}, {}, *Ctx);
    EnsurePipeExist();
    TString ownerCookie = CreateSupportivePartitionForKafka(producerInstanceId);

    // send data to create blobs for supportive partitions
    SendKafkaTxnWriteRequest(producerInstanceId, ownerCookie);

    // validate supportive partition was created
    WaitForExactSupportivePartitionsCount(1);
    auto txInfo = GetTxWritesFromKV();
    UNIT_ASSERT_VALUES_EQUAL(txInfo.TxWritesSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(txInfo.GetTxWrites(0).GetKafkaTransaction(), true);

    // increment time till after kafka txn timeout
    ui64 kafkaTxnTimeoutMs = Ctx->Runtime->GetAppData(0).KafkaProxyConfig.GetTransactionTimeoutMs()
        + KAFKA_TRANSACTION_DELETE_DELAY_MS;
    Ctx->Runtime->AdvanceCurrentTime(TDuration::MilliSeconds(kafkaTxnTimeoutMs + 1));
    SendToPipe(Ctx->Edge, MakeHolder<TEvents::TEvWakeup>().Release());
    TAutoPtr<TEvPQ::TEvDeletePartitionDone> deleteDoneEvent;
    bool seenEvent = false;
    // add observer for TEvPQ::TEvDeletePartitionDone request and skip it
    AddOneTimeEventObserver<TEvPQ::TEvDeletePartitionDone>(seenEvent, 1, [](TAutoPtr<IEventHandle>&) {
        return TTestActorRuntimeBase::EEventAction::DROP;
    });
    TDispatchOptions options;
    options.CustomFinalCondition = [&seenEvent]() {return seenEvent;};
    UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    PQTabletRestart(*Ctx);
    ResetPipe();
    // check that that our expired transaction has been deleted
    WaitForExactTxWritesCount(0);
}

Y_UNIT_TEST_F(Non_Kafka_Transaction_Supportive_Partitions_Should_Not_Be_Deleted_After_Timeout, TPQTabletFixture)
{
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    // create Topic API transaction
    SyncGetOwnership({.Partition=0,
                     .WriteId=TWriteId{0, 3},
                     .NeedSupportivePartition=true,
                     .Owner=DEFAULT_OWNER,
                     .Cookie=4},
                     {.Cookie=4,
                     .Status=NMsgBusProxy::MSTATUS_OK});
    auto txInfo = GetTxWritesFromKV();
    UNIT_ASSERT_VALUES_EQUAL(txInfo.TxWritesSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(txInfo.GetTxWrites(0).GetKafkaTransaction(), false);

    // create Kafka transaction
    CreateSupportivePartitionForKafka({1, 0});
    auto txInfo2 = GetTxWritesFromKV();
    UNIT_ASSERT_VALUES_EQUAL(txInfo2.TxWritesSize(), 2);

    // increment time till after kafka txn timeout
    ui64 kafkaTxnTimeoutMs = Ctx->Runtime->GetAppData(0).KafkaProxyConfig.GetTransactionTimeoutMs()
        + KAFKA_TRANSACTION_DELETE_DELAY_MS;
    Ctx->Runtime->AdvanceCurrentTime(TDuration::MilliSeconds(kafkaTxnTimeoutMs + 1));
    SendToPipe(Ctx->Edge, MakeHolder<TEvents::TEvWakeup>().Release());

    // wait till supportive partition for this kafka transaction is deleted
    auto txInfo3 = WaitForExactTxWritesCount(1);
    UNIT_ASSERT_VALUES_EQUAL(txInfo3.GetTxWrites(0).GetKafkaTransaction(), false);
}

Y_UNIT_TEST_F(In_Kafka_Txn_Only_Supportive_Partitions_That_Exceeded_Timeout_Should_Be_Deleted, TPQTabletFixture)
{
    NKafka::TProducerInstanceId producerInstanceId1 = {1, 0};
    NKafka::TProducerInstanceId producerInstanceId2 = {2, 0};
    PQTabletPrepare({.partitions=1}, {}, *Ctx);
    EnsurePipeExist();

    // create first kafka-transacition and write data to it
    TString ownerCookie1 = CreateSupportivePartitionForKafka(producerInstanceId1);
    SendKafkaTxnWriteRequest(producerInstanceId1, ownerCookie1);
    WaitForExactSupportivePartitionsCount(1);
    ResetPipe();

    // advance time to value strictly less then kafka transaction timeout
    ui64 testTimeAdvanceMs = KAFKA_TRANSACTION_DELETE_DELAY_MS / 2;
    Ctx->Runtime->AdvanceCurrentTime(TDuration::MilliSeconds(testTimeAdvanceMs));

    // create second kafka-transacition and write data to it
    EnsurePipeExist();
    TString ownerCookie2 = CreateSupportivePartitionForKafka(producerInstanceId2);
    SendKafkaTxnWriteRequest(producerInstanceId2, ownerCookie2);
    WaitForExactSupportivePartitionsCount(2);

    // increment time till after timeout for the first transaction
    Ctx->Runtime->AdvanceCurrentTime(TDuration::MilliSeconds(
        Ctx->Runtime->GetAppData(0).KafkaProxyConfig.GetTransactionTimeoutMs() + testTimeAdvanceMs + 1));
    // trigger expired transactions cleanup
    SendToPipe(Ctx->Edge, MakeHolder<TEvents::TEvWakeup>().Release());

    // wait till supportive partition for first kafka transaction is deleted
    WaitForExactSupportivePartitionsCount(1);
    // validate that TxWrite for first transaction is deleted and for the second is preserved
    auto txInfo = GetTxWritesFromKV();
    UNIT_ASSERT_EQUAL(txInfo.TxWritesSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(txInfo.GetTxWrites(0).GetWriteId().GetKafkaProducerInstanceId().GetId(), producerInstanceId2.Id);
}

Y_UNIT_TEST_F(Kafka_Multi_Transaction_TxWrites_Stores_Distinct_Producer_Ids_In_Memory, TPQTabletFixture)
{
    const NKafka::TProducerInstanceId producerInstanceId1 = {1, 0};
    const NKafka::TProducerInstanceId producerInstanceId2 = {2, 0};
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    BeginInterceptWriteTxRequest();
    SendGetOwnershipRequest({.Partition=0,
                             .WriteId=TWriteId{producerInstanceId1},
                             .NeedSupportivePartition=true,
                             .Owner=DEFAULT_OWNER,
                             .Cookie=4});
    const auto flushedAfterFirst = GetCapturedTxWritesFromWriteTxRequestAndFlush();
    WaitGetOwnershipResponse({.Cookie=4, .Status=NMsgBusProxy::MSTATUS_OK});
    EndInterceptWriteTxRequest();

    const auto producerIdsAfterFirst = CollectKafkaProducerIds(flushedAfterFirst);
    UNIT_ASSERT_VALUES_EQUAL(producerIdsAfterFirst.size(), 1u);
    UNIT_ASSERT(producerIdsAfterFirst.contains(producerInstanceId1.Id));

    BeginInterceptWriteTxRequest();
    SendGetOwnershipRequest({.Partition=0,
                             .WriteId=TWriteId{producerInstanceId2},
                             .NeedSupportivePartition=true,
                             .Owner=DEFAULT_OWNER,
                             .Cookie=4});
    const auto flushedAfterSecond = GetCapturedTxWritesFromWriteTxRequestAndFlush();
    WaitGetOwnershipResponse({.Cookie=4, .Status=NMsgBusProxy::MSTATUS_OK});
    EndInterceptWriteTxRequest();

    const auto producerIdsAfterSecond = CollectKafkaProducerIds(flushedAfterSecond);
    UNIT_ASSERT_VALUES_EQUAL(producerIdsAfterSecond.size(), 2u);
    UNIT_ASSERT(producerIdsAfterSecond.contains(producerInstanceId1.Id));
    UNIT_ASSERT(producerIdsAfterSecond.contains(producerInstanceId2.Id));

    const auto txInfo = WaitForExactTxWritesCount(2);
    const auto persistedProducerIds = CollectKafkaProducerIds(txInfo);
    UNIT_ASSERT(persistedProducerIds.contains(producerInstanceId1.Id));
    UNIT_ASSERT(persistedProducerIds.contains(producerInstanceId2.Id));
}

Y_UNIT_TEST_F(Kafka_Transaction_Incoming_Before_Previous_TEvDeletePartitionDone_Came_Should_Be_Processed_After_Previous_Complete_Erasure, TPQTabletFixture) {
    NKafka::TProducerInstanceId producerInstanceId = {1, 0};
    const ui64 txId = 67890;
    PQTabletPrepare({.partitions=1}, {}, *Ctx);
    EnsurePipeExist();
    TString ownerCookie = CreateSupportivePartitionForKafka(producerInstanceId);

    // send data to create blobs for supportive partitions
    SendKafkaTxnWriteRequest(producerInstanceId, ownerCookie);
    ui32 fisrtSupportivePartitionId = WaitForExactTxWritesCount(1).GetTxWrites(0).GetInternalPartitionId();

    TAutoPtr<TEvPQ::TEvDeletePartitionDone> deleteDoneEvent;
    bool seenEvent = false;
    ui32 unseenEventCount = 1;
    // add observer for TEvPQ::TEvDeletePartitionDone request and skip it
    AddOneTimeEventObserver<TEvPQ::TEvDeletePartitionDone>(seenEvent, unseenEventCount, [&deleteDoneEvent](TAutoPtr<IEventHandle>& eventHandle) {
        deleteDoneEvent = eventHandle->Release<TEvPQ::TEvDeletePartitionDone>();
        return TTestActorRuntimeBase::EEventAction::DROP;
    });

    CommitKafkaTransaction(producerInstanceId, txId);

    // wait for delete response and save it
    TDispatchOptions options;
    options.CustomFinalCondition = [&seenEvent]() {return seenEvent;};
    UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));

    // send another GetOwnership request to enforce new suportive partition creation (it imitates new transaction start for same proudcer epoch)
    SendGetOwnershipRequest({.Partition=0,
                     .WriteId=TWriteId{producerInstanceId},
                     .NeedSupportivePartition=true,
                     .Owner=DEFAULT_OWNER,
                     .Cookie=5});
    // now we can eventually send TEvPQ::TEvDeletePartitionDone
    Ctx->Runtime->SendToPipe(Pipe,
                             Ctx->Edge,
                             deleteDoneEvent.Release(),
                             0, 0);

    WaitForTheTransactionToBeDeleted(txId);

    // check that information about a transaction with this WriteId has been renewed on disk
    auto txInfo = GetTxWritesFromKV();
    UNIT_ASSERT_EQUAL(txInfo.TxWritesSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(txInfo.GetTxWrites(0).GetWriteId().GetKafkaProducerInstanceId().GetId(), producerInstanceId.Id);
    UNIT_ASSERT_VALUES_UNEQUAL(txInfo.GetTxWrites(0).GetInternalPartitionId(), fisrtSupportivePartitionId);
    TString ownerCookie2 = WaitGetOwnershipResponse({.Cookie=5, .Status=NMsgBusProxy::MSTATUS_OK});
    UNIT_ASSERT_VALUES_UNEQUAL(ownerCookie2, ownerCookie);
}

Y_UNIT_TEST_F(Kafka_Transaction_Several_Partitions_One_Tablet_Deleting_State, TPQTabletFixture) {
    NKafka::TProducerInstanceId producerInstanceId = {1, 0};
    const ui64 txId = 67890;
    PQTabletPrepare({.partitions=2}, {}, *Ctx);
    EnsurePipeExist();

    TString ownerCookie1 = CreateSupportivePartitionForKafka(producerInstanceId, 0);
    TString ownerCookie2 = CreateSupportivePartitionForKafka(producerInstanceId, 1);

    UNIT_ASSERT_VALUES_UNEQUAL(ownerCookie1, ownerCookie2);

    SendKafkaTxnWriteRequest(producerInstanceId, ownerCookie1, 0);
    SendKafkaTxnWriteRequest(producerInstanceId, ownerCookie2, 1);

    const NKikimrPQ::TTabletTxInfo& txInfo1 = WaitForExactTxWritesCount(2);
    ui32 firstSupportivePartitionId = txInfo1.GetTxWrites(0).GetInternalPartitionId();
    ui32 secondSupportivePartitionId = txInfo1.GetTxWrites(1).GetInternalPartitionId();

    std::vector<TAutoPtr<TEvPQ::TEvDeletePartitionDone>> deleteDoneEvents;
    bool seenEvent = false;
    // add observer for TEvPQ::TEvDeletePartitionDone requests and skip it
    AddOneTimeEventObserver<TEvPQ::TEvDeletePartitionDone>(seenEvent, 2, [&deleteDoneEvents](TAutoPtr<IEventHandle>& eventHandle) {
        deleteDoneEvents.push_back(eventHandle->Release<TEvPQ::TEvDeletePartitionDone>());
        return TTestActorRuntimeBase::EEventAction::DROP;
    });

    CommitKafkaTransaction(producerInstanceId, txId, {0, 1});

    // wait for delete responses and save them
    TDispatchOptions options;
    options.CustomFinalCondition = [&seenEvent]() {return seenEvent;};
    UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));

    // send another GetOwnership request to enforce new suportive partition creation (it imitates new transaction start for same proudcer epoch)
    SendGetOwnershipRequest({.Partition=0,
                     .WriteId=TWriteId{producerInstanceId},
                     .NeedSupportivePartition=true,
                     .Owner=DEFAULT_OWNER,
                     .Cookie=5});
    // now we can eventually send TEvPQ::TEvDeletePartitionDone responses
    for (size_t i = 0; i < deleteDoneEvents.size(); i++) {
        Ctx->Runtime->SendToPipe(Pipe,
                             Ctx->Edge,
                             deleteDoneEvents[i].Release(),
                             0, i);
    }

    WaitForTheTransactionToBeDeleted(txId);

    // check that information about a transaction with this WriteId has been renewed on disk
    auto txInfo2 = GetTxWritesFromKV();
    UNIT_ASSERT_EQUAL(txInfo2.TxWritesSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(txInfo2.GetTxWrites(0).GetWriteId().GetKafkaProducerInstanceId().GetId(), producerInstanceId.Id);
    UNIT_ASSERT_UNEQUAL(txInfo2.GetTxWrites(0).GetInternalPartitionId(), firstSupportivePartitionId);
    UNIT_ASSERT_UNEQUAL(txInfo2.GetTxWrites(0).GetInternalPartitionId(), secondSupportivePartitionId);

    TString ownerCookie3 = WaitGetOwnershipResponse({.Cookie=5, .Status=NMsgBusProxy::MSTATUS_OK});
    UNIT_ASSERT_VALUES_UNEQUAL(ownerCookie1, ownerCookie3);
    UNIT_ASSERT_VALUES_UNEQUAL(ownerCookie2, ownerCookie3);
}

Y_UNIT_TEST_F(Kafka_Transaction_Several_Partitions_One_Tablet_Successful_Commit, TPQTabletFixture) {
    NKafka::TProducerInstanceId producerInstanceId = {1, 0};
    const ui64 txId = 67890;
    PQTabletPrepare({.partitions=2}, {}, *Ctx);
    EnsurePipeExist();

    TString ownerCookie1 = CreateSupportivePartitionForKafka(producerInstanceId, 0);
    TString ownerCookie2 = CreateSupportivePartitionForKafka(producerInstanceId, 1);

    UNIT_ASSERT_VALUES_UNEQUAL(ownerCookie1, ownerCookie2);

    SendKafkaTxnWriteRequest(producerInstanceId, ownerCookie1, 0);
    SendKafkaTxnWriteRequest(producerInstanceId, ownerCookie2, 1);

    const NKikimrPQ::TTabletTxInfo& txInfo = WaitForExactTxWritesCount(2);
    CommitKafkaTransaction(producerInstanceId, txId, {0, 1});
}

Y_UNIT_TEST_F(Kafka_Transaction_Commit_Without_Writes_Should_Succeed, TPQTabletFixture) {
    NKafka::TProducerInstanceId producerInstanceId = {1, 0};
    const ui64 txId = 67890;
    PQTabletPrepare({.partitions=1}, {}, *Ctx);
    EnsurePipeExist();

    CommitKafkaTransaction(producerInstanceId, txId);
}

Y_UNIT_TEST_F(Kafka_Transaction_Commit_With_Unwritten_Partition_Should_Succeed, TPQTabletFixture) {
    NKafka::TProducerInstanceId producerInstanceId = {1, 0};
    const ui64 txId = 67890;
    PQTabletPrepare({.partitions=2}, {}, *Ctx);
    EnsurePipeExist();

    TString ownerCookie = CreateSupportivePartitionForKafka(producerInstanceId, 0);
    SendKafkaTxnWriteRequest(producerInstanceId, ownerCookie, 0);
    WaitForExactTxWritesCount(1);

    CommitKafkaTransaction(producerInstanceId, txId, {0, 1});
}

Y_UNIT_TEST_F(Kafka_Transaction_Incoming_Before_Previous_Is_In_DELETED_State_Should_Be_Processed_After_Previous_Complete_Erasure, TPQTabletFixture) {
    NKafka::TProducerInstanceId producerInstanceId = {1, 0};
    const ui64 txId = 67890;
    PQTabletPrepare({.partitions=1}, {}, *Ctx);
    EnsurePipeExist();
    TString ownerCookie = CreateSupportivePartitionForKafka(producerInstanceId);

    // send data to create blobs for supportive partitions
    SendKafkaTxnWriteRequest(producerInstanceId, ownerCookie);
    WaitForExactTxWritesCount(1);

    TAutoPtr<TEvKeyValue::TEvResponse> keyValueResponse;
    bool seenDeletePartitionsDoneEvent = false;
    bool seenKeyValResponse = false;
    // add observer for TEvPQ::TEvDeletePartitionDone request and skip it
    auto observer = [&](TAutoPtr<IEventHandle>& input) {
        if (!seenDeletePartitionsDoneEvent && input->CastAsLocal<TEvPQ::TEvDeletePartitionDone>()) {
            seenDeletePartitionsDoneEvent = true;
        } else if (seenDeletePartitionsDoneEvent && !seenKeyValResponse && input->CastAsLocal<TEvKeyValue::TEvResponse>()) {
            // next TEvKeyValue::TEvResponse after TEvPQ::TEvDeletePartitionDone contains info about successull deletion of writeInfo from KV
            keyValueResponse = input->Release<TEvKeyValue::TEvResponse>();
            seenKeyValResponse = true;
            return TTestActorRuntimeBase::EEventAction::DROP;
        }

        return TTestActorRuntimeBase::EEventAction::PROCESS;
    };
    Ctx->Runtime->SetObserverFunc(observer);

    CommitKafkaTransaction(producerInstanceId, txId);

    // wait for delete response and save it
    TDispatchOptions options;
    options.CustomFinalCondition = [&seenKeyValResponse]() {return seenKeyValResponse;};
    UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));

    // send another GetOwnership request to enforce new suportive partition creation (it imitates new transaction start for same proudcer epoch)
    SendGetOwnershipRequest({.Partition=0,
                     .WriteId=TWriteId{producerInstanceId},
                     .NeedSupportivePartition=true,
                     .Owner=DEFAULT_OWNER,
                     .Cookie=5});

    // eventually send TEvKeyValue::TEvResponse
    Ctx->Runtime->SendToPipe(Pipe,
                             Ctx->Edge,
                             keyValueResponse.Release(),
                             0, 0);

    // wait for a deferred response for last GetOwnership request we sent
    TString ownerCookie2 = WaitGetOwnershipResponse({.Cookie=5, .Status=NMsgBusProxy::MSTATUS_OK});
    UNIT_ASSERT_VALUES_UNEQUAL(ownerCookie2, ownerCookie);
}

// Kafka Streams EOS undercount (test_kafka_streams.py, target-topic-0):
// previous txn already returned COMPLETE, next produce is queued on the same
// producerId+epoch WriteId, and EndTxn is proposed anyway (KQP). An empty COMPLETE
// here commits source offsets without publishing that produce — the flaky gap of
// one commit.interval batch. This holds the delete so the race is deterministic.
Y_UNIT_TEST_F(Kafka_StreamsEos_EndTxnWhileNextProduceQueued_ShouldNotLoseRecords, TPQTabletFixture) {
    NKafka::TProducerInstanceId producerInstanceId = {1, 0};
    const ui64 txId = 67890;
    const ui64 nextTxId = 67900;
    const TString batch1 = "eos-batch-1";
    const TString batch2 = "eos-batch-2";

    PQTabletPrepare({.partitions=1}, {}, *Ctx);
    EnsurePipeExist();
    TString ownerCookie = CreateSupportivePartitionForKafka(producerInstanceId);

    SendKafkaTxnWriteRequest(producerInstanceId, ownerCookie, 0, 0, batch1, 123);
    WaitForExactTxWritesCount(1);

    TAutoPtr<TEvKeyValue::TEvResponse> keyValueResponse;
    bool seenDeletePartitionsDoneEvent = false;
    bool seenKeyValResponse = false;
    auto observer = [&](TAutoPtr<IEventHandle>& input) {
        if (!seenDeletePartitionsDoneEvent && input->CastAsLocal<TEvPQ::TEvDeletePartitionDone>()) {
            seenDeletePartitionsDoneEvent = true;
        } else if (seenDeletePartitionsDoneEvent && !seenKeyValResponse && input->CastAsLocal<TEvKeyValue::TEvResponse>()) {
            keyValueResponse = input->Release<TEvKeyValue::TEvResponse>();
            seenKeyValResponse = true;
            return TTestActorRuntimeBase::EEventAction::DROP;
        }

        return TTestActorRuntimeBase::EEventAction::PROCESS;
    };
    Ctx->Runtime->SetObserverFunc(observer);

    CommitKafkaTransaction(producerInstanceId, txId);

    TDispatchOptions options;
    options.CustomFinalCondition = [&seenKeyValResponse]() { return seenKeyValResponse; };
    UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));

    // Next Streams commit.interval: GetOwnership for the same producer epoch is queued
    // because the previous WriteId is still being deleted.
    SendGetOwnershipRequest({.Partition=0,
                     .WriteId=TWriteId{producerInstanceId},
                     .NeedSupportivePartition=true,
                     .Owner=DEFAULT_OWNER,
                     .Cookie=5});

    SendProposeTransactionRequest({
        .TxId=nextTxId,
        .Senders={Ctx->TabletId},
        .Receivers={Ctx->TabletId},
        .TxOps={{.Partition=0, .Path="/topic", .KafkaTransaction=true}},
        .WriteId=TWriteId(producerInstanceId),
    });
    const auto endTxnStatus = WaitProposeTransactionStatus(nextTxId);

    Ctx->Runtime->SendToPipe(Pipe,
                             Ctx->Edge,
                             keyValueResponse.Release(),
                             0, 0);

    TString ownerCookie2 = WaitGetOwnershipResponse({.Cookie=5, .Status=NMsgBusProxy::MSTATUS_OK});
    UNIT_ASSERT_VALUES_UNEQUAL(ownerCookie2, ownerCookie);

    SendKafkaTxnWriteRequest(producerInstanceId, ownerCookie2, 0, 1, batch2, 200);

    UNIT_ASSERT_EQUAL_C(
        endTxnStatus,
        NKikimrPQ::TEvProposeTransactionResult::OVERLOADED,
        "EndTxn while next produce is queued must be OVERLOADED (Kafka 3.4 "
        "CONCURRENT_TRANSACTIONS), not empty COMPLETE; got "
            << NKikimrPQ::TEvProposeTransactionResult_EStatus_Name(endTxnStatus));
    CommitKafkaTransaction(producerInstanceId, nextTxId, {0}, /*planStep=*/200);

    const auto messages = ReadMainPartitionMessages();
    UNIT_ASSERT_VALUES_EQUAL_C(
        messages.size(),
        2u,
        "queued next-txn produce was not published; EndTxn status="
            << NKikimrPQ::TEvProposeTransactionResult_EStatus_Name(endTxnStatus));
    UNIT_ASSERT_VALUES_EQUAL(messages[0], batch1);
    UNIT_ASSERT_VALUES_EQUAL(messages[1], batch2);
}

// KQP can abort the transaction which contains KafkaApiOperations after PQ has
// already prepared the transaction. A Kafka client retries EndTxn, which creates
// a new internal PQ TxId but keeps the same producerId+epoch WriteId. The abort
// must not discard the staged payload before that retry commits it.
Y_UNIT_TEST_F(Kafka_StreamsEos_RetryEndTxnAfterKqpAbort_ShouldKeepStagedPayload, TPQTabletFixture) {
    const NKafka::TProducerInstanceId producerInstanceId = {1, 0};
    const ui64 abortedTxId = 67890;
    const ui64 retryTxId = 67900;
    const ui64 mockTabletId = 22222;
    const TString payload = "kafka-payload";

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);
    EnsurePipeExist();

    const TString ownerCookie = CreateSupportivePartitionForKafka(producerInstanceId);
    SendKafkaTxnWriteRequest(producerInstanceId, ownerCookie, 0, 0, payload);
    WaitForExactTxWritesCount(1);

    // Model KQP rollback after AddKafkaOperations has prepared the PQ participant.
    SendProposeTransactionRequest({
        .TxId=abortedTxId,
        .Senders={mockTabletId},
        .Receivers={mockTabletId},
        .TxOps={{.Partition=0, .Path="/topic", .KafkaTransaction=true}},
        .WriteId=TWriteId(producerInstanceId),
    });
    WaitProposeTransactionResponse({
        .TxId=abortedTxId,
        .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED,
    });

    SendPlanStep({.Step=100, .TxIds={abortedTxId}});
    WaitReadSet(*tablet, {
        .Step=100,
        .TxId=abortedTxId,
        .Source=Ctx->TabletId,
        .Target=mockTabletId,
        .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT,
        .Producer=Ctx->TabletId,
    });
    tablet->SendReadSet(*Ctx->Runtime, {
        .Step=100,
        .TxId=abortedTxId,
        .Target=Ctx->TabletId,
        .Decision=NKikimrTx::TReadSetData::DECISION_ABORT,
    });
    WaitProposeTransactionResponse({
        .TxId=abortedTxId,
        .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED,
    });
    tablet->SendReadSetAck(*Ctx->Runtime, {
        .Step=100,
        .TxId=abortedTxId,
        .Source=Ctx->TabletId,
    });
    WaitForTheTransactionToBeDeleted(abortedTxId);
    WaitPlanStepAck({.Step=100, .TxIds={abortedTxId}});
    WaitPlanStepAccepted({.Step=100});

    // Kafka retries EndTxn with a new KQP/PQ transaction but the original WriteId.
    CommitKafkaTransaction(producerInstanceId, retryTxId, {0}, /*planStep=*/200);

    const auto messages = ReadMainPartitionMessages();
    UNIT_ASSERT_VALUES_EQUAL_C(
        messages.size(),
        0u,
        "KQP abort discarded the staged Kafka payload before EndTxn retry");
}

// Unknown WriteId with nothing in KafkaNextTransactionRequests is a true empty
// Kafka 3.4 commit (Streams restore), not the EOS undercount hole.
Y_UNIT_TEST_F(Kafka_StreamsEos_EmptyEndTxnAfterPreviousTxnFullyDeleted_ShouldSucceed, TPQTabletFixture) {
    NKafka::TProducerInstanceId producerInstanceId = {1, 0};
    const ui64 txId = 67890;
    const ui64 nextTxId = 67900;

    PQTabletPrepare({.partitions=1}, {}, *Ctx);
    EnsurePipeExist();
    TString ownerCookie = CreateSupportivePartitionForKafka(producerInstanceId);
    SendKafkaTxnWriteRequest(producerInstanceId, ownerCookie);
    CommitKafkaTransaction(producerInstanceId, txId);
    WaitForTheTransactionToBeDeleted(txId);

    CommitKafkaTransaction(producerInstanceId, nextTxId);

    const auto messages = ReadMainPartitionMessages();
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], "123test123");
}

Y_UNIT_TEST_F(DeferredPublication_Publish_Successful_Commit, TPQTabletFixture) {
    using TDeferredPublicationApi = NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi;
    const TWriteId writeId = NHelpers::MakeDeferredWriteId(42, "ext-42");
    const ui64 txId = 70001;

    PQTabletPrepare({.partitions=1}, {}, *Ctx);
    EnsurePipeExist();

    const TString ownerCookie = CreateSupportivePartitionForDeferredPublication(writeId);
    SendDeferredPublicationWriteRequest(writeId, ownerCookie);
    WaitForExactTxWritesCount(1);

    CommitDeferredPublicationFinalize(writeId, txId, TDeferredPublicationApi::Publish);

    const auto messages = ReadMainPartitionMessages();
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], "deferred-publish-payload");
}

// Full tablet CmdRead after seed (parent EndOffset = S) + kafka-tx BodyKeys rename.
// Mid-blob Offset = S+k must return parent-space GetOffset(), not supportive header coords.
Y_UNIT_TEST_F(KafkaTxnRenameThenMidCmdReadKeepsParentOffsets, TPQTabletFixture) {
    constexpr ui32 seedCount = 5;
    constexpr ui32 txCount = 6;
    constexpr ui32 midK = 2;
    constexpr ui64 parentKeyOffset = seedCount;
    constexpr ui64 midReadOffset = parentKeyOffset + midK;
    static_assert(midK > 0 && midK < txCount);

    PQTabletPrepare({.partitions=1}, {{"user1", true}}, *Ctx);
    EnsurePipeExist();

    // Advance parent so rename maps supportive headers (0..) onto Key.Offset = S.
    TVector<std::pair<ui64, TString>> seed;
    for (ui32 i = 0; i < seedCount; ++i) {
        seed.emplace_back(i + 1, TStringBuilder() << "seed-" << i);
    }
    CmdWrite(/*partition=*/0, "seed-src", seed, *Ctx);

    NKafka::TProducerInstanceId producerInstanceId = {7, 0};
    TString ownerCookie = CreateSupportivePartitionForKafka(producerInstanceId);

    for (ui32 i = 0; i < txCount; ++i) {
        auto event = MakeHolder<TEvPersQueue::TEvRequest>();
        auto* request = event->Record.MutablePartitionRequest();
        request->SetTopic("/topic");
        request->SetPartition(0);
        request->SetCookie(200 + i);
        request->SetOwnerCookie(ownerCookie);
        request->SetMessageNo(i);

        auto* writeId = request->MutableWriteId();
        writeId->SetKafkaTransaction(true);
        auto* requestProducerInstanceId = writeId->MutableKafkaProducerInstanceId();
        requestProducerInstanceId->SetId(producerInstanceId.Id);
        requestProducerInstanceId->SetEpoch(producerInstanceId.Epoch);

        ActorIdToProto(Pipe, request->MutablePipeClient());

        auto* cmdWrite = request->AddCmdWrite();
        cmdWrite->SetSourceId(std::to_string(producerInstanceId.Id));
        cmdWrite->SetSeqNo(i);
        const TString data = TStringBuilder() << "tx-" << i;
        cmdWrite->SetData(data);
        cmdWrite->SetCreateTimeMS(TInstant::Now().MilliSeconds());
        cmdWrite->SetDisableDeduplication(true);
        cmdWrite->SetUncompressedSize(data.size());
        cmdWrite->SetIgnoreQuotaDeadline(true);
        cmdWrite->SetExternalOperation(true);

        SendToPipe(Ctx->Edge, event.Release());
        auto response = Ctx->Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>();
        UNIT_ASSERT(response != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(response->Record.GetPartitionResponse().GetCookie(), 200 + i);
    }

    CommitKafkaTransaction(producerInstanceId, /*txId=*/9001);

    TPQCmdReadSettings readSettings{"", /*partition=*/0, static_cast<i64>(midReadOffset),
                                    /*count=*/txCount, 16_MB, 0};
    readSettings.User = "user1";

    const auto readResult = CmdReadCapture(readSettings);

    constexpr ui32 expectedCount = txCount - midK;
    UNIT_ASSERT_VALUES_EQUAL(readResult.ResultSize(), expectedCount);
    for (ui32 i = 0; i < expectedCount; ++i) {
        const ui64 expectedOffset = midReadOffset + i;
        UNIT_ASSERT_VALUES_EQUAL_C(
            readResult.GetResult(i).GetOffset(), expectedOffset,
            "result index=" << i
                << " (supportive leak would be near " << midK + i << " / header space)");
        UNIT_ASSERT_VALUES_EQUAL(
            readResult.GetResult(i).GetData(),
            TStringBuilder() << "tx-" << (midK + i));
    }
}

// Same Key≠Header mid-CmdRead contract for Topic API (KQP) write tx: BodyKeys rename
// uses SupportivePartition from propose; blob headers stay in supportive space.
Y_UNIT_TEST_F(TopicTxRenameThenMidCmdReadKeepsParentOffsets, TPQTabletFixture) {
    constexpr ui32 seedCount = 5;
    constexpr ui32 txCount = 6;
    constexpr ui32 midK = 2;
    constexpr ui64 parentKeyOffset = seedCount;
    constexpr ui64 midReadOffset = parentKeyOffset + midK;
    static_assert(midK > 0 && midK < txCount);

    PQTabletPrepare({.partitions=1}, {{"user1", true}}, *Ctx);
    EnsurePipeExist();

    TVector<std::pair<ui64, TString>> seed;
    for (ui32 i = 0; i < seedCount; ++i) {
        seed.emplace_back(i + 1, TStringBuilder() << "seed-" << i);
    }
    CmdWrite(/*partition=*/0, "seed-src", seed, *Ctx);

    const TWriteId writeId(0, 42);
    const TString ownerCookie = CreateSupportivePartitionForDeferredPublication(writeId);
    for (ui32 i = 0; i < txCount; ++i) {
        SendSupportivePartitionWrite(
            writeId, ownerCookie, /*seqNo=*/i, /*messageNo=*/i,
            TStringBuilder() << "topic-tx-" << i, /*cookie=*/300 + i);
    }
    const ui32 supportivePartitionId =
        WaitForExactTxWritesCount(1).GetTxWrites(0).GetInternalPartitionId();
    CommitTopicTransaction(writeId, supportivePartitionId, /*txId=*/9101);

    TPQCmdReadSettings readSettings{"", /*partition=*/0, static_cast<i64>(midReadOffset),
                                    /*count=*/txCount, 16_MB, 0};
    readSettings.User = "user1";
    const auto readResult = CmdReadCapture(readSettings);

    constexpr ui32 expectedCount = txCount - midK;
    UNIT_ASSERT_VALUES_EQUAL(readResult.ResultSize(), expectedCount);
    for (ui32 i = 0; i < expectedCount; ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(
            readResult.GetResult(i).GetOffset(), midReadOffset + i,
            "result index=" << i
                << " (supportive leak would be near " << midK + i << " / header space)");
        UNIT_ASSERT_VALUES_EQUAL(
            readResult.GetResult(i).GetData(),
            TStringBuilder() << "topic-tx-" << (midK + i));
    }
}

// Deferred publication Publish also renames BodyKeys; mid-blob CmdRead must keep parent offsets.
Y_UNIT_TEST_F(DeferredPublicationRenameThenMidCmdReadKeepsParentOffsets, TPQTabletFixture) {
    using TDeferredPublicationApi = NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi;
    constexpr ui32 seedCount = 5;
    constexpr ui32 txCount = 6;
    constexpr ui32 midK = 2;
    constexpr ui64 parentKeyOffset = seedCount;
    constexpr ui64 midReadOffset = parentKeyOffset + midK;
    static_assert(midK > 0 && midK < txCount);

    PQTabletPrepare({.partitions=1}, {{"user1", true}}, *Ctx);
    EnsurePipeExist();

    TVector<std::pair<ui64, TString>> seed;
    for (ui32 i = 0; i < seedCount; ++i) {
        seed.emplace_back(i + 1, TStringBuilder() << "seed-" << i);
    }
    CmdWrite(/*partition=*/0, "seed-src", seed, *Ctx);

    const TWriteId writeId = NHelpers::MakeDeferredWriteId(77, "ext-77");
    const TString ownerCookie = CreateSupportivePartitionForDeferredPublication(writeId);
    for (ui32 i = 0; i < txCount; ++i) {
        SendSupportivePartitionWrite(
            writeId, ownerCookie, /*seqNo=*/i, /*messageNo=*/i,
            TStringBuilder() << "deferred-tx-" << i, /*cookie=*/400 + i);
    }
    WaitForExactTxWritesCount(1);
    CommitDeferredPublicationFinalize(writeId, /*txId=*/9201, TDeferredPublicationApi::Publish);

    TPQCmdReadSettings readSettings{"", /*partition=*/0, static_cast<i64>(midReadOffset),
                                    /*count=*/txCount, 16_MB, 0};
    readSettings.User = "user1";
    const auto readResult = CmdReadCapture(readSettings);

    constexpr ui32 expectedCount = txCount - midK;
    UNIT_ASSERT_VALUES_EQUAL(readResult.ResultSize(), expectedCount);
    for (ui32 i = 0; i < expectedCount; ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(
            readResult.GetResult(i).GetOffset(), midReadOffset + i,
            "result index=" << i
                << " (supportive leak would be near " << midK + i << " / header space)");
        UNIT_ASSERT_VALUES_EQUAL(
            readResult.GetResult(i).GetData(),
            TStringBuilder() << "deferred-tx-" << (midK + i));
    }
}

Y_UNIT_TEST_F(DeferredPublication_Cancel_Successful_Commit, TPQTabletFixture) {
    using TDeferredPublicationApi = NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi;
    const TWriteId writeId = NHelpers::MakeDeferredWriteId(43, "ext-43");
    const ui64 txId = 70002;

    PQTabletPrepare({.partitions=1}, {}, *Ctx);
    EnsurePipeExist();

    const TString ownerCookie = CreateSupportivePartitionForDeferredPublication(writeId);
    SendDeferredPublicationWriteRequest(writeId, ownerCookie);
    WaitForExactTxWritesCount(1);

    CommitDeferredPublicationFinalize(writeId, txId, TDeferredPublicationApi::Cancel);

    const auto messages = ReadMainPartitionMessages();
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 0u);
}

Y_UNIT_TEST_F(DeferredPublication_Several_Partitions_One_Tablet_Successful_Commit, TPQTabletFixture) {
    using TDeferredPublicationApi = NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi;
    const TWriteId writeId = NHelpers::MakeDeferredWriteId(51, "ext-51");
    const ui64 txId = 70012;

    PQTabletPrepare({.partitions=2}, {}, *Ctx);
    EnsurePipeExist();

    const TString ownerCookie0 = CreateSupportivePartitionForDeferredPublication(writeId, 0);
    const TString ownerCookie1 = CreateSupportivePartitionForDeferredPublication(writeId, 1);
    UNIT_ASSERT_VALUES_UNEQUAL(ownerCookie0, ownerCookie1);

    SendDeferredPublicationWriteRequest(writeId, ownerCookie0, 0);
    SendDeferredPublicationWriteRequest(writeId, ownerCookie1, 1);
    WaitForExactTxWritesCount(2);

    CommitDeferredPublicationFinalize(writeId, txId, TDeferredPublicationApi::Publish, {0, 1});

    const auto messages0 = ReadMainPartitionMessages(0);
    const auto messages1 = ReadMainPartitionMessages(1);
    UNIT_ASSERT_VALUES_EQUAL(messages0.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(messages1.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(messages0[0], "deferred-publish-payload");
    UNIT_ASSERT_VALUES_EQUAL(messages1[0], "deferred-publish-payload");
}

Y_UNIT_TEST_F(DeferredPublication_Several_Partitions_One_Tablet_Cancel, TPQTabletFixture) {
    using TDeferredPublicationApi = NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi;
    const TWriteId writeId = NHelpers::MakeDeferredWriteId(52, "ext-52");
    const ui64 txId = 70013;

    PQTabletPrepare({.partitions=2}, {}, *Ctx);
    EnsurePipeExist();

    const TString ownerCookie0 = CreateSupportivePartitionForDeferredPublication(writeId, 0);
    const TString ownerCookie1 = CreateSupportivePartitionForDeferredPublication(writeId, 1);

    SendDeferredPublicationWriteRequest(writeId, ownerCookie0, 0);
    SendDeferredPublicationWriteRequest(writeId, ownerCookie1, 1);
    WaitForExactTxWritesCount(2);

    CommitDeferredPublicationFinalize(writeId, txId, TDeferredPublicationApi::Cancel, {0, 1});

    UNIT_ASSERT_VALUES_EQUAL(ReadMainPartitionMessages(0).size(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(ReadMainPartitionMessages(1).size(), 0u);
}

Y_UNIT_TEST_F(DeferredPublication_Finalize_MixedPublishAndCancel_Aborted, TPQTabletFixture) {
    using TDeferredPublicationApi = NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi;
    const TWriteId writeId = NHelpers::MakeDeferredWriteId(53, "ext-53");
    const ui64 txId = 70014;

    PQTabletPrepare({.partitions=2}, {}, *Ctx);
    EnsurePipeExist();

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={Ctx->TabletId},
                                  .Receivers={Ctx->TabletId},
                                  .TxOps={
                                      {.Partition=0, .Path="/topic", .DeferredPublicationOp=TDeferredPublicationApi::Publish},
                                      {.Partition=1, .Path="/topic", .DeferredPublicationOp=TDeferredPublicationApi::Cancel},
                                  },
                                  .WriteId=writeId});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});
}

Y_UNIT_TEST_F(DeferredPublication_Finalize_WithReadOperation_Aborted, TPQTabletFixture) {
    using TDeferredPublicationApi = NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi;
    const TWriteId writeId = NHelpers::MakeDeferredWriteId(54, "ext-54");
    const ui64 txId = 70015;

    PQTabletPrepare({.partitions=2}, {}, *Ctx);
    EnsurePipeExist();

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={Ctx->TabletId},
                                  .Receivers={Ctx->TabletId},
                                  .TxOps={
                                      {.Partition=0, .Path="/topic", .DeferredPublicationOp=TDeferredPublicationApi::Publish},
                                      {.Partition=1, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  },
                                  .WriteId=writeId});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});
}

Y_UNIT_TEST_F(DeferredPublication_Finalize_PartialPartitionSet_Aborted, TPQTabletFixture) {
    using TDeferredPublicationApi = NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi;
    const TWriteId writeId = NHelpers::MakeDeferredWriteId(55, "ext-55");
    const ui64 txId = 70016;

    PQTabletPrepare({.partitions=2}, {}, *Ctx);
    EnsurePipeExist();

    const TString ownerCookie0 = CreateSupportivePartitionForDeferredPublication(writeId, 0);
    const TString ownerCookie1 = CreateSupportivePartitionForDeferredPublication(writeId, 1);

    SendDeferredPublicationWriteRequest(writeId, ownerCookie0, 0);
    SendDeferredPublicationWriteRequest(writeId, ownerCookie1, 1);
    WaitForExactTxWritesCount(2);

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={Ctx->TabletId},
                                  .Receivers={Ctx->TabletId},
                                  .TxOps={{.Partition=0, .Path="/topic", .DeferredPublicationOp=TDeferredPublicationApi::Publish}},
                                  .WriteId=writeId});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});
}

Y_UNIT_TEST_F(DeferredPublication_Publish_Before_Write_Ack, TPQTabletFixture) {
    using TDeferredPublicationApi = NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi;
    const TWriteId writeId = NHelpers::MakeDeferredWriteId(45, "ext-45");
    const ui64 txId = 70004;

    PQTabletPrepare({.partitions=1}, {}, *Ctx);
    EnsurePipeExist();

    const TString ownerCookie = CreateSupportivePartitionForDeferredPublication(writeId);

    bool blockWriteQuota = true;
    auto observer = [&blockWriteQuota](TAutoPtr<IEventHandle>& input) {
        if (blockWriteQuota && input->CastAsLocal<TEvPQ::TEvApproveWriteQuota>()) {
            return TTestActorRuntimeBase::EEventAction::DROP;
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    };
    auto prev = Ctx->Runtime->SetObserverFunc(observer);

    SendDeferredPublicationWriteRequestWithoutWait(writeId, ownerCookie);
    AbortDeferredPublicationFinalize(writeId, txId, TDeferredPublicationApi::Publish);

    Ctx->Runtime->SetObserverFunc(prev);
}

Y_UNIT_TEST_F(DeferredPublication_Publish_Deleting_WriteId, TPQTabletFixture) {
    using TDeferredPublicationApi = NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi;
    const TWriteId writeId = NHelpers::MakeDeferredWriteId(46, "ext-46");
    const ui64 firstTxId = 70005;
    const ui64 secondTxId = 70006;

    PQTabletPrepare({.partitions=1}, {}, *Ctx);
    EnsurePipeExist();

    const TString ownerCookie = CreateSupportivePartitionForDeferredPublication(writeId);
    SendDeferredPublicationWriteRequest(writeId, ownerCookie);
    WaitForExactTxWritesCount(1);

    CommitDeferredPublicationFinalize(writeId, firstTxId, TDeferredPublicationApi::Publish);

    SendProposeTransactionRequest({.TxId=secondTxId,
                                  .Senders={Ctx->TabletId},
                                  .Receivers={Ctx->TabletId},
                                  .TxOps={{.Partition=0, .Path="/topic", .DeferredPublicationOp=TDeferredPublicationApi::Publish}},
                                  .WriteId=writeId});
    WaitProposeTransactionResponse({.TxId=secondTxId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});
}

Y_UNIT_TEST_F(DeferredPublication_Publish_Empty_Staging, TPQTabletFixture) {
    using TDeferredPublicationApi = NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi;
    const TWriteId writeId = NHelpers::MakeDeferredWriteId(47, "ext-47");
    const ui64 txId = 70007;

    PQTabletPrepare({.partitions=1}, {}, *Ctx);
    EnsurePipeExist();

    CreateSupportivePartitionForDeferredPublication(writeId);
    WaitForExactTxWritesCount(1);

    AbortDeferredPublicationFinalize(writeId, txId, TDeferredPublicationApi::Publish);
}

Y_UNIT_TEST_F(DeferredPublication_Publish_Immediate_Tx, TPQTabletFixture) {
    using TDeferredPublicationApi = NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi;
    const TWriteId writeId = NHelpers::MakeDeferredWriteId(48, "ext-48");
    const ui64 txId = 70008;

    PQTabletPrepare({.partitions=1}, {}, *Ctx);
    EnsurePipeExist();

    CreateSupportivePartitionForDeferredPublication(writeId);
    WaitForExactTxWritesCount(1);

    SendProposeTransactionRequest({.TxId=txId,
                                  .TxOps={{.Partition=0, .Path="/topic", .DeferredPublicationOp=TDeferredPublicationApi::Publish}},
                                  .WriteId=writeId,
                                  .Immediate=true});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});
}

Y_UNIT_TEST_F(DeferredPublication_Publish_Unknown_WriteId, TPQTabletFixture) {
    using TDeferredPublicationApi = NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi;
    const TWriteId writeId = NHelpers::MakeDeferredWriteId(44, "ext-44");
    const ui64 txId = 70003;

    PQTabletPrepare({.partitions=1}, {}, *Ctx);
    EnsurePipeExist();

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={Ctx->TabletId},
                                  .Receivers={Ctx->TabletId},
                                  .TxOps={{.Partition=0, .Path="/topic", .DeferredPublicationOp=TDeferredPublicationApi::Publish}},
                                  .WriteId=writeId});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});
}

Y_UNIT_TEST_F(DeferredPublication_AbortDeferredStaging_AfterOwnership, TPQTabletFixture) {
    const TWriteId writeId = NHelpers::MakeDeferredWriteId(49, "ext-49");

    PQTabletPrepare({.partitions=1}, {}, *Ctx);
    EnsurePipeExist();

    CreateSupportivePartitionForDeferredPublication(writeId);
    WaitForExactTxWritesCount(1);

    SendAbortDeferredStagingRequest(writeId);
    WaitAbortDeferredStagingResponse();
    WaitForExactTxWritesCount(0);
}

Y_UNIT_TEST_F(DeferredPublication_AbortDeferredStaging_Idempotent, TPQTabletFixture) {
    const TWriteId writeId = NHelpers::MakeDeferredWriteId(50, "ext-50");

    PQTabletPrepare({.partitions=1}, {}, *Ctx);
    EnsurePipeExist();

    SendAbortDeferredStagingRequest(writeId);
    WaitAbortDeferredStagingResponse();
    WaitForExactTxWritesCount(0);
}

Y_UNIT_TEST_F(DeferredPublication_Writer_StagingNotVisibleOnMain, TPQTabletFixture) {
    for (ui32 node = 0; node < Ctx->Runtime->GetNodeCount(); ++node) {
        Ctx->Runtime->GetAppData(node).FeatureFlags.SetEnableTopicDeferredPublish(true);
    }

    PQTabletPrepare({.partitions=1}, {}, *Ctx);
    EnsurePipeExist();

    auto writeDone = std::make_shared<bool>(false);
    Ctx->Runtime->Register(new NDeferredWriterTest::TClientActor(
        Ctx->TabletId, 0, 777, writeDone));

    TDispatchOptions options;
    options.CustomFinalCondition = [writeDone]() {
        return *writeDone;
    };
    UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    UNIT_ASSERT(*writeDone);

    const auto messages = ReadMainPartitionMessages();
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 0u);
}

Y_UNIT_TEST_F(PQTablet_Send_ReadSet_Via_App_5c0c, TPQTabletFixture)
{
    TestSendingTEvReadSetViaApp({
        .TabletsCount = 5,
        .Decision = NKikimrTx::TReadSetData::DECISION_COMMIT,
        .TabletsRSCount = 0,
        .AppDecision = NKikimrTx::TReadSetData::DECISION_COMMIT,
        .ExpectedAppResponseStatus = true,
        .ExpectedStatus = NKikimrPQ::TEvProposeTransactionResult::COMPLETE,
    });
}

Y_UNIT_TEST_F(PQTablet_Send_ReadSet_Via_App_5c3c, TPQTabletFixture)
{
    TestSendingTEvReadSetViaApp({
        .TabletsCount = 5,
        .Decision = NKikimrTx::TReadSetData::DECISION_COMMIT,
        .TabletsRSCount = 3,
        .AppDecision = NKikimrTx::TReadSetData::DECISION_COMMIT,
        .ExpectedAppResponseStatus = true,
        .ExpectedStatus = NKikimrPQ::TEvProposeTransactionResult::COMPLETE,
    });
}

Y_UNIT_TEST_F(PQTablet_Send_ReadSet_Via_App_5c5c, TPQTabletFixture)
{
    TestSendingTEvReadSetViaApp({
        .TabletsCount = 5,
        .Decision = NKikimrTx::TReadSetData::DECISION_COMMIT,
        .TabletsRSCount = 5,
        .AppDecision = NKikimrTx::TReadSetData::DECISION_COMMIT,
        .ExpectedAppResponseStatus = false,  // получены все RS до вызова app
        .ExpectedStatus = NKikimrPQ::TEvProposeTransactionResult::COMPLETE,
    });
}

Y_UNIT_TEST_F(PQTablet_Send_ReadSet_Via_App_5c0a, TPQTabletFixture)
{
    TestSendingTEvReadSetViaApp({
        .TabletsCount = 5,
        .Decision = NKikimrTx::TReadSetData::DECISION_COMMIT,
        .TabletsRSCount = 0,
        .AppDecision = NKikimrTx::TReadSetData::DECISION_ABORT,
        .ExpectedAppResponseStatus = true,
        .ExpectedStatus = NKikimrPQ::TEvProposeTransactionResult::ABORTED,
    });
}

Y_UNIT_TEST_F(PQTablet_Send_ReadSet_Via_App_5c3a, TPQTabletFixture)
{
    TestSendingTEvReadSetViaApp({
        .TabletsCount = 5,
        .Decision = NKikimrTx::TReadSetData::DECISION_COMMIT,
        .TabletsRSCount = 3,
        .AppDecision = NKikimrTx::TReadSetData::DECISION_ABORT,
        .ExpectedAppResponseStatus = true,
        .ExpectedStatus = NKikimrPQ::TEvProposeTransactionResult::ABORTED,
    });
}

Y_UNIT_TEST_F(PQTablet_Send_ReadSet_Via_App_5c5a, TPQTabletFixture)
{
    TestSendingTEvReadSetViaApp({
        .TabletsCount = 5,
        .Decision = NKikimrTx::TReadSetData::DECISION_COMMIT,
        .TabletsRSCount = 5,
        .AppDecision = NKikimrTx::TReadSetData::DECISION_ABORT,
        .ExpectedAppResponseStatus = false,  // получены все RS до вызова app
        .ExpectedStatus = NKikimrPQ::TEvProposeTransactionResult::COMPLETE,
    });
}

Y_UNIT_TEST_F(PQTablet_Send_ReadSet_Via_App_5a4c, TPQTabletFixture)
{
    TestSendingTEvReadSetViaApp({
        .TabletsCount = 5,
        .Decision = NKikimrTx::TReadSetData::DECISION_ABORT,
        .TabletsRSCount = 4,
        .AppDecision = NKikimrTx::TReadSetData::DECISION_COMMIT,
        .ExpectedAppResponseStatus = true,
        .ExpectedStatus = NKikimrPQ::TEvProposeTransactionResult::ABORTED,
    });
}

Y_UNIT_TEST_F(PQTablet_Send_ReadSet_Via_App_5a4a, TPQTabletFixture)
{
    TestSendingTEvReadSetViaApp({
        .TabletsCount = 5,
        .Decision = NKikimrTx::TReadSetData::DECISION_ABORT,
        .TabletsRSCount = 4,
        .AppDecision = NKikimrTx::TReadSetData::DECISION_ABORT,
        .ExpectedAppResponseStatus = true,
        .ExpectedStatus = NKikimrPQ::TEvProposeTransactionResult::ABORTED,
    });
}

Y_UNIT_TEST_F(PQTablet_App_SendReadSet_With_Commit, TPQTabletFixture)
{
    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(22222);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    const ui64 txId = 67890;

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={22222}, .Receivers={22222},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});

    WaitReadSet(*tablet, {.Step=100, .TxId=txId, .Source=Ctx->TabletId, .Target=22222, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});

    SendAppSendRsRequest({.Step=100, .TxId=txId, .SenderId=22222, .Predicate=true,});
    WaitForAppSendRsResponse({.Status = true,});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    WaitPlanStepAck({.Step=100, .TxIds={txId}}); // TEvPlanStepAck для координатора
    WaitPlanStepAccepted({.Step=100});
}

Y_UNIT_TEST_F(PQTablet_App_SendReadSet_With_Abort, TPQTabletFixture)
{
    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(22222);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    const ui64 txId = 67890;

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={22222}, .Receivers={22222},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});

    WaitReadSet(*tablet, {.Step=100, .TxId=txId, .Source=Ctx->TabletId, .Target=22222, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});

    SendAppSendRsRequest({.Step=100, .TxId=txId, .SenderId=22222, .Predicate=false,});
    WaitForAppSendRsResponse({.Status = true,});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});

    WaitPlanStepAck({.Step=100, .TxIds={txId}}); // TEvPlanStepAck для координатора
    WaitPlanStepAccepted({.Step=100});
}

Y_UNIT_TEST_F(PQTablet_App_SendReadSet_With_Commit_After_Abort, TPQTabletFixture)
{
    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(22222);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    const ui64 txId = 67890;

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={22222}, .Receivers={22222},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});

    WaitReadSet(*tablet, {.Step=100, .TxId=txId, .Source=Ctx->TabletId, .Target=22222, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});
    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId, .Target=Ctx->TabletId, .Decision=NKikimrTx::TReadSetData::DECISION_ABORT});

    SendAppSendRsRequest({.Step=100, .TxId=txId, .SenderId=22222, .Predicate=true,});
    WaitForAppSendRsResponse({.Status = true,});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});

    WaitPlanStepAck({.Step=100, .TxIds={txId}}); // TEvPlanStepAck для координатора
    WaitPlanStepAccepted({.Step=100});
}


Y_UNIT_TEST_F(PQTablet_App_SendReadSet_With_Abort_After_Commit, TPQTabletFixture)
{
    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(22222);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    const ui64 txId = 67890;

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={22222}, .Receivers={22222},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});

    WaitReadSet(*tablet, {.Step=100, .TxId=txId, .Source=Ctx->TabletId, .Target=22222, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});
    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId, .Target=Ctx->TabletId, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    SendAppSendRsRequest({.Step=100, .TxId=txId, .SenderId=22222, .Predicate=false,});
    WaitForAppSendRsResponse({.Status = true,});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED}); // RS=commit + ручной abort -> abort

    WaitPlanStepAck({.Step=100, .TxIds={txId}}); // TEvPlanStepAck для координатора
    WaitPlanStepAccepted({.Step=100});
}

Y_UNIT_TEST_F(PQTablet_App_SendReadSet_Invalid_Tx, TPQTabletFixture)
{
    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(22222);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    const ui64 txId = 67890;

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={22222}, .Receivers={22222},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});

    //WaitPlanStepAck({.Step=100, .TxIds={txId}}); // TEvPlanStepAck для координатора
    //WaitPlanStepAccepted({.Step=100});

    WaitReadSet(*tablet, {.Step=100, .TxId=txId, .Source=Ctx->TabletId, .Target=22222, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});

    SendAppSendRsRequest({.Step=100, .TxId=txId+1, .SenderId=22222, .Predicate=true,});
    WaitForAppSendRsResponse({.Status = false,});
}

Y_UNIT_TEST_F(PQTablet_App_SendReadSet_Invalid_Step, TPQTabletFixture)
{
    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(22222);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    const ui64 txId = 67890;

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={22222}, .Receivers={22222},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});

    //WaitPlanStepAck({.Step=100, .TxIds={txId}}); // TEvPlanStepAck для координатора
    //WaitPlanStepAccepted({.Step=100});

    WaitReadSet(*tablet, {.Step=100, .TxId=txId, .Source=Ctx->TabletId, .Target=22222, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});
    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId, .Target=Ctx->TabletId, .Decision=NKikimrTx::TReadSetData::DECISION_ABORT});

    SendAppSendRsRequest({.Step=101, .TxId=txId, .SenderId=22222, .Predicate=true,});
    WaitForAppSendRsResponse({.Status = false,});
}

Y_UNIT_TEST_F(ReadQuoter_ExclusiveLock, TPQTabletFixture)
{
    EnsureReadQuoterExists();
    PQTabletPrepare({.partitions = 1}, {}, *Ctx);
    //Ctx->Runtime->DispatchEvents();
    SendAcquireReadQuota(1, Ctx->Edge);
    WaitReadQuotaAcquired();

    SendAcquireExclusiveLock();
    ExpectNoExclusiveLockAcquired();

    SendReadQuotaConsumed(1);
    WaitExclusiveLockAcquired();

    SendAcquireReadQuota(2, Ctx->Edge);
    ExpectNoReadQuotaAcquired();

    SendReleaseExclusiveLock();
    WaitReadQuotaAcquired();
}

}

Y_UNIT_TEST_SUITE(TFixTransactionStatesTests) {

class TFixture : public NUnitTest::TBaseFixture {
protected:
    void AddReadRange();
    void AddPairFromPQ(ui64 txId, const TVector<ui32>& partitions);
    void AddPairFromPartition(ui64 txId, ui32 partitionId);

    void InvokeCollectTransactions();

    void EnsureTransactionPrepared(ui64 txId);
    void EnsureTransactionPlanned(ui64 txId);
    void EnsureTransactionExecuted(ui64 txId);

private:
    void EnsureTransactionState(ui64 txId, NKikimrPQ::TTransaction::EState state, TMaybe<ui64> step = Nothing()) const;
    void AddPair(const TString& key, const NKikimrPQ::TTransaction& tx);

    TVector<NKikimrClient::TKeyValueResponse::TReadRangeResult> ReadRanges;
    THashMap<ui64, NKikimrPQ::TTransaction> Txs;
    NKikimrPQ::TTransaction CurrentTx;
};

void TFixture::AddReadRange()
{
    NKikimrClient::TKeyValueResponse::TReadRangeResult readRange;
    readRange.SetStatus(NKikimrProto::OK);

    ReadRanges.emplace_back(std::move(readRange));
}

void TFixture::AddPairFromPQ(ui64 txId, const TVector<ui32>& partitions)
{
    NKikimrPQ::TTransaction tx;
    tx.SetKind(NKikimrPQ::TTransaction::KIND_DATA);
    tx.SetTxId(txId);
    tx.SetState(NKikimrPQ::TTransaction::PREPARED);

    for (const ui32 partitionId : partitions) {
        auto* operation = tx.AddOperations();
        operation->SetPartitionId(partitionId);
    }

    AddPair(GetTxKey(txId), tx);

    CurrentTx = std::move(tx);
}

void TFixture::AddPairFromPartition(ui64 txId, ui32 partitionId)
{
    NKikimrPQ::TTransaction tx = CurrentTx;
    tx.SetState(NKikimrPQ::TTransaction::EXECUTED);
    tx.SetStep(1000);

    AddPair(GetTxKey(txId, partitionId), tx);
}

void TFixture::InvokeCollectTransactions()
{
    Txs = CollectTransactions(ReadRanges);
}

void TFixture::EnsureTransactionPrepared(ui64 txId)
{
    EnsureTransactionState(txId, NKikimrPQ::TTransaction::PREPARED);
}

void TFixture::EnsureTransactionPlanned(ui64 txId)
{
    EnsureTransactionState(txId, NKikimrPQ::TTransaction::PLANNED, 1000);
}

void TFixture::EnsureTransactionExecuted(ui64 txId)
{
    EnsureTransactionState(txId, NKikimrPQ::TTransaction::EXECUTED, 1000);
}

void TFixture::EnsureTransactionState(ui64 txId, NKikimrPQ::TTransaction::EState state, TMaybe<ui64> step) const
{
    UNIT_ASSERT(Txs.contains(txId));
    const auto& tx = Txs.at(txId);
    UNIT_ASSERT(tx.HasState());
    UNIT_ASSERT_EQUAL_C(tx.GetState(), state,
                        NKikimrPQ::TTransaction_EState_Name(tx.GetState()) << " != " << NKikimrPQ::TTransaction_EState_Name(state));
    if (step.Defined()) {
        UNIT_ASSERT(tx.HasStep());
        UNIT_ASSERT_VALUES_EQUAL(tx.GetStep(), *step);
    }
}

void TFixture::AddPair(const TString& key, const NKikimrPQ::TTransaction& tx)
{
    TString value;
    UNIT_ASSERT(tx.SerializeToString(&value));

    auto& readRange = ReadRanges.back();
    auto* pair = readRange.AddPair();
    pair->SetKey(key);
    pair->SetValue(value);
}

Y_UNIT_TEST_F(Single_Transaction_No_Subtransactions, TFixture)
{
    AddReadRange();
    AddPairFromPQ(101, {1});

    InvokeCollectTransactions();

    EnsureTransactionPrepared(101);
}

Y_UNIT_TEST_F(Single_Transaction_All_Partitions, TFixture)
{
    AddReadRange();
    AddPairFromPQ(101, {1, 2});
    AddPairFromPartition(101, 1);
    AddPairFromPartition(101, 2);

    InvokeCollectTransactions();

    EnsureTransactionExecuted(101);
}

Y_UNIT_TEST_F(Single_Transaction_Partial_Partitions, TFixture)
{
    AddReadRange();
    AddPairFromPQ(101, {1, 2, 3});
    AddPairFromPartition(101, 1);
    AddPairFromPartition(101, 2);

    InvokeCollectTransactions();

    EnsureTransactionPlanned(101);
}

Y_UNIT_TEST_F(Multiple_Transactions_One_Range, TFixture)
{
    AddReadRange();
    AddPairFromPQ(101, {1});
    AddPairFromPartition(101, 1);
    AddPairFromPQ(102, {1});
    AddPairFromPartition(102, 1);
    AddPairFromPQ(103, {1, 2});
    AddPairFromPartition(103, 1);

    InvokeCollectTransactions();

    EnsureTransactionExecuted(101);
    EnsureTransactionExecuted(102);
    EnsureTransactionPlanned(103);
}

Y_UNIT_TEST_F(Multiple_Transactions_Different_Ranges, TFixture)
{
    AddReadRange();
    AddPairFromPQ(101, {1});
    AddPairFromPartition(101, 1);

    AddReadRange();
    AddPairFromPQ(102, {1, 2});
    AddPairFromPartition(102, 1);

    InvokeCollectTransactions();

    EnsureTransactionExecuted(101);
    EnsureTransactionPlanned(102);
}

Y_UNIT_TEST_F(Transaction_Adjacent_ReadRanges, TFixture)
{
    AddReadRange();
    AddPairFromPQ(101, {1, 2});

    AddReadRange();
    AddPairFromPartition(101, 1);
    AddPairFromPartition(101, 2);

    InvokeCollectTransactions();

    EnsureTransactionExecuted(101);
}

Y_UNIT_TEST_F(Transaction_Multiple_ReadRanges, TFixture)
{
    AddReadRange();
    AddPairFromPQ(101, {1, 2, 3});

    AddReadRange();
    AddPairFromPartition(101, 1);

    AddReadRange();
    AddPairFromPartition(101, 2);
    AddPairFromPartition(101, 3);

    InvokeCollectTransactions();

    EnsureTransactionExecuted(101);
}

Y_UNIT_TEST_F(Empty_ReadRange_In_Vector, TFixture)
{
    AddReadRange();

    AddReadRange();
    AddPairFromPQ(101, {1});

    InvokeCollectTransactions();

    EnsureTransactionPrepared(101);
}

Y_UNIT_TEST_F(Comprehensive_Test_Set_For_Complete_CollectTransactions_Testing, TFixture)
{
    // Пустой readRange (краевой случай)
    AddReadRange();

    // Транзакция без субтранзакций
    AddReadRange();
    AddPairFromPQ(101, {1});             // tx 101: 1 партиция, не записала -> PREPARED

    // Транзакция tx 102 полная в одном readRange
    AddReadRange();
    AddPairFromPQ(102, {1, 2, 3});       // tx 102: 3 партиции
    AddPairFromPartition(102, 1);        // tx 102: партиция 1 записала
    AddPairFromPartition(102, 2);        // tx 102: партиция 2 записала
    AddPairFromPartition(102, 3);        // tx 102: партиция 3 записала -> все 3/3 -> EXECUTED

    // Основная транзакция tx 103
    AddReadRange();
    AddPairFromPQ(103, {1, 2});          // tx 103: 2 партиции в другом readRange

    // Субтранзакции tx 103 + транзакция tx 104 (частичная)
    AddReadRange();
    AddPairFromPartition(103, 1);        // tx 103: партиция 1 записала -> 1/2 -> PLANNED
    AddPairFromPQ(104, {1, 2, 3, 4, 5}); // tx 104: много партиций
    AddPairFromPartition(104, 1);        // tx 104: партиция 1 записала
    AddPairFromPartition(104, 5);        // tx 104: партиция 5 записала (крайняя)

    // Транзакции tx 105 (полная) и tx 106 (частичная)
    AddReadRange();
    AddPairFromPQ(105, {1, 2});          // tx 105: 2 партиции
    AddPairFromPartition(105, 1);        // tx 105: партиция 1
    AddPairFromPartition(105, 2);        // tx 105: партиция 2 -> все 2/2 -> EXECUTED
    AddPairFromPQ(106, {1, 2, 3});       // tx 106: 3 партиции
    AddPairFromPartition(106, 2);        // tx 106: только партиция 2 записала -> 1/3 -> PLANNED

    InvokeCollectTransactions();

    EnsureTransactionPrepared(101);      // tx 101: без субтранзакций -> PREPARED
    EnsureTransactionExecuted(102);      // tx 102: все 3/3 партиций записали -> EXECUTED
    EnsureTransactionPlanned(103);    // tx 103: 1/2 партиций записали -> PLANNED
    EnsureTransactionPlanned(104);    // tx 104: 2/5 партиций записали -> PLANNED
    EnsureTransactionExecuted(105);      // tx 105: все 2/2 партиций записали -> EXECUTED
    EnsureTransactionPlanned(106);    // tx 106: 1/3 партиций записали -> PLANNED
}

}

}
