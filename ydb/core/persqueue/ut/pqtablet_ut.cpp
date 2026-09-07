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

namespace NKikimr::NPQ {

namespace NHelpers {

struct TTxOperation {
    ui32 Partition;
    TMaybe<TString> Consumer;
    TMaybe<ui64> Begin;
    TMaybe<ui64> End;
    TString Path;
    TMaybe<ui32> SupportivePartition;
    bool KafkaTransaction = false;
    TMaybe<NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi::EOp> DeferredPublicationOp;
};

TWriteId MakeDeferredWriteId(ui64 intPublicationId, const TString& extPublicationId = "ext-publication")
{
    NKikimrPQ::TWriteId proto;
    proto.MutableDeferredPublicationApi()->SetIntPublicationId(intPublicationId);
    proto.MutableDeferredPublicationApi()->SetExtPublicationId(extPublicationId);
    return TWriteId(std::move(proto));
}

struct TConfigParams {
    TMaybe<NKikimrPQ::TPQTabletConfig> Tablet;
    TMaybe<NKikimrPQ::TBootstrapConfig> Bootstrap;
};

struct TProposeTransactionParams {
    ui64 TxId = 0;
    TVector<ui64> Senders;
    TVector<ui64> Receivers;
    TVector<TTxOperation> TxOps;
    TMaybe<TConfigParams> Configs;
    TMaybe<TWriteId> WriteId;
    TMaybe<bool> Immediate;
};

struct TPlanStepParams {
    ui64 Step;
    TVector<ui64> TxIds;
    TMaybe<TActorId> Sender;
};

struct TReadSetParams {
    ui64 Step = 0;
    ui64 TxId = 0;
    ui64 Source = 0;
    ui64 Target = 0;
    bool Predicate = false;
};

struct TDropTabletParams {
    ui64 TxId = 0;
};

struct TCancelTransactionProposalParams {
    ui64 TxId = 0;
};

struct TGetOwnershipRequestParams {
    TMaybe<ui32> Partition;
    TMaybe<ui64> MsgNo;
    TMaybe<TWriteId> WriteId;
    TMaybe<bool> NeedSupportivePartition;
    TMaybe<TString> Owner; // o
    TMaybe<ui64> Cookie;
};

struct TWriteRequestParams {
    TMaybe<TString> Topic;
    TMaybe<ui32> Partition;
    TMaybe<TString> Owner;
    TMaybe<ui64> MsgNo;
    TMaybe<TWriteId> WriteId;
    TMaybe<TString> SourceId; // w
    TMaybe<ui64> SeqNo;       // w
    TMaybe<TString> Data;     // w
    //TMaybe<TInstant> CreateTime;
    //TMaybe<TInstant> WriteTime;
    TMaybe<ui64> Cookie;
};

struct TAppSendReadSetParams {
  ui64 Step = 0;
  ui64 TxId = 0;
  TMaybe<ui64> SenderId;
  bool Predicate = true;
};

using NKikimr::NPQ::NHelpers::CreatePQTabletMock;
using TPQTabletMock = NKikimr::NPQ::NHelpers::TPQTabletMock;

} // namespace NHelpers

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

class TPQTabletFixture : public NUnitTest::TBaseFixture {
protected:

    inline static const TString DEFAULT_OWNER = "-=[ 0wn3r ]=-";
    struct TProposeTransactionResponseMatcher {
        TMaybe<ui64> TxId;
        TMaybe<NKikimrPQ::TEvProposeTransactionResult::EStatus> Status;
    };

    struct TTxOperationMatcher {
        TMaybe<ui32> Partition;
        TMaybe<TString> Consumer;
        TMaybe<ui64> Begin;
        TMaybe<ui64> End;
    };

    struct TCmdWriteTxMatcher {
        TMaybe<ui64> TxId;
        TMaybe<NKikimrPQ::TTransaction::EState> State;
        TVector<ui64> Senders;
        TVector<ui64> Receivers;
        TVector<TTxOperationMatcher> TxOps;
    };

    struct TPlanStepAckMatcher {
        TMaybe<ui64> Step;
        TVector<ui64> TxIds;
    };

    struct TPlanStepAcceptedMatcher {
        TMaybe<ui64> Step;
    };

    struct TReadSetMatcher {
        TMaybe<ui64> Step;
        TMaybe<ui64> TxId;
        TMaybe<ui64> Source;
        TMaybe<ui64> Target;
        TMaybe<NKikimrTx::TReadSetData::EDecision> Decision;
        TMaybe<ui64> Producer;
        TMaybe<size_t> Count;
    };

    struct TReadSetAckMatcher {
        TMaybe<ui64> Step;
        TMaybe<ui64> TxId;
        TMaybe<ui64> Source;
        TMaybe<ui64> Target;
        TMaybe<ui64> Consumer;
    };

    struct TDropTabletReplyMatcher {
        TMaybe<NKikimrProto::EReplyStatus> Status;
        TMaybe<ui64> TxId;
        TMaybe<ui64> TabletId;
        TMaybe<NKikimrPQ::ETabletState> State;
    };

    struct TGetOwnershipResponseMatcher {
        TMaybe<ui64> Cookie;
        TMaybe<NMsgBusProxy::EResponseStatus> Status;
        TMaybe<NPersQueue::NErrorCode::EErrorCode> ErrorCode;
    };

    struct TWriteResponseMatcher {
        TMaybe<ui64> Cookie;
    };

    struct TAppSendReadSetMatcher {
        TMaybe<bool> Status;
    };

    struct TSendReadSetViaAppTestParams {
        size_t TabletsCount = 0;
        NKikimrTx::TReadSetData::EDecision Decision = NKikimrTx::TReadSetData::DECISION_UNKNOWN;
        size_t TabletsRSCount = 0;
        NKikimrTx::TReadSetData::EDecision AppDecision = NKikimrTx::TReadSetData::DECISION_UNKNOWN;
        bool ExpectedAppResponseStatus = true;
        NKikimrPQ::TEvProposeTransactionResult::EStatus ExpectedStatus = NKikimrPQ::TEvProposeTransactionResult::COMPLETE;
    };


    using TProposeTransactionParams = NHelpers::TProposeTransactionParams;
    using TPlanStepParams = NHelpers::TPlanStepParams;
    using TReadSetParams = NHelpers::TReadSetParams;
    using TDropTabletParams = NHelpers::TDropTabletParams;
    using TCancelTransactionProposalParams = NHelpers::TCancelTransactionProposalParams;
    using TGetOwnershipRequestParams = NHelpers::TGetOwnershipRequestParams;
    using TWriteRequestParams = NHelpers::TWriteRequestParams;
    using TAppSendReadSetParams = NHelpers::TAppSendReadSetParams;

    void SetUp(NUnitTest::TTestContext&) override;
    void TearDown(NUnitTest::TTestContext&) override;

    void ResetPipe();
    void EnsurePipeExist();
    void SendToPipe(const TActorId& sender,
                    IEventBase* event,
                    ui32 node = 0, ui64 cookie = 0);

    void SendProposeTransactionRequest(const TProposeTransactionParams& params);
    void WaitProposeTransactionResponse(const TProposeTransactionResponseMatcher& matcher = {});

    void SendPlanStep(const TPlanStepParams& params);
    void WaitPlanStepAck(const TPlanStepAckMatcher& matcher = {});
    void WaitPlanStepAccepted(const TPlanStepAcceptedMatcher& matcher = {});
    void WaitForNoPlanStepAccepted(TDuration timeout = TDuration::Seconds(2));

    void WaitReadSet(NHelpers::TPQTabletMock& tablet, const TReadSetMatcher& matcher);
    void WaitReadSetEx(NHelpers::TPQTabletMock& tablet, const TReadSetMatcher& matcher);
    void SendReadSet(const TReadSetParams& params);

    void WaitReadSetAck(NHelpers::TPQTabletMock& tablet, const TReadSetAckMatcher& matcher);
    void SendReadSetAck(NHelpers::TPQTabletMock& tablet);
    void WaitForNoReadSetAck(NHelpers::TPQTabletMock& tablet);

    void SendDropTablet(const TDropTabletParams& params);
    void WaitDropTabletReply(const TDropTabletReplyMatcher& matcher);

    void StartPQWriteStateObserver();
    void WaitForPQWriteState();

    void SendCancelTransactionProposal(const TCancelTransactionProposalParams& params);

    void StartPQWriteTxsObserver(TAutoPtr<IEventHandle>* ev = nullptr);
    void WaitForPQWriteTxs();

    template <class T> void WaitForEvent(size_t count);
    void WaitForCalcPredicateResult(size_t count = 1);
    void WaitForProposePartitionConfigResult(size_t count = 1);

    void TestWaitingForTEvReadSet(size_t senders, size_t receivers);

    void StartPQWriteObserver(bool& flag, unsigned cookie, TAutoPtr<IEventHandle>* ev = nullptr);
    void WaitForPQWriteComplete(bool& flag);

    bool FoundPQWriteState = false;
    bool FoundPQWriteTxs = false;

    bool WriteTxRequestInterceptActive_ = false;
    TTestActorRuntimeBase::TEventFilter PrevWriteTxRequestFilter_;
    TAutoPtr<IEventHandle> CapturedWriteTxRequest_;

    void SendGetOwnershipRequest(const TGetOwnershipRequestParams& params);
    // returns ownerCookie
    TString WaitGetOwnershipResponse(const TGetOwnershipResponseMatcher& matcher);
    void SyncGetOwnership(const TGetOwnershipRequestParams& params,
                             const TGetOwnershipResponseMatcher& matcher);

    void SendWriteRequest(const TWriteRequestParams& params);
    void WaitWriteResponse(const TWriteResponseMatcher& matcher);

    // returns owner cookie for this supportive partition
    TString CreateSupportivePartitionForKafka(const NKafka::TProducerInstanceId& producerInstanceId, const ui32 partitionId = 0);
    void SendKafkaTxnWriteRequest(const NKafka::TProducerInstanceId& producerInstanceId, const TString& ownerCookie, const ui32 partitionId = 0);
    void CommitKafkaTransaction(NKafka::TProducerInstanceId producerInstanceId, ui64 txId, const std::vector<ui32>& partitionIds = {0});

    TString CreateSupportivePartitionForDeferredPublication(const TWriteId& writeId, ui32 partitionId = 0);
    void SendDeferredPublicationWriteRequest(const TWriteId& writeId, const TString& ownerCookie, ui32 partitionId = 0);
    void SendDeferredPublicationWriteRequestWithoutWait(const TWriteId& writeId, const TString& ownerCookie, ui32 partitionId = 0);
    void WaitDeferredPublicationWriteResponse();
    TVector<TString> ReadMainPartitionMessages(ui32 partitionId = 0, ui32 count = 10);
    NKikimrClient::TCmdReadResult CmdReadCapture(const TPQCmdReadSettings& settings);
    void CommitTopicTransaction(const TWriteId& writeId, ui32 supportivePartitionId, ui64 txId,
                                const std::vector<ui32>& partitionIds = {0});
    void SendSupportivePartitionWrite(
        const TWriteId& writeId,
        const TString& ownerCookie,
        ui64 seqNo,
        ui64 messageNo,
        const TString& data,
        ui64 cookie,
        ui32 partitionId = 0);
    void CommitDeferredPublicationFinalize(
        const TWriteId& writeId,
        ui64 txId,
        NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi::EOp op,
        const std::vector<ui32>& partitionIds = {0});
    void AbortDeferredPublicationFinalize(
        const TWriteId& writeId,
        ui64 txId,
        NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi::EOp op,
        const std::vector<ui32>& partitionIds = {0});
    void SendAbortDeferredStagingRequest(const TWriteId& writeId, ui32 partitionId = 0, ui64 cookie = 55);
    void WaitAbortDeferredStagingResponse(ui64 cookie = 55);

    std::unique_ptr<TEvPersQueue::TEvRequest> MakeGetOwnershipRequest(const TGetOwnershipRequestParams& params,
                                                                      const TActorId& pipe) const;

    void TestMultiplePQTablets(const TString& consumer1, const TString& consumer2);
    void TestParallelTransactions(const TString& consumer1, const TString& consumer2);

    void AssertTabletIsAlive(ui64 txId = 2);

    void StartPQCalcPredicateObserver(size_t& received);
    void WaitForPQCalcPredicate(size_t& received, size_t expected);

    void WaitForTxState(ui64 txId, NKikimrPQ::TTransaction::EState state);
    void WaitForExecStep(ui64 step);

    void InterceptSaveTxState(TAutoPtr<IEventHandle>& event);
    void SendSaveTxState(TAutoPtr<IEventHandle>& event);

    void WaitForTheTransactionToBeDeleted(ui64 txId);
    void AssertTransactionInKV(ui64 txId);

    TVector<TString> WaitForExactSupportivePartitionsCount(ui32 expectedCount);
    TVector<TString> GetSupportivePartitionsKeysFromKV();
    NKikimrPQ::TTabletTxInfo WaitForExactTxWritesCount(ui32 expectedCount);
    NKikimrPQ::TTabletTxInfo GetTxWritesFromKV();

    void BeginInterceptWriteTxRequest();
    void WaitForCapturedWriteTxRequest();
    NKikimrPQ::TTabletTxInfo GetCapturedTxWritesFromWriteTxRequestAndFlush();
    void EndInterceptWriteTxRequest();

    void InstallWriteTxRequestInterceptFilter();

    void SendAppSendRsRequest(const TAppSendReadSetParams& params);
    void WaitForAppSendRsResponse(const TAppSendReadSetMatcher& matcher);
    void TestSendingTEvReadSetViaApp(const TSendReadSetViaAppTestParams& params);

    template<class EventType>
    void AddOneTimeEventObserver(bool& seenEvent,
                                 ui32 unseenEventCount,
                                 std::function<TTestActorRuntimeBase::EEventAction(TAutoPtr<IEventHandle>&)> callback = [](){return TTestActorRuntimeBase::EEventAction::PROCESS;});

    void ExpectNoExclusiveLockAcquired();
    void ExpectNoReadQuotaAcquired();
    void SendAcquireExclusiveLock();
    void SendAcquireReadQuota(ui64 cookie, const TActorId& sender);
    void SendReadQuotaConsumed(ui64 cookie);
    void SendReleaseExclusiveLock();
    void WaitExclusiveLockAcquired();
    void WaitReadQuotaAcquired();

    void EnsureReadQuoterExists();

    //
    // TODO(abcdef): для тестирования повторных вызовов нужны примитивы Send+Wait
    //

    NHelpers::TPQTabletMock* CreatePQTabletMock(ui64 tabletId);

    TMaybe<TTestContext> Ctx;
    TMaybe<TFinalizer> Finalizer;

    TTestActorRuntimeBase::TEventObserver PrevEventObserver;

    TActorId Pipe;

    struct TReadQuoter {
        NKikimrPQ::TPQConfig PQConfig;
        NPersQueue::TTopicConverterPtr TopicConverter;
        NKikimrPQ::TPQTabletConfig PQTabletConfig;
        TPartitionId PartitionId;
        std::shared_ptr<TTabletCountersBase> Counters = std::make_shared<TTabletCountersBase>();
        TActorId Quoter;
    };

    TMaybe<TReadQuoter> ReadQuoter;
};

void TPQTabletFixture::SetUp(NUnitTest::TTestContext&)
{
    Ctx.ConstructInPlace();
    Ctx->EnableDetailedPQLog = true;

    Finalizer.ConstructInPlace(*Ctx);

    Ctx->Prepare();
    Ctx->Runtime->GetAppData(0).FeatureFlags.SetEnableTabletDevUiSecurePath(true);
    Ctx->Runtime->SetScheduledLimit(5'000);
}

void TPQTabletFixture::TearDown(NUnitTest::TTestContext&)
{
    ResetPipe();
}

void TPQTabletFixture::ResetPipe()
{
    if (Pipe != TActorId()) {
        Ctx->Runtime->ClosePipe(Pipe, Ctx->Edge, 0);
        Pipe = TActorId();
    }
}

void TPQTabletFixture::EnsurePipeExist()
{
    if (Pipe == TActorId()) {
        Pipe = Ctx->Runtime->ConnectToPipe(Ctx->TabletId,
                                           Ctx->Edge,
                                           0,
                                           GetPipeConfigWithRetries());
    }

    Y_ABORT_UNLESS(Pipe != TActorId());
}

void TPQTabletFixture::SendToPipe(const TActorId& sender,
                                  IEventBase* event,
                                  ui32 node, ui64 cookie)
{
    EnsurePipeExist();

    Ctx->Runtime->SendToPipe(Pipe,
                             sender,
                             event,
                             node, cookie);
}

void TPQTabletFixture::SendProposeTransactionRequest(const TProposeTransactionParams& params)
{
    auto event = MakeHolder<TEvPersQueue::TEvProposeTransactionBuilder>();
    THashSet<ui32> partitions;

    ActorIdToProto(Ctx->Edge, event->Record.MutableSourceActor());
    event->Record.SetTxId(params.TxId);

    if (params.Configs) {
        //
        // TxBody.Config
        //
        auto* body = event->Record.MutableConfig();
        if (params.Configs->Tablet.Defined()) {
            *body->MutableTabletConfig() = *params.Configs->Tablet;
        }
        if (params.Configs->Bootstrap.Defined()) {
            *body->MutableBootstrapConfig() = *params.Configs->Bootstrap;
        }
    } else {
        //
        // TxBody.Data
        //
        auto* body = event->Record.MutableData();
        for (auto& txOp : params.TxOps) {
            auto* operation = body->MutableOperations()->Add();
            operation->SetPartitionId(txOp.Partition);
            if (txOp.Begin.Defined()) {
                operation->SetCommitOffsetsBegin(*txOp.Begin);
                operation->SetCommitOffsetsEnd(*txOp.End);
                operation->SetConsumer(*txOp.Consumer);
            }
            operation->SetPath(txOp.Path);
            if (txOp.SupportivePartition.Defined()) {
                operation->SetSupportivePartition(*txOp.SupportivePartition);
            }
            if (txOp.KafkaTransaction) {
                operation->SetKafkaTransaction(true);
            }
            if (txOp.DeferredPublicationOp.Defined()) {
                operation->MutableWrite()->MutableDeferredPublication()->SetOp(*txOp.DeferredPublicationOp);
            }

            partitions.insert(txOp.Partition);
        }
        for (ui64 tabletId : params.Senders) {
            body->AddSendingShards(tabletId);
        }
        for (ui64 tabletId : params.Receivers) {
            body->AddReceivingShards(tabletId);
        }
        if (params.WriteId) {
            SetWriteId(*body, *params.WriteId);
        }
        if (params.Immediate.Defined()) {
            body->SetImmediate(*params.Immediate);
        } else {
            body->SetImmediate(params.Senders.empty() && params.Receivers.empty() && (partitions.size() == 1) && !params.WriteId.Defined());
        }
    }

    SendToPipe(Ctx->Edge,
               event.Release());
}

void TPQTabletFixture::WaitProposeTransactionResponse(const TProposeTransactionResponseMatcher& matcher)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPersQueue::TEvProposeTransactionResult>();
    UNIT_ASSERT(event != nullptr);

    if (matcher.TxId) {
        UNIT_ASSERT(event->Record.HasTxId());
        UNIT_ASSERT_VALUES_EQUAL(*matcher.TxId, event->Record.GetTxId());
    }

    if (matcher.Status) {
        UNIT_ASSERT(event->Record.HasStatus());
        UNIT_ASSERT_EQUAL_C(*matcher.Status, event->Record.GetStatus(),
                            "expected: " << NKikimrPQ::TEvProposeTransactionResult_EStatus_Name(*matcher.Status) <<
                            ", received " << NKikimrPQ::TEvProposeTransactionResult_EStatus_Name(event->Record.GetStatus()));
    }
}

void TPQTabletFixture::SendPlanStep(const TPlanStepParams& params)
{
    auto event = MakeHolder<TEvTxProcessing::TEvPlanStep>();
    event->Record.SetStep(params.Step);
    for (ui64 txId : params.TxIds) {
        auto tx = event->Record.AddTransactions();

        tx->SetTxId(txId);
        ActorIdToProto(Ctx->Edge, tx->MutableAckTo());
    }

    const TActorId sender = params.Sender.GetOrElse(Ctx->Edge);
    SendToPipe(sender,
               event.Release());
}

void TPQTabletFixture::WaitPlanStepAck(const TPlanStepAckMatcher& matcher)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvTxProcessing::TEvPlanStepAck>();
    UNIT_ASSERT(event != nullptr);

    if (matcher.Step.Defined()) {
        UNIT_ASSERT(event->Record.HasStep());
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Step, event->Record.GetStep());
    }

    UNIT_ASSERT_VALUES_EQUAL(matcher.TxIds.size(), event->Record.TxIdSize());
    for (size_t i = 0; i < event->Record.TxIdSize(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(matcher.TxIds[i], event->Record.GetTxId(i));
    }
}

void TPQTabletFixture::WaitPlanStepAccepted(const TPlanStepAcceptedMatcher& matcher)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvTxProcessing::TEvPlanStepAccepted>();
    UNIT_ASSERT(event != nullptr);

    if (matcher.Step.Defined()) {
        UNIT_ASSERT(event->Record.HasStep());
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Step, event->Record.GetStep());
    }
}

void TPQTabletFixture::WaitForNoPlanStepAccepted(TDuration timeout)
{
    bool sawAccepted = false;
    auto prev = Ctx->Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
        if (event->GetTypeRewrite() == TEvTxProcessing::TEvPlanStepAccepted::EventType) {
            sawAccepted = true;
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    });

    TDispatchOptions options;
    options.CustomFinalCondition = [&]() {
        return sawAccepted;
    };
    Ctx->Runtime->DispatchEvents(options, timeout);
    Ctx->Runtime->SetObserverFunc(prev);

    UNIT_ASSERT(!sawAccepted);
}

void TPQTabletFixture::WaitReadSet(NHelpers::TPQTabletMock& tablet, const TReadSetMatcher& matcher)
{
    auto tryMatch = [](const TReadSetMatcher& matcher, const NKikimrTx::TEvReadSet& readSet) {
        if (matcher.Step.Defined()) {
            UNIT_ASSERT(readSet.HasStep());
            UNIT_ASSERT_VALUES_EQUAL(*matcher.Step, readSet.GetStep());
        }
        if (matcher.TxId.Defined()) {
            UNIT_ASSERT(readSet.HasTxId());
            UNIT_ASSERT_VALUES_EQUAL(*matcher.TxId, readSet.GetTxId());
        }
        if (matcher.Source.Defined()) {
            UNIT_ASSERT(readSet.HasTabletSource());
            UNIT_ASSERT_VALUES_EQUAL(*matcher.Source, readSet.GetTabletSource());
        }
        if (matcher.Target.Defined()) {
            UNIT_ASSERT(readSet.HasTabletDest());
            UNIT_ASSERT_VALUES_EQUAL(*matcher.Target, readSet.GetTabletDest());
        }
        if (matcher.Decision.Defined()) {
            UNIT_ASSERT(readSet.HasReadSet());

            NKikimrTx::TReadSetData data;
            Y_ABORT_UNLESS(data.ParseFromString(readSet.GetReadSet()));

            UNIT_ASSERT_EQUAL(*matcher.Decision, data.GetDecision());
        }
        if (matcher.Producer.Defined()) {
            UNIT_ASSERT(readSet.HasTabletProducer());
            UNIT_ASSERT_VALUES_EQUAL(*matcher.Producer, readSet.GetTabletProducer());
        }
    };

    if (matcher.Step.Defined() && matcher.TxId.Defined()) {
        const ui64 step = *matcher.Step;
        const ui64 txId = *matcher.TxId;
        const auto key = std::make_pair(step, txId);

        auto p = tablet.ReadSets.find(std::make_pair(step, txId));
        if (p == tablet.ReadSets.end()) {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]() {
                return tablet.ReadSets.contains(key);
            };
            UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));

            p = tablet.ReadSets.find(key);
        }

        const auto& records = p->second;
        UNIT_ASSERT_VALUES_EQUAL(records.size(), 1);

        tryMatch(matcher, records.front());

        return;
    }

    if (!tablet.ReadSet.Defined()) {
        TDispatchOptions options;
        options.CustomFinalCondition = [&]() {
            return tablet.ReadSet.Defined();
        };
        UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    }

    auto readSet = std::move(*tablet.ReadSet);
    tablet.ReadSet = Nothing();

    tryMatch(matcher, readSet);
}

void TPQTabletFixture::WaitReadSetEx(NHelpers::TPQTabletMock& tablet, const TReadSetMatcher& matcher)
{
    TDispatchOptions options;
    options.CustomFinalCondition = [&]() {
        return tablet.ReadSets[std::make_pair(*matcher.Step, *matcher.TxId)].size() >= *matcher.Count;
    };
    UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
}

void TPQTabletFixture::SendReadSet(const TReadSetParams& params)
{
    NKikimrTx::TReadSetData payload;
    payload.SetDecision(params.Predicate ? NKikimrTx::TReadSetData::DECISION_COMMIT : NKikimrTx::TReadSetData::DECISION_ABORT);

    TString body;
    Y_ABORT_UNLESS(payload.SerializeToString(&body));

    auto event = std::make_unique<TEvTxProcessing::TEvReadSet>(params.Step,
                                                               params.TxId,
                                                               params.Source,
                                                               params.Target,
                                                               params.Source,
                                                               body,
                                                               0);

    SendToPipe(Ctx->Edge,
               event.release());
}

void TPQTabletFixture::WaitReadSetAck(NHelpers::TPQTabletMock& tablet, const TReadSetAckMatcher& matcher)
{
    if (!tablet.ReadSetAck.Defined()) {
        TDispatchOptions options;
        options.CustomFinalCondition = [&]() {
            return tablet.ReadSetAck.Defined();
        };
        UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    }

    if (matcher.Step.Defined()) {
        UNIT_ASSERT(tablet.ReadSetAck->HasStep());
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Step, tablet.ReadSetAck->GetStep());
    }
    if (matcher.TxId.Defined()) {
        UNIT_ASSERT(tablet.ReadSetAck->HasTxId());
        UNIT_ASSERT_VALUES_EQUAL(*matcher.TxId, tablet.ReadSetAck->GetTxId());
    }
    if (matcher.Source.Defined()) {
        UNIT_ASSERT(tablet.ReadSetAck->HasTabletSource());
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Source, tablet.ReadSetAck->GetTabletSource());
    }
    if (matcher.Target.Defined()) {
        UNIT_ASSERT(tablet.ReadSetAck->HasTabletDest());
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Target, tablet.ReadSetAck->GetTabletDest());
    }
    if (matcher.Consumer.Defined()) {
        UNIT_ASSERT(tablet.ReadSetAck->HasTabletConsumer());
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Consumer, tablet.ReadSetAck->GetTabletConsumer());
    }
}

void TPQTabletFixture::WaitForNoReadSetAck(NHelpers::TPQTabletMock& tablet)
{
    TDispatchOptions options;
    options.CustomFinalCondition = [&]() {
        return tablet.ReadSetAck.Defined();
    };
    Ctx->Runtime->DispatchEvents(options, TDuration::Seconds(2));

    UNIT_ASSERT(!tablet.ReadSetAck.Defined());
}

void TPQTabletFixture::SendDropTablet(const TDropTabletParams& params)
{
    auto event = MakeHolder<TEvPersQueue::TEvDropTablet>();
    event->Record.SetTxId(params.TxId);
    event->Record.SetRequestedState(NKikimrPQ::EDropped);

    SendToPipe(Ctx->Edge,
               event.Release());
}

void TPQTabletFixture::WaitDropTabletReply(const TDropTabletReplyMatcher& matcher)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPersQueue::TEvDropTabletReply>();
    UNIT_ASSERT(event != nullptr);

    if (matcher.Status.Defined()) {
        UNIT_ASSERT(event->Record.HasStatus());
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Status, event->Record.GetStatus());
    }
    if (matcher.TxId.Defined()) {
        UNIT_ASSERT(event->Record.HasTxId());
        UNIT_ASSERT_VALUES_EQUAL(*matcher.TxId, event->Record.GetTxId());
    }
    if (matcher.TabletId.Defined()) {
        UNIT_ASSERT(event->Record.HasTabletId());
        UNIT_ASSERT_VALUES_EQUAL(*matcher.TabletId, event->Record.GetTabletId());
    }
    if (matcher.State.Defined()) {
        UNIT_ASSERT(event->Record.HasActualState());
        UNIT_ASSERT_EQUAL(*matcher.State, event->Record.GetActualState());
    }
}

template <class T>
void TPQTabletFixture::WaitForEvent(size_t count)
{
    bool found = false;
    size_t received = 0;

    TTestActorRuntimeBase::TEventObserver prev;
    auto observer = [&found, &prev, &received, count](TAutoPtr<IEventHandle>& event) {
        if (auto* msg = event->CastAsLocal<T>()) {
            ++received;
            found = (received >= count);
        }

        return prev ? prev(event) : TTestActorRuntimeBase::EEventAction::PROCESS;
    };

    prev = Ctx->Runtime->SetObserverFunc(observer);

    TDispatchOptions options;
    options.CustomFinalCondition = [&found]() {
        return found;
    };

    UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));

    Ctx->Runtime->SetObserverFunc(prev);
}

void TPQTabletFixture::WaitForCalcPredicateResult(size_t count)
{
    WaitForEvent<TEvPQ::TEvTxCalcPredicateResult>(count);
}

void TPQTabletFixture::WaitForProposePartitionConfigResult(size_t count)
{
    WaitForEvent<TEvPQ::TEvProposePartitionConfigResult>(count);
}

std::unique_ptr<TEvPersQueue::TEvRequest> TPQTabletFixture::MakeGetOwnershipRequest(const TGetOwnershipRequestParams& params,
                                                                                    const TActorId& pipe) const
{
    auto event = std::make_unique<TEvPersQueue::TEvRequest>();
    auto* request = event->Record.MutablePartitionRequest();
    auto* command = request->MutableCmdGetOwnership();

    if (params.Partition.Defined()) {
        request->SetPartition(*params.Partition);
    }
    if (params.MsgNo.Defined()) {
        request->SetMessageNo(*params.MsgNo);
    }
    if (params.WriteId.Defined()) {
        SetWriteId(*request, *params.WriteId);
    }
    if (params.NeedSupportivePartition.Defined()) {
        request->SetNeedSupportivePartition(*params.NeedSupportivePartition);
    }
    if (params.Cookie.Defined()) {
        request->SetCookie(*params.Cookie);
    }

    ActorIdToProto(pipe, request->MutablePipeClient());

    if (params.Owner.Defined()) {
        command->SetOwner(*params.Owner);
    }

    command->SetForce(true);

    return event;
}

void TPQTabletFixture::SyncGetOwnership(const TGetOwnershipRequestParams& params,
                                        const TGetOwnershipResponseMatcher& matcher)
{
    TActorId pipe = Ctx->Runtime->ConnectToPipe(Ctx->TabletId,
                                                Ctx->Edge,
                                                0,
                                                GetPipeConfigWithRetries());

    auto request = MakeGetOwnershipRequest(params, pipe);
    Ctx->Runtime->SendToPipe(pipe,
                             Ctx->Edge,
                             request.release(),
                             0, 0);
    WaitGetOwnershipResponse(matcher);

    Ctx->Runtime->ClosePipe(pipe, Ctx->Edge, 0);
}

void TPQTabletFixture::SendGetOwnershipRequest(const TGetOwnershipRequestParams& params)
{
    EnsurePipeExist();

    auto request = MakeGetOwnershipRequest(params, Pipe);

    SendToPipe(Ctx->Edge,
               request.release());
}

// returns owner cookie
TString TPQTabletFixture::WaitGetOwnershipResponse(const TGetOwnershipResponseMatcher& matcher)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>();
    UNIT_ASSERT(event != nullptr);

    if (matcher.Cookie.Defined()) {
        UNIT_ASSERT(event->Record.GetPartitionResponse().HasCookie());
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Cookie, event->Record.GetPartitionResponse().GetCookie());
    }
    if (matcher.Status.Defined()) {
        UNIT_ASSERT(event->Record.HasStatus());
        UNIT_ASSERT_VALUES_EQUAL((int)*matcher.Status, (int)event->Record.GetStatus());
    }
    if (matcher.ErrorCode.Defined()) {
        UNIT_ASSERT(event->Record.HasErrorCode());
        UNIT_ASSERT_VALUES_EQUAL((int)*matcher.ErrorCode, (int)event->Record.GetErrorCode());
    }

    return event->Record.GetPartitionResponse().GetCmdGetOwnershipResult().GetOwnerCookie();
}

void TPQTabletFixture::SendWriteRequest(const TWriteRequestParams& params)
{
    auto event = MakeHolder<TEvPersQueue::TEvRequest>();
    auto* request = event->Record.MutablePartitionRequest();

    if (params.Topic.Defined()) {
        request->SetTopic(*params.Topic);
    }
    if (params.Partition.Defined()) {
        request->SetPartition(*params.Partition);
    }
    if (params.Owner.Defined()) {
        request->SetOwnerCookie(*params.Owner);
    }
    if (params.MsgNo.Defined()) {
        request->SetMessageNo(*params.MsgNo);
    }
    if (params.WriteId.Defined()) {
        SetWriteId(*request, *params.WriteId);
    }
    if (params.Cookie.Defined()) {
        request->SetCookie(*params.Cookie);
    }

    EnsurePipeExist();
    ActorIdToProto(Pipe, request->MutablePipeClient());

    auto* command = request->AddCmdWrite();

    if (params.SourceId.Defined()) {
        command->SetSourceId(*params.SourceId);
    }
    if (params.SeqNo.Defined()) {
        command->SetSeqNo(*params.SeqNo);
    }
    if (params.Data.Defined()) {
        command->SetData(*params.Data);
    }

    SendToPipe(Ctx->Edge,
               event.Release());
}

TString TPQTabletFixture::CreateSupportivePartitionForKafka(const NKafka::TProducerInstanceId& producerInstanceId,
                                                            const ui32 partitionId) {
    EnsurePipeExist();

    auto request = MakeGetOwnershipRequest({.Partition=partitionId,
                     .WriteId=TWriteId{producerInstanceId},
                     .NeedSupportivePartition=true,
                     .Owner=DEFAULT_OWNER,
                     .Cookie=4}, Pipe);
    Ctx->Runtime->SendToPipe(Pipe,
                             Ctx->Edge,
                             request.release(),
                             0, 0);

    return WaitGetOwnershipResponse({.Cookie=4, .Status=NMsgBusProxy::MSTATUS_OK});
}

void TPQTabletFixture::SendKafkaTxnWriteRequest(const NKafka::TProducerInstanceId& producerInstanceId, const TString& ownerCookie, const ui32 partitionId) {
    auto event = MakeHolder<TEvPersQueue::TEvRequest>();
    auto* request = event->Record.MutablePartitionRequest();
    request->SetTopic("/topic");
    request->SetPartition(partitionId);
    request->SetCookie(123);
    request->SetOwnerCookie(ownerCookie);
    request->SetMessageNo(0);

    auto* writeId = request->MutableWriteId();
    writeId->SetKafkaTransaction(true);
    auto* requestProducerInstanceId = writeId->MutableKafkaProducerInstanceId();
    requestProducerInstanceId->SetId(producerInstanceId.Id);
    requestProducerInstanceId->SetEpoch(producerInstanceId.Epoch);

    EnsurePipeExist();
    ActorIdToProto(Pipe, request->MutablePipeClient());

    auto cmdWrite = request->AddCmdWrite();
    cmdWrite->SetSourceId(std::to_string(producerInstanceId.Id));
    cmdWrite->SetSeqNo(0);
    TString data = "123test123";
    cmdWrite->SetData(data);
    cmdWrite->SetCreateTimeMS(TInstant::Now().MilliSeconds());
    cmdWrite->SetDisableDeduplication(true);
    cmdWrite->SetUncompressedSize(data.size());
    cmdWrite->SetIgnoreQuotaDeadline(true);
    cmdWrite->SetExternalOperation(true);

    SendToPipe(Ctx->Edge, event.Release());

    // wait for response
    auto response = Ctx->Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>();
    UNIT_ASSERT(response != nullptr);
    UNIT_ASSERT(response->Record.GetPartitionResponse().HasCookie());
    UNIT_ASSERT_VALUES_EQUAL(123, response->Record.GetPartitionResponse().GetCookie());
}

void TPQTabletFixture::CommitKafkaTransaction(NKafka::TProducerInstanceId producerInstanceId, ui64 txId, const std::vector<ui32>& partitionIds) {
    TProposeTransactionParams params;
    params.TxId = txId;
    params.Senders = {Ctx->TabletId};
    params.Receivers = {Ctx->TabletId};
    params.WriteId = TWriteId(producerInstanceId);
    for (const ui32& partitionId : partitionIds) {
        params.TxOps.push_back({.Partition=partitionId, .Path="/topic", .KafkaTransaction=true});
    }
    SendProposeTransactionRequest(params);
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});
    SendPlanStep({.Step=100, .TxIds={txId}});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});
    WaitPlanStepAck({.Step=100, .TxIds={txId}}); // TEvPlanStepAck для координатора
    WaitPlanStepAccepted({.Step=100});
}

TString TPQTabletFixture::CreateSupportivePartitionForDeferredPublication(const TWriteId& writeId, const ui32 partitionId) {
    EnsurePipeExist();

    auto request = MakeGetOwnershipRequest({.Partition=partitionId,
                     .WriteId=writeId,
                     .NeedSupportivePartition=true,
                     .Owner=DEFAULT_OWNER,
                     .Cookie=4}, Pipe);
    Ctx->Runtime->SendToPipe(Pipe,
                             Ctx->Edge,
                             request.release(),
                             0, 0);

    return WaitGetOwnershipResponse({.Cookie=4, .Status=NMsgBusProxy::MSTATUS_OK});
}

void TPQTabletFixture::SendDeferredPublicationWriteRequestWithoutWait(
    const TWriteId& writeId,
    const TString& ownerCookie,
    const ui32 partitionId)
{
    EnsurePipeExist();

    auto event = MakeHolder<TEvPersQueue::TEvRequest>();
    auto* request = event->Record.MutablePartitionRequest();
    request->SetTopic("/topic");
    request->SetPartition(partitionId);
    request->SetCookie(123);
    request->SetOwnerCookie(ownerCookie);
    request->SetMessageNo(0);
    SetWriteId(*request, writeId);

    ActorIdToProto(Pipe, request->MutablePipeClient());

    auto* cmdWrite = request->AddCmdWrite();
    cmdWrite->SetSourceId("deferred-source");
    cmdWrite->SetSeqNo(0);
    const TString data = "deferred-publish-payload";
    cmdWrite->SetData(data);
    cmdWrite->SetCreateTimeMS(TInstant::Now().MilliSeconds());
    cmdWrite->SetDisableDeduplication(true);
    cmdWrite->SetUncompressedSize(data.size());
    cmdWrite->SetIgnoreQuotaDeadline(true);
    cmdWrite->SetExternalOperation(true);

    SendToPipe(Ctx->Edge, event.Release());
}

void TPQTabletFixture::WaitDeferredPublicationWriteResponse() {
    bool found = false;
    auto observer = [&found](TAutoPtr<IEventHandle>& event) {
        if (auto* msg = event->CastAsLocal<TEvPersQueue::TEvResponse>()) {
            const auto& partitionResponse = msg->Record.GetPartitionResponse();
            if (partitionResponse.HasCookie() && partitionResponse.GetCookie() == 123) {
                found = true;
            }
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    };
    auto prev = Ctx->Runtime->SetObserverFunc(observer);

    TDispatchOptions options;
    options.CustomFinalCondition = [&found]() {
        return found;
    };
    UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    Ctx->Runtime->SetObserverFunc(prev);
}

void TPQTabletFixture::SendDeferredPublicationWriteRequest(const TWriteId& writeId, const TString& ownerCookie, const ui32 partitionId) {
    SendDeferredPublicationWriteRequestWithoutWait(writeId, ownerCookie, partitionId);
    WaitDeferredPublicationWriteResponse();
}

void TPQTabletFixture::SendAbortDeferredStagingRequest(
    const TWriteId& writeId,
    const ui32 partitionId,
    const ui64 cookie)
{
    EnsurePipeExist();

    auto event = MakeHolder<TEvPersQueue::TEvRequest>();
    auto* request = event->Record.MutablePartitionRequest();
    request->SetTopic("/topic");
    request->SetPartition(partitionId);
    request->SetCookie(cookie);
    SetWriteId(*request, writeId);
    request->MutableCmdAbortDeferredStaging();
    ActorIdToProto(Pipe, request->MutablePipeClient());

    SendToPipe(Ctx->Edge, event.Release());
}

void TPQTabletFixture::WaitAbortDeferredStagingResponse(const ui64 cookie) {
    bool found = false;
    auto observer = [&found, cookie](TAutoPtr<IEventHandle>& event) {
        if (auto* msg = event->CastAsLocal<TEvPersQueue::TEvResponse>()) {
            const auto& partitionResponse = msg->Record.GetPartitionResponse();
            if (partitionResponse.HasCookie() && partitionResponse.GetCookie() == cookie
                && partitionResponse.HasCmdAbortDeferredStagingResult()) {
                found = true;
            }
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    };
    auto prev = Ctx->Runtime->SetObserverFunc(observer);

    TDispatchOptions options;
    options.CustomFinalCondition = [&found]() {
        return found;
    };
    UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    Ctx->Runtime->SetObserverFunc(prev);
}

TVector<TString> TPQTabletFixture::ReadMainPartitionMessages(const ui32 partitionId, const ui32 count) {
    TPQCmdReadSettings readSettings{"", partitionId, 0, count, 16_MB, 0};

    const auto readResult = CmdReadCapture(readSettings);

    TVector<TString> payloads;
    payloads.reserve(readResult.ResultSize());
    for (ui32 i = 0; i < readResult.ResultSize(); ++i) {
        payloads.push_back(readResult.GetResult(i).GetData());
    }
    return payloads;
}

NKikimrClient::TCmdReadResult TPQTabletFixture::CmdReadCapture(const TPQCmdReadSettings& settings) {
    bool found = false;
    NKikimrClient::TCmdReadResult readResult;
    auto observer = [&found, &readResult](TAutoPtr<IEventHandle>& event) {
        if (auto* msg = event->CastAsLocal<TEvPersQueue::TEvResponse>()) {
            const auto& partitionResponse = msg->Record.GetPartitionResponse();
            if (partitionResponse.HasCookie() && partitionResponse.GetCookie() == 123
                && partitionResponse.HasCmdReadResult()) {
                readResult = partitionResponse.GetCmdReadResult();
                found = true;
            }
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    };
    auto prev = Ctx->Runtime->SetObserverFunc(observer);
    BeginCmdRead(settings, *Ctx);
    TDispatchOptions options;
    options.CustomFinalCondition = [&found]() { return found; };
    UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    Ctx->Runtime->SetObserverFunc(prev);
    return readResult;
}

void TPQTabletFixture::SendSupportivePartitionWrite(
    const TWriteId& writeId,
    const TString& ownerCookie,
    const ui64 seqNo,
    const ui64 messageNo,
    const TString& data,
    const ui64 cookie,
    const ui32 partitionId)
{
    EnsurePipeExist();

    auto event = MakeHolder<TEvPersQueue::TEvRequest>();
    auto* request = event->Record.MutablePartitionRequest();
    request->SetTopic("/topic");
    request->SetPartition(partitionId);
    request->SetCookie(cookie);
    request->SetOwnerCookie(ownerCookie);
    request->SetMessageNo(messageNo);
    SetWriteId(*request, writeId);
    ActorIdToProto(Pipe, request->MutablePipeClient());

    auto* cmdWrite = request->AddCmdWrite();
    cmdWrite->SetSourceId("tx-src");
    cmdWrite->SetSeqNo(seqNo);
    cmdWrite->SetData(data);
    cmdWrite->SetCreateTimeMS(TInstant::Now().MilliSeconds());
    cmdWrite->SetDisableDeduplication(true);
    cmdWrite->SetUncompressedSize(data.size());
    cmdWrite->SetIgnoreQuotaDeadline(true);
    cmdWrite->SetExternalOperation(true);

    SendToPipe(Ctx->Edge, event.Release());
    auto response = Ctx->Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>();
    UNIT_ASSERT(response != nullptr);
    UNIT_ASSERT_VALUES_EQUAL(response->Record.GetPartitionResponse().GetCookie(), cookie);
}

void TPQTabletFixture::CommitTopicTransaction(
    const TWriteId& writeId,
    const ui32 supportivePartitionId,
    const ui64 txId,
    const std::vector<ui32>& partitionIds)
{
    EnsurePipeExist();

    TProposeTransactionParams params;
    params.TxId = txId;
    params.Senders = {Ctx->TabletId};
    params.Receivers = {Ctx->TabletId};
    params.WriteId = writeId;
    for (const ui32& partitionId : partitionIds) {
        params.TxOps.push_back({
            .Partition = partitionId,
            .Path = "/topic",
            .SupportivePartition = supportivePartitionId,
        });
    }
    SendProposeTransactionRequest(params);
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});
    SendPlanStep({.Step=100, .TxIds={txId}});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});
    WaitPlanStepAck({.Step=100, .TxIds={txId}});
    WaitPlanStepAccepted({.Step=100});
}

void TPQTabletFixture::CommitDeferredPublicationFinalize(
    const TWriteId& writeId,
    ui64 txId,
    NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi::EOp op,
    const std::vector<ui32>& partitionIds)
{
    EnsurePipeExist();

    TProposeTransactionParams params;
    params.TxId = txId;
    params.Senders = {Ctx->TabletId};
    params.Receivers = {Ctx->TabletId};
    params.WriteId = writeId;
    for (const ui32& partitionId : partitionIds) {
        params.TxOps.push_back({.Partition=partitionId, .Path="/topic", .DeferredPublicationOp=op});
    }
    SendProposeTransactionRequest(params);
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});
    SendPlanStep({.Step=100, .TxIds={txId}});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});
    WaitPlanStepAck({.Step=100, .TxIds={txId}});
    WaitPlanStepAccepted({.Step=100});
}

void TPQTabletFixture::AbortDeferredPublicationFinalize(
    const TWriteId& writeId,
    ui64 txId,
    NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi::EOp op,
    const std::vector<ui32>& partitionIds)
{
    EnsurePipeExist();

    TProposeTransactionParams params;
    params.TxId = txId;
    params.Senders = {Ctx->TabletId};
    params.Receivers = {Ctx->TabletId};
    params.WriteId = writeId;
    for (const ui32& partitionId : partitionIds) {
        params.TxOps.push_back({.Partition=partitionId, .Path="/topic", .DeferredPublicationOp=op});
    }
    SendProposeTransactionRequest(params);
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});
    SendPlanStep({.Step=100, .TxIds={txId}});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});
}

void TPQTabletFixture::WaitWriteResponse(const TWriteResponseMatcher& matcher)
{
    bool found = false;

    auto observer = [&found, &matcher](TAutoPtr<IEventHandle>& event) {
        if (auto* msg = event->CastAsLocal<TEvPersQueue::TEvResponse>()) {
            if (matcher.Cookie.Defined()) {
                if (msg->Record.HasCookie() && (*matcher.Cookie == msg->Record.GetCookie())) {
                    found = true;
                }
            }
        }

        return TTestActorRuntimeBase::EEventAction::PROCESS;
    };

    auto prev = Ctx->Runtime->SetObserverFunc(observer);

    TDispatchOptions options;
    options.CustomFinalCondition = [&found]() {
        return found;
    };

    UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));

    Ctx->Runtime->SetObserverFunc(prev);
}

void TPQTabletFixture::StartPQWriteObserver(bool& flag, unsigned cookie, TAutoPtr<IEventHandle>* ev)
{
    flag = false;

    auto observer = [&flag, cookie, ev](TAutoPtr<IEventHandle>& event) {
        if (auto* kvResponse = event->CastAsLocal<TEvKeyValue::TEvResponse>()) {
            if ((event->Sender == event->Recipient) &&
                kvResponse->Record.HasCookie() &&
                (kvResponse->Record.GetCookie() == cookie)) {
                flag = true;

                if (ev) {
                    *ev = event;
                    return TTestActorRuntimeBase::EEventAction::DROP;
                }
            }
        }

        return TTestActorRuntimeBase::EEventAction::PROCESS;
    };

    Ctx->Runtime->SetObserverFunc(observer);
}

void TPQTabletFixture::WaitForPQWriteComplete(bool& flag)
{
    TDispatchOptions options;
    options.CustomFinalCondition = [&flag]() {
        return flag;
    };
    UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
}

void TPQTabletFixture::StartPQWriteStateObserver()
{
    StartPQWriteObserver(FoundPQWriteState, 4); // TPersQueue::WRITE_STATE_COOKIE
}

void TPQTabletFixture::WaitForPQWriteState()
{
    WaitForPQWriteComplete(FoundPQWriteState);
}

void TPQTabletFixture::SendCancelTransactionProposal(const TCancelTransactionProposalParams& params)
{
    auto event = MakeHolder<TEvPersQueue::TEvCancelTransactionProposal>(params.TxId);

    SendToPipe(Ctx->Edge,
               event.Release());
}

void TPQTabletFixture::StartPQWriteTxsObserver(TAutoPtr<IEventHandle>* event)
{
    StartPQWriteObserver(FoundPQWriteTxs, 5, event); // TPersQueue::WRITE_TX_COOKIE
}

void TPQTabletFixture::WaitForPQWriteTxs()
{
    WaitForPQWriteComplete(FoundPQWriteTxs);
}

NHelpers::TPQTabletMock* TPQTabletFixture::CreatePQTabletMock(ui64 tabletId)
{
    NHelpers::TPQTabletMock* mock = nullptr;
    auto wrapCreatePQTabletMock = [&](const NActors::TActorId& tablet, NKikimr::TTabletStorageInfo* info) -> IActor* {
        mock = NHelpers::CreatePQTabletMock(tablet, info);
        return mock;
    };

    CreateTestBootstrapper(*Ctx->Runtime,
                           CreateTestTabletInfo(tabletId, NKikimrTabletBase::TTabletTypes::Dummy, TErasureType::ErasureNone),
                           wrapCreatePQTabletMock);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    Ctx->Runtime->DispatchEvents(options);

    return mock;
}

void TPQTabletFixture::AssertTabletIsAlive(ui64 txId)
{
    SendProposeTransactionRequest({.TxId=txId});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});
}

void TPQTabletFixture::TestMultiplePQTablets(const TString& consumer1, const TString& consumer2)
{
    TVector<std::pair<TString, bool>> consumers;
    consumers.emplace_back(consumer1, true);
    if (consumer1 != consumer2) {
        consumers.emplace_back(consumer2, true);
    }

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(22222);
    PQTabletPrepare({.partitions=1}, consumers, *Ctx);

    const ui64 txId_1 = 67890;
    const ui64 txId_2 = 67891;

    SendProposeTransactionRequest({.TxId=txId_1,
                                  .Senders={22222}, .Receivers={22222},
                                  .TxOps={
                                  {.Partition=0, .Consumer=consumer1, .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId_1,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendProposeTransactionRequest({.TxId=txId_2,
                                  .Senders={22222}, .Receivers={22222},
                                  .TxOps={
                                  {.Partition=0, .Consumer=consumer2, .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId_2,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId_2}});
    SendPlanStep({.Step=200, .TxIds={txId_1}});

    WaitReadSet(*tablet, {.Step=100, .TxId=txId_2, .Source=Ctx->TabletId, .Target=22222, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});
    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId_2, .Target=Ctx->TabletId, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    WaitReadSet(*tablet, {.Step=200, .TxId=txId_1, .Source=Ctx->TabletId, .Target=22222, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});
    tablet->SendReadSet(*Ctx->Runtime, {.Step=200, .TxId=txId_1, .Target=Ctx->TabletId, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    WaitProposeTransactionResponse({.TxId=txId_2,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    WaitPlanStepAck({.Step=100, .TxIds={txId_2}}); // TEvPlanStepAck for Coordinator
    WaitPlanStepAccepted({.Step=100});

    WaitProposeTransactionResponse({.TxId=txId_1,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    WaitPlanStepAck({.Step=200, .TxIds={txId_1}}); // TEvPlanStepAck for Coordinator
    WaitPlanStepAccepted({.Step=200});
}

Y_UNIT_TEST_F(Multiple_PQTablets_1, TPQTabletFixture)
{
    TestMultiplePQTablets("consumer", "consumer");
}

Y_UNIT_TEST_F(Multiple_PQTablets_2, TPQTabletFixture)
{
    TestMultiplePQTablets("consumer-1", "consumer-2");
}

void TPQTabletFixture::TestParallelTransactions(const TString& consumer1, const TString& consumer2)
{
    TVector<std::pair<TString, bool>> consumers;
    consumers.emplace_back(consumer1, true);
    if (consumer1 != consumer2) {
        consumers.emplace_back(consumer2, true);
    }

    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(22222);
    PQTabletPrepare({.partitions=1}, consumers, *Ctx);

    const ui64 txId_1 = 67890;
    const ui64 txId_2 = 67891;

    SendProposeTransactionRequest({.TxId=txId_1,
                                  .Senders={22222}, .Receivers={22222},
                                  .TxOps={
                                  {.Partition=0, .Consumer=consumer1, .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId_1,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendProposeTransactionRequest({.TxId=txId_2,
                                  .Senders={22222}, .Receivers={22222},
                                  .TxOps={
                                  {.Partition=0, .Consumer=consumer2, .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId_2,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    size_t calcPredicateResultCount = 0;
    StartPQCalcPredicateObserver(calcPredicateResultCount);

    // Transactions are planned in reverse order
    SendPlanStep({.Step=100, .TxIds={txId_2}});
    SendPlanStep({.Step=200, .TxIds={txId_1}});

    // The PQ tablet sends to the TEvTxCalcPredicate partition for both transactions
    WaitForPQCalcPredicate(calcPredicateResultCount, 2);

    // TEvReadSet messages arrive in any order
    tablet->SendReadSet(*Ctx->Runtime, {.Step=200, .TxId=txId_1, .Target=Ctx->TabletId, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});
    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId_2, .Target=Ctx->TabletId, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    // Transactions will be executed in the order they were planned
    WaitProposeTransactionResponse({.TxId=txId_2,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    WaitPlanStepAck({.Step=100, .TxIds={txId_2}}); // TEvPlanStepAck for Coordinator
    WaitPlanStepAccepted({.Step=100});

    WaitProposeTransactionResponse({.TxId=txId_1,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    WaitPlanStepAck({.Step=200, .TxIds={txId_1}}); // TEvPlanStepAck for Coordinator
    WaitPlanStepAccepted({.Step=200});
}

void TPQTabletFixture::StartPQCalcPredicateObserver(size_t& received)
{
    received = 0;

    auto observer = [&received](TAutoPtr<IEventHandle>& event) {
        if (event->CastAsLocal<TEvPQ::TEvTxCalcPredicate>()) {
            ++received;
        }

        return TTestActorRuntimeBase::EEventAction::PROCESS;
    };

    Ctx->Runtime->SetObserverFunc(observer);
}

void TPQTabletFixture::WaitForPQCalcPredicate(size_t& received, size_t expected)
{
    TDispatchOptions options;
    options.CustomFinalCondition = [&received, expected]() {
        return received >= expected;
    };
    UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
}

void TPQTabletFixture::WaitForTxState(ui64 txId, NKikimrPQ::TTransaction::EState state)
{
    const TString key = GetTxKey(txId);

    while (true) {
        auto request = std::make_unique<TEvKeyValue::TEvRequest>();
        request->Record.SetCookie(12345);
        auto cmd = request->Record.AddCmdReadRange();
        auto range = cmd->MutableRange();
        range->SetFrom(key);
        range->SetIncludeFrom(true);
        range->SetTo(key);
        range->SetIncludeTo(true);
        cmd->SetIncludeData(true);
        SendToPipe(Ctx->Edge, request.release());

        auto response = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>();
        UNIT_ASSERT_VALUES_EQUAL(response->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        const auto& result = response->Record.GetReadRangeResult(0);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), static_cast<ui32>(NKikimrProto::OK));
        const auto& pair = result.GetPair(0);

        NKikimrPQ::TTransaction tx;
        Y_ABORT_UNLESS(tx.ParseFromString(pair.GetValue()));

        if (tx.GetState() == state) {
            return;
        }
    }

    UNIT_FAIL("transaction " << txId << " has not entered the " << state << " state");
}

void TPQTabletFixture::WaitForExecStep(ui64 step)
{
    while (true) {
        auto request = std::make_unique<TEvKeyValue::TEvRequest>();
        request->Record.SetCookie(12345);
        auto cmd = request->Record.AddCmdReadRange();
        auto range = cmd->MutableRange();
        range->SetFrom("_txinfo");
        range->SetIncludeFrom(true);
        range->SetTo("_txinfo");
        range->SetIncludeTo(true);
        cmd->SetIncludeData(true);
        SendToPipe(Ctx->Edge, request.release());

        auto response = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>();
        UNIT_ASSERT_VALUES_EQUAL(response->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        const auto& result = response->Record.GetReadRangeResult(0);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), static_cast<ui32>(NKikimrProto::OK));
        const auto& pair = result.GetPair(0);

        NKikimrPQ::TTabletTxInfo txInfo;
        Y_ABORT_UNLESS(txInfo.ParseFromString(pair.GetValue()));

        if (txInfo.GetExecStep() == step) {
            return;
        }
    }

    UNIT_FAIL("expected execution step " << step);
}

namespace {

constexpr ui32 WRITE_TX_COOKIE = 5; // TPersQueue::WRITE_TX_COOKIE
constexpr const char* TX_INFO_KEY = "_txinfo";

NKikimrPQ::TTabletTxInfo ParseTxWritesFromWriteTxRequest(const NKikimrClient::TKeyValueRequest& request)
{
    for (const auto& cmd : request.GetCmdWrite()) {
        if (cmd.GetKey() == TX_INFO_KEY) {
            NKikimrPQ::TTabletTxInfo info;
            UNIT_ASSERT(info.ParseFromString(cmd.GetValue()));
            return info;
        }
    }
    UNIT_FAIL("WRITE_TX request has no _txinfo");
    return {};
}

THashSet<i64> CollectKafkaProducerIds(const NKikimrPQ::TTabletTxInfo& info)
{
    THashSet<i64> producerIds;
    for (size_t i = 0; i < info.TxWritesSize(); ++i) {
        const auto& writeId = info.GetTxWrites(i).GetWriteId();
        if (writeId.GetKafkaTransaction()) {
            producerIds.insert(writeId.GetKafkaProducerInstanceId().GetId());
        }
    }
    return producerIds;
}

} // namespace

void TPQTabletFixture::InstallWriteTxRequestInterceptFilter()
{
    auto filter = [this](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) -> bool {
        if (auto* msg = event->CastAsLocal<TEvKeyValue::TEvRequest>()) {
            if (msg->Record.HasCookie() && msg->Record.GetCookie() == WRITE_TX_COOKIE) {
                CapturedWriteTxRequest_ = event;
                return true;
            }
        }
        return false;
    };
    PrevWriteTxRequestFilter_ = Ctx->Runtime->SetEventFilter(filter);
}

void TPQTabletFixture::BeginInterceptWriteTxRequest()
{
    UNIT_ASSERT(!WriteTxRequestInterceptActive_);
    CapturedWriteTxRequest_.Reset();
    InstallWriteTxRequestInterceptFilter();
    WriteTxRequestInterceptActive_ = true;
}

void TPQTabletFixture::WaitForCapturedWriteTxRequest()
{
    UNIT_ASSERT(WriteTxRequestInterceptActive_);
    if (CapturedWriteTxRequest_) {
        return;
    }

    TDispatchOptions options;
    options.CustomFinalCondition = [this]() {
        return CapturedWriteTxRequest_.Get() != nullptr;
    };
    UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    UNIT_ASSERT(CapturedWriteTxRequest_);
}

NKikimrPQ::TTabletTxInfo TPQTabletFixture::GetCapturedTxWritesFromWriteTxRequestAndFlush()
{
    WaitForCapturedWriteTxRequest();

    const auto& request = CapturedWriteTxRequest_->Get<TEvKeyValue::TEvRequest>()->Record;
    const NKikimrPQ::TTabletTxInfo info = ParseTxWritesFromWriteTxRequest(request);

    Ctx->Runtime->SetEventFilter(PrevWriteTxRequestFilter_);
    TAutoPtr<IEventHandle> requestToSend;
    requestToSend.Swap(CapturedWriteTxRequest_);

    SendSaveTxState(requestToSend);

    StartPQWriteTxsObserver();
    WaitForPQWriteTxs();

    InstallWriteTxRequestInterceptFilter();

    return info;
}

void TPQTabletFixture::EndInterceptWriteTxRequest()
{
    if (!WriteTxRequestInterceptActive_) {
        return;
    }
    Ctx->Runtime->SetEventFilter(PrevWriteTxRequestFilter_);
    WriteTxRequestInterceptActive_ = false;
    CapturedWriteTxRequest_.Reset();
}

void TPQTabletFixture::InterceptSaveTxState(TAutoPtr<IEventHandle>& ev)
{
    bool found = false;

    TTestActorRuntimeBase::TEventFilter prev;
    auto filter = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) -> bool {
        if (auto* msg = event->CastAsLocal<TEvKeyValue::TEvRequest>()) {
            if (msg->Record.HasCookie() && (msg->Record.GetCookie() == WRITE_TX_COOKIE)) {
                ev = event;
                found = true;
                return true;
            }
        }

        return false;
    };
    prev = Ctx->Runtime->SetEventFilter(filter);

    TDispatchOptions options;
    options.CustomFinalCondition = [&found]() {
        return found;
    };

    UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    UNIT_ASSERT(found);

    Ctx->Runtime->SetEventFilter(prev);
}

void TPQTabletFixture::SendSaveTxState(TAutoPtr<IEventHandle>& event)
{
    Ctx->Runtime->Send(event);
}

void TPQTabletFixture::AssertTransactionInKV(ui64 txId)
{
    EnsurePipeExist();

    auto request = std::make_unique<TEvKeyValue::TEvRequest>();
    request->Record.SetCookie(12345);
    auto cmd = request->Record.AddCmdReadRange();
    auto range = cmd->MutableRange();
    range->SetFrom(GetTxKey(txId));
    range->SetIncludeFrom(true);
    range->SetTo(GetTxKey(txId + 1));
    range->SetIncludeTo(false);
    cmd->SetIncludeData(false);
    SendToPipe(Ctx->Edge, request.release());

    auto response = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>();
    UNIT_ASSERT_VALUES_EQUAL(response->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);

    const auto& result = response->Record.GetReadRangeResult(0);
    if (result.GetStatus() == static_cast<ui32>(NKikimrProto::OK)) {
        UNIT_ASSERT(result.PairSize() > 0);
        return;
    }

    if (result.GetStatus() == NKikimrProto::NODATA) {
        UNIT_FAIL("Transaction " << txId << " was not found in KV");
    }

    UNIT_FAIL("Unexpected status from KV tablet " << result.GetStatus());
}

void TPQTabletFixture::WaitForTheTransactionToBeDeleted(ui64 txId)
{
    for (size_t i = 0; i < 200; ++i) {
        auto request = std::make_unique<TEvKeyValue::TEvRequest>();
        request->Record.SetCookie(12345);
        auto cmd = request->Record.AddCmdReadRange();
        auto range = cmd->MutableRange();
        range->SetFrom(GetTxKey(txId));
        range->SetIncludeFrom(true);
        range->SetTo(GetTxKey(txId + 1));
        range->SetIncludeTo(false);
        cmd->SetIncludeData(false);
        SendToPipe(Ctx->Edge, request.release());

        auto response = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>();
        UNIT_ASSERT_VALUES_EQUAL(response->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);

        const auto& result = response->Record.GetReadRangeResult(0);
        if (result.GetStatus() == NKikimrProto::NODATA) {
            return;
        }

        if (result.GetStatus() == static_cast<ui32>(NKikimrProto::OK)) {
            Ctx->Runtime->SimulateSleep(TDuration::MilliSeconds(300));
            continue;
        }

        UNIT_FAIL("Unexpected status from KV tablet " << result.GetStatus());
    }

    UNIT_FAIL("Too many attempts");
}

TVector<TString> TPQTabletFixture::WaitForExactSupportivePartitionsCount(ui32 expectedCount) {
    for (size_t i = 0; i < 200; ++i) {
        auto result = GetSupportivePartitionsKeysFromKV();

        if (result.empty() && expectedCount == 0) {
            return result;
        } else if (expectedCount == result.size()) {
            return result;
        } else {
            Ctx->Runtime->SimulateSleep(TDuration::MilliSeconds(300));
        }
    }

    UNIT_FAIL("Too many attempts");
    return {};
}

NKikimrPQ::TTabletTxInfo TPQTabletFixture::WaitForExactTxWritesCount(ui32 expectedCount) {
    for (size_t i = 0; i < 200; ++i) {
        auto result = GetTxWritesFromKV();

        if (result.TxWritesSize() == 0 && expectedCount == 0) {
            return result;
        } else if (expectedCount == result.TxWritesSize()) {
            return result;
        } else {
            Ctx->Runtime->SimulateSleep(TDuration::MilliSeconds(300));
        }
    }

    UNIT_FAIL("Too many attempts");
    return {};
}

std::string GetSupportivePartitionKeyFrom() {
    return std::string{TKeyPrefix::EServiceType::ServiceTypeData};
}

std::string GetSupportivePartitionKeyTo() {
    return std::string{static_cast<char>(TKeyPrefix::EServiceType::ServiceTypeData + 1)};
}

TVector<TString> TPQTabletFixture::GetSupportivePartitionsKeysFromKV() {
    auto request = std::make_unique<TEvKeyValue::TEvRequest>();
    request->Record.SetCookie(12345);
    auto cmd = request->Record.AddCmdReadRange();
    auto range = cmd->MutableRange();
    range->SetFrom(GetSupportivePartitionKeyFrom());
    range->SetIncludeFrom(true);
    range->SetTo(GetSupportivePartitionKeyTo());
    range->SetIncludeTo(false);
    cmd->SetIncludeData(false);
    SendToPipe(Ctx->Edge, request.release());

    auto response = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>();
    UNIT_ASSERT_VALUES_EQUAL(response->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);

    TVector<TString> supportivePartitionsKeys;
    const auto& result = response->Record.GetReadRangeResult(0);
    if (result.GetStatus() == static_cast<ui32>(NKikimrProto::OK)) {
        for (ui32 i = 0; i < result.PairSize(); i++) {
            supportivePartitionsKeys.emplace_back(result.GetPair(i).GetKey());
        }
        return supportivePartitionsKeys;
    } else if (result.GetStatus() == NKikimrProto::NODATA) {
        return supportivePartitionsKeys;
    } else {
        UNIT_FAIL("Unexpected status from KV tablet" << result.GetStatus());
        return {};
    }
}

NKikimrPQ::TTabletTxInfo TPQTabletFixture::GetTxWritesFromKV() {
    auto request = std::make_unique<TEvKeyValue::TEvRequest>();
    request->Record.SetCookie(12345);
    auto* cmd = request->Record.AddCmdRead();
    cmd->SetKey("_txinfo");
    SendToPipe(Ctx->Edge, request.release());

    auto response = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>();
    UNIT_ASSERT_VALUES_EQUAL(response->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);

    const auto& result = response->Record.GetReadResult(0);
    if (result.GetStatus() == static_cast<ui32>(NKikimrProto::OK)) {
        NKikimrPQ::TTabletTxInfo info;
        if (!info.ParseFromString(result.GetValue())) {
            UNIT_FAIL("tx writes read error");
        }
        return info;
    } else if (result.GetStatus() == NKikimrProto::NODATA) {
        return {};
    } else {
        UNIT_FAIL("Unexpected status from KV tablet" << result.GetStatus());
        return {};
    }
}

void TPQTabletFixture::SendAppSendRsRequest(const TAppSendReadSetParams& params) {
    auto makeEv = [this, &params]() {
        NActorsProto::TRemoteHttpInfo pb;
        pb.SetMethod(HTTP_METHOD_GET);
        pb.SetPath("/app/secure");
        auto addParam = [&](const TString& key, const TString& value) {
            auto* kv = pb.AddQueryParams();
            kv->SetKey(key);
            kv->SetValue(value);
        };
        addParam("TabletID", ToString(Ctx->TabletId));
        addParam("SendReadSet", "1");
        addParam("decision", params.Predicate ? "commit" : "abort");
        addParam("step", ToString(params.Step));
        addParam("txId", ToString(params.TxId));
        if (params.SenderId.Defined()) {
            addParam("senderTablet", ToString(*params.SenderId));
        } else {
            addParam("allSenderTablets", "1");
        }
        pb.SetUserToken(NACLib::TUserToken(BUILTIN_ACL_ROOT, {}).SerializeAsString());
        return std::make_unique<NActors::NMon::TEvRemoteHttpInfo>(std::move(pb));
    };
    Ctx->Runtime->SendToPipe(Ctx->TabletId, Ctx->Edge, makeEv().release(), 0, GetPipeConfigWithRetries());
}

void TPQTabletFixture::WaitForAppSendRsResponse(const TAppSendReadSetMatcher& matcher) {
    THolder<NMon::TEvRemoteJsonInfoRes> handle = Ctx->Runtime->GrabEdgeEvent<NMon::TEvRemoteJsonInfoRes>();
    UNIT_ASSERT(handle != nullptr);
    const TString& response = handle->Json;
    NJson::TJsonValue value;
    UNIT_ASSERT(ReadJsonTree(response, &value, false));
    if (matcher.Status.Defined()) {
        const bool resultOk = value["result"].GetStringSafe() == "OK"sv;
        UNIT_ASSERT_VALUES_EQUAL(resultOk, *matcher.Status);
    }
}

template<class EventType>
void TPQTabletFixture::AddOneTimeEventObserver(bool& seenEvent, ui32 unseenEventCount, std::function<TTestActorRuntimeBase::EEventAction(TAutoPtr<IEventHandle>&)> callback) {
    auto observer = [&seenEvent, unseenEventCount, callback](TAutoPtr<IEventHandle>& input) mutable {
        if (!seenEvent && input->CastAsLocal<EventType>()) {
            unseenEventCount--;
            if (unseenEventCount == 0) {
                seenEvent = true;
            }
            return callback(input);
        }

        return TTestActorRuntimeBase::EEventAction::PROCESS;
    };
    Ctx->Runtime->SetObserverFunc(observer);
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

void TPQTabletFixture::TestWaitingForTEvReadSet(size_t sendersCount, size_t receiversCount)
{
    const ui64 txId = 67890;

    TVector<NHelpers::TPQTabletMock*> tablets;
    TVector<ui64> senders;
    TVector<ui64> receivers;

    //
    // senders
    //
    for (size_t i = 0; i < sendersCount; ++i) {
        senders.push_back(22222 + i);
        tablets.push_back(CreatePQTabletMock(senders.back()));
    }

    //
    // receivers
    //
    for (size_t i = 0; i < receiversCount; ++i) {
        receivers.push_back(33333 + i);
        tablets.push_back(CreatePQTabletMock(receivers.back()));
    }

    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders=senders, .Receivers=receivers,
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"}
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});

    WaitForCalcPredicateResult();

    //
    // The tablet received the predicate value from the partition, but has not yet saved the transaction state.
    // Therefore, the transaction has not yet entered the WAIT_RS state
    //

    for (size_t i = 0; i < sendersCount; ++i) {
        tablets[i]->SendReadSet(*Ctx->Runtime,
                                {.Step=100, .TxId=txId, .Target=Ctx->TabletId, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});
    }

    WaitProposeTransactionResponse({.TxId=txId,
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

Y_UNIT_TEST_F(Deferred_PlanStepAck_Future_Unknown_Waits_WriteTx, TPQTabletFixture)
{
    // All-unknown future PlanStep: ack only after successful WRITE_TX; PlanStep not advanced.
    const ui64 unknownTxId = 424301;

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

    SendPlanStep({.Step=100, .TxIds={unknownTxId}});

    {
        TDispatchOptions options;
        options.CustomFinalCondition = [&]() {
            return !heldRequests.empty();
        };
        UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    }

    WaitForNoPlanStepAccepted();

    holdWriteTx = false;
    for (auto& held : heldRequests) {
        Ctx->Runtime->Send(held.Release());
    }
    heldRequests.clear();
    Ctx->Runtime->SetObserverFunc(prev);

    WaitPlanStepAck({.Step=100, .TxIds={unknownTxId}});
    WaitPlanStepAccepted({.Step=100});
}

Y_UNIT_TEST_F(Deferred_PlanStepAck_Empty_Future_Waits_WriteTx, TPQTabletFixture)
{
    // Empty Transactions + future step: same deferred fence.
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

    SendPlanStep({.Step=100, .TxIds={}});

    {
        TDispatchOptions options;
        options.CustomFinalCondition = [&]() {
            return !heldRequests.empty();
        };
        UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    }

    WaitForNoPlanStepAccepted();

    holdWriteTx = false;
    for (auto& held : heldRequests) {
        Ctx->Runtime->Send(held.Release());
    }
    heldRequests.clear();
    Ctx->Runtime->SetObserverFunc(prev);

    // Empty Transactions list: only TEvPlanStepAccepted is sent (no per-TxId AckTo).
    WaitPlanStepAccepted({.Step=100});
}

Y_UNIT_TEST_F(Deferred_PlanStepAck_Past_Retransmit_After_Delete, TPQTabletFixture)
{
    // After execute+delete, retransmit of the same step is all-unknown with step <= PlanStep;
    // ack is still deferred until WRITE_TX (stale-leader fence).
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

    // Drain PlanStep ack/accepted from the known-tx path (sent at EXECUTED).
    WaitPlanStepAck({.Step=100, .TxIds={txId}});
    WaitPlanStepAccepted({.Step=100});

    TVector<TAutoPtr<IEventHandle>> heldRequests;
    bool holdWriteTx = true;
    ui32 planStepAcceptedCount = 0;
    auto prev = Ctx->Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
        if (event->GetTypeRewrite() == TEvTxProcessing::TEvPlanStepAccepted::EventType) {
            ++planStepAcceptedCount;
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

    const ui32 acceptedBeforeRetransmit = planStepAcceptedCount;
    SendPlanStep({.Step=100, .TxIds={txId}});

    {
        TDispatchOptions options;
        options.CustomFinalCondition = [&]() {
            return !heldRequests.empty();
        };
        UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    }

    UNIT_ASSERT_VALUES_EQUAL(planStepAcceptedCount, acceptedBeforeRetransmit);

    holdWriteTx = false;
    for (auto& held : heldRequests) {
        Ctx->Runtime->Send(held.Release());
    }
    heldRequests.clear();
    Ctx->Runtime->SetObserverFunc(prev);

    WaitPlanStepAck({.Step=100, .TxIds={txId}});
    WaitPlanStepAccepted({.Step=100});
}

Y_UNIT_TEST_F(Deferred_PlanStepAck_Multiple_Unknown_One_WriteTx, TPQTabletFixture)
{
    // Several all-unknown PlanSteps while WRITE_TX is held flush together after one cycle.
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    TVector<TAutoPtr<IEventHandle>> heldRequests;
    bool holdWriteTx = true;
    THashSet<ui64> acceptedSteps;
    auto prev = Ctx->Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
        if (auto* msg = event->CastAsLocal<TEvTxProcessing::TEvPlanStepAccepted>()) {
            acceptedSteps.insert(msg->Record.GetStep());
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

    for (ui64 i = 0; i < 3; ++i) {
        SendPlanStep({.Step=100 + i, .TxIds={424310 + i}});
    }

    {
        TDispatchOptions options;
        options.CustomFinalCondition = [&]() {
            return !heldRequests.empty();
        };
        UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    }

    UNIT_ASSERT(acceptedSteps.empty());

    holdWriteTx = false;
    for (auto& held : heldRequests) {
        Ctx->Runtime->Send(held.Release());
    }
    heldRequests.clear();

    TDispatchOptions options;
    options.CustomFinalCondition = [&]() {
        return acceptedSteps.size() >= 3;
    };
    UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    Ctx->Runtime->SetObserverFunc(prev);

    for (ui64 i = 0; i < 3; ++i) {
        UNIT_ASSERT(acceptedSteps.contains(100 + i));
    }
}

Y_UNIT_TEST_F(Deferred_PlanStepAck_While_WriteTx_In_Progress, TPQTabletFixture)
{
    // Unknown PlanStep during an in-flight WRITE_TX is flushed after that cycle ends.
    const ui64 txId = 67890;
    const ui64 unknownTxId = 424320;
    const ui64 mockTabletId = 22222;

    CreatePQTabletMock(mockTabletId);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders={mockTabletId}, .Receivers={mockTabletId},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});

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

    SendPlanStep({.Step=200, .TxIds={unknownTxId}});

    {
        TDispatchOptions options;
        options.CustomFinalCondition = [&]() {
            return !heldResponses.empty();
        };
        UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    }

    WaitForNoPlanStepAccepted();

    holdWriteTxResponse = false;
    for (auto& held : heldResponses) {
        Ctx->Runtime->Send(held.Release());
    }
    heldResponses.clear();
    Ctx->Runtime->SetObserverFunc(prev);

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});
    WaitPlanStepAck({.Step=200, .TxIds={unknownTxId}});
    WaitPlanStepAccepted({.Step=200});
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

namespace {

constexpr TStringBuf kTopicTxInjectionOwner = "-=[ tx-reboot-owner ]=-";
// Sim-time deadline: after reboot the in-flight response is dropped and pipe is stale;
// without a timeout GrabEdgeEvent can spin forever on tablet-resolver retries
// that do not bump ScheduledCount.
constexpr TDuration kTopicTxInjectionEdgeTimeout = TDuration::Seconds(2);

// Thrown to restart the Topic-tx scenario after a mid-flight reboot/pipe reset.
class TTopicTxInjectionRetry : public yexception {
};

class TTopicTxInjectionHelper {
public:
    explicit TTopicTxInjectionHelper(TTestContext& tc)
        : Tc(tc)
    {}

    ~TTopicTxInjectionHelper() {
        ResetPipe();
    }

    void ResetPipe() {
        if (Pipe) {
            Tc.Runtime->ClosePipe(Pipe, Tc.Edge, 0);
            Pipe = {};
        }
    }

    void EnsurePipe() {
        if (!Pipe) {
            Pipe = Tc.Runtime->ConnectToPipe(Tc.TabletId, Tc.Edge, 0, GetPipeConfigWithRetries());
        }
        Y_ABORT_UNLESS(Pipe);
    }

    void SendToPipe(IEventBase* event) {
        EnsurePipe();
        Tc.Runtime->SendToPipe(Pipe, Tc.Edge, event, 0, 0);
    }

    TString CreateSupportivePartition(const TWriteId& writeId) {
        for (i32 retriesLeft = 5; retriesLeft > 0; --retriesLeft) {
            try {
                Tc.Runtime->ResetScheduledCount();
                ResetPipe();
                EnsurePipe();

                auto event = std::make_unique<TEvPersQueue::TEvRequest>();
                auto* request = event->Record.MutablePartitionRequest();
                request->SetPartition(0);
                request->SetCookie(4);
                request->SetNeedSupportivePartition(true);
                SetWriteId(*request, writeId);
                ActorIdToProto(Pipe, request->MutablePipeClient());
                auto* cmd = request->MutableCmdGetOwnership();
                cmd->SetOwner(TString{kTopicTxInjectionOwner});
                cmd->SetForce(true);

                SendToPipe(event.release());
                auto response = Tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(
                    kTopicTxInjectionEdgeTimeout);
                if (!response) {
                    continue;
                }
                if (response->Record.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
                    Tc.Runtime->DispatchEvents();
                    retriesLeft = 5;
                    continue;
                }
                UNIT_ASSERT_VALUES_EQUAL((int)response->Record.GetStatus(), (int)NMsgBusProxy::MSTATUS_OK);
                return response->Record.GetPartitionResponse().GetCmdGetOwnershipResult().GetOwnerCookie();
            } catch (const NActors::TSchedulingLimitReachedException&) {
            } catch (const NActors::TEmptyEventQueueException&) {
            }
        }
        ythrow TTopicTxInjectionRetry() << "CreateSupportivePartition: retries exhausted";
    }

    void WriteToSupportive(
        const TWriteId& writeId,
        const TString& ownerCookie,
        ui64 seqNo,
        ui64 messageNo,
        const TString& data,
        ui64 cookie)
    {
        for (i32 retriesLeft = 5; retriesLeft > 0; --retriesLeft) {
            try {
                Tc.Runtime->ResetScheduledCount();
                EnsurePipe();

                auto event = MakeHolder<TEvPersQueue::TEvRequest>();
                auto* request = event->Record.MutablePartitionRequest();
                request->SetTopic("/topic");
                request->SetPartition(0);
                request->SetCookie(cookie);
                request->SetOwnerCookie(ownerCookie);
                request->SetMessageNo(messageNo);
                SetWriteId(*request, writeId);
                ActorIdToProto(Pipe, request->MutablePipeClient());

                auto* cmdWrite = request->AddCmdWrite();
                cmdWrite->SetSourceId("tx-src");
                cmdWrite->SetSeqNo(seqNo);
                cmdWrite->SetData(data);
                cmdWrite->SetCreateTimeMS(TInstant::Now().MilliSeconds());
                cmdWrite->SetDisableDeduplication(true);
                cmdWrite->SetUncompressedSize(data.size());
                cmdWrite->SetIgnoreQuotaDeadline(true);
                cmdWrite->SetExternalOperation(true);

                SendToPipe(event.Release());
                auto response = Tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(
                    kTopicTxInjectionEdgeTimeout);
                if (!response) {
                    ResetPipe();
                    continue;
                }
                if (response->Record.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
                    ResetPipe();
                    Tc.Runtime->DispatchEvents();
                    retriesLeft = 5;
                    continue;
                }
                if (response->Record.GetErrorCode() != NPersQueue::NErrorCode::OK) {
                    ythrow TTopicTxInjectionRetry()
                        << "WriteToSupportive error="
                        << static_cast<int>(response->Record.GetErrorCode());
                }
                UNIT_ASSERT_VALUES_EQUAL(response->Record.GetPartitionResponse().GetCookie(), cookie);
                return;
            } catch (const NActors::TSchedulingLimitReachedException&) {
                ResetPipe();
            } catch (const NActors::TEmptyEventQueueException&) {
                ResetPipe();
            }
        }
        ythrow TTopicTxInjectionRetry() << "WriteToSupportive: retries exhausted";
    }

    ui32 WaitSupportivePartitionId(const TWriteId& writeId) {
        for (size_t i = 0; i < 40; ++i) {
            try {
                Tc.Runtime->ResetScheduledCount();
                EnsurePipe();
                auto request = std::make_unique<TEvKeyValue::TEvRequest>();
                request->Record.SetCookie(12345);
                request->Record.AddCmdRead()->SetKey("_txinfo");
                SendToPipe(request.release());

                auto response = Tc.Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>(
                    kTopicTxInjectionEdgeTimeout);
                if (!response) {
                    ResetPipe();
                    continue;
                }
                UNIT_ASSERT_VALUES_EQUAL(response->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
                const auto& result = response->Record.GetReadResult(0);
                if (result.GetStatus() == static_cast<ui32>(NKikimrProto::OK)) {
                    NKikimrPQ::TTabletTxInfo info;
                    UNIT_ASSERT(info.ParseFromString(result.GetValue()));
                    for (const auto& txWrite : info.GetTxWrites()) {
                        if (GetWriteId(txWrite) == writeId) {
                            return txWrite.GetInternalPartitionId();
                        }
                    }
                } else if (result.GetStatus() != NKikimrProto::NODATA) {
                    ythrow TTopicTxInjectionRetry() << "WaitSupportivePartitionId: KV status "
                        << result.GetStatus();
                }
                Tc.Runtime->SimulateSleep(TDuration::MilliSeconds(50));
            } catch (const NActors::TSchedulingLimitReachedException&) {
                ythrow TTopicTxInjectionRetry() << "WaitSupportivePartitionId: scheduling limit";
            } catch (const NActors::TEmptyEventQueueException&) {
                ythrow TTopicTxInjectionRetry() << "WaitSupportivePartitionId: empty queue";
            }
        }
        ythrow TTopicTxInjectionRetry() << "supportive partition id did not appear in _txinfo";
    }

    void DrainEdgeProposeAndPlanResults() {
        for (;;) {
            auto event = Tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvProposeTransactionResult>(
                TDuration::MilliSeconds(1));
            if (!event) {
                break;
            }
        }
        DrainPlanStepSideEffects();
    }

    // Best-effort: PlanStep may emit Ack/Accepted; under injection they can be dropped.
    void DrainPlanStepSideEffects() {
        for (ui32 i = 0; i < 4; ++i) {
            Tc.Runtime->GrabEdgeEvent<TEvTxProcessing::TEvPlanStepAck>(TDuration::MilliSeconds(1));
            Tc.Runtime->GrabEdgeEvent<TEvTxProcessing::TEvPlanStepAccepted>(TDuration::MilliSeconds(1));
        }
    }

    void CommitTopicTransaction(const TWriteId& writeId, ui32 supportivePartitionId, ui64 txId) {
        for (i32 retriesLeft = 5; retriesLeft > 0; --retriesLeft) {
            try {
                Tc.Runtime->ResetScheduledCount();
                ResetPipe();
                EnsurePipe();

                {
                    auto event = MakeHolder<TEvPersQueue::TEvProposeTransactionBuilder>();
                    ActorIdToProto(Tc.Edge, event->Record.MutableSourceActor());
                    event->Record.SetTxId(txId);
                    auto* body = event->Record.MutableData();
                    auto* operation = body->MutableOperations()->Add();
                    operation->SetPartitionId(0);
                    operation->SetPath("/topic");
                    operation->SetSupportivePartition(supportivePartitionId);
                    body->AddSendingShards(Tc.TabletId);
                    body->AddReceivingShards(Tc.TabletId);
                    SetWriteId(*body, writeId);
                    body->SetImmediate(false);
                    SendToPipe(event.Release());
                }

                {
                    auto event = Tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvProposeTransactionResult>(
                        kTopicTxInjectionEdgeTimeout);
                    if (!event) {
                        continue;
                    }
                    if (event->Record.GetTxId() != txId) {
                        continue;
                    }
                    if (event->Record.GetStatus() != NKikimrPQ::TEvProposeTransactionResult::PREPARED) {
                        ythrow TTopicTxInjectionRetry()
                            << "Propose PREPARE status="
                            << static_cast<int>(event->Record.GetStatus());
                    }
                }

                {
                    auto event = MakeHolder<TEvTxProcessing::TEvPlanStep>();
                    event->Record.SetStep(100 + txId);
                    auto* tx = event->Record.AddTransactions();
                    tx->SetTxId(txId);
                    ActorIdToProto(Tc.Edge, tx->MutableAckTo());
                    SendToPipe(event.Release());
                }

                bool gotComplete = false;
                for (ui32 waitRound = 0; waitRound < 8 && !gotComplete; ++waitRound) {
                    auto event = Tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvProposeTransactionResult>(
                        kTopicTxInjectionEdgeTimeout);
                    if (!event) {
                        break;
                    }
                    if (event->Record.GetTxId() != txId) {
                        continue;
                    }
                    if (event->Record.GetStatus() == NKikimrPQ::TEvProposeTransactionResult::PREPARED) {
                        // Late PREPARED after PlanStep — ignore and keep waiting for COMPLETE.
                        continue;
                    }
                    if (event->Record.GetStatus() != NKikimrPQ::TEvProposeTransactionResult::COMPLETE) {
                        ythrow TTopicTxInjectionRetry()
                            << "Propose COMPLETE status="
                            << static_cast<int>(event->Record.GetStatus());
                    }
                    gotComplete = true;
                }
                if (!gotComplete) {
                    ythrow TTopicTxInjectionRetry() << "missing COMPLETE after PlanStep";
                }

                DrainPlanStepSideEffects();
                return;
            } catch (const NActors::TSchedulingLimitReachedException&) {
                ResetPipe();
            } catch (const NActors::TEmptyEventQueueException&) {
                ResetPipe();
            }
        }
        ythrow TTopicTxInjectionRetry() << "CommitTopicTransaction: retries exhausted";
    }

private:
    TTestContext& Tc;
    TActorId Pipe;
};

ui64 GetEndOffset(TTestContext& tc) {
    for (i32 retriesLeft = 5; retriesLeft > 0; --retriesLeft) {
        try {
            tc.Runtime->ResetScheduledCount();
            auto request = MakeHolder<TEvPersQueue::TEvOffsets>();
            tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());
            auto result = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvOffsetsResponse>(
                kTopicTxInjectionEdgeTimeout);
            if (!result || result->Record.PartResultSize() == 0) {
                continue;
            }
            if (result->Record.GetPartResult(0).GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
                tc.Runtime->DispatchEvents();
                retriesLeft = 5;
                continue;
            }
            return result->Record.GetPartResult(0).GetEndOffset();
        } catch (const NActors::TSchedulingLimitReachedException&) {
        } catch (const NActors::TEmptyEventQueueException&) {
        }
    }
    ythrow TTopicTxInjectionRetry() << "GetEndOffset failed";
}

NKikimrClient::TCmdReadResult CaptureCmdReadResult(
    TTestContext& tc, ui64 offset, ui32 count)
{
    for (i32 retriesLeft = 5; retriesLeft > 0; --retriesLeft) {
        try {
            tc.Runtime->ResetScheduledCount();
            auto request = MakeHolder<TEvPersQueue::TEvRequest>();
            auto* req = request->Record.MutablePartitionRequest();
            req->SetPartition(0);
            req->SetCookie(123);
            auto* read = req->MutableCmdRead();
            read->SetClientId("user");
            read->SetSessionId("");
            read->SetOffset(offset);
            read->SetCount(count);
            read->SetBytes(16_MB);
            read->SetReadToBlobEnd(true);
            tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, request.Release(), 0, GetPipeConfigWithRetries());

            auto response = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(
                kTopicTxInjectionEdgeTimeout);
            if (!response) {
                continue;
            }
            if (response->Record.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
                tc.Runtime->DispatchEvents();
                retriesLeft = 5;
                continue;
            }
            UNIT_ASSERT_VALUES_EQUAL(
                (int)response->Record.GetErrorCode(), (int)NPersQueue::NErrorCode::OK);
            UNIT_ASSERT(response->Record.GetPartitionResponse().HasCmdReadResult());
            return response->Record.GetPartitionResponse().GetCmdReadResult();
        } catch (const NActors::TSchedulingLimitReachedException&) {
        } catch (const NActors::TEmptyEventQueueException&) {
        }
    }
    ythrow TTopicTxInjectionRetry() << "CaptureCmdReadResult failed";
}

void AssertTopicTxCommittedPayloads(
    TTestContext& tc,
    ui64 readFrom,
    ui32 txCount,
    TMaybe<ui32> expectedAttempt)
{
    PQGetPartInfo(0, readFrom + txCount, tc);
    const auto readResult = CaptureCmdReadResult(tc, readFrom, txCount);
    UNIT_ASSERT_VALUES_EQUAL(readResult.ResultSize(), txCount);
    for (ui32 i = 0; i < txCount; ++i) {
        const auto& row = readResult.GetResult(i);
        UNIT_ASSERT_VALUES_EQUAL(row.GetOffset(), readFrom + i);
        if (expectedAttempt.Defined()) {
            UNIT_ASSERT_VALUES_EQUAL(
                row.GetData(),
                TStringBuilder() << "topic-tx-reboot-" << *expectedAttempt << "-" << i);
        } else {
            UNIT_ASSERT_C(
                row.GetData().StartsWith("topic-tx-reboot-"),
                "unexpected payload=" << row.GetData());
        }
    }
}

void TopicTxWriteAndCommitScenario(TTestContext& tc, bool& activeZone) {
    constexpr ui32 txCount = 2;

    PQTabletPrepare({.partitions=1}, {{"user1", true}}, tc);

    TTopicTxInjectionHelper helper(tc);
    for (ui32 attempt = 0; attempt < 20; ++attempt) {
        try {
            tc.Runtime->ResetScheduledCount();
            helper.ResetPipe();
            helper.DrainEdgeProposeAndPlanResults();
            activeZone = false;

            const ui64 endBefore = GetEndOffset(tc);
            // Previous attempt may have committed while a later GrabEdgeEvent failed.
            if (endBefore >= txCount) {
                AssertTopicTxCommittedPayloads(
                    tc, endBefore - txCount, txCount, /*expectedAttempt=*/Nothing());
                return;
            }

            const TWriteId writeId(0, 1000 + attempt);
            const ui64 txId = 9301 + attempt;

            // Ownership + supportive writes stay outside the injection zone so the reboot/pipe
            // matrix focuses on propose/plan (where Topic tx durability matters).
            const TString ownerCookie = helper.CreateSupportivePartition(writeId);
            for (ui32 i = 0; i < txCount; ++i) {
                helper.WriteToSupportive(
                    writeId, ownerCookie, /*seqNo=*/i, /*messageNo=*/i,
                    TStringBuilder() << "topic-tx-reboot-" << attempt << "-" << i,
                    /*cookie=*/500 + i);
            }
            const ui32 supportivePartitionId = helper.WaitSupportivePartitionId(writeId);

            activeZone = true;
            helper.CommitTopicTransaction(writeId, supportivePartitionId, txId);
            activeZone = false;

            AssertTopicTxCommittedPayloads(tc, endBefore, txCount, attempt);
            return;
        } catch (const TTopicTxInjectionRetry&) {
            activeZone = false;
        } catch (const NActors::TSchedulingLimitReachedException&) {
            activeZone = false;
        } catch (const NActors::TEmptyEventQueueException&) {
            activeZone = false;
        }
    }
    UNIT_FAIL("TopicTxWriteAndCommitScenario: retries exhausted");
}

void RunTopicTxWriteInjectionTest(
    std::function<void(
        const TVector<ui64>&,
        std::function<TTestActorRuntime::TEventFilter()>,
        std::function<void(const TString&, std::function<void(TTestActorRuntime&)>, bool&)>)> runner)
{
    TTestContext tabletIds;
    const TVector<ui64> rebootTablets{tabletIds.TabletId};
    runner(
        rebootTablets,
        [&]() { return tabletIds.InitialEventsFilter.Prepare(); },
        [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& activeZone) {
            TTestContext tc;
            TFinalizer finalizer(tc);
            activeZone = false;
            tc.Prepare(dispatchName, setup, activeZone);
            tc.Runtime->SetScheduledLimit(2'000);
            TopicTxWriteAndCommitScenario(tc, activeZone);
        });
}

} // namespace

// Topic API write-tx commit must survive tablet reboots mid-propose/plan.
Y_UNIT_TEST(TopicTxWriteWithTabletReboots) {
    RunTopicTxWriteInjectionTest([](const auto& tabletIds, auto filterFactory, auto testFunc) {
        RunTestWithReboots(tabletIds, filterFactory, testFunc);
    });
}

// Same Topic write-tx commit path under pipe client resets.
Y_UNIT_TEST(TopicTxWriteWithPipeResets) {
    RunTopicTxWriteInjectionTest([](const auto& tabletIds, auto filterFactory, auto testFunc) {
        RunTestWithPipeResets(tabletIds, filterFactory, testFunc);
    });
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

void TPQTabletFixture::TestSendingTEvReadSetViaApp(const TSendReadSetViaAppTestParams& params)
{
    Y_ABORT_UNLESS(params.TabletsRSCount <= params.TabletsCount);
    const ui64 txId = 67890;

    TVector<NHelpers::TPQTabletMock*> tablets;
    TVector<ui64> tabletIds;
    for (size_t i = 0; i < params.TabletsCount; ++i) {
        tabletIds.push_back(22222 + i);
        tablets.push_back(CreatePQTabletMock(tabletIds.back()));
    }

    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    SendProposeTransactionRequest({.TxId=txId,
                                  .Senders=tabletIds, .Receivers=tabletIds,
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"}
                                  }});
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId}});

    for (auto* tablet : tablets) {
        WaitReadSet(*tablet, {.Step=100, .TxId=txId, .Source=Ctx->TabletId, .Target=tablet->TabletID(), .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});
    }
    for (size_t i = 0; i < Min(params.TabletsRSCount, params.TabletsCount); ++i) {
        tablets[i]->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId, .Target=Ctx->TabletId, .Decision=params.Decision});
    }
    Ctx->Runtime->SimulateSleep(TDuration::MilliSeconds(500));

    SendAppSendRsRequest({.Step=100, .TxId=txId, .SenderId=Nothing(), .Predicate=(params.AppDecision == NKikimrTx::TReadSetData::DECISION_COMMIT),});
    WaitForAppSendRsResponse({.Status = params.ExpectedAppResponseStatus,});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=params.ExpectedStatus});

    WaitPlanStepAccepted({.Step=100});
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


void TPQTabletFixture::ExpectNoExclusiveLockAcquired()
{
    EnsureReadQuoterExists();
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvExclusiveLockAcquired>(TDuration::Seconds(5));
    UNIT_ASSERT(event == nullptr);
}

void TPQTabletFixture::ExpectNoReadQuotaAcquired()
{
    EnsureReadQuoterExists();
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvApproveReadQuota>(TDuration::Seconds(10));
    UNIT_ASSERT(event == nullptr);
}

void TPQTabletFixture::SendAcquireExclusiveLock()
{
    EnsureReadQuoterExists();

    Ctx->Runtime->Send(ReadQuoter->Quoter,
                       Ctx->Edge,
                       new TEvPQ::TEvAcquireExclusiveLock());
}

class TEvReadTestEventHandle: public NActors::IEventHandle {
public:
    TEvReadTestEventHandle(THolder<TEvPQ::TEvRead>&& event, const TActorId& sender)
        : NActors::IEventHandle(TActorId{}, sender, event.Release())
    {}
};

void TPQTabletFixture::SendAcquireReadQuota(ui64 cookie, const TActorId& sender) {
    EnsureReadQuoterExists();

    auto request = MakeHolder<TEvPQ::TEvRead>(
        cookie,
        0, // offset
        99999, // lastOffset
        0, // partNo
        9999, // count
        "", // sessionId
        "client", // clientId
        999, // timeout
        99999, // size
        true, // readToBlobEnd
        99999, // maxTimeLagMs
        0, // readTimestampMs
        "", // clientDC
        false, // externalOperation
        TActorId{} // pipeClient
    );
    auto handle = new TEvReadTestEventHandle(std::move(request), sender);
    Ctx->Runtime->Send(ReadQuoter->Quoter,
                       Ctx->Edge,
                       new TEvPQ::TEvRequestQuota(cookie, handle));
}

void TPQTabletFixture::SendReadQuotaConsumed(ui64 cookie)
{
    EnsureReadQuoterExists();

    Ctx->Runtime->Send(ReadQuoter->Quoter,
                       Ctx->Edge,
                       new TEvPQ::TEvConsumed(1024, 0, cookie, "client"));
}

void TPQTabletFixture::SendReleaseExclusiveLock()
{
    EnsureReadQuoterExists();

    Ctx->Runtime->Send(ReadQuoter->Quoter,
                       Ctx->Edge,
                       new TEvPQ::TEvReleaseExclusiveLock());
}

void TPQTabletFixture::WaitExclusiveLockAcquired()
{
    EnsureReadQuoterExists();
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvExclusiveLockAcquired>();
    UNIT_ASSERT(event);
}

void TPQTabletFixture::WaitReadQuotaAcquired()
{
    EnsureReadQuoterExists();
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvApproveReadQuota>();
    UNIT_ASSERT(event);
}

void TPQTabletFixture::EnsureReadQuoterExists()
{
    if (ReadQuoter) {
        return;
    }

    Cerr << "Ctx->Edge=" << Ctx->Edge << Endl;

    ReadQuoter.ConstructInPlace();
    ReadQuoter->Quoter = Ctx->Runtime->Register(new NPQ::TReadQuoter(ReadQuoter->PQConfig,
                                                                     ReadQuoter->TopicConverter,
                                                                     ReadQuoter->PQTabletConfig,
                                                                     ReadQuoter->PartitionId,
                                                                     TActorId{}, // TabletActor
                                                                     Ctx->Edge,
                                                                     1234567890, // TabletId
                                                                     ReadQuoter->Counters));
    Ctx->Runtime->EnableScheduleForActor(ReadQuoter->Quoter);
    Ctx->Runtime->Send(ReadQuoter->Quoter, TActorId{}, new TEvents::TEvBootstrap());
    //Ctx->Runtime->DispatchEvents();
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
