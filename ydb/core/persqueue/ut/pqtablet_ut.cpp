#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/pqtablet/common/constants.h>
#include <ydb/core/persqueue/pqtablet/partition/partition.h>
#include <ydb/core/persqueue/pqtablet/quota/read_quoter.h>
#include <ydb/core/persqueue/pqtablet/fix_transaction_states.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/protos/counters_keyvalue.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h>
#include <ydb/public/lib/base/msgbus_status.h>

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/event.h>
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
};

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
};

struct TPlanStepParams {
    ui64 Step;
    TVector<ui64> TxIds;
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

    std::unique_ptr<TEvPersQueue::TEvRequest> MakeGetOwnershipRequest(const TGetOwnershipRequestParams& params,
                                                                      const TActorId& pipe) const;

    void TestMultiplePQTablets(const TString& consumer1, const TString& consumer2);
    void TestParallelTransactions(const TString& consumer1, const TString& consumer2);

    void StartPQCalcPredicateObserver(size_t& received);
    void WaitForPQCalcPredicate(size_t& received, size_t expected);

    void WaitForTxState(ui64 txId, NKikimrPQ::TTransaction::EState state);
    void WaitForExecStep(ui64 step);

    void InterceptSaveTxState(TAutoPtr<IEventHandle>& event);
    void SendSaveTxState(TAutoPtr<IEventHandle>& event);

    void WaitForTheTransactionToBeDeleted(ui64 txId);

    TVector<TString> WaitForExactSupportivePartitionsCount(ui32 expectedCount);
    TVector<TString> GetSupportivePartitionsKeysFromKV();
    NKikimrPQ::TTabletTxInfo WaitForExactTxWritesCount(ui32 expectedCount);
    NKikimrPQ::TTabletTxInfo GetTxWritesFromKV();

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
        body->SetImmediate(params.Senders.empty() && params.Receivers.empty() && (partitions.size() == 1) && !params.WriteId.Defined());
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

    SendToPipe(Ctx->Edge,
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

void TPQTabletFixture::InterceptSaveTxState(TAutoPtr<IEventHandle>& ev)
{
    bool found = false;

    TTestActorRuntimeBase::TEventFilter prev;
    auto filter = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) -> bool {
        if (auto* msg = event->CastAsLocal<TEvKeyValue::TEvRequest>()) {
            if (msg->Record.HasCookie() && (msg->Record.GetCookie() == 5)) { // WRITE_TX_COOKIE
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

void TPQTabletFixture::WaitForTheTransactionToBeDeleted(ui64 txId)
{
    const TString key = GetTxKey(txId);

    for (size_t i = 0; i < 200; ++i) {
        auto request = std::make_unique<TEvKeyValue::TEvRequest>();
        request->Record.SetCookie(12345);
        auto cmd = request->Record.AddCmdReadRange();
        auto range = cmd->MutableRange();
        range->SetFrom(key);
        range->SetIncludeFrom(true);
        range->SetTo(key);
        range->SetIncludeTo(true);
        cmd->SetIncludeData(false);
        SendToPipe(Ctx->Edge, request.release());

        auto response = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>();
        UNIT_ASSERT_VALUES_EQUAL(response->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);

        const auto& result = response->Record.GetReadRangeResult(0);
        if (result.GetStatus() == static_cast<ui32>(NKikimrProto::OK)) {
            Ctx->Runtime->SimulateSleep(TDuration::MilliSeconds(300));
            continue;
        }

        if (result.GetStatus() == NKikimrProto::NODATA) {
            return;
        }
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
        TCgiParameters cgi{
            {"TabletID", ToString(Ctx->TabletId)},
            {"SendReadSet", "1"},
            {"decision", params.Predicate ? "commit" : "abort"},
            {"step", ToString(params.Step)},
            {"txId", ToString(params.TxId)},
        };
        if (params.SenderId.Defined()) {
            cgi.InsertUnescaped("senderTablet", ToString(*params.SenderId));
        } else {
            cgi.InsertUnescaped("allSenderTablets", "1");
        }
        return std::make_unique<NActors::NMon::TEvRemoteHttpInfo>(TStringBuilder() << "/app?" << cgi.Print());
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

    // PQ отправит запрос в KV, а потом пришлёт ответ на TEvReadSet
    SendProposeTransactionRequest({.TxId=txId + 1,
                                  .Configs=NHelpers::TConfigParams{
                                  .Tablet=tabletConfig,
                                  .Bootstrap=NHelpers::MakeBootstrapConfig(),
                                  }});

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

    // We will send a TEvProposeTransaction to delete the previous transaction
    SendProposeTransactionRequest({.TxId=txId + 1,
                                  .Senders={mockTabletId}, .Receivers={mockTabletId},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});

    // Instead of TEvReadSetAck, the PQ tablet will receive TEvClientConnected with the Dead flag. The transaction
    // will switch from the WAIT_RS_AKS state to the DELETING state.
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

    // We will send a TEvProposeTransaction to delete the previous transaction
    SendProposeTransactionRequest({.TxId=txId + 1,
                                  .Senders={Ctx->TabletId}, .Receivers={Ctx->TabletId},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});

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

    // We will send a TEvProposeTransaction to delete the previous transaction
    SendProposeTransactionRequest({.TxId=txId + 1,
                                  .Senders={Ctx->TabletId}, .Receivers={Ctx->TabletId},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});

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

    auto request = MakeHolder<TEvPQ::TEvRead>(cookie, 0, 99999, 0, 9999, "", "client", 999, 99999, 99999, 0, "", false, TActorId{});
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
