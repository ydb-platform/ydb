#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/partition.h>
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

using NKikimr::NPQ::NHelpers::CreatePQTabletMock;
using TPQTabletMock = NKikimr::NPQ::NHelpers::TPQTabletMock;

}

Y_UNIT_TEST_SUITE(TPQTabletTests) {

class TPQTabletFixture : public NUnitTest::TBaseFixture {
protected:
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

    using TProposeTransactionParams = NHelpers::TProposeTransactionParams;
    using TPlanStepParams = NHelpers::TPlanStepParams;
    using TReadSetParams = NHelpers::TReadSetParams;
    using TDropTabletParams = NHelpers::TDropTabletParams;
    using TCancelTransactionProposalParams = NHelpers::TCancelTransactionProposalParams;
    using TGetOwnershipRequestParams = NHelpers::TGetOwnershipRequestParams;
    using TWriteRequestParams = NHelpers::TWriteRequestParams;

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
    void SendReadSet(const TReadSetParams& params);

    void WaitReadSetAck(NHelpers::TPQTabletMock& tablet, const TReadSetAckMatcher& matcher);
    void SendReadSetAck(NHelpers::TPQTabletMock& tablet);

    void SendDropTablet(const TDropTabletParams& params);
    void WaitDropTabletReply(const TDropTabletReplyMatcher& matcher);

    void StartPQWriteStateObserver();
    void WaitForPQWriteState();

    void SendCancelTransactionProposal(const TCancelTransactionProposalParams& params);

    void StartPQWriteTxsObserver();
    void WaitForPQWriteTxs();

    template <class T> void WaitForEvent(size_t count);
    void WaitForCalcPredicateResult(size_t count = 1);
    void WaitForProposePartitionConfigResult(size_t count = 1);

    void TestWaitingForTEvReadSet(size_t senders, size_t receivers);

    void StartPQWriteObserver(bool& flag, unsigned cookie);
    void WaitForPQWriteComplete(bool& flag);

    bool FoundPQWriteState = false;
    bool FoundPQWriteTxs = false;

    void SendGetOwnershipRequest(const TGetOwnershipRequestParams& params);
    void WaitGetOwnershipResponse(const TGetOwnershipResponseMatcher& matcher);
    void SyncGetOwnership(const TGetOwnershipRequestParams& params,
                          const TGetOwnershipResponseMatcher& matcher);

    void SendWriteRequest(const TWriteRequestParams& params);
    void WaitWriteResponse(const TWriteResponseMatcher& matcher);

    std::unique_ptr<TEvPersQueue::TEvRequest> MakeGetOwnershipRequest(const TGetOwnershipRequestParams& params,
                                                                      const TActorId& pipe) const;

    //
    // TODO(abcdef): для тестирования повторных вызовов нужны примитивы Send+Wait
    //

    NHelpers::TPQTabletMock* CreatePQTabletMock(ui64 tabletId);

    TMaybe<TTestContext> Ctx;
    TMaybe<TFinalizer> Finalizer;

    TTestActorRuntimeBase::TEventObserver PrevEventObserver;

    TActorId Pipe;
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
    auto event = MakeHolder<TEvPersQueue::TEvProposeTransaction>();
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
                operation->SetBegin(*txOp.Begin);
                operation->SetEnd(*txOp.End);
                operation->SetConsumer(*txOp.Consumer);
            }
            operation->SetPath(txOp.Path);
            if (txOp.SupportivePartition.Defined()) {
                operation->SetSupportivePartition(*txOp.SupportivePartition);
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
    if (!tablet.ReadSet.Defined()) {
        TDispatchOptions options;
        options.CustomFinalCondition = [&]() {
            return tablet.ReadSet.Defined();
        };
        UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));
    }

    auto readSet = std::move(*tablet.ReadSet);
    tablet.ReadSet = Nothing();

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

void TPQTabletFixture::WaitGetOwnershipResponse(const TGetOwnershipResponseMatcher& matcher)
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

void TPQTabletFixture::StartPQWriteObserver(bool& flag, unsigned cookie)
{
    flag = false;

    auto observer = [&flag, cookie](TAutoPtr<IEventHandle>& event) {
        if (auto* kvResponse = event->CastAsLocal<TEvKeyValue::TEvResponse>()) {
            if (kvResponse->Record.HasCookie()) {
            }
            if ((event->Sender == event->Recipient) &&
                kvResponse->Record.HasCookie() &&
                (kvResponse->Record.GetCookie() == cookie)) {
                flag = true;
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

void TPQTabletFixture::StartPQWriteTxsObserver()
{
    StartPQWriteObserver(FoundPQWriteTxs, 5); // TPersQueue::WRITE_TX_COOKIE
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

Y_UNIT_TEST_F(Multiple_PQTablets, TPQTabletFixture)
{
    NHelpers::TPQTabletMock* tablet = CreatePQTabletMock(22222);
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    const ui64 txId_1 = 67890;
    const ui64 txId_2 = 67891;

    SendProposeTransactionRequest({.TxId=txId_1,
                                  .Senders={22222}, .Receivers={22222},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId_1,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendProposeTransactionRequest({.TxId=txId_2,
                                  .Senders={22222}, .Receivers={22222},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    WaitProposeTransactionResponse({.TxId=txId_2,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    SendPlanStep({.Step=100, .TxIds={txId_2}});
    SendPlanStep({.Step=200, .TxIds={txId_1}});

    //
    // TODO(abcdef): проверить, что в команде CmdWrite есть информация о транзакции
    //

    WaitPlanStepAck({.Step=100, .TxIds={txId_2}}); // TEvPlanStepAck для координатора
    WaitPlanStepAccepted({.Step=100});

    WaitPlanStepAck({.Step=200, .TxIds={txId_1}}); // TEvPlanStepAck для координатора
    WaitPlanStepAccepted({.Step=200});

    //
    // транзакция txId_2
    //
    WaitReadSet(*tablet, {.Step=100, .TxId=txId_2, .Source=Ctx->TabletId, .Target=22222, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});
    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId_2, .Target=Ctx->TabletId, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    WaitProposeTransactionResponse({.TxId=txId_2,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    tablet->SendReadSetAck(*Ctx->Runtime, {.Step=100, .TxId=txId_2, .Source=Ctx->TabletId});
    WaitReadSetAck(*tablet, {.Step=100, .TxId=txId_2, .Source=22222, .Target=Ctx->TabletId, .Consumer=Ctx->TabletId});

    //
    // TODO(abcdef): проверить, что удалена информация о транзакции
    //

    //
    // транзакция txId_1
    //
    WaitReadSet(*tablet, {.Step=200, .TxId=txId_1, .Source=Ctx->TabletId, .Target=22222, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});
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

    //
    // TODO(abcdef): проверить, что в команде CmdWrite есть информация о транзакции
    //

    WaitPlanStepAck({.Step=100, .TxIds={txId}}); // TEvPlanStepAck для координатора
    WaitPlanStepAccepted({.Step=100});

    WaitReadSet(*tablet, {.Step=100, .TxId=txId, .Source=Ctx->TabletId, .Target=22222, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT, .Producer=Ctx->TabletId});
    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId, .Target=Ctx->TabletId, .Decision=NKikimrTx::TReadSetData::DECISION_ABORT});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});

    tablet->SendReadSetAck(*Ctx->Runtime, {.Step=100, .TxId=txId, .Source=Ctx->TabletId});
    WaitReadSetAck(*tablet, {.Step=100, .TxId=txId, .Source=22222, .Target=Ctx->TabletId, .Consumer=Ctx->TabletId});

    //
    // TODO(abcdef): проверить, что удалена информация о транзакции
    //
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

    //
    // TODO(abcdef): проверить, что в команде CmdWrite есть информация о транзакции
    //

    WaitPlanStepAck({.Step=100, .TxIds={txId}}); // TEvPlanStepAck для координатора
    WaitPlanStepAccepted({.Step=100});

    WaitReadSet(*tablet, {.Step=100, .TxId=txId, .Source=Ctx->TabletId, .Target=22222, .Decision=NKikimrTx::TReadSetData::DECISION_ABORT, .Producer=Ctx->TabletId});
    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId, .Target=Ctx->TabletId, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});

    tablet->SendReadSetAck(*Ctx->Runtime, {.Step=100, .TxId=txId, .Source=Ctx->TabletId});
    WaitReadSetAck(*tablet, {.Step=100, .TxId=txId, .Source=22222, .Target=Ctx->TabletId, .Consumer=Ctx->TabletId});

    //
    // TODO(abcdef): проверить, что удалена информация о транзакции
    //
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

    WaitPlanStepAck({.Step=100, .TxIds={txId_1}}); // TEvPlanStepAck для координатора
    SendDropTablet({.TxId=67890});                 // TEvDropTable когда выполняется транзакция
    WaitPlanStepAccepted({.Step=100});

    WaitProposeTransactionResponse({.TxId=txId_1,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

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

    WaitPlanStepAck({.Step=100, .TxIds={txId}}); // TEvPlanStepAck для координатора
    WaitPlanStepAccepted({.Step=100});
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

    SendCancelTransactionProposal({.TxId=txId});

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
                            .Owner="-=[ 0wn3r ]=-",
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
                     .Owner="-=[ 0wn3r ]=-",
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
                     .Owner="-=[ 0wn3r ]=-",
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

    tablet->SendReadSet(*Ctx->Runtime, {.Step=100, .TxId=txId, .Target=Ctx->TabletId, .Decision=NKikimrTx::TReadSetData::DECISION_COMMIT});

    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    tablet->SendReadSetAck(*Ctx->Runtime, {.Step=100, .TxId=txId, .Source=Ctx->TabletId});
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

    SendPlanStep({.Step=100, .TxIds={txId_1, txId_2}});
    WaitPlanStepAck({.Step=100, .TxIds={txId_1, txId_2}});
    WaitPlanStepAccepted({.Step=100});
}

}

}
