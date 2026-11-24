#include "pqtablet_fixture.h"

namespace NKikimr::NPQ {

namespace NHelpers {

TWriteId MakeDeferredWriteId(ui64 intPublicationId, const TString& extPublicationId)
{
    NKikimrPQ::TWriteId proto;
    proto.MutableDeferredPublicationApi()->SetIntPublicationId(intPublicationId);
    proto.MutableDeferredPublicationApi()->SetExtPublicationId(extPublicationId);
    return TWriteId(std::move(proto));
}

} // namespace NHelpers

namespace {

constexpr const char* TX_INFO_KEY = "_txinfo";

std::string GetSupportivePartitionKeyFrom() {
    return std::string{TKeyPrefix::EServiceType::ServiceTypeData};
}

std::string GetSupportivePartitionKeyTo() {
    return std::string{static_cast<char>(TKeyPrefix::EServiceType::ServiceTypeData + 1)};
}

} // namespace

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

NKikimrPQ::TEvProposeTransactionResult::EStatus TPQTabletFixture::WaitProposeTransactionStatus(ui64 txId)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPersQueue::TEvProposeTransactionResult>();
    UNIT_ASSERT(event != nullptr);
    UNIT_ASSERT(event->Record.HasTxId());
    UNIT_ASSERT_VALUES_EQUAL(txId, event->Record.GetTxId());
    UNIT_ASSERT(event->Record.HasStatus());
    return event->Record.GetStatus();
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

void TPQTabletFixture::SendKafkaTxnWriteRequest(const NKafka::TProducerInstanceId& producerInstanceId, const TString& ownerCookie, const ui32 partitionId,
                                                const ui64 seqNo, const TString& data, const ui64 cookie, const bool waitResponse,
                                                const ui32 kafkaBatchSize) {
    auto event = MakeHolder<TEvPersQueue::TEvRequest>();
    auto* request = event->Record.MutablePartitionRequest();
    request->SetTopic("/topic");
    request->SetPartition(partitionId);
    request->SetCookie(cookie);
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
    cmdWrite->SetSeqNo(seqNo);
    cmdWrite->SetData(data);
    cmdWrite->SetCreateTimeMS(TInstant::Now().MilliSeconds());
    cmdWrite->SetDisableDeduplication(true);
    cmdWrite->SetUncompressedSize(data.size());
    cmdWrite->SetIgnoreQuotaDeadline(true);
    cmdWrite->SetExternalOperation(true);
    if (kafkaBatchSize > 0) {
        cmdWrite->SetLogicalMessageCount(kafkaBatchSize);
        cmdWrite->SetIsBatch(true);
        cmdWrite->SetMaxSeqNo(seqNo + kafkaBatchSize - 1);
    }

    SendToPipe(Ctx->Edge, event.Release());

    if (!waitResponse) {
        return;
    }

    auto response = Ctx->Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>();
    UNIT_ASSERT(response != nullptr);
    UNIT_ASSERT_VALUES_EQUAL((int)NMsgBusProxy::MSTATUS_OK, (int)response->Record.GetStatus());
    UNIT_ASSERT(response->Record.GetPartitionResponse().HasCookie());
    UNIT_ASSERT_VALUES_EQUAL(cookie, response->Record.GetPartitionResponse().GetCookie());
}

void TPQTabletFixture::ProposeKafkaTransaction(NKafka::TProducerInstanceId producerInstanceId, ui64 txId, const std::vector<ui32>& partitionIds) {
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
}

void TPQTabletFixture::WaitTransactionCompleted(ui64 txId, ui64 planStep) {
    WaitProposeTransactionResponse({.TxId=txId,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});
    WaitPlanStepAck({.Step=planStep, .TxIds={txId}}); // TEvPlanStepAck для координатора
    WaitPlanStepAccepted({.Step=planStep});
}

void TPQTabletFixture::CommitKafkaTransaction(NKafka::TProducerInstanceId producerInstanceId, ui64 txId, const std::vector<ui32>& partitionIds, ui64 planStep) {
    ProposeKafkaTransaction(producerInstanceId, txId, partitionIds);
    SendPlanStep({.Step=planStep, .TxIds={txId}});
    WaitTransactionCompleted(txId, planStep);
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
    const std::vector<ui32>& partitionIds,
    const ui64 planStep)
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
    SendPlanStep({.Step=planStep, .TxIds={txId}});
    WaitTransactionCompleted(txId, planStep);
}

void TPQTabletFixture::CommitDeferredPublicationFinalize(
    const TWriteId& writeId,
    ui64 txId,
    NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi::EOp op,
    const std::vector<ui32>& partitionIds,
    const ui64 planStep)
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
    SendPlanStep({.Step=planStep, .TxIds={txId}});
    WaitTransactionCompleted(txId, planStep);
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
    auto event = MakeHolder<TEvDataShard::TEvCancelTransactionProposal>(params.TxId);

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

} // namespace NKikimr::NPQ
