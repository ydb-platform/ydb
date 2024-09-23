#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/partition.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/protos/counters_keyvalue.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
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

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TUserActionProcessorTests) {

namespace NHelpers {

struct TCreatePartitionParams {
    TPartitionId Partition = TPartitionId{1};
    ui64 Begin = 0;
    ui64 End = 0;
    TMaybe<ui64> PlanStep;
    TMaybe<ui64> TxId;
    TVector<TTransaction> Transactions;
};

struct TCreateConsumerParams {
    TString Consumer;
    ui64 Offset = 0;
    ui32 Generation = 0;
    ui32 Step = 0;
    TString Session;
    ui64 OffsetRewindSum = 0;
    ui64 ReadRuleGeneration = 0;
};

struct TTxOperation {
    ui32 Partition;
    TString Consumer;
    ui64 Begin = 0;
    ui64 End = 0;
    TString Path;
};

struct TProposeTransactionParams {
    ui64 TxId = 0;
    TVector<ui64> Senders;
    TVector<ui64> Receivers;
    TVector<TTxOperation> TxOps;
};

}

class TUserActionProcessorFixture : public NUnitTest::TBaseFixture {
protected:
    struct TUserInfoMatcher {
        TMaybe<TString> Consumer;
        TMaybe<TString> Session;
        TMaybe<ui64> Offset;
        TMaybe<ui32> Generation;
        TMaybe<ui32> Step;
        TMaybe<ui64> ReadRuleGeneration;
    };

    struct TDeleteRangeMatcher {
        TMaybe<TString> Consumer;
    };

    struct TCmdWriteMatcher {
        TMaybe<size_t> Count;
        TMaybe<ui64> PlanStep;
        TMaybe<ui64> TxId;
        THashMap<size_t, TUserInfoMatcher> UserInfos;
        THashMap<size_t, TDeleteRangeMatcher> DeleteRanges;
    };

    struct TProxyResponseMatcher {
        TMaybe<ui64> Cookie;
        TMaybe<NMsgBusProxy::EResponseStatus> Status;
        TMaybe<NPersQueue::NErrorCode::EErrorCode> ErrorCode;
        TMaybe<ui64> Offset;
    };

    struct TErrorMatcher {
        TMaybe<ui64> Cookie;
        TMaybe<NPersQueue::NErrorCode::EErrorCode> ErrorCode;
        TMaybe<TString> Error;
    };

    struct TProposeTransactionResponseMatcher {
        TMaybe<ui64> TxId;
        TMaybe<NKikimrPQ::TEvProposeTransactionResult::EStatus> Status;
    };

    struct TCalcPredicateMatcher {
        TMaybe<ui64> Step;
        TMaybe<ui64> TxId;
        TMaybe<ui32> Partition;
        TMaybe<bool> Predicate;
    };

    struct TCommitTxDoneMatcher {
        TMaybe<ui64> Step;
        TMaybe<ui64> TxId;
        TMaybe<ui32> Partition;
    };

    struct TChangePartitionConfigMatcher {
        TMaybe<ui32> Partition;
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

    using TCreatePartitionParams = NHelpers::TCreatePartitionParams;
    using TCreateConsumerParams = NHelpers::TCreateConsumerParams;
    using TProposeTransactionParams = NHelpers::TProposeTransactionParams;

    void SetUp(NUnitTest::TTestContext&) override;
    void TearDown(NUnitTest::TTestContext&) override;

    void CreatePartitionActor(const TPartitionId& partition,
                              const TVector<TCreateConsumerParams>& consumers,
                              bool newPartition,
                              TVector<TTransaction> txs);
    void CreatePartition(const TCreatePartitionParams& params = {},
                         const TVector<TCreateConsumerParams>& consumers = {});

    void CreateSession(const TString& clientId,
                       const TString& sessionId,
                       ui32 generation = 1, ui32 step = 1,
                       ui64 cookie = 1);
    void SetOffset(const TString& clientId,
                   const TString& sessionId,
                   ui64 offset,
                   TMaybe<ui64> expected = Nothing(),
                   ui64 cookie = 1);

    void SendCreateSession(ui64 cookie,
                           const TString& clientId,
                           const TString& sessionId,
                           ui32 generation,
                           ui32 step);
    void SendSetOffset(ui64 cookie,
                       const TString& clientId,
                       ui64 offset,
                       const TString& sessionId);
    void SendGetOffset(ui64 cookie,
                       const TString& clientId);
    void WaitCmdWrite(const TCmdWriteMatcher& matcher = {});
    void WaitCmdWriteTx(const TCmdWriteTxMatcher& matcher = {});
    void SendCmdWriteResponse(NMsgBusProxy::EResponseStatus status);
    void WaitProxyResponse(const TProxyResponseMatcher &matcher = {});
    void WaitErrorResponse(const TErrorMatcher& matcher = {});

    void WaitDiskStatusRequest();
    void SendDiskStatusResponse();
    void WaitMetaReadRequest();
    void SendMetaReadResponse(TMaybe<ui64> step, TMaybe<ui64> txId);
    void WaitInfoRangeRequest();
    void SendInfoRangeResponse(ui32 partition,
                               const TVector<TCreateConsumerParams>& consumers);
    void WaitDataRangeRequest();
    void SendDataRangeResponse(ui64 begin, ui64 end);
    void WaitDataReadRequest();
    void SendDataReadResponse();

    void SendProposeTransactionRequest(ui32 partition,
                                       ui64 begin, ui64 end,
                                       const TString& client,
                                       const TString& topic,
                                       bool immediate,
                                       ui64 txId);
    void SendProposeTransactionRequest(const TProposeTransactionParams& params);
    void WaitProposeTransactionResponse(const TProposeTransactionResponseMatcher& matcher = {});

    void SendCalcPredicate(ui64 step,
                           ui64 txId,
                           const TString& consumer,
                           ui64 begin,
                           ui64 end);
    void WaitCalcPredicateResult(const TCalcPredicateMatcher& matcher = {});

    void SendCommitTx(ui64 step, ui64 txId);
    void SendRollbackTx(ui64 step, ui64 txId);
    void WaitCommitTxDone(const TCommitTxDoneMatcher& matcher = {});

    void SendChangePartitionConfig(const TVector<TCreateConsumerParams>& consumers = {});
    void WaitPartitionConfigChanged(const TChangePartitionConfigMatcher& matcher = {});

    TTransaction MakeTransaction(ui64 step, ui64 txId,
                                 TString consumer,
                                 ui64 begin, ui64 end,
                                 TMaybe<bool> predicate = Nothing());

    TMaybe<TTestContext> Ctx;
    TMaybe<TFinalizer> Finalizer;

    TActorId ActorId;

    NPersQueue::TTopicConverterPtr TopicConverter;
    NKikimrPQ::TPQTabletConfig Config;
    TActorId Pipe;
};

void TUserActionProcessorFixture::SetUp(NUnitTest::TTestContext&)
{
    Ctx.ConstructInPlace();
    Finalizer.ConstructInPlace(*Ctx);

    Ctx->Prepare();
    Ctx->Runtime->SetScheduledLimit(5'000);
}

void TUserActionProcessorFixture::TearDown(NUnitTest::TTestContext&)
{
}

void TUserActionProcessorFixture::CreatePartitionActor(const TPartitionId& id,
                                                       const TVector<TCreateConsumerParams>& consumers,
                                                       bool newPartition,
                                                       TVector<TTransaction> txs)
{
    using TKeyValueCounters = TProtobufTabletCounters<
        NKeyValue::ESimpleCounters_descriptor,
        NKeyValue::ECumulativeCounters_descriptor,
        NKeyValue::EPercentileCounters_descriptor,
        NKeyValue::ETxTypes_descriptor
    >;
    using TPersQueueCounters = TAppProtobufTabletCounters<
        NPQ::ESimpleCounters_descriptor,
        NPQ::ECumulativeCounters_descriptor,
        NPQ::EPercentileCounters_descriptor
    >;
    using TCounters = TProtobufTabletCountersPair<
        TKeyValueCounters,
        TPersQueueCounters
    >;

    TAutoPtr<TCounters> counters(new TCounters());
    TAutoPtr<TTabletCountersBase> tabletCounters = counters->GetSecondTabletCounters().Release();

    for (auto& c : consumers) {
        Config.AddReadRules(c.Consumer);
    }

    Config.SetTopicName("rt3.dc1--account--topic");
    Config.SetTopicPath("/Root/PQ/rt3.dc1--account--topic");
    Config.SetFederationAccount("account");
    Config.SetLocalDC(true);
    Config.SetYdbDatabasePath("");

    NPersQueue::TTopicNamesConverterFactory factory(true, "/Root/PQ", "dc1");

    TopicConverter = factory.MakeTopicConverter(Config);

    auto actor = new NPQ::TPartition(Ctx->TabletId,
                                     id,
                                     Ctx->Edge,
                                     Ctx->Edge,
                                     TopicConverter,
                                     true,
                                     "dcId",
                                     false,
                                     Config,
                                     *tabletCounters,
                                     newPartition,
                                     std::move(txs));
    ActorId = Ctx->Runtime->Register(actor);
}

void TUserActionProcessorFixture::CreatePartition(const TCreatePartitionParams& params,
                                                  const TVector<TCreateConsumerParams>& consumers)
{
    if ((params.Begin == 0) && (params.End == 0)) {
        CreatePartitionActor(params.Partition, consumers, true, {});
    } else {
        CreatePartitionActor(params.Partition, consumers, false, params.Transactions);

        WaitDiskStatusRequest();
        SendDiskStatusResponse();

        WaitMetaReadRequest();
        SendMetaReadResponse(params.PlanStep, params.TxId);

        WaitInfoRangeRequest();
        SendInfoRangeResponse(params.Partition.InternalPartitionId, consumers);

        WaitDataRangeRequest();
        SendDataRangeResponse(params.Begin, params.End);
    }
}

void TUserActionProcessorFixture::CreateSession(const TString& clientId,
                                                const TString& sessionId,
                                                ui32 generation, ui32 step,
                                                ui64 cookie)
{
    SendCreateSession(cookie,clientId,sessionId, generation, step);
    WaitCmdWrite({.Count=2, .UserInfos={{0, {.Session = sessionId, .Offset = 0}}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
    WaitProxyResponse({.Cookie = cookie});
}

void TUserActionProcessorFixture::SetOffset(const TString& clientId,
                                            const TString& sessionId,
                                            ui64 offset,
                                            TMaybe<ui64> expected,
                                            ui64 cookie)
{
    SendSetOffset(cookie, clientId, offset, sessionId);
    WaitCmdWrite({.Count=2, .UserInfos={{0, {.Session = sessionId, .Offset = (expected ? *expected : offset)}}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
    WaitProxyResponse({.Cookie = cookie});
}

void TUserActionProcessorFixture::SendCreateSession(ui64 cookie,
                                                    const TString& clientId,
                                                    const TString& sessionId,
                                                    ui32 generation,
                                                    ui32 step)
{
    auto event = MakeHolder<TEvPQ::TEvSetClientInfo>(cookie,
                                                     clientId,
                                                     0,
                                                     sessionId,
                                                     generation,
                                                     step,
                                                     TEvPQ::TEvSetClientInfo::ESCI_CREATE_SESSION);
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TUserActionProcessorFixture::SendSetOffset(ui64 cookie,
                                                const TString& clientId,
                                                ui64 offset,
                                                const TString& sessionId)
{
    auto event = MakeHolder<TEvPQ::TEvSetClientInfo>(cookie,
                                                     clientId,
                                                     offset,
                                                     sessionId,
                                                     0,
                                                     0);
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TUserActionProcessorFixture::SendGetOffset(ui64 cookie,
                                                const TString& clientId)
{
    auto event = MakeHolder<TEvPQ::TEvGetClientOffset>(cookie,
                                                       clientId);
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TUserActionProcessorFixture::WaitCmdWrite(const TCmdWriteMatcher& matcher)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>();
    UNIT_ASSERT(event != nullptr);

    if (matcher.Count.Defined()) {
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Count,
                                 event->Record.CmdWriteSize() + event->Record.CmdDeleteRangeSize());
    }

    //
    // TxMeta
    //
    if (matcher.PlanStep.Defined()) {
        NKikimrPQ::TPartitionTxMeta meta;
        UNIT_ASSERT(meta.ParseFromString(event->Record.GetCmdWrite(0).GetValue()));

        UNIT_ASSERT_VALUES_EQUAL(*matcher.PlanStep, meta.GetPlanStep());
    }
    if (matcher.TxId.Defined()) {
        NKikimrPQ::TPartitionTxMeta meta;
        UNIT_ASSERT(meta.ParseFromString(event->Record.GetCmdWrite(0).GetValue()));

        UNIT_ASSERT_VALUES_EQUAL(*matcher.TxId, meta.GetTxId());
    }

    //
    // CmdWrite
    //
    for (auto& [index, userInfo] : matcher.UserInfos) {
        UNIT_ASSERT(index < event->Record.CmdWriteSize());

        NKikimrPQ::TUserInfo ud;
        UNIT_ASSERT(ud.ParseFromString(event->Record.GetCmdWrite(index).GetValue()));

        if (userInfo.Session) {
            UNIT_ASSERT(ud.HasSession());
            UNIT_ASSERT_VALUES_EQUAL(*userInfo.Session, ud.GetSession());
        }
        if (userInfo.Generation) {
            UNIT_ASSERT(ud.HasGeneration());
            UNIT_ASSERT_VALUES_EQUAL(*userInfo.Generation, ud.GetGeneration());
        }
        if (userInfo.Step) {
            UNIT_ASSERT(ud.HasStep());
            UNIT_ASSERT_VALUES_EQUAL(*userInfo.Step, ud.GetStep());
        }
        if (userInfo.Offset) {
            UNIT_ASSERT(ud.HasOffset());
            UNIT_ASSERT_VALUES_EQUAL(*userInfo.Offset, ud.GetOffset());
        }
        if (userInfo.ReadRuleGeneration) {
            UNIT_ASSERT(ud.HasReadRuleGeneration());
            UNIT_ASSERT_VALUES_EQUAL(*userInfo.ReadRuleGeneration, ud.GetReadRuleGeneration());
        }
    }

    //
    // CmdDeleteRange
    //
    for (auto& [index, deleteRange] : matcher.DeleteRanges) {
        UNIT_ASSERT(index < event->Record.CmdDeleteRangeSize());

        Y_UNUSED(deleteRange);
    }
}

void TUserActionProcessorFixture::WaitCmdWriteTx(const TCmdWriteTxMatcher& matcher)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>();
    UNIT_ASSERT(event != nullptr);

    UNIT_ASSERT_VALUES_EQUAL(event->Record.GetCookie(), 5);  // WRITE_TX_PREPARED_COOKIE

    UNIT_ASSERT_VALUES_EQUAL(event->Record.CmdGetStatusSize(), 1 + matcher.TxOps.size());
}

void TUserActionProcessorFixture::SendCmdWriteResponse(NMsgBusProxy::EResponseStatus status)
{
    auto event = MakeHolder<TEvKeyValue::TEvResponse>();
    event->Record.SetStatus(status);

    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TUserActionProcessorFixture::WaitProxyResponse(const TProxyResponseMatcher& matcher)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvProxyResponse>();
    UNIT_ASSERT(event != nullptr);

    if (matcher.Cookie) {
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Cookie, event->Cookie);
    }

    if (matcher.Status) {
        UNIT_ASSERT(event->Response.HasStatus());
        UNIT_ASSERT(*matcher.Status == event->Response.GetStatus());
    }

    if (matcher.ErrorCode) {
        UNIT_ASSERT(event->Response.HasErrorCode());
        UNIT_ASSERT(*matcher.ErrorCode == event->Response.GetErrorCode());
    }

    if (matcher.Offset) {
        UNIT_ASSERT(event->Response.HasPartitionResponse());
        UNIT_ASSERT(event->Response.GetPartitionResponse().HasCmdGetClientOffsetResult());
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Offset, event->Response.GetPartitionResponse().GetCmdGetClientOffsetResult().GetOffset());
    }
}

void TUserActionProcessorFixture::WaitErrorResponse(const TErrorMatcher& matcher)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvError>();
    UNIT_ASSERT(event != nullptr);

    if (matcher.Cookie) {
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Cookie, event->Cookie);
    }

    if (matcher.ErrorCode) {
        UNIT_ASSERT(*matcher.ErrorCode == event->ErrorCode);
    }

    if (matcher.Error) {
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Error, event->Error);
    }
}

void TUserActionProcessorFixture::WaitDiskStatusRequest()
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>();
    UNIT_ASSERT(event != nullptr);

    UNIT_ASSERT(event->Record.CmdGetStatusSize() > 0);
}

void TUserActionProcessorFixture::SendDiskStatusResponse()
{
    auto event = MakeHolder<TEvKeyValue::TEvResponse>();
    event->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);

    auto result = event->Record.AddGetStatusResult();
    result->SetStatus(NKikimrProto::OK);
    result->SetStatusFlags(NKikimrBlobStorage::StatusIsValid);

    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TUserActionProcessorFixture::WaitMetaReadRequest()
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>();
    UNIT_ASSERT(event != nullptr);

    UNIT_ASSERT_VALUES_EQUAL(event->Record.CmdReadSize(), 2);
}

void TUserActionProcessorFixture::SendMetaReadResponse(TMaybe<ui64> step, TMaybe<ui64> txId)
{
    auto event = MakeHolder<TEvKeyValue::TEvResponse>();
    event->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);

    //
    // NKikimrPQ::TPartitionMeta
    //
    auto read = event->Record.AddReadResult();
    read->SetStatus(NKikimrProto::NODATA);

    //
    // NKikimrPQ::TPartitionTxMeta
    //
    read = event->Record.AddReadResult();
    if (step.Defined() || txId.Defined()) {
        NKikimrPQ::TPartitionTxMeta meta;

        if (step.Defined()) {
            meta.SetPlanStep(*step);
        }
        if (txId.Defined()) {
            meta.SetTxId(*txId);
        }

        TString out;
        Y_PROTOBUF_SUPPRESS_NODISCARD meta.SerializeToString(&out);

        read->SetStatus(NKikimrProto::OK);
        read->SetValue(out);
    } else {
        read->SetStatus(NKikimrProto::NODATA);
    }

    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TUserActionProcessorFixture::WaitInfoRangeRequest()
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>();
    UNIT_ASSERT(event != nullptr);

    UNIT_ASSERT_VALUES_EQUAL(event->Record.CmdReadRangeSize(), 1);
}

void TUserActionProcessorFixture::SendInfoRangeResponse(ui32 partition,
                                                        const TVector<TCreateConsumerParams>& consumers)
{
    auto event = MakeHolder<TEvKeyValue::TEvResponse>();
    event->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);

    auto read = event->Record.AddReadRangeResult();
    if (consumers.empty()) {
        read->SetStatus(NKikimrProto::NODATA);
    } else {
        read->SetStatus(NKikimrProto::OK);

        for (auto& c : consumers) {
            auto pair = read->AddPair();
            pair->SetStatus(NKikimrProto::OK);

            NPQ::TKeyPrefix key(NPQ::TKeyPrefix::TypeInfo, partition, NPQ::TKeyPrefix::MarkUser);
            key.Append(c.Consumer.data(), c.Consumer.size());
            pair->SetKey(key.Data(), key.Size());

            NKikimrPQ::TUserInfo userInfo;
            userInfo.SetOffset(c.Offset);
            userInfo.SetGeneration(c.Generation);
            userInfo.SetStep(c.Step);
            userInfo.SetSession(c.Session);
            userInfo.SetOffsetRewindSum(c.OffsetRewindSum);
            userInfo.SetReadRuleGeneration(c.ReadRuleGeneration);

            TString out;
            Y_PROTOBUF_SUPPRESS_NODISCARD userInfo.SerializeToString(&out);
            pair->SetValue(out);
        }
    }

    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TUserActionProcessorFixture::WaitDataRangeRequest()
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>();
    UNIT_ASSERT(event != nullptr);

    UNIT_ASSERT_VALUES_EQUAL(event->Record.CmdReadRangeSize(), 1);
}

void TUserActionProcessorFixture::SendDataRangeResponse(ui64 begin, ui64 end)
{
    Y_ABORT_UNLESS(begin <= end);

    auto event = MakeHolder<TEvKeyValue::TEvResponse>();
    event->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);

    auto read = event->Record.AddReadRangeResult();
    read->SetStatus(NKikimrProto::OK);
    auto pair = read->AddPair();
    NPQ::TKey key(NPQ::TKeyPrefix::TypeData, 1, begin, 0, end - begin, 0);
    pair->SetStatus(NKikimrProto::OK);
    pair->SetKey(key.ToString());
    //pair->SetValueSize();
    pair->SetCreationUnixTime(0);

    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TUserActionProcessorFixture::SendProposeTransactionRequest(ui32 partition,
                                                                ui64 begin, ui64 end,
                                                                const TString& client,
                                                                const TString& topic,
                                                                bool immediate,
                                                                ui64 txId)
{
    auto event = MakeHolder<TEvPersQueue::TEvProposeTransaction>();

    ActorIdToProto(Ctx->Edge, event->Record.MutableSource());
    auto* body = event->Record.MutableTxBody();
    auto* operation = body->MutableOperations()->Add();
    operation->SetPartitionId(partition);
    operation->SetBegin(begin);
    operation->SetEnd(end);
    operation->SetConsumer(client);
    operation->SetPath(topic);
    body->SetImmediate(immediate);
    event->Record.SetTxId(txId);

    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TUserActionProcessorFixture::SendProposeTransactionRequest(const TProposeTransactionParams& params)
{
    auto event = MakeHolder<TEvPersQueue::TEvProposeTransaction>();

    //
    // Source
    //
    ActorIdToProto(Ctx->Edge, event->Record.MutableSource());

    //
    // TxBody
    //
    auto* body = event->Record.MutableTxBody();
    for (auto& txOp : params.TxOps) {
        auto* operation = body->MutableOperations()->Add();
        operation->SetPartitionId(txOp.Partition);
        operation->SetBegin(txOp.Begin);
        operation->SetEnd(txOp.End);
        operation->SetConsumer(txOp.Consumer);
        operation->SetPath(txOp.Path);
    }
    for (ui64 tabletId : params.Senders) {
        body->AddSendingShards(tabletId);
    }
    for (ui64 tabletId : params.Receivers) {
        body->AddReceivingShards(tabletId);
    }
    body->SetImmediate(params.Senders.empty() && params.Receivers.empty());

    //
    // TxId
    //
    event->Record.SetTxId(params.TxId);

    if (Pipe == TActorId()) {
        Pipe = Ctx->Runtime->ConnectToPipe(Ctx->TabletId, Ctx->Edge, 0, GetPipeConfigWithRetries());
    }

    Ctx->Runtime->SendToPipe(Pipe,
                             Ctx->Edge,
                             event.Release());
}

void TUserActionProcessorFixture::WaitProposeTransactionResponse(const TProposeTransactionResponseMatcher& matcher)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPersQueue::TEvProposeTransactionResult>();
    UNIT_ASSERT(event != nullptr);

    if (matcher.TxId) {
        UNIT_ASSERT(event->Record.HasTxId());
        UNIT_ASSERT_VALUES_EQUAL(*matcher.TxId, event->Record.GetTxId());
    }

    if (matcher.Status) {
        UNIT_ASSERT(event->Record.HasStatus());
        UNIT_ASSERT(*matcher.Status == event->Record.GetStatus());
    }
}

void TUserActionProcessorFixture::SendCalcPredicate(ui64 step,
                                                    ui64 txId,
                                                    const TString& consumer,
                                                    ui64 begin,
                                                    ui64 end)
{
    auto event = MakeHolder<TEvPQ::TEvTxCalcPredicate>(step, txId);
    event->AddOperation(consumer, begin, end);

    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TUserActionProcessorFixture::WaitCalcPredicateResult(const TCalcPredicateMatcher& matcher)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvTxCalcPredicateResult>();
    UNIT_ASSERT(event != nullptr);

    if (matcher.Step) {
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Step, event->Step);
    }
    if (matcher.TxId) {
        UNIT_ASSERT_VALUES_EQUAL(*matcher.TxId, event->TxId);
    }
    if (matcher.Partition) {
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Partition, event->Partition);
    }
    if (matcher.Predicate) {
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Predicate, event->Predicate);
    }
}

void TUserActionProcessorFixture::SendCommitTx(ui64 step, ui64 txId)
{
    auto event = MakeHolder<TEvPQ::TEvTxCommit>(step, txId);
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TUserActionProcessorFixture::SendRollbackTx(ui64 step, ui64 txId)
{
    auto event = MakeHolder<TEvPQ::TEvTxRollback>(step, txId);
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TUserActionProcessorFixture::WaitCommitTxDone(const TCommitTxDoneMatcher& matcher)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvTxCommitDone>();
    UNIT_ASSERT(event != nullptr);

    if (matcher.Step) {
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Step, event->Step);
    }
    if (matcher.TxId) {
        UNIT_ASSERT_VALUES_EQUAL(*matcher.TxId, event->TxId);
    }
    if (matcher.Partition) {
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Partition, event->Partition);
    }
}

void TUserActionProcessorFixture::SendChangePartitionConfig(const TVector<TCreateConsumerParams>& consumers)
{
    auto config = Config;
    config.ClearReadRules();
    config.ClearReadRuleGenerations();

    for (auto& c : consumers) {
        config.AddReadRules(c.Consumer);
        config.AddReadRuleGenerations(c.Generation);
    }

    auto event = MakeHolder<TEvPQ::TEvChangePartitionConfig>(TopicConverter, config);
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TUserActionProcessorFixture::WaitPartitionConfigChanged(const TChangePartitionConfigMatcher& matcher)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvPartitionConfigChanged>();
    UNIT_ASSERT(event != nullptr);

    if (matcher.Partition) {
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Partition, event->Partition);
    }
}

TTransaction TUserActionProcessorFixture::MakeTransaction(ui64 step, ui64 txId,
                                                          TString consumer,
                                                          ui64 begin, ui64 end,
                                                          TMaybe<bool> predicate)
{
    auto event = MakeSimpleShared<TEvPQ::TEvTxCalcPredicate>(step, txId);
    event->AddOperation(std::move(consumer), begin, end);

    return TTransaction(event, predicate);
}

Y_UNIT_TEST_F(Batching, TUserActionProcessorFixture)
{
    CreatePartition();

    SendCreateSession(4, "client-1", "session-id-1", 2, 3);

    WaitCmdWrite({.Count=2, .UserInfos={{0, {.Session = "session-id-1", .Offset=0, .Generation=2, .Step=3}}}});

    SendCreateSession(5, "client-2", "session-id-2", 4, 5);
    SendCreateSession(6, "client-3", "session-id-3", 6, 7);

    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitProxyResponse({.Cookie=4});

    WaitCmdWrite({.Count=4, .UserInfos={
                 {0, {.Session = "session-id-2", .Offset=0, .Generation=4, .Step=5}},
                 {2, {.Session = "session-id-3", .Offset=0, .Generation=6, .Step=7}}
                 }});

    SendSetOffset(7, "client-1", 0, "session-id-1");
    SendCreateSession(8, "client-1", "session-id-2", 8, 9);
    SendSetOffset(9, "client-1", 0, "session-id-1");
    SendSetOffset(10, "client-1", 0, "session-id-2");
    SendCreateSession(11, "client-1", "session-id-3", 7, 10);

    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitProxyResponse({.Cookie=5});
    WaitProxyResponse({.Cookie=6});

    WaitCmdWrite({.Count=2, .UserInfos={
                 {0, {.Session = "session-id-2", .Offset=0, .Generation=8, .Step=9}},
                 }});

    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitProxyResponse({.Cookie=7, .Status=NMsgBusProxy::MSTATUS_OK});
    WaitProxyResponse({.Cookie=8, .Status=NMsgBusProxy::MSTATUS_OK});
    WaitErrorResponse({.Cookie=9, .ErrorCode=NPersQueue::NErrorCode::WRONG_COOKIE});
    WaitProxyResponse({.Cookie=10, .Status=NMsgBusProxy::MSTATUS_OK});
    WaitErrorResponse({.Cookie=11, .ErrorCode=NPersQueue::NErrorCode::WRONG_COOKIE});
}

Y_UNIT_TEST_F(SetOffset, TUserActionProcessorFixture)
{
    const ui32 partition = 0;
    const ui64 begin = 0;
    const ui64 end = 10;
    const TString client = "client";
    const TString session = "session";

    CreatePartition({.Partition=partition, .Begin=begin, .End=end});

    //
    // create session
    //
    CreateSession(client, session);

    //
    // regular commit (5 <= end)
    //
    SendSetOffset(1, client, 5, session);
    WaitCmdWrite({.Count=2, .UserInfos={{0, {.Session=session, .Offset=5}}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
    WaitProxyResponse({.Cookie=1, .Status=NMsgBusProxy::MSTATUS_OK});

    //
    // offset is 5
    //
    SendGetOffset(2, client);
    WaitProxyResponse({.Cookie=2, .Status=NMsgBusProxy::MSTATUS_OK, .Offset=5});

    //
    // commit to back (1 < 5)
    //
    SendSetOffset(3, client, 1, session);
    WaitCmdWrite({.Count=2, .UserInfos={{0, {.Session=session, .Offset=5}}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
    WaitProxyResponse({.Cookie=3, .Status=NMsgBusProxy::MSTATUS_OK});

    //
    // the offset has not changed
    //
    SendGetOffset(4, client);
    WaitProxyResponse({.Cookie=4, .Status=NMsgBusProxy::MSTATUS_OK, .Offset=5});

    //
    // commit to future (13 > end)
    //
    SendSetOffset(5, client, 13, session);
    WaitCmdWrite({.Count=2, .UserInfos={{0, {.Session=session, .Offset=end}}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
    WaitProxyResponse({.Cookie=5, .Status=NMsgBusProxy::MSTATUS_OK});
}

Y_UNIT_TEST_F(CommitOffsetRanges, TUserActionProcessorFixture)
{
    const ui32 partition = 0;
    const ui64 begin = 0;
    const ui64 end = 10;
    const TString client = "client";
    const TString session = "session";

    CreatePartition({.Partition=partition, .Begin=begin, .End=end});

    //
    // create session
    //
    CreateSession(client, session);

    SendProposeTransactionRequest(partition,
                                  0, 2,  // 0 --> 2
                                  client,
                                  "topic-path",
                                  true,
                                  1);
    WaitCmdWrite({.Count=2, .UserInfos={{0, {.Session="", .Offset=2}}}});

    SendProposeTransactionRequest(partition,
                                  2, 0,          // begin > end
                                  client,
                                  "topic-path",
                                  true,
                                  2);
    SendProposeTransactionRequest(partition,
                                  4, 6,          // begin > client.end
                                  client,
                                  "topic-path",
                                  true,
                                  3);
    SendProposeTransactionRequest(partition,
                                  1, 4,          // begin < client.end
                                  client,
                                  "topic-path",
                                  true,
                                  4);
    SendProposeTransactionRequest(partition,
                                  2, 4,          // begin == client.end
                                  client,
                                  "topic-path",
                                  true,
                                  5);
    SendProposeTransactionRequest(partition,
                                  4, 13,         // end > partition.end
                                  client,
                                  "topic-path",
                                  true,
                                  6);

    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
    WaitProposeTransactionResponse({.TxId=1, .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    WaitCmdWrite({.Count=2, .UserInfos={{0, {.Session="", .Offset=4}}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitProposeTransactionResponse({.TxId=2, .Status=NKikimrPQ::TEvProposeTransactionResult::BAD_REQUEST});
    WaitProposeTransactionResponse({.TxId=3, .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});
    WaitProposeTransactionResponse({.TxId=4, .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});
    WaitProposeTransactionResponse({.TxId=5, .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});
    WaitProposeTransactionResponse({.TxId=6, .Status=NKikimrPQ::TEvProposeTransactionResult::BAD_REQUEST});

    SendGetOffset(6, client);
    WaitProxyResponse({.Cookie=6, .Offset=4});
}

Y_UNIT_TEST_F(CorrectRange_Commit, TUserActionProcessorFixture)
{
    const ui32 partition = 3;
    const ui64 begin = 0;
    const ui64 end = 10;
    const TString client = "client";
    const TString session = "session";

    const ui64 step = 12345;
    const ui64 txId = 67890;

    CreatePartition({.Partition=partition, .Begin=begin, .End=end, .PlanStep=step, .TxId=10000});
    CreateSession(client, session);

    SendCalcPredicate(step, txId, client, 0, 2);
    WaitCalcPredicateResult({.Step=step, .TxId=txId, .Partition=partition, .Predicate=true});

    SendCommitTx(step, txId);

    WaitCmdWrite({.Count=3, .PlanStep=step, .TxId=txId, .UserInfos={{1, {.Session="", .Offset=2}}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitCommitTxDone({.TxId=txId, .Partition=partition});
}

Y_UNIT_TEST_F(CorrectRange_Multiple_Transactions, TUserActionProcessorFixture)
{
    const ui32 partition = 3;
    const ui64 begin = 0;
    const ui64 end = 10;
    const TString client = "client";
    const TString session = "session";

    const ui64 step = 12345;
    const ui64 txId_1 = 67890;
    const ui64 txId_2 = 67891;
    const ui64 txId_3 = 67892;

    CreatePartition({.Partition=partition, .Begin=begin, .End=end, .PlanStep=step, .TxId=10000});
    CreateSession(client, session);

    SendCalcPredicate(step, txId_1, client, 0, 1);
    WaitCalcPredicateResult({.Step=step, .TxId=txId_1, .Partition=partition, .Predicate=true});

    SendCalcPredicate(step, txId_2, client, 0, 2);
    SendCalcPredicate(step, txId_3, client, 0, 2);

    SendCommitTx(step, txId_1);

    WaitCalcPredicateResult({.Step=step, .TxId=txId_2, .Partition=partition, .Predicate=false});
    SendRollbackTx(step, txId_2);

    WaitCalcPredicateResult({.Step=step, .TxId=txId_3, .Partition=partition, .Predicate=false});
    SendRollbackTx(step, txId_3);

    WaitCmdWrite({.Count=3, .PlanStep=step, .TxId=txId_3, .UserInfos={{1, {.Session="", .Offset=1}}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitCommitTxDone({.TxId=txId_1, .Partition=partition});
}

Y_UNIT_TEST_F(CorrectRange_Multiple_Consumers, TUserActionProcessorFixture)
{
    const ui32 partition = 3;
    const ui64 begin = 0;
    const ui64 end = 10;

    const ui64 step = 12345;
    const ui64 txId = 67890;

    CreatePartition({.Partition=partition, .Begin=begin, .End=end});
    CreateSession("client-1", "session-1");
    CreateSession("client-2", "session-2");

    SendSetOffset(1, "client-1", 3, "session-1");
    SendCalcPredicate(step, txId, "client-2", 0, 1);
    SendSetOffset(2, "client-1", 6, "session-1");

    WaitCmdWrite({.Count=2, .UserInfos={{0, {.Session="session-1", .Offset=3}}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitProxyResponse({.Cookie=1, .Status=NMsgBusProxy::MSTATUS_OK});

    WaitCalcPredicateResult({.Step=step, .TxId=txId, .Partition=partition, .Predicate=true});
    SendCommitTx(step, txId);

    WaitCmdWrite({.Count=5, .UserInfos={
                 {1, {.Session="", .Offset=1}},
                 {3, {.Session="session-1", .Offset=6}}
                 }});
}

Y_UNIT_TEST_F(OldPlanStep, TUserActionProcessorFixture)
{
    const ui32 partition = 3;
    const ui64 begin = 0;
    const ui64 end = 10;

    const ui64 step = 12345;
    const ui64 txId = 67890;

    CreatePartition({.Partition=partition, .Begin=begin, .End=end, .PlanStep=99999, .TxId=55555});

    SendCommitTx(step, txId);
    WaitCommitTxDone({.TxId=txId, .Partition=partition});
}

Y_UNIT_TEST_F(AfterRestart_1, TUserActionProcessorFixture)
{
    const ui32 partition = 3;
    const ui64 begin = 0;
    const ui64 end = 10;
    const TString consumer = "client";
    const TString session = "session";

    const ui64 step = 12345;

    TVector<TTransaction> txs;
    txs.push_back(MakeTransaction(step, 11111, consumer, 0, 2, true));
    txs.push_back(MakeTransaction(step, 22222, consumer, 2, 4));

    CreatePartition({.Partition=partition,
                    .Begin=begin,
                    .End=end,
                    .PlanStep=step, .TxId=10000,
                    .Transactions=std::move(txs)},
                    {{.Consumer=consumer, .Offset=0, .Session=session}});

    SendCommitTx(step, 11111);

    WaitCalcPredicateResult({.Step=step, .TxId=22222, .Partition=partition, .Predicate=true});
    SendCommitTx(step, 22222);

    WaitCmdWrite({.Count=3, .PlanStep=step, .TxId=22222, .UserInfos={{1, {.Session="", .Offset=4}}}});
}

Y_UNIT_TEST_F(AfterRestart_2, TUserActionProcessorFixture)
{
    const ui32 partition = 3;
    const ui64 begin = 0;
    const ui64 end = 10;
    const TString consumer = "client";
    const TString session = "session";

    const ui64 step = 12345;

    TVector<TTransaction> txs;
    txs.push_back(MakeTransaction(step, 11111, consumer, 0, 2));
    txs.push_back(MakeTransaction(step, 22222, consumer, 2, 4));

    CreatePartition({.Partition=partition,
                    .Begin=begin,
                    .End=end,
                    .PlanStep=step, .TxId=10000,
                    .Transactions=std::move(txs)},
                    {{.Consumer=consumer, .Offset=0, .Session=session}});

    WaitCalcPredicateResult({.Step=step, .TxId=11111, .Partition=partition, .Predicate=true});
}

Y_UNIT_TEST_F(IncorrectRange, TUserActionProcessorFixture)
{
    const ui32 partition = 3;
    const ui64 begin = 0;
    const ui64 end = 10;
    const TString client = "client";
    const TString session = "session";

    const ui64 step = 12345;
    ui64 txId = 67890;

    CreatePartition({.Partition=partition, .Begin=begin, .End=end});
    CreateSession(client, session);

    SendCalcPredicate(step, txId, client, 4, 2);
    WaitCalcPredicateResult({.Step=step, .TxId=txId, .Partition=partition, .Predicate=false});
    SendRollbackTx(step, txId);

    WaitCmdWrite({.Count=1, .PlanStep=step, .TxId=txId});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    ++txId;

    SendCalcPredicate(step, txId, client, 2, 4);
    WaitCalcPredicateResult({.Step=step, .TxId=txId, .Partition=partition, .Predicate=false});
    SendRollbackTx(step, txId);

    WaitCmdWrite({.Count=1, .PlanStep=step, .TxId=txId});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    ++txId;

    SendCalcPredicate(step, txId, client, 0, 11);
    WaitCalcPredicateResult({.Step=step, .TxId=txId, .Partition=partition, .Predicate=false});
}

Y_UNIT_TEST_F(CorrectRange_Rollback, TUserActionProcessorFixture)
{
    const ui32 partition = 3;
    const ui64 begin = 0;
    const ui64 end = 10;
    const TString client = "client";
    const TString session = "session";

    const ui64 step = 12345;
    const ui64 txId_1 = 67890;
    const ui64 txId_2 = 67891;

    CreatePartition({.Partition=partition, .Begin=begin, .End=end});
    CreateSession(client, session);

    SendCalcPredicate(step, txId_1, client, 0, 2);
    WaitCalcPredicateResult({.Step=step, .TxId=txId_1, .Partition=partition, .Predicate=true});

    SendCalcPredicate(step, txId_2, client, 0, 5);
    SendRollbackTx(step, txId_1);

    WaitCalcPredicateResult({.Step=step, .TxId=txId_2, .Partition=partition, .Predicate=true});
}

Y_UNIT_TEST_F(ChangeConfig, TUserActionProcessorFixture)
{
    const ui32 partition = 3;
    const ui64 begin = 0;
    const ui64 end = 10;
    const TString client = "client";
    const TString session = "session";

    const ui64 step = 12345;
    const ui64 txId_1 = 67890;
    const ui64 txId_2 = 67891;

    CreatePartition({.Partition=partition, .Begin=begin, .End=end}, {
                    {.Consumer="client-1", .Offset=0, .Session="session-1"},
                    {.Consumer="client-2", .Offset=0, .Session="session-2"},
                    {.Consumer="client-3", .Offset=0, .Session="session-3"}
                    });

    SendCalcPredicate(step, txId_1, "client-1", 0, 2);
    SendChangePartitionConfig({{.Consumer="client-1", .Generation=0},
                              { .Consumer="client-3", .Generation=7}});
    //
    // consumer 'client-2' will be deleted
    //
    SendCalcPredicate(step, txId_2, "client-2", 0, 2);

    WaitCalcPredicateResult({.Step=step, .TxId=txId_1, .Partition=partition, .Predicate=true});
    SendCommitTx(step, txId_1);

    //
    // consumer 'client-2' was deleted
    //
    WaitCalcPredicateResult({.Step=step, .TxId=txId_2, .Partition=partition, .Predicate=false});
    SendRollbackTx(step, txId_2);

    WaitCmdWrite({.Count=7,
                 .PlanStep=step, .TxId=txId_2,
                 .UserInfos={
                 {1, {.Consumer="client-1", .Session="", .Offset=2}},
                 {3, {.Consumer="client-3", .Session="", .Offset=0, .ReadRuleGeneration=7}}
                 },
                 .DeleteRanges={
                 {0, {.Consumer="client-2"}}
                 }});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitPartitionConfigChanged({.Partition=partition});
}

Y_UNIT_TEST_F(EvProposeTransaction, TUserActionProcessorFixture)
{
    PQTabletPrepare({.partitions=1}, {}, *Ctx);

    const ui64 txId_1 = 67890;
    const ui64 txId_2 = 67891;
    const ui64 txId_3 = 67892;
    const ui64 tablet2 = 22222;

    SendProposeTransactionRequest({.TxId=txId_1,
                                  .Senders={tablet2}, .Receivers={tablet2},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  {.Partition=0, .Consumer="consumer", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    SendProposeTransactionRequest({.TxId=txId_2,
                                  .Senders={tablet2}, .Receivers={tablet2},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  {.Partition=0, .Consumer="consumer", .Begin=0, .End=0, .Path="/topic"},
                                  }});
    SendProposeTransactionRequest({.TxId=txId_3,
                                  .Senders={tablet2}, .Receivers={tablet2},
                                  .TxOps={
                                  {.Partition=0, .Consumer="user", .Begin=0, .End=0, .Path="/topic"},
                                  {.Partition=0, .Consumer="consumer", .Begin=0, .End=0, .Path="/topic"},
                                  }});

    //
    // TODO(abcdef): проверить, что в команде CmdWrite есть информация только о txId_1
    //

    WaitProposeTransactionResponse({.TxId=txId_1,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

    //
    // TODO(abcdef): проверить, что в команде CmdWrite есть информация о txId_2 и txId_3
    //

    WaitProposeTransactionResponse({.TxId=txId_2,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});
    WaitProposeTransactionResponse({.TxId=txId_3,
                                   .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});
}

#if 0
Y_UNIT_TEST_F(Distributed_Tx_Commit, TUserActionProcessorFixture)
{
    TTestContext tc;
    auto setupEventFilter = [&tc]() {
        return tc.InitialEventsFilter.Prepare();
    };
    auto testFunction = [&](const TString& dispatchName, std::function<void (TTestActorRuntime&)> setup, bool& activeZone) {
        TFinalizer finalizer(tc);
        tc.Prepare(dispatchName, setup, activeZone, true);
        tc.Runtime->SetScheduledLimit(1000);

        PQTabletPrepare({.partitions=1}, {}, tc);

        const ui64 step = 12345;
        const ui64 txId = 67890;
        const ui32 partition = 0;
        const TString consumer = "user";
        const ui64 tablet_1 = tc.TabletId;
        const ui64 tablet_2 = tc.TabletId + 1;

        //
        // TEvProposeTransaction от KqpDataExecuter
        //

        //
        // список получателей
        //
        SendProposeTransactionRequest({.TxId=txId,
                                      .Senders={tablet_1, tablet_2}, .Receivers={tablet_1, tablet_2},
                                      .Partition=0, .Consumer=consumer, .Begin=0, .End=2});
        WaitCmdWrite({.TxId=txId,
                     .State=PREPARED,
                     .Senders={tablet_1, tablet_2}, .Receivers={tablet_1, tablet_2},
                     .Partition=0, .Consumer="user", .Begin=0, .End=2});
        SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
        WaitProposeTransactionResponse({.TxId=txId, .Status=NKikimrPQ::TEvProposeTransactionResult::PREPARED});

        //
        // TEvMediatorPlanStep от медиатора
        //
        SendMediatorPlanStep({.Step=step, .TxIds={txId}});
        WaitCmdWrite({.Step=step, .TxId=txId, .State=PLANNED});
        SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
        WaitPlanStepAck({.Step=step, .TxIds={txIds}}); // TEvPlanStepAck для координатора
        WaitPlanStepAccepted({.Step=step});

        //
        // TEvTxCalcPredicate для партиции
        //

        //
        // работу с партицией не перехватывать
        //
        //WaitCalcPredicateRequest({.Step=step, .TxId=txId, .Consumer=consumer, .Begin=0, .End=2});
        //SendCalcPredicateResult({.Step=step, .TxId=txId, .Partition=partition, .Predicate=true});
        WaitCmdWrite({.TxId=txId, .State=EXECUTING, .Partition=partition, .Predicate=true});
        SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

        //
        // TEvReadSet другой таблетке
        //
        ui64 seqNo;
        WaitReadSet({.Src=tablet_1, .Dst=tablet_2, .Step=step, .TxId=txId, .ReadSet={.Predicate=true}}, seqNo);

        //
        // TEvReadSet от другой таблетки
        //
        SendReadSet({.Src=tablet_2, .Dst=tablet_1, .Step=step, .TxId=txId, .ReadSet={.Predicate=true}, .SeqNo=11111});
        //WaitCmdWrite();
        //SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
        //
        // перенести после commit
        //
        WaitReadSetAck({.Src=tablet_1, .Dst=tablet_2, .Step=step, .TxId=txId, .SeqNo=11111});

        //
        // TEvReadSetAck от другой таблетки
        //
        //WaitCmdWrite();
        //SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

        //
        // TEvTxCommit для партиции
        //
        WaitCommitTx({.Step=step, .TxId=txId, .Consumer=consumer, .Begin=0, .End=2});
        SendCommitTxDone({.Step=step, .TxId=txId, .Partition=0});

        WaitCmdWrite({.TxId=txId, .State=EXECUTED});
        // удалить транзакцию из шага
        SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

        //
        // TEvProposeTransactionResult для KqpDataExecuter
        //
        WaitProposeTransactionResponse({.TxId=txId, .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

        SendReadSetAck({.Src=tablet_2, .Dst=tablet_1, .Step=step, .TxId=txId, .SeqNo=seqNo});
        //
        // после сщ
        WaitCmdDeleteRanges(...);
    };
}
#endif

} // Y_UNIT_TEST_SUITE(TUserActionProcessorTests)

} // namespace NKikimr::NPQ
