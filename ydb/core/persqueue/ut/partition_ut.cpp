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

#include "make_config.h"

namespace NKikimr::NPQ {

namespace NHelpers {

struct TConfigParams {
    ui64 Version = 0;
    TVector<TCreateConsumerParams> Consumers;
    NKikimrPQ::TPQTabletConfig::EMeteringMode MeteringMode = NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS;
};

struct TCreatePartitionParams {
    TPartitionId Partition = TPartitionId{1};
    ui64 Begin = 0;
    ui64 End = 0;
    TMaybe<ui64> PlanStep;
    TMaybe<ui64> TxId;
    TVector<TTransaction> Transactions;
    TConfigParams Config;
};

}

class TFakePartitionActor : public TActor<TFakePartitionActor> {
    STFUNC(StateFunc) {
        Y_UNUSED(ev);
    }

public:
    TFakePartitionActor()
        : TActor(&TThis::StateFunc)
    {}
};

class TPartitionTestWrapper {
public:
    TPartitionTestWrapper(TInitMetaStep* metaStep)
        : MetaStep(metaStep)
    {}

    void LoadMeta(const NKikimrPQ::TPartitionCounterData& data);
private:
    TInitMetaStep* MetaStep;
};

void TPartitionTestWrapper::LoadMeta(const NKikimrPQ::TPartitionCounterData& counters)
{
    NKikimrClient::TResponse kvResponse;
    TString strMeta;

    auto* readResult = kvResponse.AddReadResult();
    readResult->SetStatus(NKikimrProto::OK);
    NKikimrPQ::TPartitionMeta meta;
    meta.MutableCounterData()->CopyFrom(counters);
    auto ok = meta.SerializeToString(&strMeta);
    UNIT_ASSERT(ok);
    readResult->SetValue(strMeta);
    auto* txRead = kvResponse.AddReadResult(); // Empty TxMeta
    txRead->SetStatus(NKikimrProto::OK);
    NKikimrPQ::TPartitionTxMeta txMeta;

    strMeta.clear();
    ok = txMeta.SerializeToString(&strMeta);
    UNIT_ASSERT(ok);

    txRead->SetValue(strMeta);
    MetaStep->LoadMeta(kvResponse, Nothing());

    UNIT_ASSERT_VALUES_EQUAL(counters.GetMessagesWrittenTotal(), MetaStep->Partition()->MsgsWrittenTotal.Value());
    UNIT_ASSERT_VALUES_EQUAL(counters.GetMessagesWrittenGrpc(), MetaStep->Partition()->MsgsWrittenGrpc.Value());
    UNIT_ASSERT_VALUES_EQUAL(counters.GetBytesWrittenTotal(), MetaStep->Partition()->BytesWrittenTotal.Value());
    UNIT_ASSERT_VALUES_EQUAL(counters.GetBytesWrittenGrpc(), MetaStep->Partition()->BytesWrittenGrpc.Value());
    UNIT_ASSERT_VALUES_EQUAL(counters.GetBytesWrittenUncompressed(), MetaStep->Partition()->BytesWrittenUncompressed.Value());

#define CMP_HISTOGRAM(ProtoField)                                               \
    UNIT_ASSERT_VALUES_EQUAL(actual.size(), counters.ProtoField##Size());       \
    for (ui64 i = 0; i < actual.size(); i++) {                                  \
        UNIT_ASSERT_VALUES_EQUAL_C(actual[i], counters.Get##ProtoField(i), i);  \
    }

    auto actual = MetaStep->Partition()->MessageSize.GetValues();
    CMP_HISTOGRAM(MessagesSizes);

}


Y_UNIT_TEST_SUITE(TPartitionTests) {
using TSrcIdMap = THashMap<TString, std::pair<ui64, ui64>>;


class TPartitionFixture : public NUnitTest::TBaseFixture {
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
        TMaybe<char> TypeInfo;
        TMaybe<ui32> Partition;
        TMaybe<char> Mark;
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
        TMaybe<TPartitionId> Partition;
        TMaybe<bool> Predicate;
        bool Ok = true;
        static TCalcPredicateMatcher EmptyMatcher() {
            TCalcPredicateMatcher ret;
            return ret;
        }
    };

    struct TCommitTxDoneMatcher {
        TMaybe<ui64> Step;
        TMaybe<ui64> TxId;
        TMaybe<TPartitionId> Partition;
    };

    struct TChangePartitionConfigMatcher {
        TMaybe<TPartitionId> Partition;
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

    using TCreateConsumerParams = NHelpers::TCreateConsumerParams;
    using TCreatePartitionParams = NHelpers::TCreatePartitionParams;
    using TConfigParams = NHelpers::TConfigParams;

    void SetUp(NUnitTest::TTestContext&) override;
    void TearDown(NUnitTest::TTestContext&) override;

    TPartition* CreatePartitionActor(const TPartitionId& partition,
                              const TConfigParams& config,
                              bool newPartition,
                              TVector<TTransaction> txs);
    TPartition* CreatePartition(const TCreatePartitionParams& params = {},
                         const TConfigParams& config = {});

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

    void WaitConfigRequest();
    void SendConfigResponse(const TConfigParams& config);
    void WaitDiskStatusRequest();
    void SendDiskStatusResponse(TMaybe<ui64>* cookie = nullptr);
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
    void WaitProposeTransactionResponse(const TProposeTransactionResponseMatcher& matcher = {});

    void SendCalcPredicate(ui64 step,
                           ui64 txId,
                           const TString& consumer,
                           ui64 begin,
                           ui64 end,
                           const TActorId& suppPartitionId = {});
    void WaitCalcPredicateResult(const TCalcPredicateMatcher& matcher = TCalcPredicateMatcher::EmptyMatcher());

    void SendCommitTx(ui64 step, ui64 txId);
    void SendRollbackTx(ui64 step, ui64 txId);
    void WaitCommitTxDone(const TCommitTxDoneMatcher& matcher = {});

    void SendChangePartitionConfig(const TConfigParams& config = {});
    void WaitPartitionConfigChanged(const TChangePartitionConfigMatcher& matcher = {});

    TTransaction MakeTransaction(ui64 step, ui64 txId,
                                 TString consumer,
                                 ui64 begin, ui64 end,
                                 TMaybe<bool> predicate = Nothing());

    void SendSubDomainStatus(bool subDomainOutOfSpace = false);
    void SendReserveBytes(const ui64 cookie, const ui32 size, const TString& ownerCookie, const ui64 messageNo, bool lastRequest = false);
    void SendChangeOwner(const ui64 cookie, const TString& owner, const TActorId& pipeClient, const bool force = true);
    void SendWrite(const ui64 cookie, const ui64 messageNo, const TString& ownerCookie, const TMaybe<ui64> offset, const TString& data,
                   bool ignoreQuotaDeadline = false, ui64 seqNo = 0);
    void SendGetWriteInfo();
    void ShadowPartitionCountersTest(bool isFirstClass);

    void TestWriteSubDomainOutOfSpace(TDuration quotaWaitDuration, bool ignoreQuotaDeadline);
    void WaitKeyValueRequest(TMaybe<ui64>& cookie);

    void CmdChangeOwner(ui64 cookie, const TString& sourceId, TDuration duration, TString& ownerCookie);

    void EmulateKVTablet();
    TActorId CreateFakePartition() const;
    bool WaitWriteInfoRequest(const TActorId& supportivePart);
    void SendEvent(IEventBase* event);
    void SendEvent(IEventBase* event, const TActorId& from, const TActorId& to);

    TMaybe<TTestContext> Ctx;
    TMaybe<TFinalizer> Finalizer;

    TActorId ActorId;

    NPersQueue::TTopicConverterPtr TopicConverter;
    NKikimrPQ::TPQTabletConfig Config;

    TAutoPtr<TTabletCountersBase> TabletCounters;

};

void TPartitionFixture::SetUp(NUnitTest::TTestContext&)
{
    Ctx.ConstructInPlace();
    Finalizer.ConstructInPlace(*Ctx);

    Ctx->Prepare();
    Ctx->Runtime->SetScheduledLimit(5'000);
}

TActorId TPartitionFixture::CreateFakePartition() const {
    return Ctx->Runtime->Register(new TFakePartitionActor());
}

void TPartitionFixture::TearDown(NUnitTest::TTestContext&)
{
}

TPartition* TPartitionFixture::CreatePartitionActor(const TPartitionId& id,
                                             const TConfigParams& config,
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
    TabletCounters = counters->GetSecondTabletCounters().Release();

    Config = MakeConfig(config.Version,
                        config.Consumers,
                        1,
                        config.MeteringMode);
    Config.SetLocalDC(true);

    NPersQueue::TTopicNamesConverterFactory factory(true, "/Root/PQ", "dc1");
    TopicConverter = factory.MakeTopicConverter(Config);
    TActorId quoterId;
    if (Ctx->Runtime->GetAppData(0).PQConfig.GetQuotingConfig().GetEnableQuoting()) {
        quoterId = Ctx->Runtime->Register(new TWriteQuoter(
                TopicConverter,
                Config,
                Ctx->Runtime->GetAppData().PQConfig,
                id,
                Ctx->Edge,
                Ctx->TabletId,
                Config.GetLocalDC(),
                *TabletCounters
        ));
    }
    auto* actor = new NPQ::TPartition(Ctx->TabletId,
                                     id,
                                     Ctx->Edge,
                                     0,
                                     Ctx->Edge,
                                     TopicConverter,
                                     "dcId",
                                     false,
                                     Config,
                                     *TabletCounters,
                                     false,
                                     1,
                                     quoterId,
                                     newPartition,
                                     std::move(txs));
    ActorId = Ctx->Runtime->Register(actor);
    return actor;
}

TPartition* TPartitionFixture::CreatePartition(const TCreatePartitionParams& params,
                                        const TConfigParams& config)
{
    TPartition* ret;
    if ((params.Begin == 0) && (params.End == 0)) {
        ret = CreatePartitionActor(params.Partition, config, true, {});

        WaitConfigRequest();
        SendConfigResponse(params.Config);
    } else {
        TVector<TTransaction> copyTx;
        for (const auto& origTx : params.Transactions) {
            copyTx.emplace_back(origTx.Tx, origTx.Predicate);
            copyTx.back().ChangeConfig = origTx.ChangeConfig;
            copyTx.back().SendReply = origTx.SendReply;
            copyTx.back().ProposeConfig = origTx.ProposeConfig;
        }
        ret = CreatePartitionActor(params.Partition, config, false, std::move(copyTx));

        WaitConfigRequest();
        SendConfigResponse(params.Config);

        WaitDiskStatusRequest();
        SendDiskStatusResponse();

        WaitMetaReadRequest();
        SendMetaReadResponse(params.PlanStep, params.TxId);

        WaitInfoRangeRequest();
        SendInfoRangeResponse(params.Partition.InternalPartitionId, params.Config.Consumers);

        WaitDataRangeRequest();
        SendDataRangeResponse(params.Begin, params.End);
    }
    return ret;
}

void TPartitionFixture::CreateSession(const TString& clientId,
                                      const TString& sessionId,
                                      ui32 generation, ui32 step,
                                      ui64 cookie)
{
    SendCreateSession(cookie,clientId,sessionId, generation, step);
    WaitCmdWrite({.Count=2, .UserInfos={{0, {.Session = sessionId, .Offset = 0}}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
    WaitProxyResponse({.Cookie = cookie});
}

void TPartitionFixture::SetOffset(const TString& clientId,
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

void TPartitionFixture::SendEvent(IEventBase* event) {
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event));
}

void TPartitionFixture::SendEvent(IEventBase* event, const TActorId& from, const TActorId& to) {
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(to, from, event));
}

void TPartitionFixture::SendCreateSession(ui64 cookie,
                                          const TString& clientId,
                                          const TString& sessionId,
                                          ui32 generation,
                                          ui32 step)
{
    auto event = MakeHolder<TEvPQ::TEvSetClientInfo>(cookie,
                                                     clientId,
                                                     0,
                                                     sessionId,
                                                     0,
                                                     generation,
                                                     step,
                                                     TActorId{},
                                                     TEvPQ::TEvSetClientInfo::ESCI_CREATE_SESSION);
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::SendSetOffset(ui64 cookie,
                                      const TString& clientId,
                                      ui64 offset,
                                      const TString& sessionId)
{
    auto event = MakeHolder<TEvPQ::TEvSetClientInfo>(cookie,
                                                     clientId,
                                                     offset,
                                                     sessionId,
                                                     0,
                                                     0,
                                                     0,
                                                     TActorId{});
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::SendGetOffset(ui64 cookie,
                                      const TString& clientId)
{
    auto event = MakeHolder<TEvPQ::TEvGetClientOffset>(cookie,
                                                       clientId);
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::WaitCmdWrite(const TCmdWriteMatcher& matcher)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>();
    UNIT_ASSERT(event != nullptr);
    Cerr << "Got cmd write: \n" << event->Record.DebugString() << Endl;
    for (unsigned i = 0; i < event->Record.CmdWriteSize(); ++i) {
        auto& cmd = event->Record.GetCmdWrite(i);
        TString key = cmd.GetKey();

        UNIT_ASSERT(key.size() >= 1);
        switch (key[0]) {
        case TKeyPrefix::TypeTxMeta: {
            NKikimrPQ::TPartitionTxMeta meta;
            UNIT_ASSERT(meta.ParseFromString(event->Record.GetCmdWrite(i).GetValue()));
            if (matcher.PlanStep.Defined()) {
                UNIT_ASSERT_VALUES_EQUAL(*matcher.PlanStep, meta.GetPlanStep());
            }
            if (matcher.TxId.Defined()) {
                UNIT_ASSERT_VALUES_EQUAL(*matcher.TxId, meta.GetTxId());
            }
            break;
        }
        case TKeyPrefix::TypeInfo: {
            UNIT_ASSERT(key.size() >= (1 + 10 + 1)); // type + partition + mark
            if (key[11] != TKeyPrefix::MarkUser) {
                break;
            }

            NKikimrPQ::TUserInfo ud;
            UNIT_ASSERT(ud.ParseFromString(event->Record.GetCmdWrite(i).GetValue()));

            bool match = false;
            for (auto& [_, userInfo] : matcher.UserInfos) {
                if (userInfo.Session && ud.HasSession()) {
                    if (*userInfo.Session != ud.GetSession()) {
                        continue;
                    }

                    match = true;

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

                if (match) {
                    break;
                }
            }

            UNIT_ASSERT(match);

            break;
        }
        }
    }

    //
    // CmdDeleteRange
    //
    for (auto& [index, deleteRange] : matcher.DeleteRanges) {
        UNIT_ASSERT(index < event->Record.CmdDeleteRangeSize());
        UNIT_ASSERT(event->Record.GetCmdDeleteRange(index).HasRange());

        auto& range = event->Record.GetCmdDeleteRange(index).GetRange();
        TString key = range.GetFrom();
        UNIT_ASSERT(key.Size() > (1 + 10 + 1)); // type + partition + mark + consumer

        if (deleteRange.Partition.Defined()) {
            auto partition = FromString<ui32>(key.substr(1, 10));
            UNIT_ASSERT_VALUES_EQUAL(*deleteRange.Partition, partition);
        }
        if (deleteRange.Consumer.Defined()) {
            TString consumer = key.substr(12);
            UNIT_ASSERT_VALUES_EQUAL(*deleteRange.Consumer, consumer);
        }
    }
}

void TPartitionFixture::WaitCmdWriteTx(const TCmdWriteTxMatcher& matcher)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>();
    UNIT_ASSERT(event != nullptr);

    UNIT_ASSERT_VALUES_EQUAL(event->Record.GetCookie(), 5);  // WRITE_TX_PREPARED_COOKIE

    UNIT_ASSERT_VALUES_EQUAL(event->Record.CmdGetStatusSize(), 1 + matcher.TxOps.size());
}

void TPartitionFixture::SendCmdWriteResponse(NMsgBusProxy::EResponseStatus status)
{
    auto event = MakeHolder<TEvKeyValue::TEvResponse>();
    event->Record.SetStatus(status);

    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::SendSubDomainStatus(bool subDomainOutOfSpace)
{
    auto event = MakeHolder<TEvPQ::TEvSubDomainStatus>();
    event->Record.SetSubDomainOutOfSpace(subDomainOutOfSpace);

    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::SendReserveBytes(const ui64 cookie, const ui32 size, const TString& ownerCookie, const ui64 messageNo, bool lastRequest)
{
    auto event = MakeHolder<TEvPQ::TEvReserveBytes>(cookie, size, ownerCookie, messageNo, lastRequest);
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::SendWrite
        (const ui64 cookie, const ui64 messageNo, const TString& ownerCookie, const TMaybe<ui64> offset, const TString& data,
        bool ignoreQuotaDeadline, ui64 seqNo
) {
    TEvPQ::TEvWrite::TMsg msg;
    msg.SourceId = "SourceId";
    msg.SeqNo = seqNo ? seqNo : messageNo;
    msg.PartNo = 0;
    msg.TotalParts = 1;
    msg.TotalSize = data.size();
    msg.CreateTimestamp = TMonotonic::Now().Seconds();
    msg.WriteTimestamp = TMonotonic::Now().Seconds();
    msg.ReceiveTimestamp = TMonotonic::Now().Seconds();
    msg.DisableDeduplication = false;
    msg.Data = data;
    msg.UncompressedSize = data.size();
    msg.PartitionKey = "PartitionKey";
    msg.ExplicitHashKey = "ExplicitHashKey";
    msg.External = false;
    msg.IgnoreQuotaDeadline = ignoreQuotaDeadline;

    TVector<TEvPQ::TEvWrite::TMsg> msgs;
    msgs.push_back(msg);

    auto event = MakeHolder<TEvPQ::TEvWrite>(cookie, messageNo, ownerCookie, offset, std::move(msgs), false, std::nullopt);
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::SendChangeOwner(const ui64 cookie, const TString& owner, const TActorId& pipeClient, const bool force)
{
    auto event = MakeHolder<TEvPQ::TEvChangeOwner>(cookie, owner, pipeClient, Ctx->Edge, force, true);
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::SendGetWriteInfo() {
    auto event = MakeHolder<TEvPQ::TEvGetWriteInfoRequest>();
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::WaitProxyResponse(const TProxyResponseMatcher& matcher)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvProxyResponse>();
    UNIT_ASSERT(event != nullptr);

    if (matcher.Cookie) {
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Cookie, event->Cookie);
    }

    if (matcher.Status) {
        UNIT_ASSERT(event->Response->HasStatus());
        UNIT_ASSERT(*matcher.Status == event->Response->GetStatus());
    }

    if (matcher.ErrorCode) {
        UNIT_ASSERT(event->Response->HasErrorCode());
        UNIT_ASSERT(*matcher.ErrorCode == event->Response->GetErrorCode());
    }

    if (matcher.Offset) {
        UNIT_ASSERT(event->Response->HasPartitionResponse());
        UNIT_ASSERT(event->Response->GetPartitionResponse().HasCmdGetClientOffsetResult());
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Offset, event->Response->GetPartitionResponse().GetCmdGetClientOffsetResult().GetOffset());
    }
}

void TPartitionFixture::WaitErrorResponse(const TErrorMatcher& matcher)
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

void TPartitionFixture::WaitConfigRequest()
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>();
    UNIT_ASSERT(event != nullptr);

    UNIT_ASSERT_VALUES_EQUAL(event->Record.CmdReadSize(), 1);
}

void TPartitionFixture::SendConfigResponse(const TConfigParams& config)
{
    auto event = MakeHolder<TEvKeyValue::TEvResponse>();
    event->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);

    auto read = event->Record.AddReadResult();
    if (config.Consumers.empty()) {
        read->SetStatus(NKikimrProto::NODATA);
    } else {
        read->SetStatus(NKikimrProto::OK);

        TString out;
        Y_ABORT_UNLESS(MakeConfig(config.Version,
                            config.Consumers,
                            1,
                            config.MeteringMode).SerializeToString(&out));

        read->SetValue(out);
    }

    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::WaitDiskStatusRequest()
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>();
    UNIT_ASSERT(event != nullptr);

    UNIT_ASSERT(event->Record.CmdGetStatusSize() > 0);
}

void TPartitionFixture::SendDiskStatusResponse(TMaybe<ui64>* cookie)
{
    auto event = MakeHolder<TEvKeyValue::TEvResponse>();
    if (cookie && cookie->Defined()) {
        event->Record.SetCookie(cookie->GetRef());
    }
    event->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);

    auto result = event->Record.AddGetStatusResult();
    result->SetStatus(NKikimrProto::OK);
    result->SetStatusFlags(NKikimrBlobStorage::StatusIsValid);

    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::WaitMetaReadRequest()
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>();
    UNIT_ASSERT(event != nullptr);

    UNIT_ASSERT_VALUES_EQUAL(event->Record.CmdReadSize(), 2);
}

void TPartitionFixture::SendMetaReadResponse(TMaybe<ui64> step, TMaybe<ui64> txId)
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

void TPartitionFixture::WaitInfoRangeRequest()
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>();
    UNIT_ASSERT(event != nullptr);

    UNIT_ASSERT_VALUES_EQUAL(event->Record.CmdReadRangeSize(), 1);
}

void TPartitionFixture::SendInfoRangeResponse(ui32 partition,
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

            NPQ::TKeyPrefix key(NPQ::TKeyPrefix::TypeInfo, TPartitionId(partition), NPQ::TKeyPrefix::MarkUser);
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

void TPartitionFixture::WaitDataRangeRequest()
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>();
    UNIT_ASSERT(event != nullptr);

    UNIT_ASSERT_VALUES_EQUAL(event->Record.CmdReadRangeSize(), 1);
}

void TPartitionFixture::SendDataRangeResponse(ui64 begin, ui64 end)
{
    Y_ABORT_UNLESS(begin <= end);

    auto event = MakeHolder<TEvKeyValue::TEvResponse>();
    event->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);

    auto read = event->Record.AddReadRangeResult();
    read->SetStatus(NKikimrProto::OK);
    auto pair = read->AddPair();
    NPQ::TKey key(NPQ::TKeyPrefix::TypeData, TPartitionId(1), begin, 0, end - begin, 0);
    pair->SetStatus(NKikimrProto::OK);
    pair->SetKey(key.ToString());
    //pair->SetValueSize();
    pair->SetCreationUnixTime(0);

    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::SendProposeTransactionRequest(ui32 partition,
                                                      ui64 begin, ui64 end,
                                                      const TString& client,
                                                      const TString& topic,
                                                      bool immediate,
                                                      ui64 txId)
{
    auto event = MakeHolder<TEvPersQueue::TEvProposeTransaction>();

    ActorIdToProto(Ctx->Edge, event->Record.MutableSourceActor());
    auto* body = event->Record.MutableData();
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

void TPartitionFixture::WaitProposeTransactionResponse(const TProposeTransactionResponseMatcher& matcher)
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

void TPartitionFixture::SendCalcPredicate(ui64 step,
                                          ui64 txId,
                                          const TString& consumer,
                                          ui64 begin,
                                          ui64 end,
                                          const TActorId& suppPartitionId)
{
    auto event = MakeHolder<TEvPQ::TEvTxCalcPredicate>(step, txId);
    if (suppPartitionId) {
        event->SupportivePartitionActor = suppPartitionId;
    } else {
        event->AddOperation(consumer, begin, end);
    }

    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::WaitCalcPredicateResult(const TCalcPredicateMatcher& matcher)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvTxCalcPredicateResult>(TDuration::Seconds(1));
    if (matcher.Ok) {
        UNIT_ASSERT(event != nullptr);
    } else {
        UNIT_ASSERT(event == nullptr);
        return;
    }

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

void TPartitionFixture::SendCommitTx(ui64 step, ui64 txId)
{
    auto event = MakeHolder<TEvPQ::TEvTxCommit>(step, txId);
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::SendRollbackTx(ui64 step, ui64 txId)
{
    auto event = MakeHolder<TEvPQ::TEvTxRollback>(step, txId);
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::WaitCommitTxDone(const TCommitTxDoneMatcher& matcher)
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

void TPartitionFixture::SendChangePartitionConfig(const TConfigParams& config)
{
    auto event = MakeHolder<TEvPQ::TEvChangePartitionConfig>(TopicConverter, MakeConfig(config.Version,
                                                                                        config.Consumers,
                                                                                        1,
                                                                                        config.MeteringMode));
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::WaitPartitionConfigChanged(const TChangePartitionConfigMatcher& matcher)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvPartitionConfigChanged>();
    UNIT_ASSERT(event != nullptr);

    if (matcher.Partition) {
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Partition, event->Partition);
    }
}

TTransaction TPartitionFixture::MakeTransaction(ui64 step, ui64 txId,
                                                TString consumer,
                                                ui64 begin, ui64 end,
                                                TMaybe<bool> predicate)
{
    auto event = MakeSimpleShared<TEvPQ::TEvTxCalcPredicate>(step, txId);
    event->AddOperation(std::move(consumer), begin, end);

    return TTransaction(event, predicate);
}

template<class TIterable>
void CompareVectors(const TVector<ui64>& expected, const TIterable& actual) {
    auto i = 0u;
    for (auto val : actual) {
        if (i < expected.size()) {
            UNIT_ASSERT_VALUES_EQUAL_C(expected[i], val, i);
            i++;
        } else {
            UNIT_ASSERT_VALUES_EQUAL_C(val, 0, "Mismatch on " << i);
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(i, expected.size());
}

void TPartitionFixture::ShadowPartitionCountersTest(bool isFirstClass) {
    const TPartitionId partition{0, TWriteId{0, 1111}, 123};
    const ui64 begin = 0;
    const ui64 end = 10;
    const TString session = "session";
    Ctx->Runtime->GetAppData().PQConfig.MutableQuotingConfig()->SetEnableQuoting(true);
    Ctx->Runtime->GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(isFirstClass);

    CreatePartition({.Partition=partition, .Begin=begin, .End=end});

    ui64 cookie = 1;

    SendChangeOwner(cookie, "owner1", Ctx->Edge, true);
    auto ownerEvent = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvProxyResponse>(TDuration::Seconds(1));
    UNIT_ASSERT(ownerEvent != nullptr);
    auto ownerCookie = ownerEvent->Response->GetPartitionResponse().GetCmdGetOwnershipResult().GetOwnerCookie();

    TAutoPtr<IEventHandle> handle;
    std::function<bool(const TEvPQ::TEvProxyResponse&)> truth = [&](const TEvPQ::TEvProxyResponse& e) { return cookie == e.Cookie; };

    TString data{500, 'd'};
    //auto fullData = data;
    ui64 currTotalSize = 0, currUncSize = 0;
    ui64 accWaitTime = 0, partWaitTime = 0;
    NKikimrPQ::TPartitionCounterData finalCounters;

    Ctx->Runtime->SetObserverFunc(
        [&](TAutoPtr<IEventHandle>& ev) {
            if (auto* msg = ev->CastAsLocal<TEvKeyValue::TEvRequest>()) {
                for (auto& w : msg->Record.GetCmdWrite()) {
                    if (w.GetKey().StartsWith("J")) {
                        NKikimrPQ::TPartitionMeta meta;
                        bool res = meta.ParseFromString(w.GetValue());
                        UNIT_ASSERT(res);
                        UNIT_ASSERT(meta.HasCounterData());
                        auto& counterData = meta.GetCounterData();
                        UNIT_ASSERT_VALUES_EQUAL(counterData.GetMessagesWrittenTotal(), cookie - 1);
                        UNIT_ASSERT_VALUES_EQUAL(counterData.GetMessagesWrittenGrpc(),isFirstClass ? cookie - 1 : 0);
                        UNIT_ASSERT(counterData.GetBytesWrittenUncompressed() > currUncSize);
                        currUncSize = counterData.GetBytesWrittenUncompressed();
                        UNIT_ASSERT_VALUES_EQUAL(counterData.GetBytesWrittenGrpc(), isFirstClass ? counterData.GetBytesWrittenTotal() : 0);
                        UNIT_ASSERT(counterData.GetBytesWrittenTotal() > currTotalSize);
                        currTotalSize = counterData.GetBytesWrittenTotal();

                        if (cookie == 11) {
                            finalCounters = std::move(counterData);
                        }
                    }
                }
                SendDiskStatusResponse();
                return TTestActorRuntimeBase::EEventAction::DROP;
            } else if (auto* msg = ev->CastAsLocal<TEvPQ::TEvRequestQuota>()) {
                Ctx->Runtime->Send(new IEventHandle(
                    ev->Sender, TActorId{},
                    new TEvPQ::TEvApproveWriteQuota(msg->Cookie, TDuration::MilliSeconds(accWaitTime), TDuration::MilliSeconds(partWaitTime))
                ));
                accWaitTime += 1000;
                partWaitTime += 10;
                return TTestActorRuntimeBase::EEventAction::DROP;
            } else if (auto* msg = ev->CastAsLocal<TEvPQ::TEvConsumed>()) {
                return TTestActorRuntimeBase::EEventAction::DROP;
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
    });

    for (auto i = 0u; i != 10; i++) {
        Cerr << "Send write: " << i << Endl;
        SendWrite(++cookie, i, ownerCookie, 100 + i, data, false, i + 1);
        auto eventErr = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvError>(TDuration::Seconds(1));
        if(eventErr != nullptr) {
            Cerr << "Got error: " << eventErr->Error << Endl;
            UNIT_FAIL("");
        }
        auto event = Ctx->Runtime->GrabEdgeEventIf<TEvPQ::TEvProxyResponse>(handle, truth, TDuration::Seconds(1));
        UNIT_ASSERT(event != nullptr);
        data += data;
    }
    TVector<ui64> msgSizesExpected{2, 2, 1, 1, 1, 1, 1, 1};
    CompareVectors(msgSizesExpected, finalCounters.GetMessagesSizes());
    SendGetWriteInfo();
    {
        auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvGetWriteInfoResponse>(TDuration::Seconds(1));
        UNIT_ASSERT(event != nullptr);
        Cerr << "Got write info response. Body keys: " << event->BodyKeys.size() << ", head: " << event->BlobsFromHead.size() << ", src id info: " << event->SrcIdInfo.size() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(event->MessagesWrittenTotal, 10);
        UNIT_ASSERT_VALUES_EQUAL(event->MessagesWrittenGrpc, 10 * (ui8)isFirstClass);
        UNIT_ASSERT_VALUES_EQUAL(event->BytesWrittenTotal, currTotalSize);
        UNIT_ASSERT_VALUES_EQUAL(event->BytesWrittenGrpc, currTotalSize * (ui8)isFirstClass);
        UNIT_ASSERT_VALUES_EQUAL(event->BytesWrittenUncompressed, currUncSize);

        CompareVectors(msgSizesExpected, event->MessagesSizes);
    }
}

void TPartitionFixture::WaitKeyValueRequest(TMaybe<ui64>& cookie)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>();
    UNIT_ASSERT(event != nullptr);
    if (event->Record.HasCookie()) {
        cookie = event->Record.GetCookie();
    } else {
        cookie = Nothing();
    }
}

void TPartitionFixture::EmulateKVTablet()
{
     TMaybe<ui64> cookie;
     WaitKeyValueRequest(cookie);
     SendDiskStatusResponse(&cookie);
     Cerr << "Send disk status response with cookie: " << cookie.GetOrElse(0) << Endl;
}

void TPartitionFixture::TestWriteSubDomainOutOfSpace(TDuration quotaWaitDuration, bool ignoreQuotaDeadline)
{
    Ctx->Runtime->GetAppData().FeatureFlags.SetEnableTopicDiskSubDomainQuota(true);
    Ctx->Runtime->GetAppData().PQConfig.MutableQuotingConfig()->SetQuotaWaitDurationMs(quotaWaitDuration.MilliSeconds());
    Ctx->Runtime->SetLogPriority( NKikimrServices::PERSQUEUE, NActors::NLog::PRI_DEBUG);

    CreatePartition({
                    .Partition=TPartitionId{1},
                    .Begin=0, .End=10,
                    //
                    // partition configuration
                    //
                    .Config={.Version=1, .Consumers={{.Consumer="client-1", .Offset=3}}}
                    },
                    //
                    // tablet configuration
                    //
                    {.Version=2, .Consumers={{.Consumer="client-1"}}});

    TMaybe<ui64> kvCookie;

    SendSubDomainStatus(true);

    ui64 cookie = 1;
    ui64 messageNo = 0;
    TString ownerCookie;
    CmdChangeOwner(cookie, "owner1", TDuration::Seconds(1), ownerCookie);

    TAutoPtr<IEventHandle> handle;
    std::function<bool(const TEvPQ::TEvProxyResponse&)> truth = [&](const TEvPQ::TEvProxyResponse& e) {
        return cookie == e.Cookie;
    };

    TString data = "data for write";

    // First message will be processed because used storage 0 and limit 0. That is, the limit is not exceeded.
    SendWrite(++cookie, messageNo, ownerCookie, (messageNo + 1) * 100, data, ignoreQuotaDeadline);
    messageNo++;

    WaitKeyValueRequest(kvCookie); // the partition saves the TEvPQ::TEvWrite event
    SendDiskStatusResponse(&kvCookie);

    {
        auto event = Ctx->Runtime->GrabEdgeEventIf<TEvPQ::TEvProxyResponse>(handle, truth, TDuration::Seconds(1));
        UNIT_ASSERT(event != nullptr);
    }

    // Second message will not be processed because the limit is exceeded.
    SendWrite(++cookie, messageNo, ownerCookie, (messageNo + 1) * 100, data, ignoreQuotaDeadline);
    messageNo++;

    {
        auto event = Ctx->Runtime->GrabEdgeEventIf<TEvPQ::TEvProxyResponse>(handle, truth, TDuration::Seconds(1));
        UNIT_ASSERT(event == nullptr);
    }

    // SudDomain quota available - second message will be processed..
    SendSubDomainStatus(false);
    WaitKeyValueRequest(kvCookie); // the partition saves the TEvPQ::TEvWrite event
    SendDiskStatusResponse(&kvCookie);

    {
        auto event = Ctx->Runtime->GrabEdgeEventIf<TEvPQ::TEvProxyResponse>(handle, truth, TDuration::Seconds(1));
        UNIT_ASSERT(event != nullptr);
        UNIT_ASSERT_EQUAL(NMsgBusProxy::MSTATUS_OK, event->Response->GetStatus());
    }
}

struct TTestUserAct {
    TSrcIdMap SourceIds = {};
    TString ClientId = {};
    std::pair<ui64, ui64> OffsetRange = {0, 0};
    ui64 Offset = 0;
    bool IsImmediateTx;
    ui64 TxId;
    TString OwnerCookie = {};
    ui64 MessageNo = 0;
    TActorId SupportivePartitionId = {};
};

struct TTxBatchingTestParams {
    ui64 TxStep = 1;
    ui64 ConsumersCount = 0;
    TVector<TString> WriterSessions = {};
    THashSet<ui64> ConsumerSessions = {};
    ui64 WritersCount = 0;
    ui64 EndOffset = 50;
};

class TPartitionTxTestHelper : public TPartitionFixture {
private:
    auto AddWriteTxImpl(const TSrcIdMap& srcIdsAffected, ui64 txId, ui64 step);

    void AddWriteInfoObserver(bool success, const NPQ::TSourceIdMap& srcIdInfo, const TActorId& supportivePart);
    void SendWriteInfoResponseImpl(const TActorId& supportiveId, const TActorId& partitionId, bool status);
    void WaitTxPredicateReplyImpl(ui64 txId, bool status);
    TString GetOwnerCookie(const TString& srcId, const TActorId& pipe);

    THashMap<ui64, TTestUserAct> UserActs;
    ui64 NextActId = 0;
    ui64 TxStep = 1;
    ui64 Id = 0;
    TDeque<ui64> BatchSizes;
    bool HadKvRequest = false;
    THashMap<TActorId, bool> ExpectedWriteInfoRequests;
    TQueue<std::pair<TActorId, TActorId>> RecievedWriteInfoRequests;
    TAdaptiveLock Lock;
    THashMap<TActorId, NPQ::TSourceIdMap> WriteInfoData;

    TVector<std::pair<TString, TString>> Sessions;
    THashMap<TString, std::pair<TString, ui64>> Owners;
public:
    void Init(const TTxBatchingTestParams& params = {})
    {
        TxStep = params.TxStep;
        Ctx->Runtime->GetAppData(0).PQConfig.MutableQuotingConfig()->SetEnableQuoting(false);
        Ctx->Runtime->SetObserverFunc([this](TAutoPtr<IEventHandle>& ev) {
            if (auto* msg = ev->CastAsLocal<TEvPQ::TEvGetWriteInfoRequest>()) {
                with_lock(this->Lock) {
                    RecievedWriteInfoRequests.emplace(ev->Recipient, ev->Sender);
                }
            } else if (auto* msg = ev->CastAsLocal<TEvPQ::TEvTxBatchComplete>()) {
                Cerr << "Got batch complete: " << msg->BatchSize << Endl;
                with_lock(Lock) {
                    BatchSizes.push_back(msg->BatchSize);
                }
            } else if (auto* msg = ev->CastAsLocal<TEvKeyValue::TEvRequest>()) {
                with_lock(Lock) {
                    HadKvRequest = true;
                }
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        });
        const TPartitionId partition{0};
        TCreatePartitionParams partitionParams{.Partition=partition, .Begin=0, .End=params.EndOffset, .PlanStep = 0};
        for (auto i = 0u; i != params.ConsumersCount; i++) {
            partitionParams.Config.Consumers.emplace_back(TCreateConsumerParams{.Consumer="client-" + std::to_string(i), .Offset=0});
        }
        CreatePartition(std::move(partitionParams));
        for (const auto& srcId : params.WriterSessions) {
            Owners[srcId] = {GetOwnerCookie(srcId, TActorId{1, Id++}), 0};
        }
        for (ui64 i = 0; i < params.ConsumersCount; i++) {
            auto clientId = TStringBuilder() << "client-" << i;
            TStringBuilder session{};
            if (params.ConsumerSessions.contains(i+1)) {
                session << "session-" << clientId;
                CreateSession(clientId, session);
            }
            Sessions.emplace_back(clientId, session);
        }

        ResetBatchCompletion();
    }

    ui64 GetTxId() {
        return NextActId++;
    }

    ui64 AddAndSendNormalWrite(const TString& srcId, ui64 startSeqnNo, ui64 lastSeqNo);
    ui64 MakeAndSendWriteTx(const TSrcIdMap& srcIdsAffected);
    ui64 MakeAndSendImmediateTx(const TSrcIdMap& srcIdsAffected);
    ui64 MakeAndSendNormalOffsetCommit(ui64 client, ui64 offset);
    ui64 MakeAndSendTxOffsetCommit(ui64 client, ui64 begin, ui64 end);
    ui64 MakeAndSendImmediateTxOffsetCommit(ui64 client, ui64 begin, ui64 end);

    void SendTxCommit(ui64 userActId);
    void SendTxRollback(ui64 userActId);
    void WaitWriteInfoRequest(ui64 userActId, bool autoRespond = false);
    void SendWriteInfoResponse(ui64 userActId, bool status = true);
    void WaitTxPredicateReply(ui64 userActId);
    void WaitTxPredicateFailure(ui64 userActId);
    void ExpectNoTxPredicateReply();
    void WaitKvRequest();
    void ExpectNoKvRequest();
    void SendKvResponse();
    void WaitCommitDone(ui64 userActId);
    void WaitImmediateTxComplete(ui64 userActId, bool status);
    void ExpectNoCommitDone();
    void ExpectNoBatchCompletion();
    void WaitBatchCompletion(ui64 userActsCount);
    void ResetBatchCompletion();
};

ui64 TPartitionTxTestHelper::MakeAndSendNormalOffsetCommit(ui64 client, ui64 offset) {
    const auto& [clientId, session] = Sessions[client - 1];
    Y_ABORT_UNLESS(!session.empty());
    TTestUserAct act{.ClientId = clientId, .Offset = offset, .IsImmediateTx = false, .TxId = 0};
    auto id = NextActId++;
    SendSetOffset(id, clientId, offset, session);
    UserActs.emplace(id, std::move(act));
    return id;
}

ui64 TPartitionTxTestHelper::MakeAndSendImmediateTxOffsetCommit(ui64 client, ui64 begin, ui64 end) {
    auto id = NextActId++;
    const auto& [clientId, _] = Sessions[client - 1];

    TTestUserAct act{.ClientId = clientId, .OffsetRange = {begin, end}, .IsImmediateTx = true, .TxId = id};
    SendProposeTransactionRequest(0, begin, end, clientId, "topic-path", true, act.TxId);
    UserActs.emplace(id, std::move(act));
    return id;
}

ui64 TPartitionTxTestHelper::MakeAndSendTxOffsetCommit(ui64 client, ui64 begin, ui64 end) {
    const auto& [clientId, _] = Sessions[client - 1];
    auto id = NextActId++;
    TTestUserAct act{.ClientId = clientId, .OffsetRange = {begin, end}, .IsImmediateTx = false, .TxId = id};
    auto event = MakeHolder<TEvPQ::TEvTxCalcPredicate>(TxStep, act.TxId);
    event->AddOperation(clientId, begin, end);
    SendEvent(event.Release());
    UserActs.emplace(id, std::move(act));
    Cerr << "Created Tx with id " << act.TxId << " as act# " << id << Endl;
    return id;
}

void TPartitionTxTestHelper::SendWriteInfoResponseImpl(const TActorId& supportiveId, const TActorId& partitionId, bool status) {
    if (!status) {
        SendEvent(
            new TEvPQ::TEvGetWriteInfoError(0, ""), supportiveId, partitionId
        );
        return;
    }
    NPQ::TSourceIdMap SrcIds;
    auto* reply = new TEvPQ::TEvGetWriteInfoResponse();
    auto iter = this->WriteInfoData.find(supportiveId);
    Y_ABORT_UNLESS(!iter.IsEnd());
    reply->SrcIdInfo = iter->second;
    SendEvent(reply, supportiveId, partitionId);
}

void TPartitionTxTestHelper::WaitWriteInfoRequest(ui64 userActId, bool autoRespond) {
    auto iter = UserActs.find(userActId);
    Y_ABORT_UNLESS(!iter.IsEnd());
    const auto& act = iter->second;
    auto checkIfRecieved = [&]() {
        TActorId parentPartitionId, supportiveId;
        with_lock(Lock) {
            if (RecievedWriteInfoRequests.size()) {
                std::tie(supportiveId, parentPartitionId) = RecievedWriteInfoRequests.front();
                RecievedWriteInfoRequests.pop();
            }
        }
        if (!parentPartitionId) {
            return false;
        }
        UNIT_ASSERT_VALUES_EQUAL(supportiveId, act.SupportivePartitionId);
        if (autoRespond) {
            SendWriteInfoResponseImpl(supportiveId, parentPartitionId, true);
        }
        return true;
    };
    if (checkIfRecieved()) {
        return;
    }
    Ctx->Runtime->DispatchEvents();
    auto res = checkIfRecieved();
    UNIT_ASSERT(res);
}

void TPartitionTxTestHelper::SendWriteInfoResponse(ui64 userActId, bool status) {
    auto actIter = UserActs.find(userActId);
    Y_ABORT_UNLESS(!actIter.IsEnd());

    SendWriteInfoResponseImpl(actIter->second.SupportivePartitionId, ActorId, status);
}

void TPartitionTxTestHelper::WaitTxPredicateReply(ui64 userActId) {
    return WaitTxPredicateReplyImpl(userActId, true);
}

void TPartitionTxTestHelper::WaitTxPredicateFailure(ui64 userActId) {
    return WaitTxPredicateReplyImpl(userActId, false);
}

void TPartitionTxTestHelper::ExpectNoKvRequest() {
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>(TDuration::Seconds(1));
    UNIT_ASSERT(event == nullptr);
}

void TPartitionTxTestHelper::SendTxCommit(ui64 userActId) {
    auto actIter = UserActs.find(userActId);
    SendCommitTx(TxStep, actIter->second.TxId);
}

void TPartitionTxTestHelper::SendTxRollback(ui64 userActId) {
    auto actIter = UserActs.find(userActId);
    SendRollbackTx(TxStep, actIter->second.TxId);
}

void TPartitionTxTestHelper::WaitCommitDone(ui64 userActId) {
    auto actIter = UserActs.find(userActId);
    Cerr << "Wait tx committed for tx " << actIter->second.TxId << Endl;
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvTxCommitDone>(TDuration::Seconds(1));
    UNIT_ASSERT(event != nullptr);
    UNIT_ASSERT_VALUES_EQUAL(event->TxId, actIter->second.TxId);
}

void TPartitionTxTestHelper::WaitImmediateTxComplete(ui64 userActId, bool status) {
    auto actIter = UserActs.find(userActId);
    Cerr << "Wait immediate tx complete " << actIter->second.TxId << Endl;
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPersQueue::TEvProposeTransactionResult>(TDuration::Seconds(1));
    UNIT_ASSERT(event != nullptr);
    UNIT_ASSERT_VALUES_EQUAL(event->Record.GetTxId(), actIter->second.TxId);
    Cerr << "Got propose resutl: " << event->Record.DebugString() << Endl;
    if (status) {
        UNIT_ASSERT(event->Record.GetStatus() == NKikimrPQ::TEvProposeTransactionResult::COMPLETE);
    } else {
        UNIT_ASSERT(event->Record.GetStatus() != NKikimrPQ::TEvProposeTransactionResult::COMPLETE);
    }
}
void TPartitionTxTestHelper::ExpectNoCommitDone() {
    Cerr << "Wait for no tx committed\n";
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvTxCommitDone>(TDuration::Seconds(1));
    UNIT_ASSERT(event == nullptr);
}


void TPartitionTxTestHelper::ExpectNoTxPredicateReply() {
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvTxCalcPredicateResult>(TDuration::Seconds(1));
    if (event != nullptr) {
        Cerr << "Got tx predicate reply for " << event->TxId << Endl;
        UNIT_FAIL("");
    }
}

void TPartitionTxTestHelper::ExpectNoBatchCompletion() {
    with_lock(Lock) {
        BatchSizes.clear();
    }
    Ctx->Runtime->DispatchEvents();
    with_lock(Lock) {
        UNIT_ASSERT(BatchSizes.empty());
    }
}

void TPartitionTxTestHelper::WaitBatchCompletion(ui64 actsCount) {
    Cerr << "Wait batch completion\n";
    ui64 result = 0;

    auto check = [&]() {
        with_lock(Lock) {
            if (!BatchSizes.empty()) {
                result = BatchSizes.front();
                BatchSizes.pop_front();
                Y_ABORT_UNLESS(result);
            }
        }
    };
    check();
    if (!result) {
        Ctx->Runtime->DispatchEvents();
        check();
    }
    UNIT_ASSERT_VALUES_EQUAL(result, actsCount);
}

void TPartitionTxTestHelper::ResetBatchCompletion() {
    Ctx->Runtime->DispatchEvents();
    with_lock(Lock) {
        BatchSizes.clear();
    };
}
void TPartitionTxTestHelper::WaitKvRequest() {
    Cerr << "Wait kv request\n";
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>(TDuration::Seconds(1));
    bool ok = (event != nullptr);
    with_lock(Lock) {
        if (HadKvRequest) {
            ok = ok || HadKvRequest;
            HadKvRequest = false;
        }
    }
    UNIT_ASSERT(ok);
    return;
}

void TPartitionTxTestHelper::SendKvResponse() {
    with_lock(Lock) {
        HadKvRequest = false;
    }
    TMaybe<ui64> c;
    return SendDiskStatusResponse(&c);
}

ui64 TPartitionTxTestHelper::AddAndSendNormalWrite(
        const TString& srcId, ui64 startSeqnNo, ui64 lastSeqNo
) {
    auto& [owner, messageNo] = Owners[srcId];
    Y_ABORT_UNLESS(!owner.empty());

    auto id = NextActId++;
    TTestUserAct act {
        .SourceIds = {{srcId, {startSeqnNo, lastSeqNo}}},
        .IsImmediateTx = false,
        .TxId = 0,
        .OwnerCookie = Owners[srcId].first,
        .MessageNo = messageNo
    };

    TString data = "data to write";
    auto makeMsg = [&](const TString& srcId, ui64 seqNo) {
        TEvPQ::TEvWrite::TMsg msg;
        msg.SourceId = srcId;
        msg.SeqNo = seqNo;
        msg.PartNo = 0;
        msg.TotalParts = 1;
        msg.TotalSize = data.size();
        msg.CreateTimestamp = TMonotonic::Now().Seconds();
        msg.WriteTimestamp = TMonotonic::Now().Seconds();
        msg.ReceiveTimestamp = TMonotonic::Now().Seconds();
        msg.DisableDeduplication = false;
        msg.Data = data;
        msg.Data = data;
        msg.UncompressedSize = data.size();
        msg.External = false;
        msg.IgnoreQuotaDeadline = false;
        return msg;
    };
    TVector<TEvPQ::TEvWrite::TMsg> msgs;

    for (const auto& [sourceId, seqNoRange] : act.SourceIds) {
        for (auto seqNo = seqNoRange.first; seqNo <= seqNoRange.second; seqNo++) {
            msgs.push_back(makeMsg( sourceId, seqNo));
        }
    }
    auto event = MakeHolder<TEvPQ::TEvWrite>(id, messageNo, act.OwnerCookie, id * 10, std::move(msgs), false, std::nullopt);
    SendEvent(event.Release());
    UserActs.emplace(id, act);
    messageNo++;
    return id;
}

auto TPartitionTxTestHelper::AddWriteTxImpl(const TSrcIdMap& srcIdsAffected, ui64 txId, ui64 step) {
    auto id = NextActId++;
    TTestUserAct act{.IsImmediateTx = (step != 0), .TxId = txId, .SupportivePartitionId = CreateFakePartition()};
    NPQ::TSourceIdMap srcIdMap;

    for (const auto& [key, val] : srcIdsAffected) {
        TSourceIdInfo srcInfo{val.second, val.second, TInstant::Zero()};
        srcInfo.MinSeqNo = val.first;
        srcIdMap.emplace(key, std::move(srcInfo));
    }
    auto iter = UserActs.insert(std::make_pair(id, act)).first;

    with_lock(Lock) {
        WriteInfoData.emplace(act.SupportivePartitionId, std::move(srcIdMap));
    }
    return iter;
}

ui64 TPartitionTxTestHelper::MakeAndSendWriteTx(const TSrcIdMap& srcIdsAffected) {
    auto actIter = AddWriteTxImpl(srcIdsAffected, NextActId++, TxStep);
    auto event = MakeHolder<TEvPQ::TEvTxCalcPredicate>(TxStep, actIter->second.TxId);
    event->SupportivePartitionActor = actIter->second.SupportivePartitionId;
    Cerr << "Create distr tx with id = " << actIter->second.TxId << " and act no: " << actIter->first << Endl;

    SendEvent(event.Release());
    return actIter->first;
}

ui64 TPartitionTxTestHelper::MakeAndSendImmediateTx(const TSrcIdMap& srcIdsAffected) {
    auto actIter = AddWriteTxImpl(srcIdsAffected, NextActId++, 0);

    auto event = MakeHolder<TEvPersQueue::TEvProposeTransaction>();

    ActorIdToProto(Ctx->Edge, event->Record.MutableSourceActor());
    auto* body = event->Record.MutableData();
    body->SetImmediate(true);
    ActorIdToProto(actIter->second.SupportivePartitionId, event->Record.MutableSupportivePartitionActor());
    event->Record.SetTxId(actIter->second.TxId);
    SendEvent(event.Release());
    Cerr << "Create immediate tx with id = " << actIter->second.TxId << " and act no: " << actIter->first << Endl;
    return actIter->first;

}

TString TPartitionTxTestHelper::GetOwnerCookie(const TString& srcId, const TActorId& pipe) {
    SendChangeOwner(1, srcId, pipe, true);
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvProxyResponse>(TDuration::Seconds(1));
    UNIT_ASSERT(event != nullptr);
    return event->Response->GetPartitionResponse().GetCmdGetOwnershipResult().GetOwnerCookie();
}

void TPartitionTxTestHelper::WaitTxPredicateReplyImpl(ui64 userActId, bool status) {
    auto txId = UserActs.find(userActId)->second.TxId;
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvTxCalcPredicateResult>(TDuration::Seconds(1));
    UNIT_ASSERT(event != nullptr);
    UNIT_ASSERT_VALUES_EQUAL(event->TxId, txId);
    UNIT_ASSERT_VALUES_EQUAL(event->Predicate, status);
}

Y_UNIT_TEST_F(Batching, TPartitionFixture)
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

Y_UNIT_TEST_F(SetOffset, TPartitionFixture)
{
    const TPartitionId partition{0};
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

Y_UNIT_TEST_F(TooManyImmediateTxs, TPartitionTxTestHelper)
{
    const TPartitionId partition{0};
    //const ui64 begin = 0;
    const ui64 end = 2'000;
    const TString client = "client";
    const TString session = "session";
    Init(TTxBatchingTestParams{.EndOffset=end});

    CreateSession(client, session);
    auto txTmp = MakeAndSendWriteTx({});

    for (ui64 txId = 1; txId <= 1'002; ++txId) {
        SendProposeTransactionRequest(partition.InternalPartitionId,
                                      txId - 1, txId, // range
                                      client,
                                      "topic-path",
                                      true,
                                      txId);
    }

    WaitWriteInfoRequest(txTmp, true);
    SendTxRollback(txTmp);

    // //
    // // the first command in the queue will start writing
    // //
    // WaitCmdWrite({.Count=2, .UserInfos={{0, {.Session=session, .Offset=1}}}});

    //
    // messages from 2 to 1001 will be queued and the OVERLOADED error will be returned to the last one
    //
    WaitProposeTransactionResponse({.TxId=1'001, .Status=NKikimrPQ::TEvProposeTransactionResult::OVERLOADED});
    WaitProposeTransactionResponse({.TxId=1'002, .Status=NKikimrPQ::TEvProposeTransactionResult::OVERLOADED});

    //
    // the writing has ended
    //
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
    WaitProposeTransactionResponse({.TxId=1, .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    //
    // the commands from the queue will be executed as one
    //
    WaitCmdWrite({.Count=2, .UserInfos={{0, {.Session=session, .Offset=1'000}}}});

    //
    // while the writing is in progress, another command has arrived
    //
    SendProposeTransactionRequest(partition.InternalPartitionId,
                                  1'000, 1'002, // range
                                  client,
                                  "topic-path",
                                  true,
                                  1'003);
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    //
    // it will be processed
    //
    WaitCmdWrite({.Count=2, .UserInfos={{0, {.Session=session, .Offset=1'002}}}});
}

Y_UNIT_TEST_F(CommitOffsetRanges, TPartitionFixture)
{
    const TPartitionId partition{0};
    const ui64 begin = 0;
    const ui64 end = 10;
    const TString client = "client";
    const TString session = "session";

    CreatePartition({.Partition=partition, .Begin=begin, .End=end});

    //
    // create session
    //
    CreateSession(client, session);

    SendProposeTransactionRequest(partition.InternalPartitionId,
                                  0, 2,  // 0 --> 2
                                  client,
                                  "topic-path",
                                  true,
                                  1);
    WaitCmdWrite({.Count=2, .UserInfos={{0, {.Session=session, .Offset=2}}}});

    SendProposeTransactionRequest(partition.InternalPartitionId,
                                  2, 0,          // begin > end
                                  client,
                                  "topic-path",
                                  true,
                                  2);
    SendProposeTransactionRequest(partition.InternalPartitionId,
                                  4, 6,          // begin > client.end
                                  client,
                                  "topic-path",
                                  true,
                                  3);
    SendProposeTransactionRequest(partition.InternalPartitionId,
                                  1, 4,          // begin < client.end
                                  client,
                                  "topic-path",
                                  true,
                                  4);
    SendProposeTransactionRequest(partition.InternalPartitionId,
                                  2, 4,          // begin == client.end
                                  client,
                                  "topic-path",
                                  true,
                                  5);
    SendProposeTransactionRequest(partition.InternalPartitionId,
                                  4, 13,         // end > partition.end
                                  client,
                                  "topic-path",
                                  true,

                                  6);

    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
    WaitProposeTransactionResponse({.TxId=1, .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});

    WaitCmdWrite({.Count=2, .UserInfos={{0, {.Session=session, .Offset=4}}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitProposeTransactionResponse({.TxId=2, .Status=NKikimrPQ::TEvProposeTransactionResult::BAD_REQUEST});
    WaitProposeTransactionResponse({.TxId=3, .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});
    WaitProposeTransactionResponse({.TxId=4, .Status=NKikimrPQ::TEvProposeTransactionResult::ABORTED});
    WaitProposeTransactionResponse({.TxId=5, .Status=NKikimrPQ::TEvProposeTransactionResult::COMPLETE});
    WaitProposeTransactionResponse({.TxId=6, .Status=NKikimrPQ::TEvProposeTransactionResult::BAD_REQUEST});

    SendGetOffset(6, client);
    WaitProxyResponse({.Cookie=6, .Offset=4});
}

Y_UNIT_TEST_F(CorrectRange_Commit, TPartitionFixture)
{
    const TPartitionId partition{3};
    const ui64 begin = 0;
    const ui64 end = 10;
    const TString client = "client";
    const TString session = "session";

    const ui64 step = 12345;
    const ui64 txId = 67890;

    CreatePartition({.Partition=partition, .Begin=begin, .End=end, .PlanStep=step, .TxId=10000});
    CreateSession(client, session);

    SendCalcPredicate(step, txId, client, 0, 2);
    WaitCalcPredicateResult({.Step=step, .TxId=txId, .Partition=TPartitionId(partition), .Predicate=true});

    SendCommitTx(step, txId);

    WaitCmdWrite({.Count=3, .PlanStep=step, .TxId=txId, .UserInfos={{1, {.Session=session, .Offset=2}}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitCommitTxDone({.TxId=txId, .Partition=TPartitionId(partition)});
}

Y_UNIT_TEST_F(CorrectRange_Multiple_Transactions, TPartitionFixture)
{
    const TPartitionId partition{3};
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
    WaitCalcPredicateResult({.Step=step, .TxId=txId_1, .Partition=TPartitionId(partition), .Predicate=true});

    SendCalcPredicate(step, txId_2, client, 0, 2);
    SendCalcPredicate(step, txId_3, client, 0, 2);

    SendCommitTx(step, txId_1);

    WaitCalcPredicateResult({.Step=step, .TxId=txId_2, .Partition=TPartitionId(partition), .Predicate=false});
    SendRollbackTx(step, txId_2);

    WaitCalcPredicateResult({.Step=step, .TxId=txId_3, .Partition=TPartitionId(partition), .Predicate=false});
    SendRollbackTx(step, txId_3);

    WaitCmdWrite({.Count=3, .PlanStep=step, .TxId=txId_3, .UserInfos={{1, {.Session=session, .Offset=1}}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitCommitTxDone({.TxId=txId_1, .Partition=TPartitionId(partition)});
}

Y_UNIT_TEST_F(CorrectRange_Multiple_Consumers, TPartitionFixture)
{
    const TPartitionId partition{3};
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

    WaitCalcPredicateResult({.Step=step, .TxId=txId, .Partition=TPartitionId(partition), .Predicate=true});
    SendCommitTx(step, txId);

    WaitCmdWrite({.Count=5, .UserInfos={
                 {1, {.Session="session-2", .Offset=1}},
                 {3, {.Session="session-1", .Offset=6}}
                 }});
}

Y_UNIT_TEST_F(OldPlanStep, TPartitionFixture)
{
    const TPartitionId partition{3};
    const ui64 begin = 0;
    const ui64 end = 10;

    const ui64 step = 12345;
    const ui64 txId = 67890;

    CreatePartition({.Partition=partition, .Begin=begin, .End=end, .PlanStep=99999, .TxId=55555});

    SendCommitTx(step, txId);
    WaitCommitTxDone({.TxId=txId, .Partition=TPartitionId(partition)});
}

Y_UNIT_TEST_F(AfterRestart_1, TPartitionFixture)
{
    const TPartitionId partition{3};
    const ui64 begin = 0;
    const ui64 end = 10;
    const TString consumer = "client";
    const TString session = "session";

    const ui64 step = 12345;

    TVector<TTransaction> txs;
    txs.push_back(MakeTransaction(step, 11111, consumer, 0, 2, true));
    txs.push_back(MakeTransaction(step, 22222, consumer, 2, 4));

    CreatePartition({
                    .Partition=partition,
                    .Begin=begin,
                    .End=end,
                    .PlanStep=step, .TxId=10000,
                    .Transactions=std::move(txs),
                    .Config={.Consumers={{.Consumer=consumer, .Offset=0, .Session=session}}}
                    });

    SendCommitTx(step, 11111);

    WaitCalcPredicateResult({.Step=step, .TxId=22222, .Partition=TPartitionId(partition), .Predicate=true});
    SendCommitTx(step, 22222);

    WaitCmdWrite({.Count=3, .PlanStep=step, .TxId=22222, .UserInfos={{1, {.Session=session, .Offset=4}}}});
}

Y_UNIT_TEST_F(AfterRestart_2, TPartitionFixture)
{
    const TPartitionId partition{3};
    const ui64 begin = 0;
    const ui64 end = 10;
    const TString consumer = "client";
    const TString session = "session";

    const ui64 step = 12345;

    TVector<TTransaction> txs;
    txs.push_back(MakeTransaction(step, 11111, consumer, 0, 2));
    txs.push_back(MakeTransaction(step, 22222, consumer, 2, 4));

    CreatePartition({
                    .Partition=partition,
                    .Begin=begin,
                    .End=end,
                    .PlanStep=step, .TxId=10000,
                    .Transactions=std::move(txs),
                    .Config={.Consumers={{.Consumer=consumer, .Offset=0, .Session=session}}}
                    });

    WaitCalcPredicateResult({.Step=step, .TxId=11111, .Partition=TPartitionId(partition), .Predicate=true});
}

Y_UNIT_TEST_F(IncorrectRange, TPartitionFixture)
{
    const TPartitionId partition{3};
    const ui64 begin = 0;
    const ui64 end = 10;
    const TString client = "client";
    const TString session = "session";

    const ui64 step = 12345;
    ui64 txId = 67890;

    CreatePartition({.Partition=partition, .Begin=begin, .End=end});
    CreateSession(client, session);

    SendCalcPredicate(step, txId, client, 4, 2);
    WaitCalcPredicateResult({.Step=step, .TxId=txId, .Partition=TPartitionId(partition), .Predicate=false});
    SendRollbackTx(step, txId);

    WaitCmdWrite({.Count=1, .PlanStep=step, .TxId=txId});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    ++txId;

    SendCalcPredicate(step, txId, client, 2, 4);
    WaitCalcPredicateResult({.Step=step, .TxId=txId, .Partition=TPartitionId(partition), .Predicate=false});
    SendRollbackTx(step, txId);

    WaitCmdWrite({.Count=1, .PlanStep=step, .TxId=txId});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    ++txId;

    SendCalcPredicate(step, txId, client, 0, 11);
    WaitCalcPredicateResult({.Step=step, .TxId=txId, .Partition=TPartitionId(partition), .Predicate=false});
}

Y_UNIT_TEST_F(CorrectRange_Rollback, TPartitionFixture)
{
    const TPartitionId partition{3};
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
    WaitCalcPredicateResult({.Step=step, .TxId=txId_1, .Partition=TPartitionId(partition), .Predicate=true});

    SendCalcPredicate(step, txId_2, client, 0, 5);
    SendRollbackTx(step, txId_1);

    WaitCalcPredicateResult({.Step=step, .TxId=txId_2, .Partition=TPartitionId(partition), .Predicate=true});
}

Y_UNIT_TEST_F(ChangeConfig, TPartitionFixture)
{
    const TPartitionId partition{3};
    const ui64 begin = 0;
    const ui64 end = 10;

    const ui64 step = 12345;
    const ui64 txId_1 = 67890;
    const ui64 txId_2 = 67891;

    CreatePartition({
                    .Partition=partition, .Begin=begin, .End=end,
                    .Config={.Consumers={
                    {.Consumer="client-1", .Offset=0, .Session="session-1"},
                    {.Consumer="client-2", .Offset=0, .Session="session-2"},
                    {.Consumer="client-3", .Offset=0, .Session="session-3"},
                    }}
    });

    SendCalcPredicate(step, txId_1, "client-1", 0, 2);
    Cerr << "Send change config\n";
    SendChangePartitionConfig({.Version=2,
                              .Consumers={
                              {.Consumer="client-1", .Generation=0},
                              {.Consumer="client-3", .Generation=7}
                              }});
    //
    // consumer 'client-2' will be deleted
    //
    SendCalcPredicate(step, txId_2, "client-2", 0, 2);

    WaitCalcPredicateResult({.Step=step, .TxId=txId_1, .Partition=TPartitionId(partition), .Predicate=true});
    SendCommitTx(step, txId_1);
    Cerr << "Wait cmd write (initial)\n";
    WaitCmdWrite({.Count=8,
                 .PlanStep=step, .TxId=txId_1,
                 .UserInfos={
                    {1, {.Consumer="client-1", .Session="session-1", .Offset=2}},
                 },
                 });

    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
    Cerr << "Wait commit 1 done\n";
    WaitCommitTxDone({.TxId=txId_1, .Partition=TPartitionId(partition)});

    //
    // update config
    //
    // WaitCmdWrite({.Count=8,
    //              .PlanStep=step, .TxId=txId_1,
    //              .UserInfos={
    //              {1, {.Consumer="client-1", .Session="session-1", .Offset=2}},
    //              },
    // });
    Cerr << "Wait cmd write (change config)\n";
    WaitCmdWrite({.Count=8,
                 .PlanStep=step, .TxId=txId_1,
                 .UserInfos={
                 {1, {.Consumer="client-1", .Session="session-1", .Offset=2, .ReadRuleGeneration=0}},
                 {3, {.Consumer="client-3", .Session="", .Offset=0, .ReadRuleGeneration=7}}
                 },
                 .DeleteRanges={
                 {0, {.Partition=3, .Consumer="client-2"}}
                 }});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
    Cerr << "Wait config changed\n";
    WaitPartitionConfigChanged({.Partition=TPartitionId(partition)});

    //
    // consumer 'client-2' was deleted
    //
    WaitCalcPredicateResult({.Step=step, .TxId=txId_2, .Partition=TPartitionId(partition), .Predicate=false});
    SendRollbackTx(step, txId_2);
}

Y_UNIT_TEST_F(TabletConfig_Is_Newer_That_PartitionConfig, TPartitionFixture)
{
    CreatePartition({
                    .Partition=TPartitionId{3},
                    .Begin=0, .End=10,
                    //
                    // конфиг партиции
                    //
                    .Config={.Version=1, .Consumers={{.Consumer="client-1", .Offset=3}}}
                    },
                    //
                    // конфиг таблетки
                    //
                    {.Version=2, .Consumers={{.Consumer="client-2"}}});

    WaitCmdWrite({.Count=5,
                 .UserInfos={
                 {0, {.Consumer="client-2", .Session="", .Offset=0, .ReadRuleGeneration=0}}
                 },
                 .DeleteRanges={
                 {0, {.Partition=3, .Consumer="client-1"}}
                 }});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
}

void TPartitionFixture::CmdChangeOwner(ui64 cookie, const TString& sourceId, TDuration duration, TString& ownerCookie)
{
    SendChangeOwner(cookie, sourceId, Ctx->Edge);

    EmulateKVTablet();

    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvProxyResponse>(duration);
    UNIT_ASSERT(event != nullptr);
    ownerCookie = event->Response->GetPartitionResponse().GetCmdGetOwnershipResult().GetOwnerCookie();
}


Y_UNIT_TEST_F(ReserveSubDomainOutOfSpace, TPartitionFixture)
{
    Ctx->Runtime->GetAppData().FeatureFlags.SetEnableTopicDiskSubDomainQuota(true);

    CreatePartition({
                    .Partition=TPartitionId{1},
                    .Begin=0, .End=10,
                    //
                    // partition configuration
                    //
                    .Config={.Version=1, .Consumers={{.Consumer="client-1", .Offset=3}}}
                    },
                    //
                    // tablet configuration
                    //
                    {.Version=2, .Consumers={{.Consumer="client-1"}}});

    SendSubDomainStatus(true);
    //EmulateKVTablet();
    ui64 cookie = 1;
    ui64 messageNo = 0;
    TString ownerCookie;

    CmdChangeOwner(cookie, "owner1", TDuration::Seconds(1), ownerCookie);

    TAutoPtr<IEventHandle> handle;
    std::function<bool(const TEvPQ::TEvProxyResponse&)> truth = [&](const TEvPQ::TEvProxyResponse& e) {
        return cookie == e.Cookie;
    };

    // First message will be processed because used storage 0 and limit 0. That is, the limit is not exceeded.
    SendReserveBytes(++cookie, 7, ownerCookie, messageNo++);

    // Second message will not be processed because the limit is exceeded.
    SendReserveBytes(++cookie, 13, ownerCookie, messageNo++);

    {
        auto reserveEvent = Ctx->Runtime->GrabEdgeEventIf<TEvPQ::TEvProxyResponse>(handle, truth, TDuration::Seconds(1));
        UNIT_ASSERT(reserveEvent == nullptr);
    }

    // SudDomain quota available - second message will be processed..
    SendSubDomainStatus(false);

    {
        auto reserveEvent = Ctx->Runtime->GrabEdgeEventIf<TEvPQ::TEvProxyResponse>(handle, truth, TDuration::Seconds(1));
        UNIT_ASSERT(reserveEvent != nullptr);
    }
}

Y_UNIT_TEST_F(WriteSubDomainOutOfSpace, TPartitionFixture)
{
    Ctx->Runtime->GetAppData().FeatureFlags.SetEnableTopicDiskSubDomainQuota(true);
    Ctx->Runtime->GetAppData().PQConfig.MutableQuotingConfig()->SetQuotaWaitDurationMs(300);
    CreatePartition({
                    .Partition=TPartitionId{1},
                    .Begin=0, .End=10,
                    //
                    // partition configuration
                    //
                    .Config={.Version=1, .Consumers={{.Consumer="client-1", .Offset=3}}}
                    },
                    //
                    // tablet configuration
                    //
                    {.Version=2, .Consumers={{.Consumer="client-1"}}});
    TMaybe<ui64> kvCookie;

    SendSubDomainStatus(true);

    ui64 cookie = 1;
    ui64 messageNo = 0;
    TString ownerCookie;

    CmdChangeOwner(cookie, "owner1", TDuration::Seconds(1), ownerCookie);

    TAutoPtr<IEventHandle> handle;
    std::function<bool(const TEvPQ::TEvError&)> truth = [&](const TEvPQ::TEvError& e) {
        return cookie == e.Cookie;
    };

    TString data = "data for write";

    // First message will be processed because used storage 0 and limit 0. That is, the limit is not exceeded.
    SendWrite(++cookie, messageNo, ownerCookie, (messageNo + 1) * 100, data);
    messageNo++;

    WaitKeyValueRequest(kvCookie); // the partition saves the TEvPQ::TEvWrite event
    SendDiskStatusResponse(&kvCookie);

    {
        auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvProxyResponse>(TDuration::Seconds(1));
        UNIT_ASSERT(event != nullptr);
    }

    // Second message will not be processed because the limit is exceeded.
    SendWrite(++cookie, messageNo, ownerCookie, (messageNo + 1) * 100, data);
    messageNo++;

    {
        auto event = Ctx->Runtime->GrabEdgeEventIf<TEvPQ::TEvError>(handle, truth, TDuration::Seconds(1));
        UNIT_ASSERT(event != nullptr);
        UNIT_ASSERT_EQUAL(NPersQueue::NErrorCode::OVERLOAD, event->ErrorCode);
    }
}

Y_UNIT_TEST_F(WriteSubDomainOutOfSpace_DisableExpiration, TPartitionFixture)
{
    TestWriteSubDomainOutOfSpace(TDuration::MilliSeconds(0), false);
}

Y_UNIT_TEST_F(WriteSubDomainOutOfSpace_IgnoreQuotaDeadline, TPartitionFixture)
{
    TestWriteSubDomainOutOfSpace(TDuration::MilliSeconds(300), true);
}

Y_UNIT_TEST_F(GetPartitionWriteInfoSuccess, TPartitionFixture) {
    Ctx->Runtime->SetLogPriority( NKikimrServices::PERSQUEUE, NActors::NLog::PRI_DEBUG);
    Ctx->Runtime->GetAppData().PQConfig.MutableQuotingConfig()->SetEnableQuoting(false);

    CreatePartition({
                    .Partition=TPartitionId{2, TWriteId{0, 10}, 100'001},
                    //
                    // partition configuration
                    //
                    .Config={.Version=1, .Consumers={}}
                    },
                    //
                    // tablet configuration
                    //
                    {.Version=2, .Consumers={}}
    );

    ui64 cookie = 1;

    SendChangeOwner(cookie, "owner1", Ctx->Edge, true);
    auto ownerEvent = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvProxyResponse>(TDuration::Seconds(1));
    UNIT_ASSERT(ownerEvent != nullptr);
    auto ownerCookie = ownerEvent->Response->GetPartitionResponse().GetCmdGetOwnershipResult().GetOwnerCookie();

    TAutoPtr<IEventHandle> handle;
    auto truth = [&](const TEvPQ::TEvProxyResponse& e) { return cookie == e.Cookie; };

    TString data = "data for write";

    for (auto i = 0; i < 3; i++) {
        SendWrite(++cookie, i, ownerCookie, i + 100, data, true, (i+1)*2);
        SendDiskStatusResponse();
        {
            auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvError>(TDuration::Seconds(1));
            UNIT_ASSERT(event == nullptr);
        }
        auto event = Ctx->Runtime->GrabEdgeEventIf<TEvPQ::TEvProxyResponse>(handle, truth, TDuration::Seconds(1));
        UNIT_ASSERT(event != nullptr);
    }
    SendWrite(++cookie, 3, ownerCookie, 110, data, true, 7);
    SendDiskStatusResponse();
    {
        auto event = Ctx->Runtime->GrabEdgeEventIf<TEvPQ::TEvProxyResponse>(handle, truth, TDuration::Seconds(1));
        UNIT_ASSERT(event != nullptr);
    }
    SendGetWriteInfo();
    {
        {
            auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvGetWriteInfoError>(TDuration::Seconds(1));
            UNIT_ASSERT(event == nullptr);

        }
        auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvGetWriteInfoResponse>(TDuration::Seconds(1));
        UNIT_ASSERT(event != nullptr);
        Cerr << "Got write info resposne. Body keys: " << event->BodyKeys.size() << ", head: " << event->BlobsFromHead.size() << ", src id info: " << event->SrcIdInfo.size() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(event->BodyKeys.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(event->BlobsFromHead.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(event->SrcIdInfo.size(), 1);

        UNIT_ASSERT_VALUES_EQUAL(event->SrcIdInfo.begin()->second.MinSeqNo, 2);
        UNIT_ASSERT_VALUES_EQUAL(event->SrcIdInfo.begin()->second.SeqNo, 7);
        UNIT_ASSERT_VALUES_EQUAL(event->SrcIdInfo.begin()->second.Offset, 110);

        Cerr << "Body key 1: " << event->BodyKeys.begin()->Key.ToString() << ", size: " << event->BodyKeys.begin()->CumulativeSize << Endl;
        Cerr << "Body key last " << event->BodyKeys.back().Key.ToString() << ", size: " << event->BodyKeys.back().CumulativeSize << Endl;
        Cerr << "Head blob 1 size: " << event->BlobsFromHead.begin()->GetBlobSize() << Endl;
        UNIT_ASSERT(event->BodyKeys.begin()->Key.ToString().StartsWith("D0000100001_"));
        UNIT_ASSERT(event->BlobsFromHead.begin()->GetBlobSize() > 0);
    }

} // GetPartitionWriteInfoSuccess

Y_UNIT_TEST_F(GetPartitionWriteInfoError, TPartitionFixture) {
    CreatePartition({
                    .Partition=TPartitionId{2, TWriteId{0, 10}, 100'001},
                    .Begin=0, .End=10,
                    //
                    // partition configuration
                    //
                    .Config={.Version=1, .Consumers={}}
                    },
                    //
                    // tablet configuration
                    //
                    {.Version=2, .Consumers={}}
    );

    ui64 cookie = 1;

    SendChangeOwner(cookie, "owner1", Ctx->Edge, true);
    auto ownerEvent = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvProxyResponse>(TDuration::Seconds(1));
    UNIT_ASSERT(ownerEvent != nullptr);
    auto ownerCookie = ownerEvent->Response->GetPartitionResponse().GetCmdGetOwnershipResult().GetOwnerCookie();

    TAutoPtr<IEventHandle> handle;
    std::function<bool(const TEvPQ::TEvError&)> truth = [&](const TEvPQ::TEvError& e) { return cookie == e.Cookie; };

    TString data = "data for write";

    SendWrite(++cookie, 0, ownerCookie, 100, data, false, 1);

    {
        SendGetWriteInfo();
        auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvGetWriteInfoError>(TDuration::Seconds(1));
        UNIT_ASSERT(event != nullptr);
    }

    SendDiskStatusResponse();
    {
        auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvProxyResponse>(handle, TDuration::Seconds(1));
        UNIT_ASSERT(event != nullptr);
    }
    {
        SendGetWriteInfo();
        Cerr << "Wait write info error(2)\n";
        auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvGetWriteInfoError>(TDuration::Seconds(1));
        UNIT_ASSERT(event != nullptr);
    }
} // GetPartitionWriteInfoErrors

Y_UNIT_TEST_F(ShadowPartitionCounters, TPartitionFixture) {
    ShadowPartitionCountersTest(false);
}

Y_UNIT_TEST_F(ShadowPartitionCountersFirstClass, TPartitionFixture) {
    ShadowPartitionCountersTest(true);
}

Y_UNIT_TEST_F(ShadowPartitionCountersRestore, TPartitionFixture) {
    const TPartitionId partitionId{0, TWriteId{0, 1111}, 123};
    const ui64 begin = 0;
    const ui64 end = 10;
    const TString session = "session";
    Ctx->Runtime->GetAppData().PQConfig.MutableQuotingConfig()->SetEnableQuoting(true);
    Ctx->Runtime->GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);

    auto* partition = CreatePartition({.Partition=partitionId, .Begin=begin, .End=end});
    auto initializer = MakeHolder<TInitializer>(partition);
    auto metaStep = MakeHolder<TInitMetaStep>(initializer.Get());
    TPartitionTestWrapper wrapper{metaStep.Get()};
    NKikimrPQ::TPartitionCounterData countersProto;
    //auto protoStr =
    countersProto.SetMessagesWrittenTotal(1011);
    countersProto.SetMessagesWrittenGrpc(707);
    countersProto.SetBytesWrittenTotal(100500);
    countersProto.SetBytesWrittenGrpc(9000);
    countersProto.SetBytesWrittenUncompressed(123456789);
    for(ui64 i = 0; i < 14; i++) {
        countersProto.AddMessagesSizes(i * 5);
    }
    wrapper.LoadMeta(countersProto);
    metaStep.Reset();
    initializer.Reset();
}

Y_UNIT_TEST_F(DataTxCalcPredicateOk, TPartitionTxTestHelper)
{
    Init();
    CreateSession("client", "session");
    i64 cookie = 1;

    auto tx1 = MakeAndSendWriteTx({});
    WaitWriteInfoRequest(tx1, true);
    Cerr << "Wait first predicate result " << Endl;
    WaitTxPredicateReply(tx1);

    auto tx2 = MakeAndSendWriteTx({{"src1", {1, 10}}});
    WaitWriteInfoRequest(tx2, true);
    SendTxCommit(tx1);
    Cerr << "Wait second predicate result " << Endl;
    WaitTxPredicateReply(tx2);
    SendTxCommit(tx2);
    EmulateKVTablet();

    TString data = "data for write";

    SendChangeOwner(cookie, "owner1", Ctx->Edge, true);
    auto ownerEvent = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvProxyResponse>(TDuration::Seconds(1));
    UNIT_ASSERT(ownerEvent != nullptr);
    auto ownerCookie = ownerEvent->Response->GetPartitionResponse().GetCmdGetOwnershipResult().GetOwnerCookie();

    SendWrite(++cookie, 0, ownerCookie, 51, data, false, 5);
    EmulateKVTablet();
    WaitProxyResponse({.Cookie=cookie});

    Cerr << "Wait third predicate result " << Endl;
    auto tx3 = MakeAndSendWriteTx({{"src1", {1, 10}}, {"SourceId", {6, 10}}});
    WaitWriteInfoRequest(tx3, true);
    WaitTxPredicateReply(tx3);
    SendTxCommit(tx3);
}

Y_UNIT_TEST_F(DataTxCalcPredicateError, TPartitionTxTestHelper)
{
    Init(TTxBatchingTestParams{.EndOffset=1});
    i64 cookie = 1;
    SendChangeOwner(cookie, "SourceId", Ctx->Edge, true);
    auto ownerEvent = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvProxyResponse>(TDuration::Seconds(1));
    UNIT_ASSERT(ownerEvent != nullptr);
    auto ownerCookie = ownerEvent->Response->GetPartitionResponse().GetCmdGetOwnershipResult().GetOwnerCookie();

    TString data = "data for write";
    SendWrite(++cookie, 0, ownerCookie, 11, data, false, 4);
    Cerr << "Wait write response\n";
    WaitKvRequest();
    SendKvResponse();
    WaitProxyResponse({.Cookie=cookie});

    Cerr << "Wait second predicate result " << Endl;
    auto tx2 = MakeAndSendWriteTx({{"src1", {3, 10}}, {"SourceId", {3, 10}}});
    WaitWriteInfoRequest(tx2, true);
    WaitTxPredicateFailure(tx2);
}


Y_UNIT_TEST_F(DataTxCalcPredicateOrder, TPartitionTxTestHelper)
{
    Init();
    auto tx1 = MakeAndSendWriteTx({{"src1", {1, 10}}});
    WaitWriteInfoRequest(tx1, true);
    WaitTxPredicateReply(tx1);

    auto tx2 = MakeAndSendWriteTx({{"src1", {11, 20}}});
    SendTxCommit(tx1);
    WaitWriteInfoRequest(tx2, true);
    WaitTxPredicateReply(tx2);
    SendTxCommit(tx2);

    EmulateKVTablet();
    WaitCommitDone(tx1);
    WaitCommitDone(tx2);
}

Y_UNIT_TEST_F(NonConflictingActsBatchOk, TPartitionTxTestHelper) {
    TTxBatchingTestParams params {.WriterSessions{"src3", "src4"}};
    Init(std::move(params));
    ResetBatchCompletion();

    auto tx1 = MakeAndSendWriteTx({{"src1", {1, 3}}});
    AddAndSendNormalWrite("src3", 7, 12);
    auto immTx1 = MakeAndSendImmediateTx({{"src4", {1, 7}}});
    AddAndSendNormalWrite("src4", 7, 12); // Conflict with imm tx = allowed
    auto immTx2 = MakeAndSendImmediateTx({{"src4", {12, 15}}}); // Immediate txs confilict - allowed;
    ExpectNoTxPredicateReply();
    ExpectNoKvRequest();
    auto tx2 = MakeAndSendWriteTx({{"src-other", {4, 6}}});
    auto tx3 = MakeAndSendWriteTx({{"src2", {4, 6}}});

    WaitWriteInfoRequest(tx1);
    WaitWriteInfoRequest(immTx1, true);
    WaitWriteInfoRequest(immTx2, true);
    WaitWriteInfoRequest(tx2);
    WaitWriteInfoRequest(tx3);

    SendWriteInfoResponse(tx3);

    ExpectNoBatchCompletion();
    SendWriteInfoResponse(tx1);
    WaitTxPredicateReply(tx1);
    SendWriteInfoResponse(tx2);
    WaitTxPredicateReply(tx2);
    WaitTxPredicateReply(tx3);

    WaitBatchCompletion(5 + 6 + 6); //5 txs and immediate txs + 2 normal writes with 6 messages each;

    SendTxCommit(tx3);
    SendTxRollback(tx2);
    ExpectNoKvRequest();
    SendTxCommit(tx1);
    WaitKvRequest();
    SendKvResponse();
    WaitCommitDone(tx1);
    WaitImmediateTxComplete(immTx1, true);
    WaitImmediateTxComplete(immTx2, true);
    WaitCommitDone(tx3);
}

Y_UNIT_TEST_F(ConflictingActsInSeveralBatches, TPartitionTxTestHelper) {
    TTxBatchingTestParams params {.WriterSessions{"src1", "src4"},.EndOffset=1};
    Init(std::move(params));

    auto tx1 = MakeAndSendWriteTx({{"src1", {1, 3}}});
    auto tx2 = MakeAndSendWriteTx({{"src2", {4, 6}}});
    auto tx3 = MakeAndSendWriteTx({{"src1", {4, 6}}});

    AddAndSendNormalWrite("src1", 7, 12);
    AddAndSendNormalWrite("src4", 1, 2);
    auto tx5 = MakeAndSendWriteTx({{"src4", {4, 5}}});
    AddAndSendNormalWrite("src4", 7, 12);
    auto immTx1 = MakeAndSendImmediateTx({{"src4", {13, 15}}});

    WaitWriteInfoRequest(tx1, true);
    WaitWriteInfoRequest(tx2, true);
    WaitWriteInfoRequest(tx3, true);
    WaitWriteInfoRequest(tx5, true);
    WaitWriteInfoRequest(immTx1, true);

    WaitTxPredicateReply(tx1);
    WaitTxPredicateReply(tx2);
    WaitBatchCompletion(2);

    SendTxCommit(tx1);
    SendTxRollback(tx2);
    ExpectNoKvRequest();

    WaitTxPredicateReply(tx3);
    WaitBatchCompletion(1);
    SendTxCommit(tx3);

    //2 Normal writes with src1 & src4
    WaitBatchCompletion(6 + 2); // Normal writes produce 1 act for each message
    ExpectNoTxPredicateReply();
    WaitKvRequest();
    SendKvResponse();
    WaitCommitDone(tx1);
    WaitCommitDone(tx3);
    WaitTxPredicateReply(tx5);
    WaitBatchCompletion(1);
    SendTxCommit(tx5);
    WaitBatchCompletion(1 + 6); //Normal write & immTx for src4;

    WaitKvRequest();
    SendKvResponse();
    WaitCommitDone(tx5);
    WaitImmediateTxComplete(immTx1, true);
}

Y_UNIT_TEST_F(ConflictingTxIsAborted, TPartitionTxTestHelper) {
    return; //ToDo - enable after proper commit is in place;
    TTxBatchingTestParams params {.WriterSessions{"src2"}};
    Init(std::move(params));

    auto tx1 = MakeAndSendWriteTx({{"src1", {1, 3}}});
    auto tx2 = MakeAndSendWriteTx({{"src1", {2, 4}}});

    WaitWriteInfoRequest(tx1, true);
    WaitWriteInfoRequest(tx2, true);

    WaitBatchCompletion(1);

    SendTxCommit(tx1);
    ExpectNoKvRequest();

    WaitTxPredicateFailure(tx2);
    WaitKvRequest();
    SendKvResponse();
    WaitCommitDone(tx1);

    //Part 2 - with immediate tx - different batch;
    AddAndSendNormalWrite("src2", 7, 12);
    auto tx3 = MakeAndSendWriteTx({{"src2", {12, 15}}});
    Y_UNUSED(tx3);
    WaitBatchCompletion(1);
    WaitKvRequest();
    SendKvResponse();
    ExpectNoCommitDone();
}

Y_UNIT_TEST_F(ConflictingTxProceedAfterRollback, TPartitionTxTestHelper) {
    Init();

    auto tx1 = MakeAndSendWriteTx({{"src1", {1, 3}}, {"src2", {5, 10}}});
    auto tx2 = MakeAndSendWriteTx({{"src1", {2, 4}}});
    auto immTx = MakeAndSendImmediateTx({{"src2", {3, 12}}});

    WaitWriteInfoRequest(tx1, true);
    WaitWriteInfoRequest(tx2, true);
    WaitWriteInfoRequest(immTx, true);
    WaitTxPredicateReply(tx1);

    WaitBatchCompletion(1);

    SendTxRollback(tx1);
    ExpectNoKvRequest();

    WaitTxPredicateReply(tx2);
    WaitBatchCompletion(2);
    SendTxCommit(tx2);

    WaitKvRequest();
    SendKvResponse();
    WaitCommitDone(tx2);
    WaitImmediateTxComplete(immTx, true);
}

class TBatchingConditionsTest {
    TPartitionTxTestHelper* TxHelper;
    ui64 SeqNo = 1;
    ui64 TxTmp;

public:
    TString SrcId = "src1";

public:
    TBatchingConditionsTest(TPartitionTxTestHelper* helper)
        : TxHelper(helper)
    {
        TxHelper->Init({.WriterSessions={SrcId}, .EndOffset = 1});
//        Owner = TxHelper->GetOwnerCookie(SrcId, TActorId(ui64(1), SeqNo));
    }

    void Start() {
        TxTmp = TxHelper->MakeAndSendWriteTx({});
    }
    void Process() {
        TxHelper->WaitWriteInfoRequest(TxTmp, true);
        TxHelper->WaitTxPredicateReply(TxTmp);
        TxHelper->SendTxRollback(TxTmp);
    }

    ui64 AddTx() {
        return TxHelper->MakeAndSendWriteTx({{SrcId, {SeqNo, SeqNo}}});
        SeqNo++;
    }
    ui64 AddImmediateTx() {
        return TxHelper->MakeAndSendImmediateTx({{SrcId, {SeqNo, SeqNo}}});
        SeqNo++;
    }
    void AddNormalWrite() {
        TxHelper->AddAndSendNormalWrite(SrcId, SeqNo, SeqNo);
        SeqNo++;
    }
};

Y_UNIT_TEST_F(DifferentWriteTxBatchingOptions, TPartitionTxTestHelper) {
    auto wrapper = TBatchingConditionsTest(this);

    // 1. ImmTx -> NormWrite -> ImmTx -> NormWrite = All batched
    {
    wrapper.Start();
    wrapper.AddNormalWrite();
    auto immTx1 = wrapper.AddImmediateTx();
    wrapper.AddNormalWrite();
    auto immTx2 = wrapper.AddImmediateTx();
    wrapper.Process();
    WaitWriteInfoRequest(immTx1, true);
    WaitWriteInfoRequest(immTx2, true);
    WaitBatchCompletion(4 + 1);
    EmulateKVTablet();
    WaitImmediateTxComplete(immTx1, true);
    WaitImmediateTxComplete(immTx2, true);
    }
    {
    // 2. ImmTx -> WriteTx = KVRequest
    ResetBatchCompletion();
    wrapper.Start();
    auto immTx = wrapper.AddImmediateTx();
    auto tx = wrapper.AddTx();
    wrapper.Process();
    WaitWriteInfoRequest(immTx, true);
    WaitWriteInfoRequest(tx, true);
    WaitBatchCompletion(1+1);
    ExpectNoTxPredicateReply();
    EmulateKVTablet();
    WaitImmediateTxComplete(immTx, true);
    ExpectNoCommitDone();
    WaitTxPredicateReply(tx);
    SendTxCommit(tx);
    WaitBatchCompletion(1);
    EmulateKVTablet();
    WaitCommitDone(tx);
    }
    {
    // 3. NormWrite -> WriteTx = KVRequest
    ResetBatchCompletion();
    wrapper.Start();
    wrapper.AddNormalWrite();
    auto tx = wrapper.AddTx();
    wrapper.Process();
    WaitWriteInfoRequest(tx, true);
    WaitBatchCompletion(1+1);
    ExpectNoTxPredicateReply();
    EmulateKVTablet();
    WaitTxPredicateReply(tx);
    SendTxCommit(tx);
    WaitBatchCompletion(1);
    EmulateKVTablet();
    WaitCommitDone(tx);
    }
    {
    // 4. WriteTx -> NormWrite = 2 batches
    ResetBatchCompletion();
    wrapper.Start();
    auto tx = wrapper.AddTx();
    wrapper.AddNormalWrite();
    wrapper.Process();
    WaitWriteInfoRequest(tx, true);
    WaitTxPredicateReply(tx);
    WaitBatchCompletion(1+1);
    ExpectNoKvRequest();
    SendTxCommit(tx);
    WaitBatchCompletion(1);
    EmulateKVTablet();
    WaitCommitDone(tx);
    }
    {
    // 5. WriteTx -> ImmTx = 2 batches
    ResetBatchCompletion();
    wrapper.Start();
    auto tx = wrapper.AddTx();
    auto immTx = wrapper.AddImmediateTx();
    wrapper.Process();
    WaitWriteInfoRequest(tx, true);
    WaitWriteInfoRequest(immTx, true);
    WaitBatchCompletion(1+1);
    WaitTxPredicateReply(tx);
    SendTxCommit(tx);
    WaitBatchCompletion(1);
    ExpectNoCommitDone();
    EmulateKVTablet();
    WaitCommitDone(tx);
    WaitImmediateTxComplete(immTx, true);
    }
}
Y_UNIT_TEST_F(FailedTxsDontBlock, TPartitionTxTestHelper) {
    Init({.WriterSessions={"src1", "src2"}, .EndOffset = 1});
    // Failed WriteTx doesn't block
    {
    auto txTmp = MakeAndSendWriteTx({});
    AddAndSendNormalWrite("src1", 1, 5);
    auto tx = MakeAndSendWriteTx({{"src1", {1, 10}}});
    auto immTx = MakeAndSendImmediateTx({{"src1", {6, 10}}});

    WaitWriteInfoRequest(txTmp, true);
    WaitTxPredicateReply(txTmp);
    SendTxRollback(txTmp);

    WaitWriteInfoRequest(tx, true);
    WaitWriteInfoRequest(immTx, true);
    WaitBatchCompletion(5 + 1);
    ExpectNoTxPredicateReply();
    EmulateKVTablet();
    WaitTxPredicateFailure(tx);
    WaitBatchCompletion(2);
    SendTxRollback(tx);

    EmulateKVTablet();
    WaitImmediateTxComplete(immTx, true);
    }
    {
    AddAndSendNormalWrite("src2", 1, 10);
    EmulateKVTablet();
    ResetBatchCompletion();

    auto txTmp = MakeAndSendWriteTx({});
    auto immTx = MakeAndSendImmediateTx({{"src2", {5, 15}}});
    auto tx = MakeAndSendWriteTx({{"srcId2", {11, 15}}});
    WaitWriteInfoRequest(txTmp, true);
    WaitTxPredicateReply(txTmp);
    SendTxRollback(txTmp);

    WaitWriteInfoRequest(immTx, true);
    WaitWriteInfoRequest(tx, true);
    WaitBatchCompletion(2 + 1);
    WaitTxPredicateReply(tx);
    ExpectNoKvRequest();
    SendTxCommit(tx);
    EmulateKVTablet();
    WaitImmediateTxComplete(immTx, false);
    WaitCommitDone(tx);
    }
}

Y_UNIT_TEST_F(NonConflictingCommitsBatch, TPartitionTxTestHelper) {
    TTxBatchingTestParams params{
        .ConsumersCount= 3,
        .ConsumerSessions={1},
        .EndOffset=50
    };
    Init(std::move(params));

    //Just block processing so every message arrives before batching starts
    auto txTmp = MakeAndSendWriteTx({});
    MakeAndSendNormalOffsetCommit(1, 5);
    auto tx1 = MakeAndSendTxOffsetCommit(3, 0, 5);
    auto tx2 = MakeAndSendTxOffsetCommit(2, 0, 5);
    MakeAndSendNormalOffsetCommit(1, 10);
    auto txImm1 = MakeAndSendImmediateTxOffsetCommit(1, 0, 15);
    WaitWriteInfoRequest(txTmp, true);
    WaitTxPredicateReply(txTmp);
    SendTxRollback(txTmp);

    WaitTxPredicateReply(tx1);
    WaitTxPredicateReply(tx2);

    WaitBatchCompletion(5 + 1 /*tmpTx*/);
    SendTxCommit(tx1);
    SendTxCommit(tx2);
    WaitKvRequest();
    SendKvResponse();

    WaitCommitDone(tx1);
    WaitCommitDone(tx2);
    WaitImmediateTxComplete(txImm1, false);
}

Y_UNIT_TEST_F(ConflictingCommitsInSeveralBatches, TPartitionTxTestHelper) {
    TTxBatchingTestParams params{
        .ConsumersCount= 2,
        .ConsumerSessions={1},
        .EndOffset=50
    };
    Init(std::move(params));

    //Just block processing so every message arrives before batching starts
    auto txTmp = MakeAndSendWriteTx({});

    MakeAndSendNormalOffsetCommit(1, 2);
    auto tx1 = MakeAndSendTxOffsetCommit(1, 2, 5);
    auto tx2 = MakeAndSendTxOffsetCommit(1, 5, 10);
    MakeAndSendNormalOffsetCommit(1, 20);
    ResetBatchCompletion();

    WaitWriteInfoRequest(txTmp, true);
    WaitTxPredicateReply(txTmp);

    WaitBatchCompletion(2);
    SendTxRollback(txTmp);

    ExpectNoTxPredicateReply();
    WaitKvRequest();
    SendKvResponse();

    WaitTxPredicateReply(tx1);
    WaitBatchCompletion(1);
    ExpectNoTxPredicateReply();
    SendTxCommit(tx1);
    ExpectNoKvRequest();

    WaitTxPredicateReply(tx2);
    WaitBatchCompletion(1);
    SendTxCommit(tx2);
    WaitBatchCompletion(1);

    WaitKvRequest();
    SendKvResponse();
    WaitCommitDone(tx1);
    WaitCommitDone(tx2);



    txTmp = MakeAndSendWriteTx({});
    auto immTx1 = MakeAndSendImmediateTxOffsetCommit(2, 0, 5);
    auto immTx2 = MakeAndSendImmediateTxOffsetCommit(2, 5, 10);
    WaitWriteInfoRequest(txTmp, true);
    WaitTxPredicateReply(txTmp);
    SendTxRollback(txTmp);

    WaitBatchCompletion(2 + 1);
    WaitKvRequest();
    SendKvResponse();
    WaitImmediateTxComplete(immTx1, true);
    WaitImmediateTxComplete(immTx2, true);
}

Y_UNIT_TEST_F(ConflictingCommitFails, TPartitionTxTestHelper) {
    TTxBatchingTestParams params{
        .ConsumersCount= 2,
        .ConsumerSessions={1, 2},
        .EndOffset=50
    };
    Init(std::move(params));

    auto txTmp = MakeAndSendWriteTx({});

    auto tx1 = MakeAndSendTxOffsetCommit(1, 0, 5);
    auto tx2 = MakeAndSendTxOffsetCommit(1, 0, 3);

    WaitWriteInfoRequest(txTmp, true);
    WaitTxPredicateReply(txTmp);
    SendTxRollback(txTmp);

    WaitTxPredicateReply(tx1);
    WaitBatchCompletion(1 + 1);

    SendTxCommit(tx1);
    WaitTxPredicateFailure(tx2);
    SendTxRollback(tx2);

    WaitKvRequest();
    SendKvResponse();
    WaitCommitDone(tx1);
    ExpectNoCommitDone();

    //Part2
    ResetBatchCompletion();
    txTmp = MakeAndSendWriteTx({});

    MakeAndSendNormalOffsetCommit(2, 3);
    auto tx3 = MakeAndSendTxOffsetCommit(2, 0, 3);

    WaitWriteInfoRequest(txTmp, true);
    WaitTxPredicateReply(txTmp);
    SendTxRollback(txTmp);

    ExpectNoTxPredicateReply();
    WaitBatchCompletion(2);
    WaitKvRequest();
    SendKvResponse();
    WaitTxPredicateFailure(tx3);
    WaitBatchCompletion(1);
    SendTxRollback(tx3);

    WaitKvRequest(); //No user operatiions completed but TxId has changed which will be saved
    SendKvResponse();

    //Part3
    txTmp = MakeAndSendWriteTx({});

    auto immTx3_1 = MakeAndSendImmediateTxOffsetCommit(2, 3, 6);
    auto immTx3_2 = MakeAndSendImmediateTxOffsetCommit(2, 4, 7);

    WaitWriteInfoRequest(txTmp, true);
    WaitTxPredicateReply(txTmp);
    SendTxRollback(txTmp);

    WaitKvRequest();
    SendKvResponse();
    WaitImmediateTxComplete(immTx3_1, true);
    WaitImmediateTxComplete(immTx3_2, false);
}

Y_UNIT_TEST_F(ConflictingCommitProccesAfterRollback, TPartitionTxTestHelper) {
    TTxBatchingTestParams params{
        .ConsumersCount = 2,
        .EndOffset=50
    };
    Init(std::move(params));

    auto tx1 = MakeAndSendTxOffsetCommit(1, 0, 5);
    auto tx2 = MakeAndSendTxOffsetCommit(1, 0, 3);

    WaitTxPredicateReply(tx1);
    WaitBatchCompletion(1);

    SendTxRollback(tx1);
    ExpectNoKvRequest();

    WaitTxPredicateReply(tx2);
    WaitBatchCompletion(1);
    SendTxCommit(tx2);

    WaitKvRequest();
    SendKvResponse();
    WaitCommitDone(tx2);
    ExpectNoCommitDone();
}

Y_UNIT_TEST_F(TestBatchingWithChangeConfig, TPartitionTxTestHelper) {
    Init({.ConsumersCount = 2});
    auto txTmp = MakeAndSendWriteTx({});
    auto immTx1 = MakeAndSendImmediateTxOffsetCommit(1, 0, 5);
    SendChangePartitionConfig({.Version=2,
                                .Consumers={
                                {.Consumer="client-0", .Offset=5, .Generation=0},
                                {.Consumer="client-1", .Generation=7}
                                }});
    auto immTx2 = MakeAndSendImmediateTxOffsetCommit(1, 5, 10);
    WaitWriteInfoRequest(txTmp, true);
    SendTxRollback(txTmp);
    WaitBatchCompletion(2);
    ExpectNoBatchCompletion();
    EmulateKVTablet();
    WaitImmediateTxComplete(immTx1, true);
    WaitBatchCompletion(1);
    EmulateKVTablet();
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvPartitionConfigChanged>();
    WaitBatchCompletion(1); // immTx2
    EmulateKVTablet();
    WaitImmediateTxComplete(immTx2, true);
}

Y_UNIT_TEST_F(TestBatchingWithProposeConfig, TPartitionTxTestHelper) {
    Init({.ConsumersCount = 2});
    auto txTmp = MakeAndSendWriteTx({});
    auto immTx1 = MakeAndSendImmediateTxOffsetCommit(1, 0, 5);

    auto proposeTxId = GetTxId();
    auto event = std::make_unique<TEvPQ::TEvProposePartitionConfig>(1, proposeTxId);

    event->TopicConverter = TopicConverter;
    auto copy = Config;
    copy.SetVersion(10);
    auto* newConsumer = copy.AddConsumers();

    newConsumer->SetName("client-0");
    newConsumer->SetGeneration(0);

    event->Config = std::move(copy);
    SendEvent(event.release());
    auto immTx2 = MakeAndSendImmediateTxOffsetCommit(1, 5, 10);

    WaitWriteInfoRequest(txTmp, true);
    SendTxRollback(txTmp);
    WaitBatchCompletion(2);
    ExpectNoBatchCompletion();
    EmulateKVTablet();
    WaitImmediateTxComplete(immTx1, true);

    SendCommitTx(1, proposeTxId);
    //ToDo - wait propose result;
    WaitBatchCompletion(1);
    EmulateKVTablet();
    WaitCommitTxDone({.TxId=proposeTxId});
    WaitBatchCompletion(1);
    EmulateKVTablet();
    WaitImmediateTxComplete(immTx2, true);
}

Y_UNIT_TEST_F(GetUsedStorage, TPartitionFixture) {
    auto* actor = CreatePartition({
                    .Partition=TPartitionId{2, TWriteId{0, 10}, 100'001},
                    .Begin=0, .End=10,
                    //
                    // partition configuration
                    //
                    .Config={.Version=1, .Consumers={}, .MeteringMode = NKikimrPQ::TPQTabletConfig::METERING_MODE_RESERVED_CAPACITY}
                    },
                    //
                    // tablet configuration
                    //
                    {.Version=2, .Consumers={}, .MeteringMode = NKikimrPQ::TPQTabletConfig::METERING_MODE_RESERVED_CAPACITY}
    );

    auto now = TInstant::Now();

    // Check integer overflow when reserved size great than used size
    // LOGBROKER-9105
    auto usedStorage = actor->GetUsedStorage(now + TDuration::Minutes(1));
    UNIT_ASSERT_VALUES_EQUAL(0, usedStorage);


} // GetPartitionWriteInfoErrors

} // End of suite

} // namespace
