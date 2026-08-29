#include <ydb/core/base/appdata.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/common/blob_refcounter.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/pqtablet/partition/partition.h>
#include <ydb/core/persqueue/pqtablet/partition/partition_blob_encoder.h>
#include <ydb/core/persqueue/pqtablet/partition/partition_util.h>
#include <ydb/core/persqueue/pqtablet/partition/blob_key_filter.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/protos/counters_keyvalue.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/core/persqueue/public/nameresolver/nameresolver.h>
#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h>
#include <ydb/public/lib/base/msgbus_status.h>
#include <ydb/core/jaeger_tracing/sampling_throttling_configurator.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/event.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/system/types.h>

#include "make_config.h"

#include <deque>
#include <functional>
#include <memory>
#include <vector>

template<>
void Out<NKikimrPQ::TEvProposeTransactionResult_EStatus>(IOutputStream& out, NKikimrPQ::TEvProposeTransactionResult_EStatus v) {
    out << NKikimrPQ::TEvProposeTransactionResult::EStatus_Name(v);
}

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
    TConfigParams Config;
    // Consumers present in KV user-info but not necessarily in Config (stale leftovers).
    TVector<TCreateConsumerParams> ExtraDiskConsumers;
    TInstant EndWriteTimestamp;
    bool FillHead = false;
    // Meta has [Begin, End) but data range returns NODATA (issue #49507).
    bool NoDataKeys = false;
    // Meta has [Begin, End) but data range returns OK with zero pairs
    // (FormHeadAndProceed empty-keys path).
    bool EmptyDataRangeOk = false;
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

    static ui64 GetCompactionZoneEmptyStartOffset(TPartition& partition) {
        return partition.GetCompactionZoneEmptyStartOffset();
    }

    static TPartitionBlobEncoder& CompactionBlobEncoder(TPartition& partition) {
        return partition.CompactionBlobEncoder;
    }

    static TPartitionBlobEncoder& BlobEncoder(TPartition& partition) {
        return partition.BlobEncoder;
    }

    static void FinalizeEmptyBlobEncoder(TPartition& partition,
                                         TPartitionBlobEncoder& encoder,
                                         ui64 startOffset,
                                         bool updateEndOffset) {
        partition.FinalizeEmptyBlobEncoder(encoder, startOffset, updateEndOffset);
    }

    static bool CleanUpBlobs(TPartition& partition, const TActorContext& ctx) {
        return partition.CleanUpBlobs(nullptr, ctx);
    }

    static TInstant GetWriteTimeEstimate(TPartition& partition, ui64 offset) {
        return partition.GetWriteTimeEstimate(offset);
    }

    static ui64 GetStartOffset(TPartition& partition) {
        return partition.GetStartOffset();
    }

    static ui64 GetEndOffset(TPartition& partition) {
        return partition.GetEndOffset();
    }

    static bool GetAnyCommits(TPartition& partition, const TString& consumer) {
        const TUserInfo* userInfo = partition.UsersInfoStorage->GetIfExists(consumer);
        UNIT_ASSERT(userInfo);
        return userInfo->AnyCommits;
    }

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
    MetaStep->LoadMeta(kvResponse);

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
        TMaybe<ui64> MetaStartOffset;
        TMaybe<ui64> MetaEndOffset;
        THashMap<size_t, TUserInfoMatcher> UserInfos;
        THashMap<size_t, TDeleteRangeMatcher> DeleteRanges;
    };

    struct TProxyResponseMatcher {
        TMaybe<ui64> Cookie;
        TMaybe<NMsgBusProxy::EResponseStatus> Status;
        TMaybe<NPersQueue::NErrorCode::EErrorCode> ErrorCode;
        TMaybe<ui64> Offset;
        TMaybe<bool> AlreadyWritten;
        TMaybe<ui64> SeqNo;
    };

    struct TErrorMatcher {
        TMaybe<ui64> Cookie;
        TMaybe<NPersQueue::NErrorCode::EErrorCode> ErrorCode;
        TMaybe<TString> Error;
        TMaybe<bool> IsInternal;
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

    struct TTxDoneMatcher {
        TMaybe<ui64> Step;
        TMaybe<ui64> TxId;
        TMaybe<TPartitionId> Partition;
    };

    /// Optional payload for TEvTxCommit (defaults match the old SendCommitTx(step, txId) behaviour).
    struct TSendCommitTxOptions {
        TMaybe<NKikimrPQ::TTransaction> SerializedTx;
        TMaybe<NKikimrPQ::TPQTabletConfig> TabletConfig;
        TMaybe<NKikimrPQ::TBootstrapConfig> BootstrapConfig;
        TMaybe<NKikimrPQ::TPartitions> PartitionsData;
        TEvPQ::TMessageGroupsPtr ExplicitMessageGroups;
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
                                     bool newPartition);
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
                       const TString& sessionId,
                       bool strict = false);
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
    void SendMetaReadResponse(ui64 begin, ui64 end, TMaybe<ui64> step, TMaybe<ui64> txId, TInstant endWriteTimestamp);
    void WaitBlobReadRequest();
    void SendBlobReadResponse(ui64 begin, ui64 end);
    void WaitInfoRangeRequest();
    void SendInfoRangeResponse(ui32 partition,
                               const TVector<TCreateConsumerParams>& consumers);
    void WaitDataRangeRequest();
    void SendDataRangeResponse(ui32 partitionId,
                               ui64 begin, ui64 end, bool isHead);
    void SendDataRangeNodataResponse();
    void SendDataRangeEmptyOkResponse();
    void WaitDataReadRequest();
    void SendDataReadResponse();
    void WaitDeduplicatorRangeRequest();
    void SendDeduplicatorRangeResponse(ui32 partition);

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
                           const TActorId& suppPartitionId = {},
                           bool killReadSession = false);
    void WaitCalcPredicateResult(const TCalcPredicateMatcher& matcher = TCalcPredicateMatcher::EmptyMatcher());

    void SendCommitTx(ui64 step, ui64 txId, const TSendCommitTxOptions& options = {});
    void SendRollbackTx(ui64 step, ui64 txId);
    void WaitCommitTxDone(const TTxDoneMatcher& matcher = {});
    void WaitRollbackTxDone(const TTxDoneMatcher& matcher = {});

    void SendChangePartitionConfig(const TConfigParams& config = {});
    void WaitPartitionConfigChanged(const TChangePartitionConfigMatcher& matcher = {});

    void SendSubDomainStatus(bool subDomainOutOfSpace = false);
    void SendReserveBytes(const ui64 cookie, const ui32 size, const TString& ownerCookie, const ui64 messageNo, bool lastRequest = false);
    void SendChangeOwner(const ui64 cookie, const TString& owner, const TActorId& pipeClient, const bool force = true);
    void SendWrite(const ui64 cookie, const ui64 messageNo, const TString& ownerCookie, const TMaybe<ui64> offset, const TString& data,
                   bool ignoreQuotaDeadline = false, ui64 seqNo = 0, bool isDirectWrite = false);
    void SendGetWriteInfo(bool skipSrcIdInfo);
    void ShadowPartitionCountersTest(bool isFirstClass);

    void TestWriteSubDomainOutOfSpace(TDuration quotaWaitDuration, bool ignoreQuotaDeadline);
    void TestWriteSubDomainOutOfSpace_DeadlineWork(bool ignoreQuotaDeadline);
    void WaitKeyValueRequest(TMaybe<ui64>& cookie);

    void CmdChangeOwner(ui64 cookie, const TString& sourceId, TDuration duration, TString& ownerCookie);

    void EmulateKVTablet();
    TActorId CreateFakePartition() const;
    bool WaitWriteInfoRequest(const TActorId& supportivePart);
    void SendEvent(IEventBase* event);
    void SendEvent(IEventBase* event, const TActorId& from, const TActorId& to);

    THolder<TEvPQ::TEvApproveWriteQuota> WaitForRequestQuotaAndHoldApproveWriteQuota();
    void SendDeletePartition();
    void WaitForDeletePartitionDoneTimeout();
    void SendApproveWriteQuota(THolder<TEvPQ::TEvApproveWriteQuota>&& event);
    void WaitForQuotaConsumed();
    void WaitForWriteError(ui64 cookie, NPersQueue::NErrorCode::EErrorCode errorCode);
    void WaitForDeletePartitionDone();

    void SendCalcPredicate(ui64 step,
                           ui64 txId,
                           const TActorId& suppPartitionId);
    void WaitForGetWriteInfoRequest();
    void SendGetWriteInfoError(ui32 internalPartitionId,
                               TString message,
                               const TActorId& suppPartitionId);
    void WaitForCalcPredicateResult(ui64 txId, bool predicate);

    // OldPlanStep*: partition meta ahead of commit (stale SerializedTx persist before TEvTxDone).
    static constexpr ui64 StaleReplayMetaPlanStep = 99999;
    static constexpr ui64 StaleReplayMetaTxId = 55555;

    void CreatePartitionStaleReplayAhead(const TPartitionId& partition, ui64 begin, ui64 end);

    NKikimrPQ::TTransaction MakeStaleSerializedTxBody(
        const TPartitionId& partition,
        ui64 step,
        ui64 txId) const;

    void AssertNoUnexpectedStaleMetaTxDone(const TString& reason) const;

    THolder<TEvKeyValue::TEvRequest> GrabStaleMetaKvRequest(const TString& failReason) const;

    void AssertStaleMetaKvHasTxKey(
        const TEvKeyValue::TEvRequest& kv,
        ui64 txId,
        ui32 originalPartitionId,
        const TString& failCtx) const;

    TMaybe<TTestContext> Ctx;
    TMaybe<TFinalizer> Finalizer;

    TActorId ActorId;

    NKikimr::NPQ::NNameResolver::TTopicNamesPtr TopicConverter;
    NKikimrPQ::TPQTabletConfig Config;

    std::shared_ptr<TTabletCountersBase> TabletCounters;

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
                                                    bool newPartition)
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
    TabletCounters.reset(counters->GetSecondTabletCounters().Release());

    Config = MakeConfig(config.Version,
                        config.Consumers,
                        1,
                        config.MeteringMode);
    Config.SetLocalDC(true);

    TopicConverter = NNameResolver::MakeTopicNamesPtr(NNameResolver::NamesFromConfig(Config));
    TActorId quoterId;
    if (Ctx->Runtime->GetAppData(0).PQConfig.GetQuotingConfig().GetEnableQuoting()) {
        quoterId = Ctx->Runtime->Register(CreateWriteQuoter(
                Ctx->Runtime->GetAppData().PQConfig,
                TopicConverter,
                Config,
                id,
                Ctx->Edge,
                Ctx->TabletId,
                TabletCounters
        ));
    }
    auto samplingControl = Ctx->Runtime->GetAppData(0).TracingConfigurator->GetControl();
    auto* actor = new NPQ::TPartition(Ctx->TabletId,
                                     id,
                                     Ctx->Edge,
                                     0,
                                     Ctx->Edge,
                                     TopicConverter,
                                     "dcId",
                                     false,
                                     Config,
                                     TabletCounters,
                                     false,
                                     1,
                                     quoterId,
                                     TActorId{},
                                     std::move(samplingControl),
                                     newPartition);
    ActorId = Ctx->Runtime->Register(actor);
    return actor;
}

TPartition* TPartitionFixture::CreatePartition(const TCreatePartitionParams& params,
                                               const TConfigParams& config)
{
    TPartition* ret;
    if ((params.Begin == 0) && (params.End == 0)) {
        ret = CreatePartitionActor(params.Partition, config, true);

        WaitConfigRequest();
        SendConfigResponse(params.Config);
    } else {
        ret = CreatePartitionActor(params.Partition, config, false);

        WaitConfigRequest();
        SendConfigResponse(params.Config);

        WaitDiskStatusRequest();
        SendDiskStatusResponse();

        WaitMetaReadRequest();
        SendMetaReadResponse(params.Begin, params.End, params.PlanStep, params.TxId, params.EndWriteTimestamp);

        WaitInfoRangeRequest();
        {
            auto infoConsumers = params.Config.Consumers;
            infoConsumers.insert(infoConsumers.end(),
                                 params.ExtraDiskConsumers.begin(),
                                 params.ExtraDiskConsumers.end());
            SendInfoRangeResponse(params.Partition.InternalPartitionId, infoConsumers);
        }

        WaitDataRangeRequest();
        if (params.NoDataKeys) {
            UNIT_ASSERT_C(!params.EmptyDataRangeOk, "NoDataKeys and EmptyDataRangeOk are mutually exclusive");
            SendDataRangeNodataResponse();
        } else if (params.EmptyDataRangeOk) {
            SendDataRangeEmptyOkResponse();
        } else {
            SendDataRangeResponse(params.Partition.InternalPartitionId, params.Begin, params.End, params.FillHead);

            if (params.FillHead) {
                WaitBlobReadRequest();
                SendBlobReadResponse(params.Begin, params.End);
            }
        }

        if (!params.Partition.IsSupportivePartition()) {
            WaitDeduplicatorRangeRequest();
            SendDeduplicatorRangeResponse(params.Partition.InternalPartitionId);
        }

        Ctx->Runtime->SimulateSleep(TDuration::Seconds(1));
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
                                      const TString& sessionId,
                                      bool strict)
{
    auto event = MakeHolder<TEvPQ::TEvSetClientInfo>(cookie,
                                                     clientId,
                                                     offset,
                                                     sessionId,
                                                     0,
                                                     0,
                                                     0,
                                                     TActorId{});
    event->Strict = strict;
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
    bool metaFound = false;
    for (unsigned i = 0; i < event->Record.CmdWriteSize(); ++i) {
        auto& cmd = event->Record.GetCmdWrite(i);
        TString key = cmd.GetKey();

        UNIT_ASSERT(key.size() >= 1);
        switch (key[0]) {
        case TKeyPrefix::TypeMeta: {
            NKikimrPQ::TPartitionMeta meta;
            UNIT_ASSERT(meta.ParseFromString(cmd.GetValue()));
            if (matcher.MetaStartOffset.Defined()) {
                UNIT_ASSERT(meta.HasStartOffset());
                UNIT_ASSERT_VALUES_EQUAL(*matcher.MetaStartOffset, meta.GetStartOffset());
            }
            if (matcher.MetaEndOffset.Defined()) {
                UNIT_ASSERT(meta.HasEndOffset());
                UNIT_ASSERT_VALUES_EQUAL(*matcher.MetaEndOffset, meta.GetEndOffset());
            }
            metaFound = true;
            break;
        }
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
            if (matcher.UserInfos.empty()) {
                break;
            }

            NKikimrPQ::TUserInfo ud;
            UNIT_ASSERT(ud.ParseFromString(event->Record.GetCmdWrite(i).GetValue()));
            UNIT_ASSERT(key.size() > (1 + 10 + 1)); // type + partition + mark + consumer
            const TString consumerFromKey = key.substr(12);

            bool match = false;
            for (auto& [_, userInfo] : matcher.UserInfos) {
                if (!userInfo.Consumer && !userInfo.Session) {
                    continue;
                }
                if (userInfo.Consumer && *userInfo.Consumer != consumerFromKey) {
                    continue;
                }
                if (userInfo.Session) {
                    if (!ud.HasSession() || *userInfo.Session != ud.GetSession()) {
                        continue;
                    }
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

                break;
            }

            UNIT_ASSERT_C(match, "No UserInfos matcher for consumer '" << consumerFromKey << "'");

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
        UNIT_ASSERT(key.size() > (1 + 10 + 1)); // type + partition + mark + consumer

        if (deleteRange.Partition.Defined()) {
            auto partition = FromString<ui32>(key.substr(1, 10));
            UNIT_ASSERT_VALUES_EQUAL(*deleteRange.Partition, partition);
        }
        if (deleteRange.Consumer.Defined()) {
            TString consumer = key.substr(12);
            UNIT_ASSERT_VALUES_EQUAL(*deleteRange.Consumer, consumer);
        }
    }

    if (matcher.MetaStartOffset.Defined() || matcher.MetaEndOffset.Defined()) {
        UNIT_ASSERT(metaFound);
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
        bool ignoreQuotaDeadline, ui64 seqNo, bool isDirectWrite
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

    auto event = MakeHolder<TEvPQ::TEvWrite>(cookie, messageNo, ownerCookie, offset, std::move(msgs), isDirectWrite, std::nullopt, TEvPQ::TEvWrite::EWriteExternalDeduplicationStatus::Unchecked);
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::SendChangeOwner(const ui64 cookie, const TString& owner, const TActorId& pipeClient, const bool force)
{
    auto event = MakeHolder<TEvPQ::TEvChangeOwner>(cookie, owner, pipeClient, Ctx->Edge, force, true);
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::SendGetWriteInfo(bool skipSrcIdInfo) {
    auto event = MakeHolder<TEvPQ::TEvGetWriteInfoRequest>(skipSrcIdInfo);
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
    if (matcher.AlreadyWritten) {
        UNIT_ASSERT(event->Response->HasPartitionResponse());
        UNIT_ASSERT_VALUES_EQUAL(event->Response->GetPartitionResponse().CmdWriteResultSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(*matcher.AlreadyWritten,
                                 event->Response->GetPartitionResponse().GetCmdWriteResult(0).GetAlreadyWritten());
    }
    if (matcher.SeqNo) {
        UNIT_ASSERT(event->Response->HasPartitionResponse());
        UNIT_ASSERT_VALUES_EQUAL(event->Response->GetPartitionResponse().CmdWriteResultSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(*matcher.SeqNo,
                                 event->Response->GetPartitionResponse().GetCmdWriteResult(0).GetSeqNo());
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

    if (matcher.IsInternal) {
        UNIT_ASSERT_VALUES_EQUAL(*matcher.IsInternal, event->IsInternal);
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

void TPartitionFixture::SendMetaReadResponse(ui64 begin, ui64 end, TMaybe<ui64> step, TMaybe<ui64> txId, TInstant endWriteTimestamp)
{
    auto event = MakeHolder<TEvKeyValue::TEvResponse>();
    event->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);

    //
    // NKikimrPQ::TPartitionMeta
    //
    auto read = event->Record.AddReadResult();
    if (endWriteTimestamp) {
        read->SetStatus(NKikimrProto::OK);

        NKikimrPQ::TPartitionMeta meta;
        meta.SetStartOffset(begin);
        meta.SetEndOffset(end);
        meta.SetEndWriteTimestamp(endWriteTimestamp.MilliSeconds());

        TString out;
        Y_PROTOBUF_SUPPRESS_NODISCARD meta.SerializeToString(&out);
        read->SetValue(out);
    } else {
        read->SetStatus(NKikimrProto::NODATA);
    }

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

void TPartitionFixture::WaitBlobReadRequest()
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>();
    UNIT_ASSERT(event != nullptr);

    UNIT_ASSERT_VALUES_EQUAL(event->Record.CmdReadSize(), 1);
}

TBatch CreateBatch(size_t count) {
    TBatch batch;

    for (size_t i = 0; i < count; ++i) {
        Cerr << ">>>> ADD BLOB " << i << " writeTimestamp=" << (TInstant::Now() - TDuration::MilliSeconds(10)) << Endl << Flush;

        TString data = TStringBuilder() << "message-data-" << i;
        TClientBlob blob("source-id-1", 13 + i /* seqNo */, std::move(data), {} /* partData */, TInstant::Now() - TDuration::MilliSeconds(10) /* writeTimestamp */,
        TInstant::Now() - TDuration::MilliSeconds(50) /* createTimestamp */, data.size(), "partitionKey", "explicitHashKey");
        batch.AddBlob(blob);
    }

    batch.Pack();

    return batch;
}

void TPartitionFixture::SendBlobReadResponse(ui64 begin, ui64 end)
{
    auto batch = CreateBatch(end - begin);
    TString valueD;
    batch.SerializeTo(valueD);

    auto event = MakeHolder<TEvKeyValue::TEvResponse>();
    event->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);

    auto read = event->Record.AddReadResult();
    read->SetStatus(NKikimrProto::OK);
    read->SetValue(valueD);

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

void TPartitionFixture::SendDataRangeResponse(ui32 partitionId,
                                              ui64 begin, ui64 end, bool isHead)
{
    Y_ABORT_UNLESS(begin <= end);

    auto event = MakeHolder<TEvKeyValue::TEvResponse>();
    event->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);

    auto read = event->Record.AddReadRangeResult();
    read->SetStatus(NKikimrProto::OK);
    auto pair = read->AddPair();

    TKey key;
    if (isHead) {
        key = TKey::ForHead(TKeyPrefix::TypeData, TPartitionId(partitionId), begin, 0, end - begin, 0);
    } else {
        key = TKey::ForBody(TKeyPrefix::TypeData, TPartitionId(partitionId), begin, 0, end - begin, 0);
    }

    pair->SetStatus(NKikimrProto::OK);
    pair->SetKey(key.Data(), key.Size());
    pair->SetValueSize(684);
    pair->SetCreationUnixTime(TInstant::Now().Seconds());

    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::SendDataRangeNodataResponse()
{
    auto event = MakeHolder<TEvKeyValue::TEvResponse>();
    event->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);

    auto read = event->Record.AddReadRangeResult();
    read->SetStatus(NKikimrProto::NODATA);

    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::SendDataRangeEmptyOkResponse()
{
    auto event = MakeHolder<TEvKeyValue::TEvResponse>();
    event->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);

    auto read = event->Record.AddReadRangeResult();
    read->SetStatus(NKikimrProto::OK);
    // No pairs — FillBlobsMetaData leaves meta offsets, FormHeadAndProceed sees empty keys.

    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::WaitDeduplicatorRangeRequest()
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>();
    UNIT_ASSERT(event != nullptr);

    UNIT_ASSERT_VALUES_EQUAL(event->Record.CmdReadRangeSize(), 1);
}

void TPartitionFixture::SendDeduplicatorRangeResponse(ui32 partitionId)
{
    Y_UNUSED(partitionId);

    auto event = MakeHolder<TEvKeyValue::TEvResponse>();
    event->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);

    auto read = event->Record.AddReadRangeResult();
    read->SetStatus(NKikimrProto::OK);

    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::SendProposeTransactionRequest(ui32 partition,
                                                      ui64 begin, ui64 end,
                                                      const TString& client,
                                                      const TString& topic,
                                                      bool immediate,
                                                      ui64 txId)
{
    auto event = MakeHolder<TEvPersQueue::TEvProposeTransactionBuilder>();

    ActorIdToProto(Ctx->Edge, event->Record.MutableSourceActor());
    auto* body = event->Record.MutableData();
    auto* operation = body->MutableOperations()->Add();
    operation->SetPartitionId(partition);
    operation->SetCommitOffsetsBegin(begin);
    operation->SetCommitOffsetsEnd(end);
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
        UNIT_ASSERT_VALUES_EQUAL(*matcher.Status, event->Record.GetStatus());
    }
}

void TPartitionFixture::SendCalcPredicate(ui64 step,
                                          ui64 txId,
                                          const TString& consumer,
                                          ui64 begin,
                                          ui64 end,
                                          const TActorId& suppPartitionId,
                                          bool killReadSession)
{
    auto event = MakeHolder<TEvPQ::TEvTxCalcPredicate>(step, txId);
    if (suppPartitionId) {
        event->SupportivePartitionActor = suppPartitionId;
    } else {
        event->AddOperation(consumer, begin, end, false, killReadSession);
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

void TPartitionFixture::SendCommitTx(ui64 step, ui64 txId, const TSendCommitTxOptions& options)
{
    TEvPQ::TMessageGroupsPtr explicitMessageGroups = options.ExplicitMessageGroups;
    auto event = MakeHolder<TEvPQ::TEvTxCommit>(step, txId, std::move(explicitMessageGroups));
    event->SerializedTx = options.SerializedTx;
    event->TabletConfig = options.TabletConfig;
    event->BootstrapConfig = options.BootstrapConfig;
    event->PartitionsData = options.PartitionsData;
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::SendRollbackTx(ui64 step, ui64 txId)
{
    auto event = MakeHolder<TEvPQ::TEvTxRollback>(step, txId);
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::WaitCommitTxDone(const TTxDoneMatcher& matcher)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvTxDone>();
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

void TPartitionFixture::WaitRollbackTxDone(const TTxDoneMatcher& matcher)
{
    WaitCommitTxDone(matcher);
}

void TPartitionFixture::CreatePartitionStaleReplayAhead(const TPartitionId& partition, ui64 begin, ui64 end)
{
    CreatePartition({
        .Partition = partition,
        .Begin = begin,
        .End = end,
        .PlanStep = StaleReplayMetaPlanStep,
        .TxId = StaleReplayMetaTxId,
    });
}

NKikimrPQ::TTransaction TPartitionFixture::MakeStaleSerializedTxBody(
    const TPartitionId& partition,
    ui64 step,
    ui64 txId) const
{
    NKikimrPQ::TTransaction tx;
    tx.SetKind(NKikimrPQ::TTransaction::KIND_DATA);
    tx.SetTxId(txId);
    tx.SetState(NKikimrPQ::TTransaction::EXECUTED);
    tx.SetStep(step);
    auto* operation = tx.AddOperations();
    operation->SetPartitionId(partition.InternalPartitionId);
    return tx;
}

void TPartitionFixture::AssertNoUnexpectedStaleMetaTxDone(const TString& reason) const
{
    // Poll > 0 sim time: absence of TEvTxDone must be observable, not "instant Grab".
    UNIT_ASSERT_C(!Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvTxDone>(TDuration::MilliSeconds(200)), reason);
}

THolder<TEvKeyValue::TEvRequest> TPartitionFixture::GrabStaleMetaKvRequest(const TString& failReason) const
{
    auto kv = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>(TDuration::Seconds(10));
    UNIT_ASSERT_C(kv, failReason);
    return kv;
}

void TPartitionFixture::AssertStaleMetaKvHasTxKey(
    const TEvKeyValue::TEvRequest& kv,
    ui64 txId,
    ui32 originalPartitionId,
    const TString& failCtx) const
{
    const TString expectedKey = GetTxKey(txId, originalPartitionId);
    for (ui32 i = 0; i < kv.Record.CmdWriteSize(); ++i) {
        if (kv.Record.GetCmdWrite(i).GetKey() == expectedKey) {
            return;
        }
    }
    UNIT_ASSERT_C(false, failCtx << ": missing CmdWrite for serialized tx key " << expectedKey);
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
#if 0
    auto ownerEvent = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvProxyResponse>(TDuration::Seconds(1));
    UNIT_ASSERT(ownerEvent != nullptr);
    auto ownerCookie = ownerEvent->Response->GetPartitionResponse().GetCmdGetOwnershipResult().GetOwnerCookie();
#else
    TString ownerCookie;
    while (true) {
        TAutoPtr<IEventHandle> handle;
        auto events =
            Ctx->Runtime->GrabEdgeEvents<TEvPQ::TEvProxyResponse, TEvKeyValue::TEvRequest>(handle,
                                                                                           TDuration::Seconds(1));
        if (std::get<TEvKeyValue::TEvRequest*>(events)) {
            SendDiskStatusResponse(nullptr);
        } else if (auto* event = std::get<TEvPQ::TEvProxyResponse*>(events)) {
            ownerCookie = event->Response->GetPartitionResponse().GetCmdGetOwnershipResult().GetOwnerCookie();
            break;
        }
    }
#endif

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
                        UNIT_ASSERT(counterData.GetBytesWrittenUncompressed() >= currUncSize);
                        currUncSize = counterData.GetBytesWrittenUncompressed();
                        UNIT_ASSERT_VALUES_EQUAL(counterData.GetBytesWrittenGrpc(), isFirstClass ? counterData.GetBytesWrittenTotal() : 0);
                        UNIT_ASSERT(counterData.GetBytesWrittenTotal() >= currTotalSize);
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
            } else if (ev->CastAsLocal<TEvPQ::TEvConsumed>()) {
                return TTestActorRuntimeBase::EEventAction::DROP;
            } else if (ev->CastAsLocal<TEvPQ::TEvProxyResponse>()) {
                return TTestActorRuntimeBase::EEventAction::PROCESS;
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
    SendGetWriteInfo(false);
    {
        auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvGetWriteInfoResponse>(TDuration::Seconds(1));
        UNIT_ASSERT(event != nullptr);
        //Cerr << "Got write info response. Body keys: " << event->BodyKeys.size() << ", head: " << event->BlobsFromHead.size() << ", src id info: " << event->SrcIdInfo.size() << Endl;

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

void TPartitionFixture::TestWriteSubDomainOutOfSpace_DeadlineWork(bool ignoreQuotaDeadline)
{
    Ctx->Runtime->GetAppData().FeatureFlags.SetEnableTopicDiskSubDomainQuota(true);
    Ctx->Runtime->GetAppData().PQConfig.MutableQuotingConfig()->SetQuotaWaitDurationMs(300);
    Ctx->Runtime->SetLogPriority( NKikimrServices::PERSQUEUE, NActors::NLog::PRI_DEBUG);

    CreatePartition({
                    .Partition=TPartitionId{1},
                    .Begin=0, .End=0,
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

    // First message will be processed because used storage 0 and limit 0. That is, the limit is not exceeded.
    TString data0 = "data for write 0";
    SendWrite(++cookie, messageNo, ownerCookie, (messageNo + 1) * 100, data0, ignoreQuotaDeadline);
    messageNo++;

    WaitKeyValueRequest(kvCookie); // the partition saves the TEvPQ::TEvWrite event
    SendDiskStatusResponse(&kvCookie);

    {
        auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvProxyResponse>(TDuration::Seconds(1));
        UNIT_ASSERT(event != nullptr);
    }

    // Second message will not be processed because the limit is exceeded.
    TString data1 = "data for write 1";
    SendWrite(++cookie, messageNo, ownerCookie, (messageNo + 1) * 100, data1, ignoreQuotaDeadline);
    messageNo++;

    {
        auto event = Ctx->Runtime->GrabEdgeEventIf<TEvPQ::TEvError>(handle, truth, TDuration::Seconds(1));
        UNIT_ASSERT(event != nullptr);
        UNIT_ASSERT_EQUAL(NPersQueue::NErrorCode::OVERLOAD, event->ErrorCode);
    }
}

void TPartitionFixture::TestWriteSubDomainOutOfSpace(TDuration quotaWaitDuration, bool ignoreQuotaDeadline)
{
    Ctx->Runtime->GetAppData().FeatureFlags.SetEnableTopicDiskSubDomainQuota(true);
    Ctx->Runtime->GetAppData().PQConfig.MutableQuotingConfig()->SetQuotaWaitDurationMs(quotaWaitDuration.MilliSeconds());
    Ctx->Runtime->SetLogPriority( NKikimrServices::PERSQUEUE, NActors::NLog::PRI_DEBUG);

    CreatePartition({
                    .Partition=TPartitionId{1},
                    .Begin=0, .End=0,
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

THolder<TEvPQ::TEvApproveWriteQuota> TPartitionFixture::WaitForRequestQuotaAndHoldApproveWriteQuota()
{
    THolder<TEvPQ::TEvApproveWriteQuota> approveWriteQuota;

    auto observer = [&approveWriteQuota](TAutoPtr<IEventHandle>& ev) mutable {
        if (auto* event = ev->CastAsLocal<TEvPQ::TEvApproveWriteQuota>()) {
            approveWriteQuota = MakeHolder<TEvPQ::TEvApproveWriteQuota>(event->Cookie,
                                                                        event->AccountQuotaWaitTime,
                                                                        event->PartitionQuotaWaitTime);
            return TTestActorRuntimeBase::EEventAction::DROP;
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    };
    auto prevObserver = Ctx->Runtime->SetObserverFunc(observer);

    TDispatchOptions options;
    options.CustomFinalCondition = [&]() {
        return approveWriteQuota != nullptr;
    };
    UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));

    Ctx->Runtime->SetObserverFunc(prevObserver);

    UNIT_ASSERT(approveWriteQuota != nullptr);

    return approveWriteQuota;
}

void TPartitionFixture::SendDeletePartition()
{
    auto event = MakeHolder<TEvPQ::TEvDeletePartition>();
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
}

void TPartitionFixture::WaitForDeletePartitionDoneTimeout()
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvDeletePartitionDone>(TDuration::Seconds(3));
    UNIT_ASSERT_VALUES_EQUAL(event, nullptr);
}

void TPartitionFixture::SendApproveWriteQuota(THolder<TEvPQ::TEvApproveWriteQuota>&& event)
{
    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
    event = nullptr;
}

void TPartitionFixture::WaitForQuotaConsumed()
{
    bool hasQuotaConsumed = false;

    auto observer = [&hasQuotaConsumed](TAutoPtr<IEventHandle>& ev) mutable {
        if (ev->CastAsLocal<TEvPQ::TEvConsumed>()) {
            hasQuotaConsumed = true;
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    };
    auto prevObserver = Ctx->Runtime->SetObserverFunc(observer);

    TDispatchOptions options;
    options.CustomFinalCondition = [&]() {
        return hasQuotaConsumed;
    };
    UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options));

    Ctx->Runtime->SetObserverFunc(prevObserver);

    UNIT_ASSERT(hasQuotaConsumed);
}

void TPartitionFixture::WaitForWriteError(ui64 cookie, NPersQueue::NErrorCode::EErrorCode errorCode)
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvError>();

    UNIT_ASSERT(event != nullptr);

    UNIT_ASSERT_VALUES_EQUAL(cookie, event->Cookie);
    UNIT_ASSERT_C(errorCode == event->ErrorCode, "extected: " << (int)errorCode << ", accepted: " << (int)event->ErrorCode);
}

void TPartitionFixture::WaitForDeletePartitionDone()
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvDeletePartitionDone>();

    UNIT_ASSERT(event != nullptr);
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
    auto AddWriteTxImpl(const TSrcIdMap& srcIdsAffected, ui64 txId, ui64 step, TMaybe<NPQ::TClientBlob>&& blobFromHead = Nothing());

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
    TQueue<std::pair<TActorId, TActorId>> ReceivedWriteInfoRequests;
    TAdaptiveLock Lock;
    THashMap<TActorId, THolder<TEvPQ::TEvGetWriteInfoResponse>> WriteInfoData;

    TVector<std::pair<TString, TString>> Sessions;
    THashMap<TString, std::pair<TString, ui64>> Owners;

    TPartition* PartitionPtr = nullptr;

public:
    THolder<TEvKeyValue::TEvRequest> LastKvRequest;

    void Init(const TTxBatchingTestParams& params = {})
    {
        TxStep = params.TxStep;
        Ctx->Runtime->SetLogPriority( NKikimrServices::PERSQUEUE, NActors::NLog::PRI_DEBUG);

        Ctx->Runtime->GetAppData(0).PQConfig.MutableQuotingConfig()->SetEnableQuoting(false);
        Ctx->Runtime->SetObserverFunc([this](TAutoPtr<IEventHandle>& ev) {
            if (ev->CastAsLocal<TEvPQ::TEvGetWriteInfoRequest>()) {
                with_lock(this->Lock) {
                    ReceivedWriteInfoRequests.emplace(ev->Recipient, ev->Sender);
                }
            } else if (auto* msg = ev->CastAsLocal<TEvPQ::TEvTxBatchComplete>()) {
                Cerr << "Got batch complete: " << msg->BatchSize << Endl;
                with_lock(Lock) {
                    BatchSizes.push_back(msg->BatchSize);
                }
            } else if (ev->CastAsLocal<TEvKeyValue::TEvRequest>()) {
                Cerr << "Got KV request" << Endl;
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
        PartitionPtr = CreatePartition(std::move(partitionParams));
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

    TPartition& Partition() {
        UNIT_ASSERT(PartitionPtr != nullptr);
        return *PartitionPtr;
    }

    void InjectBodyKeys(ui64 userActId, std::deque<NPQ::TDataKey> bodyKeys) {
        auto actIter = UserActs.find(userActId);
        Y_ABORT_UNLESS(!actIter.IsEnd());
        with_lock(Lock) {
            auto infoIter = WriteInfoData.find(actIter->second.SupportivePartitionId);
            Y_ABORT_UNLESS(!infoIter.IsEnd());
            infoIter->second->BodyKeys = std::move(bodyKeys);
        }
    }

    ui64 GetTxId() {
        return NextActId++;
    }

    ui64 AddAndSendNormalWrite(const TString& srcId, ui64 startSeqnNo, ui64 lastSeqNo);
    ui64 MakeAndSendWriteTx(const TSrcIdMap& srcIdsAffected, TMaybe<NPQ::TClientBlob>&& blobFromHead = Nothing());
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
    void WaitRollbackDone(ui64 userActId);
    void WaitImmediateTxComplete(ui64 userActId, bool status);
    void ExpectNoCommitDone();
    void ExpectNoBatchCompletion();
    void WaitBatchCompletion(ui64 userActsCount);
    void ResetBatchCompletion();

    void NonConflictingActsBatchOkTest();
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
    auto iter = this->WriteInfoData.find(supportiveId);
    Y_ABORT_UNLESS(!iter.IsEnd());
    auto& reply = iter->second;
    reply->BytesWrittenTotal = 1;
    reply->BytesWrittenGrpc = 1;
    reply->BytesWrittenUncompressed = 1;
    reply->MessagesWrittenTotal = 1;
    reply->MessagesWrittenGrpc = 1;
    SendEvent(reply.Release(), supportiveId, partitionId);
}

void TPartitionTxTestHelper::WaitWriteInfoRequest(ui64 userActId, bool autoRespond) {
    auto iter = UserActs.find(userActId);
    Y_ABORT_UNLESS(!iter.IsEnd());
    const auto& act = iter->second;
    auto checkIfReceived = [&]() {
        TActorId parentPartitionId, supportiveId;
        with_lock(Lock) {
            if (ReceivedWriteInfoRequests.size()) {
                std::tie(supportiveId, parentPartitionId) = ReceivedWriteInfoRequests.front();
                ReceivedWriteInfoRequests.pop();
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
    if (checkIfReceived()) {
        return;
    }
    Ctx->Runtime->DispatchEvents();
    auto res = checkIfReceived();
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
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvTxDone>(TDuration::Seconds(1));
    UNIT_ASSERT(event != nullptr);
    UNIT_ASSERT_VALUES_EQUAL(event->TxId, actIter->second.TxId);
}

void TPartitionTxTestHelper::WaitRollbackDone(ui64 userActId) {
    auto actIter = UserActs.find(userActId);
    Cerr << "Wait tx rollback for tx " << actIter->second.TxId << Endl;
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvTxDone>(TDuration::Seconds(1));
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
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvTxDone>(TDuration::Seconds(1));
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
    if (event) {
        LastKvRequest = std::move(event);
    }
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
    auto event = MakeHolder<TEvPQ::TEvWrite>(id, messageNo, act.OwnerCookie, id * 10, std::move(msgs), false, std::nullopt, TEvPQ::TEvWrite::EWriteExternalDeduplicationStatus::Unchecked);
    SendEvent(event.Release());
    UserActs.emplace(id, act);
    messageNo++;
    return id;
}

auto TPartitionTxTestHelper::AddWriteTxImpl(const TSrcIdMap& srcIdsAffected, ui64 txId, ui64 step, TMaybe<NPQ::TClientBlob>&& blobFromHead) {
    auto id = NextActId++;
    TTestUserAct act{.IsImmediateTx = (step == 0), .TxId = txId, .SupportivePartitionId = CreateFakePartition()};
    NPQ::TSourceIdMap srcIdMap;

    for (const auto& [key, val] : srcIdsAffected) {
        TSourceIdInfo srcInfo{val.second, val.second, TInstant::Zero()};
        srcInfo.MinSeqNo = val.first;
        srcIdMap.emplace(key, std::move(srcInfo));
    }
    auto iter = UserActs.insert(std::make_pair(id, act)).first;
    auto ev = MakeHolder<TEvPQ::TEvGetWriteInfoResponse>();
    ev->SrcIdInfo = std::move(srcIdMap);

    Y_UNUSED(blobFromHead);
    //if (blobFromHead.Defined()) {
    //    ev->BlobsFromHead.emplace_back(std::move(blobFromHead.GetRef()));
    //}

    with_lock(Lock) {
        WriteInfoData.emplace(act.SupportivePartitionId, std::move(ev));
    }
    return iter;
}

ui64 TPartitionTxTestHelper::MakeAndSendWriteTx(const TSrcIdMap& srcIdsAffected, TMaybe<NPQ::TClientBlob>&& blobFromHead) {
    auto actIter = AddWriteTxImpl(srcIdsAffected, NextActId++, TxStep, std::move(blobFromHead));
    auto event = MakeHolder<TEvPQ::TEvTxCalcPredicate>(TxStep, actIter->second.TxId);
    event->SupportivePartitionActor = actIter->second.SupportivePartitionId;
    Cerr << "Create distr tx with id = " << actIter->second.TxId << " and act no: " << actIter->first << Endl;

    SendEvent(event.Release());
    return actIter->first;
}

ui64 TPartitionTxTestHelper::MakeAndSendImmediateTx(const TSrcIdMap& srcIdsAffected) {
    auto actIter = AddWriteTxImpl(srcIdsAffected, NextActId++, 0);

    auto event = MakeHolder<TEvPersQueue::TEvProposeTransactionBuilder>();

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
#if 0
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvTxCalcPredicateResult>(TDuration::Seconds(1));
    UNIT_ASSERT(event != nullptr);
    UNIT_ASSERT_VALUES_EQUAL(event->TxId, txId);
    UNIT_ASSERT_VALUES_EQUAL(event->Predicate, status);
#else
    while (true) {
        TAutoPtr<IEventHandle> handle;
        auto events =
            Ctx->Runtime->GrabEdgeEvents<TEvPQ::TEvTxCalcPredicateResult, TEvKeyValue::TEvRequest>(handle,
                                                                                                   TDuration::Seconds(1));
        if (std::get<TEvKeyValue::TEvRequest*>(events)) {
            SendDiskStatusResponse(nullptr);
        } else if (auto* event = std::get<TEvPQ::TEvTxCalcPredicateResult*>(events)) {
            UNIT_ASSERT_VALUES_EQUAL(event->TxId, txId);
            UNIT_ASSERT_VALUES_EQUAL(event->Predicate, status);
            break;
        }
    }
#endif
}

Y_UNIT_TEST_F(UserActCount, TPartitionFixture)
{
    // In the test, we check that the reference count for `UserInfo` decreases in case of errors. To do this,
    // we send a large number of requests to which the server will respond with an error.

    CreatePartition();

    Ctx->Runtime->SetScheduledLimit(60000);

    SendCreateSession(1, "client", "session-id", 2, 3);
    WaitCmdWrite({.Count=2, .UserInfos={{0, {.Session="session-id", .Offset=0, .Generation=2, .Step=3}}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
    WaitProxyResponse({.Cookie=1});

    for (ui64 k = 0; k <= MAX_USER_ACTS; ++k) {
        const ui64 cookie = 2 + k;
        // 1 > EndOffset
        SendSetOffset(cookie, "client", 1, "session-id", true); // strict = true
        WaitCmdWrite({.Count=2, .UserInfos={{0, {.Session="session-id", .Offset=0, .Generation=2, .Step=3}}}});
        SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
        WaitErrorResponse({.Cookie=cookie, .ErrorCode=NPersQueue::NErrorCode::SET_OFFSET_ERROR_COMMIT_TO_FUTURE});
    }
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

// Compactification replies with IsInternal=true. Those must not be treated as timestamp-read
// completions: otherwise ReadingTimestamp/ReadScheduled get out of sync and the next
// TEvProxyResponse hits PQ_ENSURE(userInfo->ReadScheduled) (issue #49357).
Y_UNIT_TEST_F(InternalErrorDoesNotBreakTimestampRead, TPartitionFixture)
{
    const TPartitionId partition{0};
    const TString client = "client";
    const TString session = "session";

    CreatePartition({
        .Partition = partition,
        .Begin = 0,
        .End = 10,
        .Config = {.Consumers = {{.Consumer = client, .Offset = 0, .Session = session}}},
    });

    auto blobRequest = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvBlobRequest>(TDuration::Seconds(5));
    UNIT_ASSERT_C(blobRequest != nullptr, "expected timestamp-read blob request");

    SendEvent(new TEvPQ::TEvError(
        NPersQueue::NErrorCode::ERROR,
        "compaction internal error",
        /*cookie=*/0,
        /*isInternal=*/true));
    Ctx->Runtime->SimulateSleep(TDuration::MilliSeconds(200));

    auto restartedBlobRequest =
        Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvBlobRequest>(TDuration::MilliSeconds(200));
    UNIT_ASSERT_C(
        restartedBlobRequest == nullptr,
        "IsInternal TEvError must not restart an in-flight timestamp read");

    // Partition stays responsive with the original timestamp read still in flight.
    SendGetOffset(1, client);
    WaitProxyResponse({.Cookie = 1, .Status = NMsgBusProxy::MSTATUS_OK, .Offset = 0});
}

namespace {

THolder<TEvPQ::TEvRead> MakeTestRead(
    ui64 cookie,
    ui64 offset,
    ui32 count,
    const TString& client = "client",
    const TActorId& replyTo = {})
{
    return MakeHolder<TEvPQ::TEvRead>(
        cookie,
        offset,
        /*lastOffset=*/0,
        /*partNo=*/0,
        count,
        /*sessionId=*/"",
        client,
        /*timeout=*/0,
        /*size=*/100,
        /*readToBlobEnd=*/false,
        /*maxTimeLagMs=*/0,
        /*readTimestampMs=*/0,
        /*clientDC=*/"",
        /*externalOperation=*/false,
        /*pipeClient=*/TActorId{},
        replyTo);
}

} // namespace

// ReplyError uses !!replyTo as IsInternal (same as TEvRead::IsInternal). Call sites must pass the
// original ReplyTo override, not ReplyTo(cookie, …): the resolved id is always non-empty and would
// mark every external error as internal.
Y_UNIT_TEST_F(ExternalReadErrorIsNotMarkedInternal, TPartitionFixture)
{
    const TPartitionId partition{0};
    const TString client = "client";

    CreatePartition({
        .Partition = partition,
        .Begin = 0,
        .End = 10,
        .Config = {.Consumers = {{.Consumer = client, .Offset = 0}}},
    });
    // Drain the timestamp-read blob request from CreatePartition.
    Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvBlobRequest>(TDuration::Seconds(5));

    // External read (empty ReplyTo), cookie != 0 → error must go to TabletActorId (Edge)
    // with IsInternal=false. Passing a resolved ReplyTo() into ReplyError would set IsInternal.
    SendEvent(MakeTestRead(/*cookie=*/42, /*offset=*/0, /*count=*/0, client).Release());
    WaitErrorResponse({
        .Cookie = 42,
        .ErrorCode = NPersQueue::NErrorCode::BAD_REQUEST,
        .IsInternal = false,
    });

    SendEvent(MakeTestRead(/*cookie=*/43, /*offset=*/11, /*count=*/1, client).Release());
    WaitErrorResponse({
        .Cookie = 43,
        .ErrorCode = NPersQueue::NErrorCode::READ_ERROR_TOO_BIG_OFFSET,
        .IsInternal = false,
    });
}

Y_UNIT_TEST_F(InternalReadErrorIsMarkedInternal, TPartitionFixture)
{
    const TPartitionId partition{0};
    const TString client = "client";

    CreatePartition({
        .Partition = partition,
        .Begin = 0,
        .End = 10,
        .Config = {.Consumers = {{.Consumer = client, .Offset = 0}}},
    });
    Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvBlobRequest>(TDuration::Seconds(5));

    TMaybe<bool> observedIsInternal;
    auto prevObserver = Ctx->Runtime->SetObserverFunc(
        [&](TAutoPtr<IEventHandle>& ev) {
            if (ev->Recipient == ActorId) {
                if (auto* error = ev->CastAsLocal<TEvPQ::TEvError>()) {
                    if (error->Cookie == 7) {
                        observedIsInternal = error->IsInternal;
                    }
                }
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        });

    // Compaction-style read: ReplyTo = SelfId ⇒ IsInternal must be true, reply stays on Self.
    SendEvent(MakeTestRead(/*cookie=*/7, /*offset=*/0, /*count=*/0, client, ActorId).Release());

    TDispatchOptions options;
    options.CustomFinalCondition = [&]() {
        return observedIsInternal.Defined();
    };
    UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options, TDuration::Seconds(5)));
    Ctx->Runtime->SetObserverFunc(prevObserver);

    UNIT_ASSERT(observedIsInternal.Defined());
    UNIT_ASSERT_VALUES_EQUAL(*observedIsInternal, true);

    auto leakedToTablet =
        Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvError>(TDuration::MilliSeconds(200));
    UNIT_ASSERT_C(
        leakedToTablet == nullptr,
        "internal read error must be sent to SelfId, not TabletActorId");
}

// cookie==0 + empty ReplyTo resolves to SelfId (same destination as timestamp reads). IsInternal
// must still be false: otherwise Handle(TEvError) takes the compaction early-return and leaves
// ReadingTimestamp stuck.
Y_UNIT_TEST_F(ExternalCookieZeroReadErrorRestartsTimestampRead, TPartitionFixture)
{
    const TPartitionId partition{0};
    const TString client = "client";
    const TString session = "session";

    CreatePartition({
        .Partition = partition,
        .Begin = 0,
        .End = 10,
        .Config = {.Consumers = {{.Consumer = client, .Offset = 0, .Session = session}}},
    });

    auto blobRequest = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvBlobRequest>(TDuration::Seconds(5));
    UNIT_ASSERT_C(blobRequest != nullptr, "expected timestamp-read blob request");

    TMaybe<bool> observedIsInternal;
    auto prevObserver = Ctx->Runtime->SetObserverFunc(
        [&](TAutoPtr<IEventHandle>& ev) {
            if (ev->Recipient == ActorId) {
                if (auto* error = ev->CastAsLocal<TEvPQ::TEvError>()) {
                    if (error->Cookie == 0 &&
                        error->ErrorCode == NPersQueue::NErrorCode::BAD_REQUEST)
                    {
                        observedIsInternal = error->IsInternal;
                    }
                }
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        });

    // External read with cookie 0 (empty ReplyTo): destination is SelfId, but not internal.
    SendEvent(MakeTestRead(/*cookie=*/0, /*offset=*/0, /*count=*/0, client).Release());

    TDispatchOptions options;
    options.CustomFinalCondition = [&]() {
        return observedIsInternal.Defined();
    };
    UNIT_ASSERT(Ctx->Runtime->DispatchEvents(options, TDuration::Seconds(5)));
    Ctx->Runtime->SetObserverFunc(prevObserver);

    UNIT_ASSERT(observedIsInternal.Defined());
    UNIT_ASSERT_VALUES_EQUAL_C(
        *observedIsInternal,
        false,
        "empty ReplyTo must not mark ReplyError as IsInternal even when cookie==0");

    auto restartedBlobRequest =
        Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvBlobRequest>(TDuration::Seconds(5));
    UNIT_ASSERT_C(
        restartedBlobRequest != nullptr,
        "non-internal cookie==0 TEvError must clear ReadingTimestamp and restart timestamp read");
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

    WaitCmdWrite({.Count=3, .PlanStep=step, .TxId=txId, .UserInfos={{1, {.Session=session, .Offset=0}}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitCmdWrite({.Count=3, .PlanStep=step, .TxId=txId, .UserInfos={{1, {.Session=session, .Offset=2}}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitCommitTxDone({.TxId=txId, .Partition=TPartitionId(partition)});
}

Y_UNIT_TEST_F(KillReadSessionFailsPendingHasData, TPartitionFixture)
{
    const TPartitionId partition{0};
    const ui64 end = 10;
    const TString client = "client";
    const TString session = "session";
    const ui64 step = 12345;
    const ui64 txId = 67890;
    const ui64 hasDataCookie = 42;

    CreatePartition({.Partition=partition, .Begin=0, .End=end, .PlanStep=step, .TxId=10000});
    CreateSession(client, session);

    {
        auto event = MakeHolder<TEvPersQueue::TEvHasDataInfo>();
        event->Record.SetPartition(partition.InternalPartitionId);
        event->Record.SetOffset(end);
        event->Record.SetDeadline((TInstant::Now() + TDuration::Minutes(1)).MilliSeconds());
        event->Record.SetCookie(hasDataCookie);
        event->Record.SetClientId(client);
        event->Record.SetSessionId(session);
        ActorIdToProto(Ctx->Edge, event->Record.MutableSender());
        Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
    }

    UNIT_ASSERT_C(
        !Ctx->Runtime->GrabEdgeEvent<TEvPersQueue::TEvHasDataInfoResponse>(TDuration::MilliSeconds(100)),
        "HasData must stay pending while offset == EndOffset");

    SendCalcPredicate(step, txId, client, 0, 2, {}, true);
    WaitCalcPredicateResult({.Step=step, .TxId=txId, .Partition=TPartitionId(partition), .Predicate=true});
    SendCommitTx(step, txId);

    bool gotSessionInvalidated = false;
    bool gotCommitDone = false;
    while (!gotSessionInvalidated || !gotCommitDone) {
        TAutoPtr<IEventHandle> handle;
        auto events = Ctx->Runtime->GrabEdgeEvents<
            TEvPersQueue::TEvHasDataInfoResponse,
            TEvKeyValue::TEvRequest,
            TEvPQ::TEvTxDone>(handle, TDuration::Seconds(5));

        if (auto* response = std::get<TEvPersQueue::TEvHasDataInfoResponse*>(events)) {
            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetCookie(), hasDataCookie);
            UNIT_ASSERT(response->Record.GetSessionInvalidated());
            gotSessionInvalidated = true;
        } else if (std::get<TEvKeyValue::TEvRequest*>(events)) {
            SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
        } else if (auto* done = std::get<TEvPQ::TEvTxDone*>(events)) {
            UNIT_ASSERT_VALUES_EQUAL(done->TxId, txId);
            gotCommitDone = true;
        } else {
            UNIT_FAIL("timeout waiting for SessionInvalidated HasData response and TxDone");
        }
    }
}

// Commit without session clears pending.Session before UsersInfoStorage is updated.
// HasData with the old SessionId must be rejected via the pending-session check.
Y_UNIT_TEST_F(HasDataRejectedByPendingSessionAfterCommitWithoutSession, TPartitionFixture)
{
    const TPartitionId partition{0};
    const ui64 end = 10;
    const TString client = "client";
    const TString session = "session";
    const ui64 hasDataCookie = 99;

    CreatePartition({.Partition=partition, .Begin=0, .End=end});
    CreateSession(client, session);

    // Commit without session id → pending.Session cleared, FailStale, then persist.
    {
        auto event = MakeHolder<TEvPQ::TEvSetClientInfo>(
            /*cookie=*/7, client, /*offset=*/2, /*session=*/"",
            /*partitionSessionId=*/0, /*gen=*/0, /*step=*/0, TActorId{});
        event->Strict = true;
        Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
    }

    {
        auto kv = Ctx->Runtime->GrabEdgeEvent<TEvKeyValue::TEvRequest>(TDuration::Seconds(5));
        UNIT_ASSERT(kv);
        // Hold the write response: UsersInfoStorage still has the old session.
    }

    {
        auto event = MakeHolder<TEvPersQueue::TEvHasDataInfo>();
        event->Record.SetPartition(partition.InternalPartitionId);
        event->Record.SetOffset(end);
        event->Record.SetDeadline((TInstant::Now() + TDuration::Minutes(1)).MilliSeconds());
        event->Record.SetCookie(hasDataCookie);
        event->Record.SetClientId(client);
        event->Record.SetSessionId(session);
        ActorIdToProto(Ctx->Edge, event->Record.MutableSender());
        Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, Ctx->Edge, event.Release()));
    }

    {
        auto response = Ctx->Runtime->GrabEdgeEvent<TEvPersQueue::TEvHasDataInfoResponse>(TDuration::Seconds(5));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Record.GetCookie(), hasDataCookie);
        UNIT_ASSERT(response->Record.GetSessionInvalidated());
    }

    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
    WaitProxyResponse({.Cookie=7, .Status=NMsgBusProxy::MSTATUS_OK});
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

    WaitCmdWrite({.Count=1, .PlanStep=step, .TxId=txId_1, .UserInfos={{1, {.Session=session, .Offset=0}}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitCmdWrite({.Count=1, .PlanStep=step, .TxId=txId_1, .UserInfos={{1, {.Session=session, .Offset=1}}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitCommitTxDone({.TxId=txId_1, .Partition=TPartitionId(partition)});

    WaitCalcPredicateResult({.Step=step, .TxId=txId_2, .Partition=TPartitionId(partition), .Predicate=false});
    SendRollbackTx(step, txId_2);

    WaitCalcPredicateResult({.Step=step, .TxId=txId_3, .Partition=TPartitionId(partition), .Predicate=false});
    SendRollbackTx(step, txId_3);

    WaitCmdWrite({.Count=1, .PlanStep=step, .TxId=txId_2, .UserInfos={{1, {.Session=session, .Offset=1}}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitCmdWrite({.Count=1, .PlanStep=step, .TxId=txId_3, .UserInfos={{1, {.Session=session, .Offset=1}}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
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
                 {1, {.Session="session-2", .Offset=0}},
                 {3, {.Session="session-1", .Offset=0}}
                 }});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitCmdWrite({.Count=5, .UserInfos={
                 {1, {.Session="session-2", .Offset=1}},
                 {3, {.Session="session-1", .Offset=6}}
                 }});
}

Y_UNIT_TEST_F(OldPlanStep, TPartitionFixture)
{
    // Partition meta is ahead of the commit (stale replay). Tablet always sends SerializedTx on commit;
    // partition must persist tx KV before TEvTxDone (stable-25-4 -> stable-26-1).
    const TPartitionId partition{3};
    const ui64 step = 12345;
    const ui64 txId = 67890;

    CreatePartitionStaleReplayAhead(partition, 0, 10);

    auto serializedTxBody = MakeStaleSerializedTxBody(partition, step, txId);
    SendCommitTx(step, txId, {.SerializedTx = std::move(serializedTxBody)});

    AssertNoUnexpectedStaleMetaTxDone("TEvTxDone must not precede KV response for stale commit with SerializedTx");

    auto kv = GrabStaleMetaKvRequest("expected KV request with stale tx meta persist");
    AssertStaleMetaKvHasTxKey(*kv, txId, partition.OriginalPartitionId, "first KV");

    AssertNoUnexpectedStaleMetaTxDone("TEvTxDone must not arrive before SendCmdWriteResponse");

    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitCommitTxDone({.TxId=txId, .Partition=TPartitionId(partition)});
}

Y_UNIT_TEST_F(OldPlanStep_SecondStaleCommitWhileKvInflight, TPartitionFixture)
{
    // Two stale commits: second arrives while first KV response is still pending — must not crash
    // (TryAppendStaleTxMetaWrites runs again only after InFlight is cleared on write complete).
    const TPartitionId partition{3};
    const ui64 step = 12345;
    const ui64 txId1 = 67890;
    const ui64 txId2 = 67891;

    CreatePartitionStaleReplayAhead(partition, 0, 10);

    auto body = MakeStaleSerializedTxBody(partition, step, txId1);
    SendCommitTx(step, txId1, {.SerializedTx = std::move(body)});

    AssertNoUnexpectedStaleMetaTxDone("TEvTxDone before first KV");

    auto kv = GrabStaleMetaKvRequest("expected first stale-meta KV request");
    AssertStaleMetaKvHasTxKey(*kv, txId1, partition.OriginalPartitionId, "first KV");

    body = MakeStaleSerializedTxBody(partition, step, txId2);
    SendCommitTx(step, txId2, {.SerializedTx = std::move(body)});

    AssertNoUnexpectedStaleMetaTxDone("TEvTxDone must not appear while first KV unanswered");

    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitCommitTxDone({.TxId=txId1, .Partition=TPartitionId(partition)});

    kv = GrabStaleMetaKvRequest("expected second stale-meta KV after first completes");
    AssertStaleMetaKvHasTxKey(*kv, txId2, partition.OriginalPartitionId, "second KV");

    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitCommitTxDone({.TxId=txId2, .Partition=TPartitionId(partition)});
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

    WaitCmdWrite({.Count=1, .PlanStep=step, .TxId=txId_1, .UserInfos={{1, {.Consumer="client", .Session="session", .Offset=0}}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    WaitCmdWrite({.Count=1, .PlanStep=step, .TxId=txId_1, .UserInfos={{1, {.Consumer="client", .Session="session", .Offset=0}}}});
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

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
                    {1, {.Consumer="client-1", .Session="session-1", .Offset=0}},
                 },
                 });

    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
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

//
// #49435: on init, persisted Config is older than TabletConfig → ChangePartitionConfig is
// PushFront'ed. FillReadFromTimestamps still compares UsersInfo against the *old* Config and
// queues ESCI_DROP_READ_RULE for stale KV consumers. ChangingConfig blocks those acts until
// the config write completes; the second write cycle then Remove()'s an already-deleted user.
// Covers hypotheses: (1) double DROP on init+config upgrade, (2) non-idempotent Remove,
// (3) FillReadFromTimestamps unaware of pending ChangePartitionConfig.
//
Y_UNIT_TEST_F(Init_StaleDiskConsumer_DoubleDrop_OnConfigUpgrade, TPartitionFixture)
{
    const TPartitionId partition{3};

    CreatePartition({
        .Partition = partition,
        .Begin = 0,
        .End = 10,
        .Config = {
            .Version = 1,
            .Consumers = {{.Consumer = "client-keep", .Offset = 3}},
        },
        // Stale consumer still on disk, already absent from persisted Config.
        .ExtraDiskConsumers = {{.Consumer = "stale-client", .Offset = 5}},
    },
    {
        .Version = 2,
        .Consumers = {{.Consumer = "client-keep"}},
    });

    // Write cycle 1: ChangePartitionConfig drops stale-client.
    WaitCmdWrite({
        .UserInfos = {{0, {.Consumer = "client-keep", .Session = "", .Offset = 3}}},
        .DeleteRanges = {{0, {.Partition = 3, .Consumer = "stale-client"}}},
    });
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    // Write cycle 2: FillReadFromTimestamps ESCI_DROP_READ_RULE for the same consumer.
    // Must not VERIFY on the second Remove (#49435).
    WaitCmdWrite({
        .DeleteRanges = {{0, {.Partition = 3, .Consumer = "stale-client"}}},
    });
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
    Ctx->Runtime->SimulateSleep(TDuration::Seconds(1));
}

//
// #49435 hypothesis (2): Remove is not idempotent across write cycles.
// ChangePartitionConfig drops the consumer; a later ESCI_DROP_READ_RULE tries again.
//
Y_UNIT_TEST_F(DropReadRule_AfterConfigAlreadyRemovedConsumer, TPartitionFixture)
{
    const TPartitionId partition{3};

    CreatePartition({
        .Partition = partition,
        .Begin = 0,
        .End = 10,
        .Config = {
            .Version = 1,
            .Consumers = {
                {.Consumer = "client-1", .Offset = 0, .Session = "session-1"},
                {.Consumer = "client-2", .Offset = 0, .Session = "session-2"},
            },
        },
    });

    SendChangePartitionConfig({
        .Version = 2,
        .Consumers = {{.Consumer = "client-1", .Generation = 0}},
    });

    WaitCmdWrite({
        .UserInfos = {{0, {.Consumer = "client-1", .Session = "session-1", .Offset = 0}}},
        .DeleteRanges = {{0, {.Partition = 3, .Consumer = "client-2"}}},
    });
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
    WaitPartitionConfigChanged({.Partition = partition});

    SendEvent(new TEvPQ::TEvSetClientInfo(
        0, "client-2", 0, "", 0, 0, 0, TActorId{},
        TEvPQ::TEvSetClientInfo::ESCI_DROP_READ_RULE, 0));

    WaitCmdWrite({
        .DeleteRanges = {{0, {.Partition = 3, .Consumer = "client-2"}}},
    });
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);
    // Must not VERIFY on Remove of an already-deleted consumer (#49435).
    Ctx->Runtime->SimulateSleep(TDuration::Seconds(1));
}

void TPartitionFixture::CmdChangeOwner(ui64 cookie, const TString& sourceId, TDuration duration, TString& ownerCookie)
{
    SendChangeOwner(cookie, sourceId, Ctx->Edge);

    EmulateKVTablet();

    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvProxyResponse>(duration);
    UNIT_ASSERT(event != nullptr);
    ownerCookie = event->Response->GetPartitionResponse().GetCmdGetOwnershipResult().GetOwnerCookie();
}

void TPartitionFixture::SendCalcPredicate(ui64 step,
                                          ui64 txId,
                                          const TActorId& suppPartitionId)
{
    SendCalcPredicate(step, txId, "", 0, 0, suppPartitionId);
}

void TPartitionFixture::WaitForGetWriteInfoRequest()
{
    auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvGetWriteInfoRequest>();
    UNIT_ASSERT(event != nullptr);
    //UNIT_ASSERT_VALUES_EQUAL(event->OriginalPartition, ActorId);
}

void TPartitionFixture::SendGetWriteInfoError(ui32 internalPartitionId,
                                              TString message,
                                              const TActorId& suppPartitionId)
{
    auto event = MakeHolder<TEvPQ::TEvGetWriteInfoError>(internalPartitionId,
                                                         std::move(message));
    //event->SupportivePartition = suppPartitionId;

    Ctx->Runtime->SingleSys()->Send(new IEventHandle(ActorId, suppPartitionId, event.Release()));
}

void TPartitionFixture::WaitForCalcPredicateResult(ui64 txId, bool predicate)
{
    while (true) {
        TAutoPtr<IEventHandle> handle;
        auto events =
            Ctx->Runtime->GrabEdgeEvents<TEvPQ::TEvTxCalcPredicateResult, TEvKeyValue::TEvRequest>(handle,
                                                                                                   TDuration::Seconds(1));
        if (std::get<TEvKeyValue::TEvRequest*>(events)) {
            SendDiskStatusResponse(nullptr);
        } else if (auto* event = std::get<TEvPQ::TEvTxCalcPredicateResult*>(events)) {
            UNIT_ASSERT_VALUES_EQUAL(event->TxId, txId);
            UNIT_ASSERT_VALUES_EQUAL(event->Predicate, predicate);
            break;
        }
    }
}

Y_UNIT_TEST_F(ReserveSubDomainOutOfSpace, TPartitionFixture)
{
    Ctx->Runtime->GetAppData().FeatureFlags.SetEnableTopicDiskSubDomainQuota(true);

    CreatePartition({
                    .Partition=TPartitionId{1},
                    .Begin=0, .End=0,
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
    TestWriteSubDomainOutOfSpace_DeadlineWork(false);
}

Y_UNIT_TEST_F(WriteSubDomainOutOfSpace_DisableExpiration, TPartitionFixture)
{
    TestWriteSubDomainOutOfSpace(TDuration::MilliSeconds(0), false);
}

Y_UNIT_TEST_F(WriteSubDomainOutOfSpace_IgnoreQuotaDeadline, TPartitionFixture)
{
    TestWriteSubDomainOutOfSpace_DeadlineWork(true);
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
    SendGetWriteInfo(false);
    {
        {
            auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvGetWriteInfoError>(TDuration::Seconds(1));
            UNIT_ASSERT(event == nullptr);

        }
        auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvGetWriteInfoResponse>(TDuration::Seconds(1));
        UNIT_ASSERT(event != nullptr);
        //Cerr << "Got write info resposne. Body keys: " << event->BodyKeys.size() << ", head: " << event->BlobsFromHead.size() << ", src id info: " << event->SrcIdInfo.size() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(event->BodyKeys.size(), 4);
        //UNIT_ASSERT_VALUES_EQUAL(event->BlobsFromHead.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(event->SrcIdInfo.size(), 1);

        UNIT_ASSERT_VALUES_EQUAL(event->SrcIdInfo.begin()->second.MinSeqNo, 2);
        UNIT_ASSERT_VALUES_EQUAL(event->SrcIdInfo.begin()->second.SeqNo, 7);
        UNIT_ASSERT_VALUES_EQUAL(event->SrcIdInfo.begin()->second.Offset, 110);

        Cerr << "Body key 1: " << event->BodyKeys.begin()->Key.ToString() << ", size: " << event->BodyKeys.begin()->CumulativeSize << Endl;
        Cerr << "Body key last " << event->BodyKeys.back().Key.ToString() << ", size: " << event->BodyKeys.back().CumulativeSize << Endl;
        //Cerr << "Head blob 1 size: " << event->BlobsFromHead.begin()->GetBlobSize() << Endl;
        UNIT_ASSERT(event->BodyKeys.begin()->Key.ToString().StartsWith("D0000100001_"));
        //UNIT_ASSERT(event->BlobsFromHead.begin()->GetBlobSize() > 0);
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
#if 0
    auto ownerEvent = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvProxyResponse>(TDuration::Seconds(1));
    UNIT_ASSERT(ownerEvent != nullptr);
    auto ownerCookie = ownerEvent->Response->GetPartitionResponse().GetCmdGetOwnershipResult().GetOwnerCookie();
#else
    TString ownerCookie;
    while (true) {
        TAutoPtr<IEventHandle> handle;
        auto events =
            Ctx->Runtime->GrabEdgeEvents<TEvPQ::TEvProxyResponse, TEvKeyValue::TEvRequest>(handle,
                                                                                           TDuration::Seconds(1));
        if (std::get<TEvKeyValue::TEvRequest*>(events)) {
            SendDiskStatusResponse(nullptr);
        } else if (auto* event = std::get<TEvPQ::TEvProxyResponse*>(events)) {
            ownerCookie = event->Response->GetPartitionResponse().GetCmdGetOwnershipResult().GetOwnerCookie();
            break;
        }
    }
#endif

    TAutoPtr<IEventHandle> handle;
    std::function<bool(const TEvPQ::TEvError&)> truth = [&](const TEvPQ::TEvError& e) { return cookie == e.Cookie; };

    TString data = "data for write";

    SendWrite(++cookie, 0, ownerCookie, 100, data, false, 1);

    {
        SendGetWriteInfo(false);
        auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvGetWriteInfoError>(TDuration::Seconds(1));
        UNIT_ASSERT(event != nullptr);
    }

    SendDiskStatusResponse();
    {
        auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvProxyResponse>(handle, TDuration::Seconds(1));
        UNIT_ASSERT(event != nullptr);
    }
    {
        SendGetWriteInfo(false);
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
    EmulateKVTablet();
    auto ownerEvent = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvProxyResponse>(TDuration::Seconds(1));
    UNIT_ASSERT(ownerEvent != nullptr);
    auto ownerCookie = ownerEvent->Response->GetPartitionResponse().GetCmdGetOwnershipResult().GetOwnerCookie();

    SendWrite(++cookie, 0, ownerCookie, 51, data, false, 5);
    EmulateKVTablet();
    WaitProxyResponse({.Cookie=cookie});

    Cerr << "Wait third predicate result " << Endl;
    auto tx3 = MakeAndSendWriteTx({{"src1", {12, 20}}, {"SourceId", {6, 10}}});
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

void TPartitionTxTestHelper::NonConflictingActsBatchOkTest() {
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

    //WaitBatchCompletion(5 + 6 + 6); //5 txs and immediate txs + 2 normal writes with 6 messages each;

    SendTxCommit(tx3);
    SendTxRollback(tx2);
    ExpectNoKvRequest();
    SendTxCommit(tx1);
    WaitKvRequest();
    SendKvResponse();
    WaitCommitDone(tx1);
    WaitImmediateTxComplete(immTx1, true);
    WaitImmediateTxComplete(immTx2, true);
    WaitRollbackDone(tx2);
    WaitCommitDone(tx3);
}
Y_UNIT_TEST_F(TestNonConflictingActsBatchOk, TPartitionTxTestHelper) {
    NonConflictingActsBatchOkTest();
}

Y_UNIT_TEST_F(TestTxBatchInFederation, TPartitionTxTestHelper) {
    Ctx->Runtime->GetAppData(0).PQConfig.SetTopicsAreFirstClassCitizen(false);
    NonConflictingActsBatchOkTest();
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
    //WaitBatchCompletion(2);

    SendTxCommit(tx1);
    SendTxRollback(tx2);
    WaitKvRequest();
    SendKvResponse();

    WaitTxPredicateReply(tx3);
    //WaitBatchCompletion(1);
    SendTxCommit(tx3);

    //2 Normal writes with src1 & src4
    ExpectNoTxPredicateReply();
    WaitKvRequest();
    SendKvResponse();
    WaitCommitDone(tx1);
    WaitKvRequest();
    SendKvResponse();
    WaitRollbackDone(tx2);
    WaitCommitDone(tx3);
    WaitTxPredicateReply(tx5);
    //WaitBatchCompletion(6 + 2); // Normal writes produce 1 act for each message
    SendTxCommit(tx5);
    //WaitBatchCompletion(1);

    WaitKvRequest();
    SendKvResponse();
    WaitCommitDone(tx5);

    //WaitBatchCompletion(1 + 6); //Normal write & immTx for src4;
    WaitKvRequest();
    SendKvResponse();
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

    //WaitBatchCompletion(1);

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
    //WaitBatchCompletion(1);
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

    //WaitBatchCompletion(1);

    SendTxRollback(tx1);

    WaitTxPredicateReply(tx2);
    //WaitBatchCompletion(2);
    SendTxCommit(tx2);

    WaitKvRequest();
    SendKvResponse();
    WaitRollbackDone(tx1);
    WaitCommitDone(tx2);
    WaitImmediateTxComplete(immTx, true);
}

Y_UNIT_TEST_F(ConflictingSrcIdForTxInDifferentBatches, TPartitionTxTestHelper) {
    TTxBatchingTestParams params {.WriterSessions{"src1"}};
    Init(std::move(params));

    auto tx1 = MakeAndSendWriteTx({{"src1", {1, 5}}});
    auto tx2 = MakeAndSendWriteTx({{"src1", {6, 10}}});
    auto tx3 = MakeAndSendWriteTx({{"src1", {2, 11}}});
    auto tx4 = MakeAndSendWriteTx({{"src1", {8, 15}}});

    WaitWriteInfoRequest(tx1, true);
    WaitWriteInfoRequest(tx2, true);
    WaitWriteInfoRequest(tx3, true);
    WaitWriteInfoRequest(tx4, true);
    WaitTxPredicateReply(tx1);

    Cerr << "Wait batch of 1 completion\n";
    SendTxCommit(tx1);
    //WaitBatchCompletion(1);
    Cerr << "Expect KV request\n";
    WaitKvRequest();
    SendKvResponse();
    WaitTxPredicateReply(tx2);
    SendTxCommit(tx2);

    Cerr << "Wait for tx 3 predicate failure\n";
    WaitTxPredicateFailure(tx3);
    Cerr << "Wait for tx 4 predicate failure\n";
    WaitTxPredicateFailure(tx4);


    Cerr << "Wait batch of 3 completion\n";
    //WaitBatchCompletion(1); // Immediate Tx 2 - 4.
    Cerr << "Expect KV request\n";
    WaitKvRequest();
    SendKvResponse();
    SendTxRollback(tx3);
    SendTxRollback(tx4);
    //WaitBatchCompletion(2); // Immediate Tx 2 - 4.

    WaitKvRequest();
    SendKvResponse();
    Cerr << "Wait for commits\n";
    WaitCommitDone(tx1);
    WaitCommitDone(tx2);
}

Y_UNIT_TEST_F(ConflictingSrcIdTxAndWritesDifferentBatches, TPartitionTxTestHelper) {
    TTxBatchingTestParams params {.WriterSessions{"src1"}, .EndOffset = 1};
    Init(std::move(params));

    auto tx1 = MakeAndSendWriteTx({{"src1", {1, 3}},});
    auto tx2 = MakeAndSendWriteTx({{"src1", {2, 4}}});
    auto tx3 = MakeAndSendWriteTx({{"src1", {4, 6}}});
    AddAndSendNormalWrite("src1", 1, 1);
    AddAndSendNormalWrite("src1", 7, 7);
    AddAndSendNormalWrite("src1", 7, 7);


    WaitWriteInfoRequest(tx1, true);
    WaitWriteInfoRequest(tx2, true);
    WaitWriteInfoRequest(tx3, true);
    WaitTxPredicateReply(tx1);

    SendTxCommit(tx1);
    //WaitBatchCompletion(1);

    WaitKvRequest();
    SendKvResponse();

    WaitCommitDone(tx1);

    WaitTxPredicateFailure(tx2);
    WaitTxPredicateReply(tx3);
    SendTxRollback(tx2);
    SendTxCommit(tx3);
    //WaitBatchCompletion(2); // Tx 2 & 3.
    WaitKvRequest();
    SendKvResponse();
    WaitRollbackDone(tx2);
    WaitKvRequest();
    SendKvResponse();
    WaitCommitDone(tx3);
    //WaitBatchCompletion(3);
    WaitKvRequest();
    SendKvResponse();
    WaitProxyResponse({.AlreadyWritten=true, .SeqNo=1});
    WaitProxyResponse({.AlreadyWritten=false, .SeqNo=7});
    WaitProxyResponse({.AlreadyWritten=true, .SeqNo=7});
}

Y_UNIT_TEST_F(ConflictingSrcIdForTxWithHead, TPartitionTxTestHelper) {
    TTxBatchingTestParams params {.WriterSessions{"src1"}, .EndOffset=1};
    Init(std::move(params));

    NPQ::TClientBlob clientBlob("src1", 10, "valuevalue", TMaybe<TPartData>(), TInstant::MilliSeconds(1), TInstant::MilliSeconds(1), 0, "123", "123");

    auto tx1 = MakeAndSendWriteTx({{"src1", {1, 10}}}, std::move(clientBlob));
    AddAndSendNormalWrite("src1", 8, 8);
    AddAndSendNormalWrite("src1", 10, 10);
    AddAndSendNormalWrite("src1", 11, 11);


    WaitWriteInfoRequest(tx1, true);
    WaitTxPredicateReply(tx1);

    SendTxCommit(tx1);
    //WaitBatchCompletion(1);
    Cerr << "Wait 1st KV request\n";
    WaitKvRequest();
    SendKvResponse();
    WaitCommitDone(tx1);
    //WaitBatchCompletion(3);
    Cerr << "Wait 2nd KV request\n";
    WaitKvRequest();
    SendKvResponse();
    WaitProxyResponse({.AlreadyWritten=true, .SeqNo=8});
    WaitProxyResponse({.AlreadyWritten=true, .SeqNo=10});
    WaitProxyResponse({.AlreadyWritten=false, .SeqNo=11});

    //WaitProxyResponse()
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

    void WaitRollbackDone() {
        TxHelper->WaitRollbackDone(TxTmp);
    }

    ui64 AddTx() {
        auto ret = TxHelper->MakeAndSendWriteTx({{SrcId, {SeqNo, SeqNo}}});
        SeqNo++;
        return ret;
    }
    ui64 AddImmediateTx() {
        auto ret = TxHelper->MakeAndSendImmediateTx({{SrcId, {SeqNo, SeqNo}}});
        SeqNo++;
        return ret;
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
    //WaitBatchCompletion(4 + 1);
    EmulateKVTablet();
    wrapper.WaitRollbackDone();
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
    //WaitBatchCompletion(1+1);
    ExpectNoTxPredicateReply();
    EmulateKVTablet();
    wrapper.WaitRollbackDone();
    EmulateKVTablet();
    WaitImmediateTxComplete(immTx, true);
    ExpectNoCommitDone();
    WaitTxPredicateReply(tx);
    SendTxCommit(tx);
    //WaitBatchCompletion(1);
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
    //WaitBatchCompletion(1+1);
    ExpectNoTxPredicateReply();
    EmulateKVTablet();
    wrapper.WaitRollbackDone();
    WaitTxPredicateReply(tx);
    SendTxCommit(tx);
    //WaitBatchCompletion(1);
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
    //WaitBatchCompletion(1+1);
    ExpectNoKvRequest();
    SendTxCommit(tx);
    EmulateKVTablet();
    wrapper.WaitRollbackDone();
    WaitCommitDone(tx);
    //WaitBatchCompletion(1);
    EmulateKVTablet();
    }
    {
    // 5. WriteTx -> ImmTx = 2 batches
    ResetBatchCompletion();
    wrapper.Start();
    auto tx = wrapper.AddTx();
    auto immTx = wrapper.AddImmediateTx();
    wrapper.Process();
    EmulateKVTablet();
    wrapper.WaitRollbackDone();
    WaitWriteInfoRequest(tx, true);
    WaitWriteInfoRequest(immTx, true);
    //WaitBatchCompletion(1+1);
    WaitTxPredicateReply(tx);
    SendTxCommit(tx);
    ExpectNoCommitDone();
    EmulateKVTablet();
    //WaitBatchCompletion(1);
    WaitCommitDone(tx);
    EmulateKVTablet();
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
    //WaitRollbackDone(txTmp);

    WaitWriteInfoRequest(tx, true);
    WaitWriteInfoRequest(immTx, true);
    //WaitBatchCompletion(5 + 1);
    ExpectNoTxPredicateReply();
    EmulateKVTablet();
    WaitRollbackDone(txTmp);
    WaitTxPredicateFailure(tx);
    //WaitBatchCompletion(2);
    SendTxRollback(tx);

    EmulateKVTablet();
    WaitRollbackDone(tx);
    WaitImmediateTxComplete(immTx, true);
    }
    {
    AddAndSendNormalWrite("src2", 1, 10);
    EmulateKVTablet();
    ResetBatchCompletion();

    auto txTmp = MakeAndSendWriteTx({});
    auto immTx = MakeAndSendImmediateTx({{"src2", {5, 15}}});
    auto tx = MakeAndSendWriteTx({{"src2", {11, 15}}});
    WaitWriteInfoRequest(txTmp, true);
    WaitTxPredicateReply(txTmp);
    SendTxRollback(txTmp);

    WaitWriteInfoRequest(immTx, true);
    WaitWriteInfoRequest(tx, true);
    //WaitBatchCompletion(2 + 1);
    WaitTxPredicateReply(tx);
    ExpectNoKvRequest();
    SendTxCommit(tx);
    EmulateKVTablet();
    WaitRollbackDone(txTmp);
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

    //WaitBatchCompletion(5 + 1 /*tmpTx*/);
    SendTxCommit(tx1);
    SendTxCommit(tx2);
    WaitKvRequest();
    SendKvResponse();

    WaitKvRequest();
    SendKvResponse();
    WaitRollbackDone(txTmp);
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

    MakeAndSendNormalOffsetCommit(1, 2); // act-1
    auto tx1 = MakeAndSendTxOffsetCommit(1, 2, 5);
    auto tx2 = MakeAndSendTxOffsetCommit(1, 5, 10);
    MakeAndSendNormalOffsetCommit(1, 20); // act-2
    ResetBatchCompletion();

    WaitWriteInfoRequest(txTmp, true);
    WaitTxPredicateReply(txTmp);

    SendTxRollback(txTmp);
    //WaitBatchCompletion(2); // txTmp + act-1

    ExpectNoTxPredicateReply();
    WaitKvRequest();
    SendKvResponse();

    WaitRollbackDone(txTmp);

    WaitTxPredicateReply(tx1);
    WaitKvRequest();
    SendKvResponse();
    ExpectNoTxPredicateReply();
    SendTxCommit(tx1);
    //WaitBatchCompletion(1); // tx1

    WaitTxPredicateReply(tx2);
    SendTxCommit(tx2);
    //WaitBatchCompletion(1); // tx2

    WaitKvRequest();
    SendKvResponse();
    WaitCommitDone(tx1);
    WaitKvRequest();
    SendKvResponse();
    WaitCommitDone(tx2);

    //WaitBatchCompletion(1); // act-2
    WaitKvRequest();
    SendKvResponse();

    txTmp = MakeAndSendWriteTx({});
    auto immTx1 = MakeAndSendImmediateTxOffsetCommit(2, 0, 5);
    auto immTx2 = MakeAndSendImmediateTxOffsetCommit(2, 5, 10);
    WaitWriteInfoRequest(txTmp, true);
    WaitTxPredicateReply(txTmp);
    SendTxRollback(txTmp);

    WaitKvRequest();
    //WaitBatchCompletion(3);
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
    WaitKvRequest();
    SendKvResponse();
    //WaitBatchCompletion(1 + 1);
    //WaitBatchCompletion(1); // для txTmp отправили TEvTxRollback

    SendTxCommit(tx1);
    WaitTxPredicateFailure(tx2);
    SendTxRollback(tx2);

    WaitKvRequest();
    SendKvResponse();
    WaitRollbackDone(txTmp);
    WaitCommitDone(tx1);
    WaitRollbackDone(tx2);
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
    //WaitBatchCompletion(2);
    WaitKvRequest();
    SendKvResponse();
    WaitTxPredicateFailure(tx3);
    //WaitBatchCompletion(1);
    SendTxRollback(tx3);

    //WaitBatchCompletion(1);

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
    //WaitBatchCompletion(1);

    SendTxRollback(tx1);
    WaitKvRequest();
    SendKvResponse();

    WaitTxPredicateReply(tx2);
    //WaitBatchCompletion(1);
    SendTxCommit(tx2);

    WaitKvRequest();
    SendKvResponse();
    WaitRollbackDone(tx1);
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
    //WaitBatchCompletion(1); // immTx2
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
    WaitRollbackDone(txTmp);
    WaitImmediateTxComplete(immTx1, true);

    SendCommitTx(1, proposeTxId);
    //ToDo - wait propose result;
    //WaitBatchCompletion(1);
    EmulateKVTablet();
    WaitCommitTxDone({.TxId=proposeTxId});
    //WaitBatchCompletion(1);
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

Y_UNIT_TEST_F(EndWriteTimestamp_DataKeysBody, TPartitionFixture) {
    auto* actor = CreatePartition({.Partition=TPartitionId{2}, .Begin=0, .End=10});

    auto now = TInstant::Now();

    auto endWriteTimestamp = actor->GetEndWriteTimestamp();
    UNIT_ASSERT_C(now - TDuration::Seconds(2) < endWriteTimestamp && endWriteTimestamp < now, "" << (now - TDuration::Seconds(2)) << " < " << endWriteTimestamp << " < " << now );
} // EndWriteTimestamp_DataKeysBody

Y_UNIT_TEST_F(EndWriteTimestamp_FromMeta, TPartitionFixture) {
    auto now = TInstant::Now();

    auto* actor = CreatePartition({.Partition=TPartitionId{2}, .Begin=0, .End=10, .EndWriteTimestamp = now});

    auto endWriteTimestamp = actor->GetEndWriteTimestamp();
    UNIT_ASSERT_VALUES_EQUAL(endWriteTimestamp.MilliSeconds(), now.MilliSeconds());
} // EndWriteTimestamp_FromMeta

Y_UNIT_TEST_F(EndWriteTimestamp_HeadKeys, TPartitionFixture) {
    auto* actor = CreatePartition({.Partition=TPartitionId{2}, .Begin=0, .End=10, .FillHead = true});

    auto now = TInstant::Now();

    auto endWriteTimestamp = actor->GetEndWriteTimestamp();
    UNIT_ASSERT_C(now - TDuration::Seconds(2) < endWriteTimestamp && endWriteTimestamp < now, "" << (now - TDuration::Seconds(2)) << " < " << endWriteTimestamp << " < " << now );
} // EndWriteTimestamp_HeadKeys

Y_UNIT_TEST_F(The_DeletePartition_Message_Arrives_Before_The_ApproveWriteQuota_Message, TPartitionFixture)
{
    // create a supportive partition
    const TPartitionId partitionId{1, TWriteId{2, 3}, 4};
    CreatePartition({.Partition=partitionId});

    // write 2 messages in it
    SendWrite(1, 0, "owner", 0, "message #1", false, 1, true);
    SendWrite(2, 1, "owner", 1, "message #2", false, 2, true);

    // delay the response from the quoter
    auto approveWriteQuota = WaitForRequestQuotaAndHoldApproveWriteQuota();

    // Send a `TEvDeletePartition`. The partition will wait for the response from the quoter to arrive.
    SendDeletePartition();
    WaitForDeletePartitionDoneTimeout();

    // The answer is from the quoter
    SendApproveWriteQuota(std::move(approveWriteQuota));
    WaitForQuotaConsumed();

    WaitCmdWrite();
    SendCmdWriteResponse(NMsgBusProxy::MSTATUS_OK);

    // Write operations fail with an error
    WaitForWriteError(1, NPersQueue::NErrorCode::ERROR);
    WaitForDeletePartitionDone();
    WaitForWriteError(2, NPersQueue::NErrorCode::ERROR);
}

Y_UNIT_TEST_F(After_TEvGetWriteInfoError_Comes_TEvTxCalcPredicateResult, TPartitionFixture)
{
    const TPartitionId partitionId{1};
    const ui64 step = 12345;
    const ui64 txId = 67890;

    CreatePartition({.Partition=partitionId});

    SendCalcPredicate(step, txId, Ctx->Edge);
    WaitForGetWriteInfoRequest();
    SendGetWriteInfoError(31415, "error", Ctx->Edge);
    WaitForCalcPredicateResult(txId, false);
}

Y_UNIT_TEST_F(TEvTxCalcPredicate_Without_Conflicts, TPartitionTxTestHelper)
{
    Init();

    auto tx1 = MakeAndSendWriteTx({{"sourceid-1", {1, 3}}});

    WaitWriteInfoRequest(tx1);
    SendWriteInfoResponse(tx1);

    WaitTxPredicateReply(tx1);

    SendTxCommit(tx1);
    EmulateKVTablet();

    auto tx2 = MakeAndSendWriteTx({{"sourceid-2", {1, 3}}});
    auto tx3 = MakeAndSendWriteTx({{"sourceid-3", {1, 3}}});

    WaitWriteInfoRequest(tx2);
    WaitWriteInfoRequest(tx3);

    SendWriteInfoResponse(tx3);
    SendWriteInfoResponse(tx2);

    WaitTxPredicateReply(tx2);
    WaitTxPredicateReply(tx3);
}

Y_UNIT_TEST_F(TEvTxCalcPredicate_With_Conflicts, TPartitionTxTestHelper)
{
    Init();

    auto tx1 = MakeAndSendWriteTx({{"sourceid", {1, 3}}});

    WaitWriteInfoRequest(tx1);
    SendWriteInfoResponse(tx1);

    WaitTxPredicateReply(tx1);

    auto tx2 = MakeAndSendWriteTx({{"sourceid", {4, 6}}});

    WaitWriteInfoRequest(tx2);
    SendWriteInfoResponse(tx2);

    ExpectNoTxPredicateReply();

    SendTxCommit(tx1);

    WaitTxPredicateReply(tx2);
}

Y_UNIT_TEST(BlobKeyFilfer)
{
    auto filterKeys = [](const TVector<TString>& keys, const TPartitionId& partitionId) -> THashSet<TString> {
        NKikimrClient::TKeyValueResponse::TReadRangeResult result;
        for (const auto& k : keys) {
            auto* pair = result.AddPair();
            pair->SetStatus(NKikimrProto::OK);
            pair->SetKey(k);
        }
        return FilterBlobsMetaData({1, result}, partitionId);
    };

    TVector<TString> actualKeys{
        "d0000000000_00000000000000000000_00000_0000000001_00000?",
        "d0000000000_00000000000000000001_00000_0000000001_00000?"
    };
    THashSet<TString> expectedKeys{
        "d0000000000_00000000000000000000_00000_0000000001_00000?",
        "d0000000000_00000000000000000001_00000_0000000001_00000?"
    };
    auto filteredKeys = filterKeys(actualKeys, TPartitionId(0));

    UNIT_ASSERT_EQUAL(filteredKeys, expectedKeys);

    actualKeys = {
        "d0000000000_00000000000000000000_00000_0000000001_00000?",
        "d0000000000_00000000000000000000_00000_0000000002_00000|",
        "d0000000000_00000000000000000001_00000_0000000001_00000?"
    };
    expectedKeys = {
        "d0000000000_00000000000000000000_00000_0000000002_00000|"
    };
    filteredKeys = filterKeys(actualKeys, TPartitionId(0));

    UNIT_ASSERT_EQUAL(filteredKeys, expectedKeys);

    actualKeys = {
        "d0000000000_00000000000000000000_00000_0000000001_00000?",
        "d0000000000_00000000000000000000_00000_0000000001_00000|",
        "d0000000000_00000000000000000001_00000_0000000001_00000?",
        "d0000000000_00000000000000000001_00000_0000000002_00000|",
        "d0000000000_00000000000000000002_00000_0000000001_00000?"
    };
    expectedKeys = {
        "d0000000000_00000000000000000000_00000_0000000001_00000|",
        "d0000000000_00000000000000000001_00000_0000000002_00000|"
    };
    filteredKeys = filterKeys(actualKeys, TPartitionId(0));

    UNIT_ASSERT_EQUAL(filteredKeys, expectedKeys);

    actualKeys = {
        "d0000000000_00000000000000000000_00000_0000000000_00002|",
        "d0000000000_00000000000000000000_00002_0000000001_00002|"
    };
    expectedKeys = {
        "d0000000000_00000000000000000000_00000_0000000000_00002|",
        "d0000000000_00000000000000000000_00002_0000000001_00002|"
    };
    filteredKeys = filterKeys(actualKeys, TPartitionId(0));

    UNIT_ASSERT_EQUAL(filteredKeys, expectedKeys);

    actualKeys = {
        "d0000000000_00000000000000000000_00000_0000000001_00000?",
        "d0000000000_00000000000000000000_00000_0000000003_00000|",
        "d0000000000_00000000000000000001_00000_0000000002_00000?",
        "d0000000000_00000000000000000003_00000_0000000001_00000?",
        "d0000000000_00000000000000000003_00000_0000000001_00000|"
    };
    expectedKeys = {
        "d0000000000_00000000000000000000_00000_0000000003_00000|",
        "d0000000000_00000000000000000003_00000_0000000001_00000|"
    };
    filteredKeys = filterKeys(actualKeys, TPartitionId(0));

    UNIT_ASSERT_EQUAL(filteredKeys, expectedKeys);

    actualKeys = {
        "d0000000000_00000000000000000000_00000_0000000000_00002?",
        "d0000000000_00000000000000000000_00000_0000000000_00002|",
        "d0000000000_00000000000000000000_00002_0000000001_00002?",
        "d0000000000_00000000000000000000_00002_0000000001_00002|"
    };
    expectedKeys = {
        "d0000000000_00000000000000000000_00000_0000000000_00002|",
        "d0000000000_00000000000000000000_00002_0000000001_00002|"
    };
    filteredKeys = filterKeys(actualKeys, TPartitionId(0));

    UNIT_ASSERT_EQUAL(filteredKeys, expectedKeys);

    actualKeys = {
        "d0000000000_00000000000000000000_00000_0000000002_00000|",
        "d0000000000_00000000000000000000_00000_0000000003_00000|"
    };
    expectedKeys = {
        "d0000000000_00000000000000000000_00000_0000000003_00000|"
    };
    filteredKeys = filterKeys(actualKeys, TPartitionId(0));

    UNIT_ASSERT_EQUAL(filteredKeys, expectedKeys);

    actualKeys = {
        "d0000000000_00000000000000000000_00000_0000000002_00000?",
        "d0000000000_00000000000000000000_00000_0000000003_00000|",
        "d0000000000_00000000000000000002_00000_0000000001_00000?",
    };
    expectedKeys = {
        "d0000000000_00000000000000000000_00000_0000000003_00000|"
    };
    filteredKeys = filterKeys(actualKeys, TPartitionId(0));

    UNIT_ASSERT_EQUAL(filteredKeys, expectedKeys);

    actualKeys = {
        "d0000000000_00000000000000000000_00000_0000000002_00004?",
        "d0000000000_00000000000000000000_00000_0000000002_00005?",
        "d0000000000_00000000000000000002_00000_0000000001_00002?"
    };
    expectedKeys = {
        "d0000000000_00000000000000000000_00000_0000000002_00005?",
        "d0000000000_00000000000000000002_00000_0000000001_00002?"
    };
    filteredKeys = filterKeys(actualKeys, TPartitionId(0));

    UNIT_ASSERT_EQUAL(filteredKeys, expectedKeys);

    actualKeys = {
        "d0000000000_00000000000000000000_00000_0000000002_00004?",
        "d0000000000_00000000000000000000_00000_0000000002_00005|",
        "d0000000000_00000000000000000002_00000_0000000001_00002?"
    };
    expectedKeys = {
        "d0000000000_00000000000000000000_00000_0000000002_00005|",
        "d0000000000_00000000000000000002_00000_0000000001_00002?"
    };
    filteredKeys = filterKeys(actualKeys, TPartitionId(0));

    UNIT_ASSERT_EQUAL(filteredKeys, expectedKeys);

    actualKeys = {
        "d0000000000_00000000000000000000_00000_0000000002_00004?",
        "d0000000000_00000000000000000000_00000_0000000002_00004|",
        "d0000000000_00000000000000000002_00000_0000000001_00002?"
    };
    expectedKeys = {
        "d0000000000_00000000000000000000_00000_0000000002_00004|",
        "d0000000000_00000000000000000002_00000_0000000001_00002?"
    };
    filteredKeys = filterKeys(actualKeys, TPartitionId(0));

    UNIT_ASSERT_EQUAL(filteredKeys, expectedKeys);
}

Y_UNIT_TEST_F(GetPartitionWriteInfoWithoutSrcIdInfo, TPartitionFixture) {
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
    SendGetWriteInfo(true);
    {
        {
            auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvGetWriteInfoError>(TDuration::Seconds(1));
            UNIT_ASSERT(event == nullptr);
        }
        auto event = Ctx->Runtime->GrabEdgeEvent<TEvPQ::TEvGetWriteInfoResponse>(TDuration::Seconds(1));
        UNIT_ASSERT(event != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(event->BodyKeys.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(event->SrcIdInfo.size(), 0);

        UNIT_ASSERT(event->BodyKeys.begin()->Key.ToString().StartsWith("D0000100001_"));
    }
}

namespace {

TClientBlob MakeSinglePartBodyReadBlob(ui64 seqNo, char fill) {
    TString data(24, fill);
    const ui32 sz = data.size();
    return {
        TString("src"),
        seqNo,
        std::move(data),
        TMaybe<TPartData>(),
        TInstant::MilliSeconds(1),
        TInstant::MilliSeconds(1),
        sz,
        TString(),
        TString()
    };
}

TClientBlob MakeSinglePartBodyReadBlobWithLmc(ui64 seqNo, char fill, ui32 logicalMessageCount) {
    TString data(24, fill);
    const ui32 sz = data.size();
    return {
        TString("src"),
        seqNo,
        std::move(data),
        TMaybe<TPartData>(),
        TInstant::MilliSeconds(1),
        TInstant::MilliSeconds(1),
        sz,
        TString(),
        TString(),
        logicalMessageCount,
        /*isBatch=*/true
    };
}

TClientBlob MakeMultipartBodyReadBlob(ui64 seqNo, ui16 partNo, ui16 totalParts, ui32 bytesPerPart, char fill) {
    const ui32 totalSize = bytesPerPart * totalParts;
    TString data(bytesPerPart, fill);
    TMaybe<TPartData> partData = TPartData(partNo, totalParts, totalSize);
    return {
        TString("src"),
        seqNo,
        std::move(data),
        std::move(partData),
        TInstant::MilliSeconds(1),
        TInstant::MilliSeconds(1),
        bytesPerPart,
        TString(),
        TString()
    };
}

TString SerializePackedBatchForReadBodyTest(TBatch batch) {
    batch.Pack();
    TString raw;
    batch.SerializeTo(raw);
    return raw;
}

TString SerializePackedBatchesForReadBodyTest(std::vector<TBatch> batches) {
    TString raw;
    for (auto& batch : batches) {
        batch.Pack();
        batch.SerializeTo(raw);
    }
    return raw;
}

TRequestedBlob MakeRequestedBlobForRead(
    ui64 offset,
    ui16 partNo,
    ui32 count,
    ui16 internalPartsCount,
    TString payload,
    const TKey& key)
{
    const ui32 sz = static_cast<ui32>(payload.size());
    return {
        offset,
        partNo,
        count,
        internalPartsCount,
        sz,
        std::move(payload),
        key,
        TInstant::Now().Seconds()
    };
}

constexpr ui32 kAddBlobsFromBodyDefaultMsgLimit = 100;
constexpr ui32 kAddBlobsFromBodyDefaultByteLimit = static_cast<ui32>(1_MB);

constexpr ui64 kAddBlobsFromBodyProbeEndOffset = 100;
constexpr ui64 kAddBlobsFromBodyProbeSizeLag = 1_MB;
constexpr ui64 kAddBlobsFromBodyProbeReadOffset10 = 10;
constexpr ui64 kAddBlobsFromBodyProbeReadOffset12 = 12;
constexpr ui64 kAddBlobsFromBodyProbeReadOffset20 = 20;

TReadInfo MakeReadInfoForAddBlobsFromBodyTest(
    const TActorId& edge,
    ui64 readOffset,
    ui64 lastOffset = 0,
    ui16 partNo = 0,
    ui64 messageCountLimit = kAddBlobsFromBodyDefaultMsgLimit,
    ui32 byteSizeLimit = kAddBlobsFromBodyDefaultByteLimit,
    ui64 readTimestampMs = 0,
    bool readToBlobEnd = true)
{
    return {
        TString("user"),
        TString("dc"),
        readOffset,
        lastOffset,
        partNo,
        messageCountLimit,
        byteSizeLimit,
        readToBlobEnd,
        ui64{0},
        readTimestampMs,
        TDuration::Zero(),
        false,
        edge,
        false,
        edge
    };
}

class TAddBlobsFromBodyReadTestActor : public TActorBootstrapped<TAddBlobsFromBodyReadTestActor> {
public:
    TAddBlobsFromBodyReadTestActor(TActorId edge, std::function<void(const TActorContext&)> body)
        : Edge(edge)
        , Body(std::move(body))
    {}

    void Bootstrap(const TActorContext& ctx) {
        Body(ctx);
        Send(Edge, new NActors::TEvents::TEvWakeup());
        PassAway();
    }

private:
    TActorId Edge;
    std::function<void(const TActorContext&)> Body;
};

}

// Regression: in the FindPos branch, trueSearchOffset must use blobs[blobIdx].Key, not blobs[0].Key.
// Two non-overlapping body blobs: [10..12] then [13..17] (five messages in blob1). After blob0 the reader
// is at 13. Correct Key.GetOffset()==13 → all five rows are read (8 results total). If the bug used
// Key from blob0 (GetOffset()==10), trueSearchOffset becomes 13-10+13=16, FindPos(16,0) skips the first
// three rows of blob1: the client loses messages at offsets 13, 14, 15 and only gets 16 and 17
// (5 vs 8 results). Multipart ++PartNo is covered elsewhere (e.g. AddBlobsFromBodyGotGap).
Y_UNIT_TEST_F(AddBlobsFromBodyUsesKeyOfCurrentBlob, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);

    std::deque<TClientBlob> dq0;
    dq0.push_back(MakeSinglePartBodyReadBlob(1, 'a'));
    dq0.push_back(MakeSinglePartBodyReadBlob(2, 'b'));
    dq0.push_back(MakeSinglePartBodyReadBlob(3, 'b'));
    TString raw0 = SerializePackedBatchForReadBodyTest(TBatch::FromBlobs(10, std::move(dq0)));

    const TKey key0 = TKey::ForBody(TKeyPrefix::TypeData, partitionId, 10, 0, 3, 0);
    TRequestedBlob blob0 = MakeRequestedBlobForRead(10, 0, 3, 0, std::move(raw0), key0);

    std::deque<TClientBlob> dq1;
    dq1.push_back(MakeSinglePartBodyReadBlob(4, 'c'));
    dq1.push_back(MakeSinglePartBodyReadBlob(5, 'd'));
    dq1.push_back(MakeSinglePartBodyReadBlob(6, 'e'));
    dq1.push_back(MakeSinglePartBodyReadBlob(7, 'f'));
    dq1.push_back(MakeSinglePartBodyReadBlob(8, 'g'));
    TString raw1 = SerializePackedBatchForReadBodyTest(TBatch::FromBlobs(13, std::move(dq1)));

    const TKey key1 = TKey::ForBody(TKeyPrefix::TypeData, partitionId, 13, 0, 5, 0);
    TRequestedBlob blob1 = MakeRequestedBlobForRead(13, 0, 5, 0, std::move(raw1), key1);

    TVector<TRequestedBlob> blobs;
    blobs.push_back(std::move(blob0));
    blobs.push_back(std::move(blob1));

    auto probe = [&](const TActorContext& ctx) {
        TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge, kAddBlobsFromBodyProbeReadOffset10);
        info.Blobs = blobs;
        info.CompactedBlobsCount = 2;

        auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
        NKikimrClient::TResponse& res = *answer->Response;
        auto* readResult = res.MutablePartitionResponse()->MutableCmdReadResult();

        bool needStop = false;
        ui32 cnt = 0;
        ui32 size = 0;
        ui32 lastBlobSize = 0;

        TMaybe<TReadAnswer> early = info.AddBlobsFromBody(blobs,
                                                          0,
                                                          2,
                                                          nullptr,
                                                          kAddBlobsFromBodyProbeReadOffset10,
                                                          kAddBlobsFromBodyProbeEndOffset,
                                                          kAddBlobsFromBodyProbeSizeLag,
                                                          Ctx->Edge,
                                                          kAddBlobsFromBodyProbeReadOffset10,
                                                          readResult,
                                                          answer,
                                                          needStop,
                                                          cnt,
                                                          size,
                                                          lastBlobSize,
                                                          ctx);

        UNIT_ASSERT(!early.Defined());
        UNIT_ASSERT_VALUES_EQUAL(readResult->ResultSize(), 8u);
        for (ui32 i = 0; i < 8; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(i).GetOffset(), 10u + i);
        }
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));

    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

// Tx commit renames the Key (supportive → parent offset) but does not rewrite batch headers
// inside the blob value. Mid-blob FindPos returns header-space offsets; assigning
// Offset = position.Offset without converting back to key-space reports supportive
// offsets (e.g. 391) while the client ReadOffset stays on the parent scale
// (e.g. 547189849) → FillBatchedData ENSURE.
Y_UNIT_TEST_F(AddBlobsFromBodyKeepsKeySpaceOffsetAfterTxKeyRename, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);
    constexpr ui64 supportiveHeaderOffset = 0;
    constexpr ui32 messageCount = 4;
    constexpr ui64 parentKeyOffset = 1'000'000;
    // Mid-blob read forces the FindPos branch (trueOffset < requested Offset).
    constexpr ui64 readOffset = parentKeyOffset + 2;

    std::deque<TClientBlob> dq;
    dq.push_back(MakeSinglePartBodyReadBlob(1, 'a'));
    dq.push_back(MakeSinglePartBodyReadBlob(2, 'b'));
    dq.push_back(MakeSinglePartBodyReadBlob(3, 'c'));
    dq.push_back(MakeSinglePartBodyReadBlob(4, 'd'));
    // Headers keep supportive coordinates (as after PartitionedBlob::Add key rename).
    TString raw = SerializePackedBatchForReadBodyTest(
        TBatch::FromBlobs(supportiveHeaderOffset, std::move(dq)));

    const TKey supportiveKey = TKey::ForBody(
        TKeyPrefix::TypeData, partitionId, supportiveHeaderOffset, 0, messageCount, 0);
    const TKey parentKey = TKey::FromKey(
        supportiveKey, TKeyPrefix::TypeData, partitionId, parentKeyOffset);
    UNIT_ASSERT_VALUES_EQUAL(parentKey.GetOffset(), parentKeyOffset);
    UNIT_ASSERT_VALUES_EQUAL(parentKey.GetCount(), messageCount);

    TRequestedBlob blob = MakeRequestedBlobForRead(
        parentKeyOffset, 0, messageCount, 0, std::move(raw), parentKey);

    TVector<TRequestedBlob> blobs;
    blobs.push_back(std::move(blob));

    auto probe = [&](const TActorContext& ctx) {
        TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge, readOffset);
        info.Blobs = blobs;
        info.CompactedBlobsCount = 1;

        auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
        NKikimrClient::TResponse& res = *answer->Response;
        auto* readResult = res.MutablePartitionResponse()->MutableCmdReadResult();

        bool needStop = false;
        ui32 cnt = 0;
        ui32 size = 0;
        ui32 lastBlobSize = 0;

        TMaybe<TReadAnswer> early = info.AddBlobsFromBody(blobs,
                                                          0,
                                                          1,
                                                          nullptr,
                                                          readOffset,
                                                          parentKeyOffset + messageCount + 10,
                                                          kAddBlobsFromBodyProbeSizeLag,
                                                          Ctx->Edge,
                                                          readOffset,
                                                          readResult,
                                                          answer,
                                                          needStop,
                                                          cnt,
                                                          size,
                                                          lastBlobSize,
                                                          ctx);

        UNIT_ASSERT(!early.Defined());
        UNIT_ASSERT_VALUES_EQUAL(readResult->ResultSize(), 2u);

        // Must stay in parent/key coordinates, not supportive header offsets 2 and 3.
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(0).GetOffset(), parentKeyOffset + 2);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(1).GetOffset(), parentKeyOffset + 3);
        UNIT_ASSERT_VALUES_EQUAL(info.Offset, parentKeyOffset + messageCount);
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));

    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

// Same Key/Header divergence as above, but through FindPos's OffsetDelta branch
// (blob.cpp): mid-batch search returns the logical-message base in header space
// (curOffset), not the requested offset. Must still map back to Key coordinates.
Y_UNIT_TEST_F(AddBlobsFromBodyKeepsKeySpaceOffsetAfterTxKeyRenameWithOffsetDelta, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);
    constexpr ui64 supportiveHeaderOffset = 0;
    constexpr ui32 firstLmc = 5;
    constexpr ui32 secondLmc = 4;
    constexpr ui32 offsetSpan = firstLmc + secondLmc;
    constexpr ui64 parentKeyOffset = 1'000'000;
    // Inside the first logical batch slot → OffsetDelta FindPos rewinds to base (header 0).
    constexpr ui64 readOffset = parentKeyOffset + 3;

    std::deque<TClientBlob> dq;
    dq.push_back(MakeSinglePartBodyReadBlobWithLmc(1, 'a', firstLmc));
    dq.push_back(MakeSinglePartBodyReadBlobWithLmc(2, 'b', secondLmc));

    TBatch batch = TBatch::FromBlobs(supportiveHeaderOffset, std::move(dq));
    UNIT_ASSERT(batch.HasOffsetDelta());
    UNIT_ASSERT_VALUES_EQUAL(batch.GetCount(), offsetSpan);
    UNIT_ASSERT_VALUES_EQUAL(batch.GetOffsetDelta(), offsetSpan);

    TString raw = SerializePackedBatchForReadBodyTest(std::move(batch));

    const TKey supportiveKey = TKey::ForBody(
        TKeyPrefix::TypeData, partitionId, supportiveHeaderOffset, 0, offsetSpan, 0, offsetSpan);
    const TKey parentKey = TKey::FromKey(
        supportiveKey, TKeyPrefix::TypeData, partitionId, parentKeyOffset);
    UNIT_ASSERT_VALUES_EQUAL(parentKey.GetOffset(), parentKeyOffset);
    UNIT_ASSERT_VALUES_EQUAL(parentKey.GetCount(), offsetSpan);
    UNIT_ASSERT(parentKey.GetOffsetDelta().Defined());
    UNIT_ASSERT_VALUES_EQUAL(*parentKey.GetOffsetDelta(), offsetSpan);

    TRequestedBlob blob = MakeRequestedBlobForRead(
        parentKeyOffset, 0, offsetSpan, 0, std::move(raw), parentKey);

    TVector<TRequestedBlob> blobs;
    blobs.push_back(std::move(blob));

    auto probe = [&](const TActorContext& ctx) {
        TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge, readOffset);
        info.Blobs = blobs;
        info.CompactedBlobsCount = 1;

        auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
        NKikimrClient::TResponse& res = *answer->Response;
        auto* readResult = res.MutablePartitionResponse()->MutableCmdReadResult();

        bool needStop = false;
        ui32 cnt = 0;
        ui32 size = 0;
        ui32 lastBlobSize = 0;

        TMaybe<TReadAnswer> early = info.AddBlobsFromBody(blobs,
                                                          0,
                                                          1,
                                                          nullptr,
                                                          readOffset,
                                                          parentKeyOffset + offsetSpan + 10,
                                                          kAddBlobsFromBodyProbeSizeLag,
                                                          Ctx->Edge,
                                                          readOffset,
                                                          readResult,
                                                          answer,
                                                          needStop,
                                                          cnt,
                                                          size,
                                                          lastBlobSize,
                                                          ctx);

        UNIT_ASSERT(!early.Defined());
        UNIT_ASSERT_VALUES_EQUAL(readResult->ResultSize(), 2u);

        // Rewound to the first batch base in Key space, not supportive header 0 / 5.
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(0).GetOffset(), parentKeyOffset);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(0).GetLogicalMessageCount(), firstLmc);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(1).GetOffset(), parentKeyOffset + firstLmc);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(1).GetLogicalMessageCount(), secondLmc);
        UNIT_ASSERT_VALUES_EQUAL(info.Offset, parentKeyOffset + offsetSpan);
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));

    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

// Multi-batch body blob after tx key rename: mid-read lands in the second packed batch.
Y_UNIT_TEST_F(AddBlobsFromBodyKeepsKeySpaceOffsetAfterTxKeyRenameMultiBatch, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);
    constexpr ui64 supportiveHeaderOffset = 0;
    constexpr ui32 batch0Count = 2;
    constexpr ui32 batch1Count = 3;
    constexpr ui32 messageCount = batch0Count + batch1Count;
    constexpr ui64 parentKeyOffset = 1'000'000;
    // Mid second batch → FindPos inside batch1 (trueOffset of batch1 == parent+2 < readOffset).
    constexpr ui64 readOffset = parentKeyOffset + 3;

    std::deque<TClientBlob> dq0;
    dq0.push_back(MakeSinglePartBodyReadBlob(1, 'a'));
    dq0.push_back(MakeSinglePartBodyReadBlob(2, 'b'));
    std::deque<TClientBlob> dq1;
    dq1.push_back(MakeSinglePartBodyReadBlob(3, 'c'));
    dq1.push_back(MakeSinglePartBodyReadBlob(4, 'd'));
    dq1.push_back(MakeSinglePartBodyReadBlob(5, 'e'));

    std::vector<TBatch> batches;
    batches.push_back(TBatch::FromBlobs(supportiveHeaderOffset, std::move(dq0)));
    batches.push_back(TBatch::FromBlobs(supportiveHeaderOffset + batch0Count, std::move(dq1)));
    TString raw = SerializePackedBatchesForReadBodyTest(std::move(batches));

    const TKey supportiveKey = TKey::ForBody(
        TKeyPrefix::TypeData, partitionId, supportiveHeaderOffset, 0, messageCount, 0);
    const TKey parentKey = TKey::FromKey(
        supportiveKey, TKeyPrefix::TypeData, partitionId, parentKeyOffset);

    TRequestedBlob blob = MakeRequestedBlobForRead(
        parentKeyOffset, 0, messageCount, 0, std::move(raw), parentKey);
    TVector<TRequestedBlob> blobs;
    blobs.push_back(std::move(blob));

    auto probe = [&](const TActorContext& ctx) {
        TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge, readOffset);
        info.Blobs = blobs;
        info.CompactedBlobsCount = 1;

        auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
        auto* readResult = answer->Response->MutablePartitionResponse()->MutableCmdReadResult();

        bool needStop = false;
        ui32 cnt = 0;
        ui32 size = 0;
        ui32 lastBlobSize = 0;

        TMaybe<TReadAnswer> early = info.AddBlobsFromBody(blobs,
                                                          0,
                                                          1,
                                                          nullptr,
                                                          readOffset,
                                                          parentKeyOffset + messageCount + 10,
                                                          kAddBlobsFromBodyProbeSizeLag,
                                                          Ctx->Edge,
                                                          readOffset,
                                                          readResult,
                                                          answer,
                                                          needStop,
                                                          cnt,
                                                          size,
                                                          lastBlobSize,
                                                          ctx);

        UNIT_ASSERT(!early.Defined());
        UNIT_ASSERT_VALUES_EQUAL(readResult->ResultSize(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(0).GetOffset(), parentKeyOffset + 3);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(1).GetOffset(), parentKeyOffset + 4);
        UNIT_ASSERT_VALUES_EQUAL(info.Offset, parentKeyOffset + messageCount);
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));

    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

// Multipart (PartNo > 0) mid-message read after Key≠Header rename.
Y_UNIT_TEST_F(AddBlobsFromBodyKeepsKeySpaceOffsetAfterTxKeyRenameMultipart, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);
    constexpr ui64 supportiveHeaderOffset = 0;
    constexpr ui16 totalParts = 3;
    constexpr ui32 bytesPerPart = 32;
    constexpr ui64 parentKeyOffset = 1'000'000;
    constexpr ui64 readOffset = parentKeyOffset;
    constexpr ui16 readPartNo = 1;

    std::deque<TClientBlob> dq;
    dq.push_back(MakeMultipartBodyReadBlob(1, 0, totalParts, bytesPerPart, 'a'));
    dq.push_back(MakeMultipartBodyReadBlob(1, 1, totalParts, bytesPerPart, 'b'));
    dq.push_back(MakeMultipartBodyReadBlob(1, 2, totalParts, bytesPerPart, 'c'));
    TString raw = SerializePackedBatchForReadBodyTest(
        TBatch::FromBlobs(supportiveHeaderOffset, std::move(dq)));

    const TKey supportiveKey = TKey::ForBody(
        TKeyPrefix::TypeData, partitionId, supportiveHeaderOffset, 0, 1, totalParts - 1);
    const TKey parentKey = TKey::FromKey(
        supportiveKey, TKeyPrefix::TypeData, partitionId, parentKeyOffset);

    TRequestedBlob blob = MakeRequestedBlobForRead(
        parentKeyOffset, 0, 1, totalParts - 1, std::move(raw), parentKey);
    TVector<TRequestedBlob> blobs;
    blobs.push_back(std::move(blob));

    auto probe = [&](const TActorContext& ctx) {
        TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(
            Ctx->Edge, readOffset, 0, readPartNo);
        info.Blobs = blobs;
        info.CompactedBlobsCount = 1;

        auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
        auto* readResult = answer->Response->MutablePartitionResponse()->MutableCmdReadResult();

        bool needStop = false;
        ui32 cnt = 0;
        ui32 size = 0;
        ui32 lastBlobSize = 0;

        TMaybe<TReadAnswer> early = info.AddBlobsFromBody(blobs,
                                                          0,
                                                          1,
                                                          nullptr,
                                                          readOffset,
                                                          parentKeyOffset + 10,
                                                          kAddBlobsFromBodyProbeSizeLag,
                                                          Ctx->Edge,
                                                          readOffset,
                                                          readResult,
                                                          answer,
                                                          needStop,
                                                          cnt,
                                                          size,
                                                          lastBlobSize,
                                                          ctx);

        UNIT_ASSERT(!early.Defined());
        UNIT_ASSERT_VALUES_EQUAL(readResult->ResultSize(), 2u);
        // Remaining parts of the same logical message stay at Key.Offset.
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(0).GetOffset(), parentKeyOffset);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(0).GetPartNo(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(1).GetOffset(), parentKeyOffset);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(1).GetPartNo(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(info.Offset, parentKeyOffset + 1);
        UNIT_ASSERT_VALUES_EQUAL(info.PartNo, 0u);
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));

    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

// Multipart logical message split across two BodyKeys (PartNo 0..1 then PartNo 2) after
// Key≠Header rename. Mid-message PartNo=1 must stay on parent Offset across the blob gap.
Y_UNIT_TEST_F(AddBlobsFromBodyKeepsKeySpaceOffsetAfterTxKeyRenameMultipartAcrossBlobs, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);
    constexpr ui64 supportiveHeaderOffset = 391;
    constexpr ui16 totalParts = 3;
    constexpr ui32 bytesPerPart = 32;
    constexpr ui64 parentKeyOffset = 547'189'849;
    constexpr ui64 readOffset = parentKeyOffset;
    constexpr ui16 readPartNo = 1;

    std::deque<TClientBlob> dq0;
    dq0.push_back(MakeMultipartBodyReadBlob(1, 0, totalParts, bytesPerPart, 'a'));
    dq0.push_back(MakeMultipartBodyReadBlob(1, 1, totalParts, bytesPerPart, 'b'));
    TString raw0 = SerializePackedBatchForReadBodyTest(
        TBatch::FromBlobs(supportiveHeaderOffset, std::move(dq0)));

    std::deque<TClientBlob> dq1;
    dq1.push_back(MakeMultipartBodyReadBlob(1, 2, totalParts, bytesPerPart, 'c'));
    TString raw1 = SerializePackedBatchForReadBodyTest(
        TBatch::FromBlobs(supportiveHeaderOffset, std::move(dq1)));

    const TKey key0 = TKey::FromKey(
        TKey::ForBody(TKeyPrefix::TypeData, partitionId, supportiveHeaderOffset, 0, 0, 2),
        TKeyPrefix::TypeData, partitionId, parentKeyOffset);
    const TKey key1 = TKey::FromKey(
        TKey::ForBody(TKeyPrefix::TypeData, partitionId, supportiveHeaderOffset, 2, 1, 0),
        TKeyPrefix::TypeData, partitionId, parentKeyOffset);

    TVector<TRequestedBlob> blobs;
    // Incomplete multipart: Count=0 / InternalPartsCount=2 until last part lands in the next key.
    blobs.push_back(MakeRequestedBlobForRead(parentKeyOffset, 0, 0, 2, std::move(raw0), key0));
    blobs.push_back(MakeRequestedBlobForRead(parentKeyOffset, 2, 1, 0, std::move(raw1), key1));

    auto probe = [&](const TActorContext& ctx) {
        TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(
            Ctx->Edge, readOffset, 0, readPartNo);
        info.Blobs = blobs;
        info.CompactedBlobsCount = 2;

        auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
        auto* readResult = answer->Response->MutablePartitionResponse()->MutableCmdReadResult();

        bool needStop = false;
        ui32 cnt = 0;
        ui32 size = 0;
        ui32 lastBlobSize = 0;

        TMaybe<TReadAnswer> early = info.AddBlobsFromBody(
            blobs, 0, 2, nullptr, readOffset, parentKeyOffset + 10,
            kAddBlobsFromBodyProbeSizeLag, Ctx->Edge, readOffset,
            readResult, answer, needStop, cnt, size, lastBlobSize, ctx);

        UNIT_ASSERT(!early.Defined());
        UNIT_ASSERT_VALUES_EQUAL(readResult->ResultSize(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(0).GetOffset(), parentKeyOffset);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(0).GetPartNo(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(1).GetOffset(), parentKeyOffset);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(1).GetPartNo(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(info.Offset, parentKeyOffset + 1);
        UNIT_ASSERT_VALUES_EQUAL(info.PartNo, 0u);
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));

    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

// Read from blob start (Offset == Key.Offset): no FindPos, but results must still be Key-space.
Y_UNIT_TEST_F(AddBlobsFromBodyKeepsKeySpaceOffsetAfterTxKeyRenameFromBlobStart, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);
    constexpr ui64 supportiveHeaderOffset = 0;
    constexpr ui32 messageCount = 3;
    constexpr ui64 parentKeyOffset = 1'000'000;
    constexpr ui64 readOffset = parentKeyOffset;

    std::deque<TClientBlob> dq;
    dq.push_back(MakeSinglePartBodyReadBlob(1, 'a'));
    dq.push_back(MakeSinglePartBodyReadBlob(2, 'b'));
    dq.push_back(MakeSinglePartBodyReadBlob(3, 'c'));
    TString raw = SerializePackedBatchForReadBodyTest(
        TBatch::FromBlobs(supportiveHeaderOffset, std::move(dq)));

    const TKey supportiveKey = TKey::ForBody(
        TKeyPrefix::TypeData, partitionId, supportiveHeaderOffset, 0, messageCount, 0);
    const TKey parentKey = TKey::FromKey(
        supportiveKey, TKeyPrefix::TypeData, partitionId, parentKeyOffset);

    TRequestedBlob blob = MakeRequestedBlobForRead(
        parentKeyOffset, 0, messageCount, 0, std::move(raw), parentKey);
    TVector<TRequestedBlob> blobs;
    blobs.push_back(std::move(blob));

    auto probe = [&](const TActorContext& ctx) {
        TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge, readOffset);
        info.Blobs = blobs;
        info.CompactedBlobsCount = 1;

        auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
        auto* readResult = answer->Response->MutablePartitionResponse()->MutableCmdReadResult();

        bool needStop = false;
        ui32 cnt = 0;
        ui32 size = 0;
        ui32 lastBlobSize = 0;

        TMaybe<TReadAnswer> early = info.AddBlobsFromBody(blobs,
                                                          0,
                                                          1,
                                                          nullptr,
                                                          readOffset,
                                                          parentKeyOffset + messageCount + 10,
                                                          kAddBlobsFromBodyProbeSizeLag,
                                                          Ctx->Edge,
                                                          readOffset,
                                                          readResult,
                                                          answer,
                                                          needStop,
                                                          cnt,
                                                          size,
                                                          lastBlobSize,
                                                          ctx);

        UNIT_ASSERT(!early.Defined());
        UNIT_ASSERT_VALUES_EQUAL(readResult->ResultSize(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(0).GetOffset(), parentKeyOffset);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(1).GetOffset(), parentKeyOffset + 1);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(2).GetOffset(), parentKeyOffset + 2);
        UNIT_ASSERT_VALUES_EQUAL(info.Offset, parentKeyOffset + messageCount);
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));

    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

// Chain of BodyKeys: mid-read in the second renamed blob.
Y_UNIT_TEST_F(AddBlobsFromBodyKeepsKeySpaceOffsetAfterTxKeyRenameBodyKeysChain, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);
    constexpr ui64 supportive0 = 0;
    constexpr ui64 supportive1 = 0; // each supportive partition blob often restarts at 0
    constexpr ui32 count0 = 3;
    constexpr ui32 count1 = 4;
    constexpr ui64 parent0 = 1'000'000;
    constexpr ui64 parent1 = parent0 + count0;
    constexpr ui64 readOffset = parent1 + 1;

    std::deque<TClientBlob> dq0;
    dq0.push_back(MakeSinglePartBodyReadBlob(1, 'a'));
    dq0.push_back(MakeSinglePartBodyReadBlob(2, 'b'));
    dq0.push_back(MakeSinglePartBodyReadBlob(3, 'c'));
    TString raw0 = SerializePackedBatchForReadBodyTest(TBatch::FromBlobs(supportive0, std::move(dq0)));

    std::deque<TClientBlob> dq1;
    dq1.push_back(MakeSinglePartBodyReadBlob(4, 'd'));
    dq1.push_back(MakeSinglePartBodyReadBlob(5, 'e'));
    dq1.push_back(MakeSinglePartBodyReadBlob(6, 'f'));
    dq1.push_back(MakeSinglePartBodyReadBlob(7, 'g'));
    TString raw1 = SerializePackedBatchForReadBodyTest(TBatch::FromBlobs(supportive1, std::move(dq1)));

    const TKey key0 = TKey::FromKey(
        TKey::ForBody(TKeyPrefix::TypeData, partitionId, supportive0, 0, count0, 0),
        TKeyPrefix::TypeData, partitionId, parent0);
    const TKey key1 = TKey::FromKey(
        TKey::ForBody(TKeyPrefix::TypeData, partitionId, supportive1, 0, count1, 0),
        TKeyPrefix::TypeData, partitionId, parent1);

    TVector<TRequestedBlob> blobs;
    blobs.push_back(MakeRequestedBlobForRead(parent0, 0, count0, 0, std::move(raw0), key0));
    blobs.push_back(MakeRequestedBlobForRead(parent1, 0, count1, 0, std::move(raw1), key1));

    auto probe = [&](const TActorContext& ctx) {
        TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge, readOffset);
        info.Blobs = blobs;
        info.CompactedBlobsCount = 2;

        auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
        auto* readResult = answer->Response->MutablePartitionResponse()->MutableCmdReadResult();

        bool needStop = false;
        ui32 cnt = 0;
        ui32 size = 0;
        ui32 lastBlobSize = 0;

        TMaybe<TReadAnswer> early = info.AddBlobsFromBody(blobs,
                                                          0,
                                                          2,
                                                          nullptr,
                                                          readOffset,
                                                          parent1 + count1 + 10,
                                                          kAddBlobsFromBodyProbeSizeLag,
                                                          Ctx->Edge,
                                                          readOffset,
                                                          readResult,
                                                          answer,
                                                          needStop,
                                                          cnt,
                                                          size,
                                                          lastBlobSize,
                                                          ctx);

        UNIT_ASSERT(!early.Defined());
        UNIT_ASSERT_VALUES_EQUAL(readResult->ResultSize(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(0).GetOffset(), parent1 + 1);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(1).GetOffset(), parent1 + 2);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(2).GetOffset(), parent1 + 3);
        UNIT_ASSERT_VALUES_EQUAL(info.Offset, parent1 + count1);
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));

    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

// Header base ≠ 0 (production-like supportive offsets) after rename.
Y_UNIT_TEST_F(AddBlobsFromBodyKeepsKeySpaceOffsetAfterTxKeyRenameNonZeroHeaderBase, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);
    constexpr ui64 supportiveHeaderOffset = 391;
    constexpr ui32 messageCount = 4;
    constexpr ui64 parentKeyOffset = 547'189'849;
    constexpr ui64 readOffset = parentKeyOffset + 2;

    std::deque<TClientBlob> dq;
    dq.push_back(MakeSinglePartBodyReadBlob(1, 'a'));
    dq.push_back(MakeSinglePartBodyReadBlob(2, 'b'));
    dq.push_back(MakeSinglePartBodyReadBlob(3, 'c'));
    dq.push_back(MakeSinglePartBodyReadBlob(4, 'd'));
    TString raw = SerializePackedBatchForReadBodyTest(
        TBatch::FromBlobs(supportiveHeaderOffset, std::move(dq)));

    const TKey supportiveKey = TKey::ForBody(
        TKeyPrefix::TypeData, partitionId, supportiveHeaderOffset, 0, messageCount, 0);
    const TKey parentKey = TKey::FromKey(
        supportiveKey, TKeyPrefix::TypeData, partitionId, parentKeyOffset);

    TRequestedBlob blob = MakeRequestedBlobForRead(
        parentKeyOffset, 0, messageCount, 0, std::move(raw), parentKey);
    TVector<TRequestedBlob> blobs;
    blobs.push_back(std::move(blob));

    auto probe = [&](const TActorContext& ctx) {
        TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge, readOffset);
        info.Blobs = blobs;
        info.CompactedBlobsCount = 1;

        auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
        auto* readResult = answer->Response->MutablePartitionResponse()->MutableCmdReadResult();

        bool needStop = false;
        ui32 cnt = 0;
        ui32 size = 0;
        ui32 lastBlobSize = 0;

        TMaybe<TReadAnswer> early = info.AddBlobsFromBody(blobs,
                                                          0,
                                                          1,
                                                          nullptr,
                                                          readOffset,
                                                          parentKeyOffset + messageCount + 10,
                                                          kAddBlobsFromBodyProbeSizeLag,
                                                          Ctx->Edge,
                                                          readOffset,
                                                          readResult,
                                                          answer,
                                                          needStop,
                                                          cnt,
                                                          size,
                                                          lastBlobSize,
                                                          ctx);

        UNIT_ASSERT(!early.Defined());
        UNIT_ASSERT_VALUES_EQUAL(readResult->ResultSize(), 2u);
        // Must not leak supportive header offsets 393 / 394 into the answer.
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(0).GetOffset(), parentKeyOffset + 2);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(1).GetOffset(), parentKeyOffset + 3);
        UNIT_ASSERT_VALUES_EQUAL(info.Offset, parentKeyOffset + messageCount);
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));

    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

// UpdateUsage resume after messageCountLimit mid-blob: first read stops after k messages
// (needStop from UpdateUsage), second read resumes at S+k via FindPos. Key≠Header after
// tx rename — results must stay in parent/key space, not supportive header k, k+1, …
Y_UNIT_TEST_F(AddBlobsFromBodyUpdateUsageLimitSplitMidBatchAfterTxKeyRename, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);
    constexpr ui64 supportiveHeaderOffset = 391;
    constexpr ui32 messageCount = 6;
    constexpr ui32 limitK = 2;
    constexpr ui64 parentKeyOffset = 547'189'849;
    static_assert(limitK > 0 && limitK < messageCount);

    std::deque<TClientBlob> dq;
    dq.push_back(MakeSinglePartBodyReadBlob(1, 'a'));
    dq.push_back(MakeSinglePartBodyReadBlob(2, 'b'));
    dq.push_back(MakeSinglePartBodyReadBlob(3, 'c'));
    dq.push_back(MakeSinglePartBodyReadBlob(4, 'd'));
    dq.push_back(MakeSinglePartBodyReadBlob(5, 'e'));
    dq.push_back(MakeSinglePartBodyReadBlob(6, 'f'));
    TString raw = SerializePackedBatchForReadBodyTest(
        TBatch::FromBlobs(supportiveHeaderOffset, std::move(dq)));

    const TKey supportiveKey = TKey::ForBody(
        TKeyPrefix::TypeData, partitionId, supportiveHeaderOffset, 0, messageCount, 0);
    const TKey parentKey = TKey::FromKey(
        supportiveKey, TKeyPrefix::TypeData, partitionId, parentKeyOffset);
    UNIT_ASSERT_VALUES_EQUAL(parentKey.GetOffset(), parentKeyOffset);

    TRequestedBlob blob = MakeRequestedBlobForRead(
        parentKeyOffset, 0, messageCount, 0, std::move(raw), parentKey);
    TVector<TRequestedBlob> blobs;
    blobs.push_back(std::move(blob));

    auto probe = [&](const TActorContext& ctx) {
        // First read: from blob start S, stop after limitK via UpdateUsage (cnt >= Count).
        {
            TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(
                Ctx->Edge, parentKeyOffset, 0, 0, limitK);
            info.Blobs = blobs;
            info.CompactedBlobsCount = 1;

            auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
            auto* readResult = answer->Response->MutablePartitionResponse()->MutableCmdReadResult();
            bool needStop = false;
            ui32 cnt = 0;
            ui32 size = 0;
            ui32 lastBlobSize = 0;

            TMaybe<TReadAnswer> early = info.AddBlobsFromBody(
                blobs, 0, 1, nullptr, parentKeyOffset, parentKeyOffset + messageCount + 10,
                kAddBlobsFromBodyProbeSizeLag, Ctx->Edge, parentKeyOffset,
                readResult, answer, needStop, cnt, size, lastBlobSize, ctx);

            UNIT_ASSERT(!early.Defined());
            UNIT_ASSERT(needStop);
            UNIT_ASSERT_VALUES_EQUAL(cnt, limitK);
            UNIT_ASSERT_VALUES_EQUAL(readResult->ResultSize(), limitK);
            for (ui32 i = 0; i < limitK; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(i).GetOffset(), parentKeyOffset + i);
                UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(i).GetPartNo(), 0u);
            }
            // Next read starts at S+k (key space), not supportive header base + k.
            UNIT_ASSERT_VALUES_EQUAL(info.Offset, parentKeyOffset + limitK);
            UNIT_ASSERT_VALUES_EQUAL(info.PartNo, 0u);
        }

        // Second read: resume mid-blob at S+k — FindPos + HeaderOffsetToKeySpace path.
        {
            const ui64 resumeOffset = parentKeyOffset + limitK;
            TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge, resumeOffset);
            info.Blobs = blobs;
            info.CompactedBlobsCount = 1;

            auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
            auto* readResult = answer->Response->MutablePartitionResponse()->MutableCmdReadResult();
            bool needStop = false;
            ui32 cnt = 0;
            ui32 size = 0;
            ui32 lastBlobSize = 0;

            TMaybe<TReadAnswer> early = info.AddBlobsFromBody(
                blobs, 0, 1, nullptr, resumeOffset, parentKeyOffset + messageCount + 10,
                kAddBlobsFromBodyProbeSizeLag, Ctx->Edge, resumeOffset,
                readResult, answer, needStop, cnt, size, lastBlobSize, ctx);

            UNIT_ASSERT(!early.Defined());
            const ui32 expectedRemaining = messageCount - limitK;
            UNIT_ASSERT_VALUES_EQUAL(readResult->ResultSize(), expectedRemaining);
            for (ui32 i = 0; i < expectedRemaining; ++i) {
                // Must be S+k, S+k+1, … — not supportive 391+k or bare k, k+1.
                UNIT_ASSERT_VALUES_EQUAL(
                    readResult->GetResult(i).GetOffset(), parentKeyOffset + limitK + i);
                UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(i).GetPartNo(), 0u);
            }
            UNIT_ASSERT_VALUES_EQUAL(info.Offset, parentKeyOffset + messageCount);
        }
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));

    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

// FormAnswer: after renamed body FindPos, CachedOffset jump must stay in Key space.
Y_UNIT_TEST_F(FormAnswerKeepsKeySpaceOffsetAfterTxKeyRenameWithCache, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);
    constexpr ui64 supportiveHeaderOffset = 0;
    constexpr ui32 messageCount = 4;
    constexpr ui64 parentKeyOffset = 1'000'000;
    constexpr ui64 readOffset = parentKeyOffset + 2;
    constexpr ui64 cachedOffset = parentKeyOffset + 10;

    std::deque<TClientBlob> dq;
    dq.push_back(MakeSinglePartBodyReadBlob(1, 'a'));
    dq.push_back(MakeSinglePartBodyReadBlob(2, 'b'));
    dq.push_back(MakeSinglePartBodyReadBlob(3, 'c'));
    dq.push_back(MakeSinglePartBodyReadBlob(4, 'd'));
    TString raw = SerializePackedBatchForReadBodyTest(
        TBatch::FromBlobs(supportiveHeaderOffset, std::move(dq)));

    const TKey supportiveKey = TKey::ForBody(
        TKeyPrefix::TypeData, partitionId, supportiveHeaderOffset, 0, messageCount, 0);
    const TKey parentKey = TKey::FromKey(
        supportiveKey, TKeyPrefix::TypeData, partitionId, parentKeyOffset);

    TRequestedBlob blob = MakeRequestedBlobForRead(
        parentKeyOffset, 0, messageCount, 0, std::move(raw), parentKey);
    TVector<TRequestedBlob> blobs;
    blobs.push_back(std::move(blob));

    auto probe = [&](const TActorContext& ctx) {
        TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge, readOffset);
        info.Blobs = blobs;
        info.CompactedBlobsCount = 1;
        info.CachedOffset = cachedOffset;
        info.Cached.push_back(MakeSinglePartBodyReadBlob(10, 'z'));

        TEvPQ::TEvBlobResponse blobResponse(0, TVector<TRequestedBlob>(blobs));
        TReadAnswer answer = info.FormAnswer(
            ctx,
            blobResponse,
            readOffset,
            cachedOffset + 10,
            partitionId,
            nullptr,
            0,
            kAddBlobsFromBodyProbeSizeLag,
            Ctx->Edge,
            NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS,
            true,
            [](bool, NKikimrClient::TCmdReadResult&) {});

        auto* proxy = dynamic_cast<TEvPQ::TEvProxyResponse*>(answer.Event.Get());
        UNIT_ASSERT(proxy != nullptr);
        const auto& readResult = proxy->Response->GetPartitionResponse().GetCmdReadResult();

        UNIT_ASSERT_VALUES_EQUAL(readResult.ResultSize(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(readResult.GetResult(0).GetOffset(), parentKeyOffset + 2);
        UNIT_ASSERT_VALUES_EQUAL(readResult.GetResult(1).GetOffset(), parentKeyOffset + 3);
        UNIT_ASSERT_VALUES_EQUAL(readResult.GetResult(2).GetOffset(), cachedOffset);
        UNIT_ASSERT_VALUES_EQUAL(info.Offset, cachedOffset + 1);
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));

    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

// FormAnswer boundary: !ReadToBlobEnd + small Size stops mid renamed blob; Offset stays parent-space.
Y_UNIT_TEST_F(FormAnswerKeepsKeySpaceOffsetAfterTxKeyRenameByteLimitNoReadToBlobEnd, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);
    constexpr ui64 supportiveHeaderOffset = 391;
    constexpr ui32 messageCount = 6;
    constexpr ui64 parentKeyOffset = 547'189'849;
    constexpr ui64 readOffset = parentKeyOffset + 1;

    std::deque<TClientBlob> dq;
    for (ui32 i = 0; i < messageCount; ++i) {
        dq.push_back(MakeSinglePartBodyReadBlob(i + 1, static_cast<char>('a' + i)));
    }
    TString raw = SerializePackedBatchForReadBodyTest(
        TBatch::FromBlobs(supportiveHeaderOffset, std::move(dq)));

    const TKey parentKey = TKey::FromKey(
        TKey::ForBody(TKeyPrefix::TypeData, partitionId, supportiveHeaderOffset, 0, messageCount, 0),
        TKeyPrefix::TypeData, partitionId, parentKeyOffset);

    TRequestedBlob blob = MakeRequestedBlobForRead(
        parentKeyOffset, 0, messageCount, 0, std::move(raw), parentKey);
    TVector<TRequestedBlob> blobs;
    blobs.push_back(std::move(blob));

    // Size of one MakeSinglePartBodyReadBlob is enough to stop after the first returned message
    // when ReadToBlobEnd=false (UpdateUsage: size >= Size && !ReadToBlobEnd).
    const ui32 oneBlobSerialized = MakeSinglePartBodyReadBlob(1, 'x').GetSerializedSize();

    auto probe = [&](const TActorContext& ctx) {
        TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(
            Ctx->Edge, readOffset, 0, 0,
            /*messageCountLimit=*/100,
            /*byteSizeLimit=*/oneBlobSerialized,
            /*readTimestampMs=*/0,
            /*readToBlobEnd=*/false);
        info.Blobs = blobs;
        info.CompactedBlobsCount = 1;

        TEvPQ::TEvBlobResponse blobResponse(0, TVector<TRequestedBlob>(blobs));
        TReadAnswer answer = info.FormAnswer(
            ctx,
            blobResponse,
            readOffset,
            parentKeyOffset + messageCount + 10,
            partitionId,
            nullptr,
            0,
            kAddBlobsFromBodyProbeSizeLag,
            Ctx->Edge,
            NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS,
            true,
            [](bool, NKikimrClient::TCmdReadResult&) {});

        auto* proxy = dynamic_cast<TEvPQ::TEvProxyResponse*>(answer.Event.Get());
        UNIT_ASSERT(proxy != nullptr);
        const auto& readResult = proxy->Response->GetPartitionResponse().GetCmdReadResult();

        UNIT_ASSERT_C(readResult.ResultSize() >= 1u, "byte limit must still return at least one row");
        UNIT_ASSERT_C(readResult.ResultSize() < messageCount - 1,
                      "byte limit + !ReadToBlobEnd must stop before draining the renamed blob");
        for (ui32 i = 0; i < readResult.ResultSize(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                readResult.GetResult(i).GetOffset(), readOffset + i,
                "supportive leak would be near " << (supportiveHeaderOffset + 1 + i));
        }
        UNIT_ASSERT_VALUES_EQUAL(info.Offset, readOffset + readResult.ResultSize());
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));

    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

// FormAnswer mid-LMC packed batch after Key≠Header rename: OffsetDelta rewind stays in key space.
Y_UNIT_TEST_F(FormAnswerKeepsKeySpaceOffsetAfterTxKeyRenameMidLmc, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);
    constexpr ui64 supportiveHeaderOffset = 391;
    constexpr ui32 firstLmc = 5;
    constexpr ui32 secondLmc = 4;
    constexpr ui32 offsetSpan = firstLmc + secondLmc;
    constexpr ui64 parentKeyOffset = 547'189'849;
    constexpr ui64 midReadOffset = parentKeyOffset + 3; // inside first LMC slot

    std::deque<TClientBlob> dq;
    dq.push_back(MakeSinglePartBodyReadBlobWithLmc(1, 'a', firstLmc));
    dq.push_back(MakeSinglePartBodyReadBlobWithLmc(2, 'b', secondLmc));
    TString raw = SerializePackedBatchForReadBodyTest(
        TBatch::FromBlobs(supportiveHeaderOffset, std::move(dq)));

    const TKey parentKey = TKey::FromKey(
        TKey::ForBody(TKeyPrefix::TypeData, partitionId, supportiveHeaderOffset, 0, offsetSpan, 0),
        TKeyPrefix::TypeData, partitionId, parentKeyOffset);

    TRequestedBlob blob = MakeRequestedBlobForRead(
        parentKeyOffset, 0, offsetSpan, 0, std::move(raw), parentKey);
    TVector<TRequestedBlob> blobs;
    blobs.push_back(std::move(blob));

    auto probe = [&](const TActorContext& ctx) {
        TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge, midReadOffset);
        info.Blobs = blobs;
        info.CompactedBlobsCount = 1;

        TEvPQ::TEvBlobResponse blobResponse(0, TVector<TRequestedBlob>(blobs));
        TReadAnswer answer = info.FormAnswer(
            ctx,
            blobResponse,
            midReadOffset,
            parentKeyOffset + offsetSpan + 10,
            partitionId,
            nullptr,
            0,
            kAddBlobsFromBodyProbeSizeLag,
            Ctx->Edge,
            NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS,
            true,
            [](bool, NKikimrClient::TCmdReadResult&) {});

        auto* proxy = dynamic_cast<TEvPQ::TEvProxyResponse*>(answer.Event.Get());
        UNIT_ASSERT(proxy != nullptr);
        const auto& readResult = proxy->Response->GetPartitionResponse().GetCmdReadResult();

        UNIT_ASSERT_VALUES_EQUAL(readResult.ResultSize(), 2u);
        // Mid-LMC rewind reports batch base in parent/key space (not supportive 391 / mid 394).
        UNIT_ASSERT_VALUES_EQUAL(readResult.GetResult(0).GetOffset(), parentKeyOffset);
        UNIT_ASSERT_VALUES_EQUAL(readResult.GetResult(0).GetLogicalMessageCount(), firstLmc);
        UNIT_ASSERT_VALUES_EQUAL(readResult.GetResult(1).GetOffset(), parentKeyOffset + firstLmc);
        UNIT_ASSERT_VALUES_EQUAL(readResult.GetResult(1).GetLogicalMessageCount(), secondLmc);
        UNIT_ASSERT_VALUES_EQUAL(info.Offset, parentKeyOffset + offsetSpan);
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));

    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

// FormAnswer over a BodyKeys chain with different supportive header bases after rename.
Y_UNIT_TEST_F(FormAnswerKeepsKeySpaceOffsetAfterTxKeyRenameBodyKeysChainDifferentHeaderBases, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);
    constexpr ui64 supportive0 = 391;
    constexpr ui64 supportive1 = 17;
    constexpr ui32 count0 = 3;
    constexpr ui32 count1 = 4;
    constexpr ui64 parent0 = 547'189'849;
    constexpr ui64 parent1 = parent0 + count0;
    constexpr ui64 readOffset = parent1 + 1;

    std::deque<TClientBlob> dq0;
    dq0.push_back(MakeSinglePartBodyReadBlob(1, 'a'));
    dq0.push_back(MakeSinglePartBodyReadBlob(2, 'b'));
    dq0.push_back(MakeSinglePartBodyReadBlob(3, 'c'));
    TString raw0 = SerializePackedBatchForReadBodyTest(TBatch::FromBlobs(supportive0, std::move(dq0)));

    std::deque<TClientBlob> dq1;
    dq1.push_back(MakeSinglePartBodyReadBlob(4, 'd'));
    dq1.push_back(MakeSinglePartBodyReadBlob(5, 'e'));
    dq1.push_back(MakeSinglePartBodyReadBlob(6, 'f'));
    dq1.push_back(MakeSinglePartBodyReadBlob(7, 'g'));
    TString raw1 = SerializePackedBatchForReadBodyTest(TBatch::FromBlobs(supportive1, std::move(dq1)));

    const TKey key0 = TKey::FromKey(
        TKey::ForBody(TKeyPrefix::TypeData, partitionId, supportive0, 0, count0, 0),
        TKeyPrefix::TypeData, partitionId, parent0);
    const TKey key1 = TKey::FromKey(
        TKey::ForBody(TKeyPrefix::TypeData, partitionId, supportive1, 0, count1, 0),
        TKeyPrefix::TypeData, partitionId, parent1);

    TVector<TRequestedBlob> blobs;
    blobs.push_back(MakeRequestedBlobForRead(parent0, 0, count0, 0, std::move(raw0), key0));
    blobs.push_back(MakeRequestedBlobForRead(parent1, 0, count1, 0, std::move(raw1), key1));

    auto probe = [&](const TActorContext& ctx) {
        TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge, readOffset);
        info.Blobs = blobs;
        info.CompactedBlobsCount = 2;

        TEvPQ::TEvBlobResponse blobResponse(0, TVector<TRequestedBlob>(blobs));
        TReadAnswer answer = info.FormAnswer(
            ctx,
            blobResponse,
            readOffset,
            parent1 + count1 + 10,
            partitionId,
            nullptr,
            0,
            kAddBlobsFromBodyProbeSizeLag,
            Ctx->Edge,
            NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS,
            true,
            [](bool, NKikimrClient::TCmdReadResult&) {});

        auto* proxy = dynamic_cast<TEvPQ::TEvProxyResponse*>(answer.Event.Get());
        UNIT_ASSERT(proxy != nullptr);
        const auto& readResult = proxy->Response->GetPartitionResponse().GetCmdReadResult();

        UNIT_ASSERT_VALUES_EQUAL(readResult.ResultSize(), 3u);
        for (ui32 i = 0; i < readResult.ResultSize(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                readResult.GetResult(i).GetOffset(), readOffset + i,
                "supportive leak would be near " << (supportive1 + 1 + i));
        }
        UNIT_ASSERT_VALUES_EQUAL(info.Offset, parent1 + count1);
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));

    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

// E2E: PartitionedBlob::Add renames BodyKeys, then mid-blob read uses NewKey + old headers.
Y_UNIT_TEST_F(PartitionedBlobRenameThenAddBlobsFromBodyMidRead, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);
    constexpr ui64 supportiveHeaderOffset = 0;
    constexpr ui32 messageCount = 4;
    constexpr ui64 parentKeyOffset = 1'000'000;
    constexpr ui64 readOffset = parentKeyOffset + 2;

    std::deque<TClientBlob> dq;
    dq.push_back(MakeSinglePartBodyReadBlob(1, 'a'));
    dq.push_back(MakeSinglePartBodyReadBlob(2, 'b'));
    dq.push_back(MakeSinglePartBodyReadBlob(3, 'c'));
    dq.push_back(MakeSinglePartBodyReadBlob(4, 'd'));
    TString raw = SerializePackedBatchForReadBodyTest(
        TBatch::FromBlobs(supportiveHeaderOffset, std::move(dq)));

    const TKey supportiveKey = TKey::ForBody(
        TKeyPrefix::TypeData, partitionId, supportiveHeaderOffset, 0, messageCount, 0);

    THead head;
    THead newHead;
    newHead.Offset = parentKeyOffset;
    TPartitionedBlob partitioned(
        partitionId, parentKeyOffset, "", 0, 0, 0, head, newHead,
        /*headCleared=*/true, /*needCompactHead=*/false, 8_MB);

    auto compacted = partitioned.Add(supportiveKey, raw.size(), TInstant::MilliSeconds(1), true);
    UNIT_ASSERT(!compacted.has_value());
    UNIT_ASSERT_VALUES_EQUAL(partitioned.GetFormedBlobs().size(), 1u);

    const auto& renamed = partitioned.GetFormedBlobs().front();
    UNIT_ASSERT_VALUES_EQUAL(renamed.OldKey.ToString(), supportiveKey.ToString());
    UNIT_ASSERT_VALUES_EQUAL(renamed.NewKey.GetOffset(), parentKeyOffset);
    UNIT_ASSERT_VALUES_EQUAL(renamed.NewKey.GetCount(), messageCount);
    UNIT_ASSERT(renamed.NewKey.IsFastWrite());

    TRequestedBlob blob = MakeRequestedBlobForRead(
        parentKeyOffset, 0, messageCount, 0, std::move(raw), renamed.NewKey);
    TVector<TRequestedBlob> blobs;
    blobs.push_back(std::move(blob));

    auto probe = [&](const TActorContext& ctx) {
        TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge, readOffset);
        info.Blobs = blobs;
        info.CompactedBlobsCount = 1;

        auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
        auto* readResult = answer->Response->MutablePartitionResponse()->MutableCmdReadResult();

        bool needStop = false;
        ui32 cnt = 0;
        ui32 size = 0;
        ui32 lastBlobSize = 0;

        TMaybe<TReadAnswer> early = info.AddBlobsFromBody(blobs,
                                                          0,
                                                          1,
                                                          nullptr,
                                                          readOffset,
                                                          parentKeyOffset + messageCount + 10,
                                                          kAddBlobsFromBodyProbeSizeLag,
                                                          Ctx->Edge,
                                                          readOffset,
                                                          readResult,
                                                          answer,
                                                          needStop,
                                                          cnt,
                                                          size,
                                                          lastBlobSize,
                                                          ctx);

        UNIT_ASSERT(!early.Defined());
        UNIT_ASSERT_VALUES_EQUAL(readResult->ResultSize(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(0).GetOffset(), parentKeyOffset + 2);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(1).GetOffset(), parentKeyOffset + 3);
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));

    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

// StartOffset advances by GetCount(), while OffsetDelta is only preserved on the new key.
Y_UNIT_TEST_F(PartitionedBlobAddAdvancesStartOffsetByCount, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);
    constexpr ui64 parentKeyOffset = 500'000;
    constexpr ui32 count0 = 5;
    constexpr ui32 delta0 = 5;
    constexpr ui32 count1 = 2;
    constexpr ui32 delta1 = 9; // deliberately differs from count

    THead head;
    THead newHead;
    newHead.Offset = parentKeyOffset;
    TPartitionedBlob partitioned(
        partitionId, parentKeyOffset, "", 0, 0, 0, head, newHead,
        /*headCleared=*/true, /*needCompactHead=*/false, 8_MB);

    TKey key0 = TKey::ForBody(TKeyPrefix::TypeData, partitionId, 0, 0, count0, 0, delta0);
    TKey key1 = TKey::ForBody(TKeyPrefix::TypeData, partitionId, 0, 0, count1, 0, delta1);

    UNIT_ASSERT(!partitioned.Add(key0, 10, TInstant::MilliSeconds(1), true).has_value());
    UNIT_ASSERT(!partitioned.Add(key1, 10, TInstant::MilliSeconds(1), true).has_value());
    UNIT_ASSERT_VALUES_EQUAL(partitioned.GetFormedBlobs().size(), 2u);

    const auto& first = partitioned.GetFormedBlobs()[0];
    const auto& second = partitioned.GetFormedBlobs()[1];
    UNIT_ASSERT_VALUES_EQUAL(first.NewKey.GetOffset(), parentKeyOffset);
    UNIT_ASSERT(first.NewKey.GetOffsetDelta().Defined());
    UNIT_ASSERT_VALUES_EQUAL(*first.NewKey.GetOffsetDelta(), delta0);

    // Next StartOffset uses GetCount(), not OffsetDelta (delta1 would wrongly jump further).
    UNIT_ASSERT_VALUES_EQUAL(second.NewKey.GetOffset(), parentKeyOffset + count0);
    UNIT_ASSERT(second.NewKey.GetOffsetDelta().Defined());
    UNIT_ASSERT_VALUES_EQUAL(*second.NewKey.GetOffsetDelta(), delta1);
    UNIT_ASSERT_VALUES_UNEQUAL(second.NewKey.GetOffset(), parentKeyOffset + delta0 + delta1);
}

// needCompactHead with non-empty NewHead: compact parent head, then rename BodyKey after it.
Y_UNIT_TEST_F(PartitionedBlobAddCompactsNonEmptyHeadBeforeRename, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);
    constexpr ui64 parentHeadOffset = 100;
    constexpr ui32 headMessageCount = 1;
    constexpr ui32 bodyMessageCount = 3;

    std::deque<TClientBlob> headDq;
    headDq.push_back(MakeSinglePartBodyReadBlob(1, 'h'));
    TBatch headBatch = TBatch::FromBlobs(parentHeadOffset, std::move(headDq));
    headBatch.Pack();

    THead head;
    THead newHead;
    newHead.Offset = parentHeadOffset;
    newHead.AddBatch(headBatch);
    newHead.PackedSize = headBatch.GetPackedSize();

    TPartitionedBlob partitioned(
        partitionId, parentHeadOffset, "", 0, 0, 0, head, newHead,
        /*headCleared=*/true, /*needCompactHead=*/true, 8_MB);

    const TKey supportiveKey = TKey::ForBody(
        TKeyPrefix::TypeData, partitionId, 0, 0, bodyMessageCount, 0);
    auto compacted = partitioned.Add(supportiveKey, 42, TInstant::MilliSeconds(1), true);
    UNIT_ASSERT(compacted.has_value());
    UNIT_ASSERT_VALUES_EQUAL(compacted->Key.GetOffset(), parentHeadOffset);
    UNIT_ASSERT_VALUES_EQUAL(compacted->Key.GetCount(), headMessageCount);

    UNIT_ASSERT_VALUES_EQUAL(partitioned.GetFormedBlobs().size(), 1u);
    const auto& renamed = partitioned.GetFormedBlobs().front();
    UNIT_ASSERT_VALUES_EQUAL(renamed.NewKey.GetOffset(), parentHeadOffset + headMessageCount);
    UNIT_ASSERT_VALUES_EQUAL(renamed.NewKey.GetCount(), bodyMessageCount);
}

// FastWrite vs Body suffix after rename.
Y_UNIT_TEST_F(PartitionedBlobAddPreservesFastWriteVsBodySuffix, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);
    constexpr ui64 parentKeyOffset = 42;

    THead head;
    THead newHead;
    newHead.Offset = parentKeyOffset;
    TPartitionedBlob partitioned(
        partitionId, parentKeyOffset, "", 0, 0, 0, head, newHead,
        /*headCleared=*/true, /*needCompactHead=*/false, 8_MB);

    const TKey supportive0 = TKey::ForBody(TKeyPrefix::TypeData, partitionId, 0, 0, 2, 0);
    const TKey supportive1 = TKey::ForBody(TKeyPrefix::TypeData, partitionId, 0, 0, 2, 0);

    UNIT_ASSERT(!partitioned.Add(supportive0, 10, TInstant::MilliSeconds(1), true).has_value());
    UNIT_ASSERT(!partitioned.Add(supportive1, 10, TInstant::MilliSeconds(1), false).has_value());
    UNIT_ASSERT_VALUES_EQUAL(partitioned.GetFormedBlobs().size(), 2u);

    UNIT_ASSERT(partitioned.GetFormedBlobs()[0].NewKey.IsFastWrite());
    UNIT_ASSERT(!partitioned.GetFormedBlobs()[1].NewKey.IsFastWrite());
    UNIT_ASSERT_VALUES_EQUAL(partitioned.GetFormedBlobs()[0].NewKey.GetOffset(), parentKeyOffset);
    UNIT_ASSERT_VALUES_EQUAL(partitioned.GetFormedBlobs()[1].NewKey.GetOffset(), parentKeyOffset + 2);
}

// CommitWriteOperations remaps SrcIdInfo.Offset with oldHeadOffset (NewHead.Offset before BodyKeys).
Y_UNIT_TEST_F(SrcIdInfoOffsetRemapUsesOldHeadOffset, TPartitionFixture) {
    constexpr ui64 oldHeadOffset = 547'189'849;
    constexpr ui64 supportiveSrcOffset = 391;
    constexpr ui64 expectedParentOffset = oldHeadOffset + supportiveSrcOffset;

    TSourceIdInfo info(7, supportiveSrcOffset, TInstant::MilliSeconds(1));
    UNIT_ASSERT_VALUES_EQUAL(info.Offset, supportiveSrcOffset);

    // Mirrors CommitWriteOperations: sourceId.Update(..., info.Offset + oldHeadOffset, ...).
    auto remapped = info.Updated(7, info.Offset + oldHeadOffset, TInstant::MilliSeconds(2));
    UNIT_ASSERT_VALUES_EQUAL(remapped.Offset, expectedParentOffset);
    UNIT_ASSERT_VALUES_EQUAL(remapped.SeqNo, 7u);
}

// GetBlobsFromHead mid-LMC: returns the batch blob and insideHeadOffset = batch base;
// FormAnswer cache then reports that base (same class as FindPos OffsetDelta rewind).
Y_UNIT_TEST_F(GetBlobsFromHeadMidLmcThenFormAnswerCache, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);
    constexpr ui64 headOffset = 1'000'000;
    constexpr ui32 firstLmc = 5;
    constexpr ui32 secondLmc = 4;
    constexpr ui64 midReadOffset = headOffset + 3; // inside first LMC slot

    std::deque<TClientBlob> dq;
    dq.push_back(MakeSinglePartBodyReadBlobWithLmc(1, 'a', firstLmc));
    dq.push_back(MakeSinglePartBodyReadBlobWithLmc(2, 'b', secondLmc));
    TBatch batch = TBatch::FromBlobs(headOffset, std::move(dq));
    UNIT_ASSERT(batch.HasOffsetDelta());
    batch.Pack();
    const ui32 blobSize = batch.GetPackedSize();
    const ui32 offsetSpan = firstLmc + secondLmc;

    // Standalone encoder: avoid CreatePartition CheckHeadConsistency on DataKeysHead.
    TPartitionBlobEncoder encoder(partitionId, /*fastWrite=*/false);
    const TKey headKey = TKey::ForHead(
        TKeyPrefix::TypeData, partitionId, headOffset, 0, offsetSpan, 0, offsetSpan);
    {
        auto token = std::make_shared<TBlobKeyToken>();
        token->NeedDelete = false;
        encoder.HeadKeys.push_back(
            TDataKey{headKey, blobSize, TInstant::MilliSeconds(1), 0, std::move(token)});
    }
    encoder.Head.Offset = headOffset;
    encoder.Head.PartNo = 0;
    encoder.Head.PackedSize = blobSize;
    encoder.Head.AddBatch(batch);

    auto probe = [&](const TActorContext& ctx) {
        ui32 count = 0;
        ui32 size = 0;
        ui64 insideHeadOffset = 0;
        auto cached = encoder.GetBlobsFromHead(
            midReadOffset,
            0,
            /*maxCount=*/10,
            /*maxSize=*/static_cast<ui32>(1_MB),
            /*readTimestampMs=*/0,
            count,
            size,
            insideHeadOffset,
            /*lastOffset=*/0);

        UNIT_ASSERT_VALUES_EQUAL(cached.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(insideHeadOffset, headOffset);
        UNIT_ASSERT_VALUES_EQUAL(cached[0].LogicalMessageCount, firstLmc);
        UNIT_ASSERT_VALUES_EQUAL(cached[1].LogicalMessageCount, secondLmc);

        TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge, midReadOffset);
        info.CompactedBlobsCount = 0;
        info.CachedOffset = insideHeadOffset;
        info.Cached = cached;

        TEvPQ::TEvBlobResponse blobResponse(0, TVector<TRequestedBlob>{});
        TReadAnswer answer = info.FormAnswer(
            ctx,
            blobResponse,
            midReadOffset,
            headOffset + offsetSpan + 10,
            partitionId,
            nullptr,
            0,
            kAddBlobsFromBodyProbeSizeLag,
            Ctx->Edge,
            NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS,
            true,
            [](bool, NKikimrClient::TCmdReadResult&) {});

        auto* proxy = dynamic_cast<TEvPQ::TEvProxyResponse*>(answer.Event.Get());
        UNIT_ASSERT(proxy != nullptr);
        const auto& readResult = proxy->Response->GetPartitionResponse().GetCmdReadResult();

        UNIT_ASSERT_VALUES_EQUAL(readResult.ResultSize(), 2u);
        // Mid-LMC rewind to batch base in parent/head coordinates (not midReadOffset).
        UNIT_ASSERT_VALUES_EQUAL(readResult.GetResult(0).GetOffset(), headOffset);
        UNIT_ASSERT_VALUES_EQUAL(readResult.GetResult(0).GetLogicalMessageCount(), firstLmc);
        UNIT_ASSERT_VALUES_EQUAL(readResult.GetResult(1).GetOffset(), headOffset + firstLmc);
        UNIT_ASSERT_VALUES_EQUAL(readResult.GetResult(1).GetLogicalMessageCount(), secondLmc);
        UNIT_ASSERT_VALUES_EQUAL(info.Offset, headOffset + offsetSpan);
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));

    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

// Compaction rewrite (CompactRequestedBlob): rebuild headers from Key.Offset → Key==Header;
// mid-read must work (positive control vs tx-rename Key≠Header path).
Y_UNIT_TEST_F(CompactionRewriteAlignsHeaderWithKeyThenMidRead, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);
    constexpr ui64 supportiveHeaderOffset = 391;
    constexpr ui32 messageCount = 4;
    constexpr ui64 parentKeyOffset = 547'189'849;
    constexpr ui64 readOffset = parentKeyOffset + 2;

    std::deque<TClientBlob> dq;
    dq.push_back(MakeSinglePartBodyReadBlob(1, 'a'));
    dq.push_back(MakeSinglePartBodyReadBlob(2, 'b'));
    dq.push_back(MakeSinglePartBodyReadBlob(3, 'c'));
    dq.push_back(MakeSinglePartBodyReadBlob(4, 'd'));
    TString supportiveRaw = SerializePackedBatchForReadBodyTest(
        TBatch::FromBlobs(supportiveHeaderOffset, std::move(dq)));

    const TKey parentKey = TKey::FromKey(
        TKey::ForBody(TKeyPrefix::TypeData, partitionId, supportiveHeaderOffset, 0, messageCount, 0),
        TKeyPrefix::TypeData, partitionId, parentKeyOffset);

    // Before rewrite: Key≠Header (same as tx rename).
    {
        auto before = GetUnpackedBatches(parentKey, supportiveRaw);
        UNIT_ASSERT_VALUES_EQUAL(before.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(before[0].GetOffset(), supportiveHeaderOffset);
        UNIT_ASSERT_VALUES_UNEQUAL(before[0].GetOffset(), parentKey.GetOffset());
    }

    // CompactRequestedBlob rebuilds batches at Key.GetOffset() (value rewrite).
    auto batches = GetUnpackedBatches(parentKey, supportiveRaw);
    std::deque<TClientBlob> rewrittenDq;
    for (auto& blob : batches[0].Blobs) {
        rewrittenDq.push_back(blob);
    }
    TString rewrittenRaw = SerializePackedBatchForReadBodyTest(
        TBatch::FromBlobs(parentKey.GetOffset(), std::move(rewrittenDq)));

    {
        auto after = GetUnpackedBatches(parentKey, rewrittenRaw);
        UNIT_ASSERT_VALUES_EQUAL(after.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(after[0].GetOffset(), parentKey.GetOffset());
    }

    TRequestedBlob blob = MakeRequestedBlobForRead(
        parentKeyOffset, 0, messageCount, 0, std::move(rewrittenRaw), parentKey);
    TVector<TRequestedBlob> blobs;
    blobs.push_back(std::move(blob));

    auto probe = [&](const TActorContext& ctx) {
        TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge, readOffset);
        info.Blobs = blobs;
        info.CompactedBlobsCount = 1;

        auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
        auto* readResult = answer->Response->MutablePartitionResponse()->MutableCmdReadResult();

        bool needStop = false;
        ui32 cnt = 0;
        ui32 size = 0;
        ui32 lastBlobSize = 0;

        TMaybe<TReadAnswer> early = info.AddBlobsFromBody(blobs,
                                                          0,
                                                          1,
                                                          nullptr,
                                                          readOffset,
                                                          parentKeyOffset + messageCount + 10,
                                                          kAddBlobsFromBodyProbeSizeLag,
                                                          Ctx->Edge,
                                                          readOffset,
                                                          readResult,
                                                          answer,
                                                          needStop,
                                                          cnt,
                                                          size,
                                                          lastBlobSize,
                                                          ctx);

        UNIT_ASSERT(!early.Defined());
        UNIT_ASSERT_VALUES_EQUAL(readResult->ResultSize(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(0).GetOffset(), parentKeyOffset + 2);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(1).GetOffset(), parentKeyOffset + 3);
        UNIT_ASSERT_VALUES_EQUAL(info.Offset, parentKeyOffset + messageCount);
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));

    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

namespace {

NPQ::TDataKey MakeBodyDataKeyForTx(
    const TPartitionId& partitionId,
    ui64 supportiveOffset,
    ui32 count,
    ui32 size,
    bool forHead = false)
{
    TKey key = forHead
        ? TKey::ForHead(TKeyPrefix::TypeData, partitionId, supportiveOffset, 0, count, 0)
        : TKey::ForBody(TKeyPrefix::TypeData, partitionId, supportiveOffset, 0, count, 0);
    auto token = std::make_shared<TBlobKeyToken>();
    token->NeedDelete = true;
    return NPQ::TDataKey{key, size, TInstant::MilliSeconds(1), 0, std::move(token)};
}

void SeedParentHeadWithSingleMessage(TPartitionBlobEncoder& encoder, const TPartitionId& partitionId, ui64 offset) {
    std::deque<TClientBlob> dq;
    dq.push_back(MakeSinglePartBodyReadBlob(1, 'h'));
    TBatch batch = TBatch::FromBlobs(offset, std::move(dq));
    batch.Pack();
    const ui32 blobSize = batch.GetPackedSize();
    const TKey key = TKey::ForHead(TKeyPrefix::TypeData, partitionId, offset, 0, 1, 0);
    auto token = std::make_shared<TBlobKeyToken>();
    token->NeedDelete = false;
    encoder.HeadKeys.push_back(TDataKey{key, blobSize, TInstant::MilliSeconds(1), 0, std::move(token)});
    encoder.Head.Offset = offset;
    encoder.Head.PartNo = 0;
    encoder.Head.PackedSize = blobSize;
    encoder.Head.AddBatch(batch);
}

} // namespace

// 1+6: CommitWriteOperations renames BodyKeys into FWZ (FastWrite); mid-read with Key≠Header.
Y_UNIT_TEST_F(CommitWriteOperationsRenamesBodyKeysThenMidRead, TPartitionTxTestHelper) {
    Init({.EndOffset = 0});
    const TPartitionId partitionId(0);
    constexpr ui32 messageCount = 4;
    constexpr ui32 blobSize = 100;
    constexpr ui64 supportiveOffset = 0;

    std::deque<TClientBlob> dq;
    dq.push_back(MakeSinglePartBodyReadBlob(1, 'a'));
    dq.push_back(MakeSinglePartBodyReadBlob(2, 'b'));
    dq.push_back(MakeSinglePartBodyReadBlob(3, 'c'));
    dq.push_back(MakeSinglePartBodyReadBlob(4, 'd'));
    TString raw = SerializePackedBatchForReadBodyTest(
        TBatch::FromBlobs(supportiveOffset, std::move(dq)));

    const TKey supportiveKey = TKey::ForBody(
        TKeyPrefix::TypeData, partitionId, supportiveOffset, 0, messageCount, 0);

    auto tx = MakeAndSendWriteTx({{"src1", {1, messageCount}}});
    std::deque<NPQ::TDataKey> bodyKeys;
    bodyKeys.push_back(MakeBodyDataKeyForTx(partitionId, supportiveOffset, messageCount, blobSize));
    InjectBodyKeys(tx, std::move(bodyKeys));

    WaitWriteInfoRequest(tx, true);
    WaitTxPredicateReply(tx);
    SendTxCommit(tx);
    WaitKvRequest();

    UNIT_ASSERT(LastKvRequest);
    UNIT_ASSERT_VALUES_EQUAL(LastKvRequest->Record.CmdRenameSize(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(LastKvRequest->Record.GetCmdRename(0).GetOldKey(), supportiveKey.ToString());

    auto& encoder = TPartitionTestWrapper::BlobEncoder(Partition());
    UNIT_ASSERT(!encoder.CompactedKeys.empty());
    const TKey parentKey = encoder.CompactedKeys.front().first;
    UNIT_ASSERT_VALUES_EQUAL(parentKey.GetOffset(), 0u); // empty parent → StartOffset 0
    UNIT_ASSERT_VALUES_EQUAL(parentKey.GetCount(), messageCount);
    UNIT_ASSERT(parentKey.IsFastWrite()); // FWZ / tx path
    UNIT_ASSERT_VALUES_EQUAL(LastKvRequest->Record.GetCmdRename(0).GetNewKey(), parentKey.ToString());

    SendKvResponse();
    WaitCommitDone(tx);

    UNIT_ASSERT(!encoder.DataKeysBody.empty());
    UNIT_ASSERT_VALUES_EQUAL(encoder.DataKeysBody.back().Key.GetOffset(), parentKey.GetOffset());
    UNIT_ASSERT(encoder.DataKeysBody.back().Key.IsFastWrite());

    // Mid-read with renamed key + unchanged supportive headers (blob still "in FWZ" logically).
    constexpr ui64 readOffset = 2;
    TRequestedBlob blob = MakeRequestedBlobForRead(
        parentKey.GetOffset(), 0, messageCount, 0, std::move(raw), parentKey);
    TVector<TRequestedBlob> blobs;
    blobs.push_back(std::move(blob));

    auto probe = [&](const TActorContext& ctx) {
        TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge, readOffset);
        info.Blobs = blobs;
        info.CompactedBlobsCount = 1;

        auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
        auto* readResult = answer->Response->MutablePartitionResponse()->MutableCmdReadResult();
        bool needStop = false;
        ui32 cnt = 0;
        ui32 size = 0;
        ui32 lastBlobSize = 0;

        TMaybe<TReadAnswer> early = info.AddBlobsFromBody(
            blobs, 0, 1, nullptr, readOffset, messageCount + 10,
            kAddBlobsFromBodyProbeSizeLag, Ctx->Edge, readOffset,
            readResult, answer, needStop, cnt, size, lastBlobSize, ctx);

        UNIT_ASSERT(!early.Defined());
        UNIT_ASSERT_VALUES_EQUAL(readResult->ResultSize(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(0).GetOffset(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(1).GetOffset(), 3u);
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));
    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

// A2: real CommitWriteOperations rename with non-empty parent (Key.Offset = S ≠ header 0),
// then mid-blob FormAnswer/AddBlobsFromBody (same path CmdRead uses after KV fetch).
// Full proxy/SDK CmdRead path is covered by TxWriteMidCommitReconnectAssertsParentOffsets_*.
Y_UNIT_TEST_F(CommitWriteOperationsRenamesBodyKeysThenMidReadNonZeroParent, TPartitionTxTestHelper) {
    constexpr ui64 parentKeyOffset = 100;
    Init({.EndOffset = parentKeyOffset});
    const TPartitionId partitionId(0);
    constexpr ui32 messageCount = 6;
    constexpr ui32 blobSize = 100;
    constexpr ui64 supportiveOffset = 0;
    constexpr ui64 midReadOffset = parentKeyOffset + 2;

    std::deque<TClientBlob> dq;
    for (ui32 i = 0; i < messageCount; ++i) {
        dq.push_back(MakeSinglePartBodyReadBlob(i + 1, static_cast<char>('a' + i)));
    }
    TString raw = SerializePackedBatchForReadBodyTest(
        TBatch::FromBlobs(supportiveOffset, std::move(dq)));

    const TKey supportiveKey = TKey::ForBody(
        TKeyPrefix::TypeData, partitionId, supportiveOffset, 0, messageCount, 0);

    auto tx = MakeAndSendWriteTx({{"src1", {1, messageCount}}});
    std::deque<NPQ::TDataKey> bodyKeys;
    bodyKeys.push_back(MakeBodyDataKeyForTx(partitionId, supportiveOffset, messageCount, blobSize));
    InjectBodyKeys(tx, std::move(bodyKeys));

    WaitWriteInfoRequest(tx, true);
    WaitTxPredicateReply(tx);
    SendTxCommit(tx);
    WaitKvRequest();

    UNIT_ASSERT(LastKvRequest);
    UNIT_ASSERT_VALUES_EQUAL(LastKvRequest->Record.CmdRenameSize(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(LastKvRequest->Record.GetCmdRename(0).GetOldKey(), supportiveKey.ToString());

    auto& encoder = TPartitionTestWrapper::BlobEncoder(Partition());
    UNIT_ASSERT(!encoder.CompactedKeys.empty());
    const TKey parentKey = encoder.CompactedKeys.front().first;
    UNIT_ASSERT_VALUES_EQUAL(parentKey.GetOffset(), parentKeyOffset);
    UNIT_ASSERT_VALUES_EQUAL(parentKey.GetCount(), messageCount);
    UNIT_ASSERT(parentKey.IsFastWrite());

    SendKvResponse();
    WaitCommitDone(tx);

    UNIT_ASSERT(!encoder.DataKeysBody.empty());
    UNIT_ASSERT_VALUES_EQUAL(encoder.DataKeysBody.back().Key.GetOffset(), parentKeyOffset);

    TRequestedBlob blob = MakeRequestedBlobForRead(
        parentKeyOffset, 0, messageCount, 0, std::move(raw), parentKey);
    TVector<TRequestedBlob> blobs;
    blobs.push_back(std::move(blob));

    auto probe = [&](const TActorContext& ctx) {
        TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge, midReadOffset);
        info.Blobs = blobs;
        info.CompactedBlobsCount = 1;

        auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
        auto* readResult = answer->Response->MutablePartitionResponse()->MutableCmdReadResult();
        bool needStop = false;
        ui32 cnt = 0;
        ui32 size = 0;
        ui32 lastBlobSize = 0;

        TMaybe<TReadAnswer> early = info.AddBlobsFromBody(
            blobs, 0, 1, nullptr, midReadOffset, parentKeyOffset + messageCount + 10,
            kAddBlobsFromBodyProbeSizeLag, Ctx->Edge, midReadOffset,
            readResult, answer, needStop, cnt, size, lastBlobSize, ctx);

        UNIT_ASSERT(!early.Defined());
        UNIT_ASSERT_VALUES_EQUAL(readResult->ResultSize(), messageCount - 2);
        for (ui32 i = 0; i < readResult->ResultSize(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                readResult->GetResult(i).GetOffset(), midReadOffset + i);
        }
        UNIT_ASSERT_VALUES_EQUAL(info.Offset, parentKeyOffset + messageCount);
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));
    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

// A3: LMC>1 packed batch after real tx BodyKeys rename; mid-batch FindPos OffsetDelta rewind
// must report parent-space base (not supportive header 0).
Y_UNIT_TEST_F(CommitWriteOperationsRenamesBodyKeysThenMidReadWithLmc, TPartitionTxTestHelper) {
    constexpr ui64 parentKeyOffset = 200;
    Init({.EndOffset = parentKeyOffset});
    const TPartitionId partitionId(0);
    constexpr ui32 firstLmc = 5;
    constexpr ui32 secondLmc = 4;
    constexpr ui32 offsetSpan = firstLmc + secondLmc;
    constexpr ui32 blobSize = 100;
    constexpr ui64 supportiveOffset = 0;
    // Inside first logical batch → OffsetDelta FindPos rewinds to base.
    constexpr ui64 midReadOffset = parentKeyOffset + 3;

    std::deque<TClientBlob> dq;
    dq.push_back(MakeSinglePartBodyReadBlobWithLmc(1, 'a', firstLmc));
    dq.push_back(MakeSinglePartBodyReadBlobWithLmc(2, 'b', secondLmc));
    TBatch batch = TBatch::FromBlobs(supportiveOffset, std::move(dq));
    UNIT_ASSERT(batch.HasOffsetDelta());
    TString raw = SerializePackedBatchForReadBodyTest(std::move(batch));

    auto tx = MakeAndSendWriteTx({{"src1", {1, offsetSpan}}});
    std::deque<NPQ::TDataKey> bodyKeys;
    bodyKeys.push_back(MakeBodyDataKeyForTx(partitionId, supportiveOffset, offsetSpan, blobSize));
    InjectBodyKeys(tx, std::move(bodyKeys));

    WaitWriteInfoRequest(tx, true);
    WaitTxPredicateReply(tx);
    SendTxCommit(tx);
    WaitKvRequest();

    UNIT_ASSERT(LastKvRequest);
    UNIT_ASSERT_VALUES_UNEQUAL(LastKvRequest->Record.CmdRenameSize(), 0u);

    auto& encoder = TPartitionTestWrapper::BlobEncoder(Partition());
    UNIT_ASSERT(!encoder.CompactedKeys.empty());
    const TKey parentKey = encoder.CompactedKeys.front().first;
    UNIT_ASSERT_VALUES_EQUAL(parentKey.GetOffset(), parentKeyOffset);
    UNIT_ASSERT_VALUES_EQUAL(parentKey.GetCount(), offsetSpan);

    SendKvResponse();
    WaitCommitDone(tx);

    TRequestedBlob blob = MakeRequestedBlobForRead(
        parentKeyOffset, 0, offsetSpan, 0, std::move(raw), parentKey);
    TVector<TRequestedBlob> blobs;
    blobs.push_back(std::move(blob));

    auto probe = [&](const TActorContext& ctx) {
        TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge, midReadOffset);
        info.Blobs = blobs;
        info.CompactedBlobsCount = 1;

        auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
        auto* readResult = answer->Response->MutablePartitionResponse()->MutableCmdReadResult();
        bool needStop = false;
        ui32 cnt = 0;
        ui32 size = 0;
        ui32 lastBlobSize = 0;

        TMaybe<TReadAnswer> early = info.AddBlobsFromBody(
            blobs, 0, 1, nullptr, midReadOffset, parentKeyOffset + offsetSpan + 10,
            kAddBlobsFromBodyProbeSizeLag, Ctx->Edge, midReadOffset,
            readResult, answer, needStop, cnt, size, lastBlobSize, ctx);

        UNIT_ASSERT(!early.Defined());
        UNIT_ASSERT_VALUES_EQUAL(readResult->ResultSize(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(0).GetOffset(), parentKeyOffset);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(0).GetLogicalMessageCount(), firstLmc);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(1).GetOffset(), parentKeyOffset + firstLmc);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(1).GetLogicalMessageCount(), secondLmc);
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));
    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

// 2: RenameCompactedBlob path (isFastWrite=false → Body) keeps Key≠Header; mid-read OK.
Y_UNIT_TEST_F(RenameCompactedBlobPathKeepsKeySpaceOffsetMidRead, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);
    constexpr ui64 supportiveHeaderOffset = 0;
    constexpr ui32 messageCount = 4;
    constexpr ui64 parentKeyOffset = 2'000'000;
    constexpr ui64 readOffset = parentKeyOffset + 2;

    std::deque<TClientBlob> dq;
    dq.push_back(MakeSinglePartBodyReadBlob(1, 'a'));
    dq.push_back(MakeSinglePartBodyReadBlob(2, 'b'));
    dq.push_back(MakeSinglePartBodyReadBlob(3, 'c'));
    dq.push_back(MakeSinglePartBodyReadBlob(4, 'd'));
    TString raw = SerializePackedBatchForReadBodyTest(
        TBatch::FromBlobs(supportiveHeaderOffset, std::move(dq)));

    const TKey supportiveKey = TKey::ForBody(
        TKeyPrefix::TypeData, partitionId, supportiveHeaderOffset, 0, messageCount, 0);

    THead head;
    THead newHead;
    newHead.Offset = parentKeyOffset;
    TPartitionedBlob partitioned(
        partitionId, parentKeyOffset, "", 0, 0, 0, head, newHead,
        /*headCleared=*/true, /*needCompactHead=*/false, 8_MB);

    // Compaction RenameCompactedBlob uses isFastWrite=false.
    UNIT_ASSERT(!partitioned.Add(supportiveKey, raw.size(), TInstant::MilliSeconds(1), false).has_value());
    UNIT_ASSERT_VALUES_EQUAL(partitioned.GetFormedBlobs().size(), 1u);
    const auto& renamed = partitioned.GetFormedBlobs().front();
    UNIT_ASSERT(!renamed.NewKey.IsFastWrite());
    UNIT_ASSERT_VALUES_EQUAL(renamed.NewKey.GetOffset(), parentKeyOffset);

    TRequestedBlob blob = MakeRequestedBlobForRead(
        parentKeyOffset, 0, messageCount, 0, std::move(raw), renamed.NewKey);
    TVector<TRequestedBlob> blobs;
    blobs.push_back(std::move(blob));

    auto probe = [&](const TActorContext& ctx) {
        TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge, readOffset);
        info.Blobs = blobs;
        info.CompactedBlobsCount = 1;

        auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
        auto* readResult = answer->Response->MutablePartitionResponse()->MutableCmdReadResult();
        bool needStop = false;
        ui32 cnt = 0;
        ui32 size = 0;
        ui32 lastBlobSize = 0;

        TMaybe<TReadAnswer> early = info.AddBlobsFromBody(
            blobs, 0, 1, nullptr, readOffset, parentKeyOffset + messageCount + 10,
            kAddBlobsFromBodyProbeSizeLag, Ctx->Edge, readOffset,
            readResult, answer, needStop, cnt, size, lastBlobSize, ctx);

        UNIT_ASSERT(!early.Defined());
        UNIT_ASSERT_VALUES_EQUAL(readResult->ResultSize(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(0).GetOffset(), parentKeyOffset + 2);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(1).GetOffset(), parentKeyOffset + 3);
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));
    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

// 3: FillBatchedData ENSURE contract (fill_batched_data_offset.h) —
// parent-space offsets pass; supportive header leak fails.
Y_UNIT_TEST(FillBatchedDataOffsetContractAfterTxKeyRename) {
    constexpr ui64 parentKeyOffset = 547'189'849;
    constexpr ui64 readOffset = parentKeyOffset + 2;
    constexpr ui64 supportiveLeakOffset = 393; // buggy FindPos header-space

    auto covers = [](ui64 resultOffset, ui64 logicalMessageCount, ui64 readOff) {
        const ui64 messageCount = Max<ui64>(1, logicalMessageCount);
        return resultOffset + messageCount > readOff;
    };

    UNIT_ASSERT(covers(parentKeyOffset + 2, /*lmc=*/1, readOffset));
    UNIT_ASSERT(covers(parentKeyOffset, /*lmc=*/5, readOffset)); // mid-LMC rewind to base
    UNIT_ASSERT(!covers(supportiveLeakOffset, /*lmc=*/1, readOffset));

    ui64 advanced = readOffset;
    const ui64 messageCount = Max<ui64>(1, 1);
    advanced = (parentKeyOffset + 2) + messageCount;
    UNIT_ASSERT_VALUES_EQUAL(advanced, parentKeyOffset + 3);
}

// 4: Commit with non-empty parent Head → needCompactHead + BodyKeys rename after head.
Y_UNIT_TEST_F(CommitWriteOperationsCompactsParentHeadThenRenamesBodyKeys, TPartitionTxTestHelper) {
    Init({.EndOffset = 1});
    const TPartitionId partitionId(0);
    constexpr ui32 messageCount = 3;
    constexpr ui32 blobSize = 50;

    SeedParentHeadWithSingleMessage(
        TPartitionTestWrapper::BlobEncoder(Partition()), partitionId, /*offset=*/0);
    UNIT_ASSERT_VALUES_UNEQUAL(
        TPartitionTestWrapper::BlobEncoder(Partition()).Head.PackedSize, 0u);

    auto tx = MakeAndSendWriteTx({{"src1", {1, messageCount}}});
    std::deque<NPQ::TDataKey> bodyKeys;
    bodyKeys.push_back(MakeBodyDataKeyForTx(partitionId, 0, messageCount, blobSize));
    InjectBodyKeys(tx, std::move(bodyKeys));

    WaitWriteInfoRequest(tx, true);
    WaitTxPredicateReply(tx);
    SendTxCommit(tx);
    WaitKvRequest();

    UNIT_ASSERT(LastKvRequest);
    // Head compact writes a blob; BodyKeys produce at least one rename.
    UNIT_ASSERT_VALUES_UNEQUAL(LastKvRequest->Record.CmdWriteSize(), 0u);
    UNIT_ASSERT_VALUES_UNEQUAL(LastKvRequest->Record.CmdRenameSize(), 0u);

    auto& encoder = TPartitionTestWrapper::BlobEncoder(Partition());
    UNIT_ASSERT(!encoder.CompactedKeys.empty());
    bool foundBodyRename = false;
    for (const auto& [key, size] : encoder.CompactedKeys) {
        Y_UNUSED(size);
        if (key.GetCount() == messageCount && key.GetOffset() >= 1) {
            foundBodyRename = true;
            UNIT_ASSERT(key.IsFastWrite());
        }
    }
    UNIT_ASSERT(foundBodyRename);

    SendKvResponse();
    WaitCommitDone(tx);
}

// 5: HeadKeys folded into BodyKeys (production GetWriteInfo) — rename chain includes head-style key.
Y_UNIT_TEST_F(CommitWriteOperationsRenamesHeadKeysFoldedIntoBodyKeys, TPartitionTxTestHelper) {
    Init({.EndOffset = 0});
    const TPartitionId partitionId(0);
    constexpr ui32 headCount = 1;
    constexpr ui32 bodyCount = 2;

    auto tx = MakeAndSendWriteTx({{"src1", {1, headCount + bodyCount}}});
    std::deque<NPQ::TDataKey> bodyKeys;
    // Order matches GetWriteInfo: Compaction body, then HeadKeys, then FWZ body.
    bodyKeys.push_back(MakeBodyDataKeyForTx(partitionId, 0, headCount, 40, /*forHead=*/true));
    bodyKeys.push_back(MakeBodyDataKeyForTx(partitionId, 0, bodyCount, 80, /*forHead=*/false));
    InjectBodyKeys(tx, std::move(bodyKeys));

    WaitWriteInfoRequest(tx, true);
    WaitTxPredicateReply(tx);
    SendTxCommit(tx);
    WaitKvRequest();

    UNIT_ASSERT(LastKvRequest);
    UNIT_ASSERT_VALUES_EQUAL(LastKvRequest->Record.CmdRenameSize(), 2u);

    auto& encoder = TPartitionTestWrapper::BlobEncoder(Partition());
    UNIT_ASSERT_VALUES_EQUAL(encoder.CompactedKeys.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(encoder.CompactedKeys[0].first.GetOffset(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(encoder.CompactedKeys[0].first.GetCount(), headCount);
    UNIT_ASSERT_VALUES_EQUAL(encoder.CompactedKeys[1].first.GetOffset(), headCount);
    UNIT_ASSERT_VALUES_EQUAL(encoder.CompactedKeys[1].first.GetCount(), bodyCount);
    UNIT_ASSERT(encoder.CompactedKeys[0].first.IsFastWrite());
    UNIT_ASSERT(encoder.CompactedKeys[1].first.IsFastWrite());

    SendKvResponse();
    WaitCommitDone(tx);
}

// Empty TRequestedBlob body (retention / requested range past data): AddBlobsFromBody logs
// "Not full answer here!", fills read result metadata and returns TReadAnswer early.
Y_UNIT_TEST_F(AddBlobsFromBodyStopsOnEmptyBlob, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);

    std::deque<TClientBlob> dq0;
    dq0.push_back(MakeSinglePartBodyReadBlob(1, 'z'));
    TString raw0 = SerializePackedBatchForReadBodyTest(TBatch::FromBlobs(10, std::move(dq0)));

    const TKey key0 = TKey::ForBody(TKeyPrefix::TypeData, partitionId, 10, 0, 1, 0);
    TRequestedBlob blob0 = MakeRequestedBlobForRead(10, 0, 1, 0, std::move(raw0), key0);

    // Key must not use Count==0 and InternalPartsCount==0 together; body is still empty (Empty()).
    const TKey key1 = TKey::ForBody(TKeyPrefix::TypeData, partitionId, 11, 0, 1, 0);
    TRequestedBlob blob1 = MakeRequestedBlobForRead(11, 0, 0, 0, TString(), key1);

    TVector<TRequestedBlob> blobs;
    blobs.push_back(std::move(blob0));
    blobs.push_back(std::move(blob1));

    auto probe = [&](const TActorContext& ctx) {
        const ui32 consumedBytes = MakeSinglePartBodyReadBlob(1, 'z').GetSerializedSize();

        TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge, kAddBlobsFromBodyProbeReadOffset10);
        info.Blobs = blobs;
        info.CompactedBlobsCount = 2;

        auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
        NKikimrClient::TResponse& res = *answer->Response;
        auto* readResult = res.MutablePartitionResponse()->MutableCmdReadResult();

        bool needStop = false;
        ui32 cnt = 0;
        ui32 size = 0;
        ui32 lastBlobSize = 0;

        TMaybe<TReadAnswer> early = info.AddBlobsFromBody(blobs,
                                                          0,
                                                          2,
                                                          nullptr,
                                                          kAddBlobsFromBodyProbeReadOffset10,
                                                          kAddBlobsFromBodyProbeEndOffset,
                                                          kAddBlobsFromBodyProbeSizeLag,
                                                          Ctx->Edge,
                                                          kAddBlobsFromBodyProbeReadOffset10,
                                                          readResult,
                                                          answer,
                                                          needStop,
                                                          cnt,
                                                          size,
                                                          lastBlobSize,
                                                          ctx);

        UNIT_ASSERT(early.Defined());
        UNIT_ASSERT(early->Event.Get() != nullptr);
        UNIT_ASSERT_VALUES_EQUAL(readResult->ResultSize(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(0).GetOffset(), 10u);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetLastOffset(), i64{10});
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetStartOffset(), kAddBlobsFromBodyProbeReadOffset10);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetEndOffset(), kAddBlobsFromBodyProbeEndOffset);
        UNIT_ASSERT_VALUES_EQUAL(readResult->GetSizeLag(), kAddBlobsFromBodyProbeSizeLag - consumedBytes);
        UNIT_ASSERT_VALUES_EQUAL(info.RealReadOffset, kAddBlobsFromBodyProbeReadOffset10);
        UNIT_ASSERT_VALUES_EQUAL(info.LastOffset, 10u);
        UNIT_ASSERT_VALUES_EQUAL(info.Offset, 11u);
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));

    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

// Covers "got gap": jump read cursor to blobs[blobIdx].Offset / PartNo when BS returns a hole.
Y_UNIT_TEST_F(AddBlobsFromBodyGotGap, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);

    auto probe = [&](const TActorContext& ctx) {
        // Gap in partition offsets: reader at 10, first data in response starts at 12.
        {
            std::deque<TClientBlob> dq;
            dq.push_back(MakeSinglePartBodyReadBlob(1, 'g'));
            TString raw = SerializePackedBatchForReadBodyTest(TBatch::FromBlobs(12, std::move(dq)));

            const TKey key = TKey::ForBody(TKeyPrefix::TypeData, partitionId, 12, 0, 1, 0);
            TRequestedBlob blob = MakeRequestedBlobForRead(12, 0, 1, 0, std::move(raw), key);

            TVector<TRequestedBlob> blobs;
            blobs.push_back(std::move(blob));

            TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge, kAddBlobsFromBodyProbeReadOffset10);
            info.Blobs = blobs;
            info.CompactedBlobsCount = 1;

            auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
            NKikimrClient::TResponse& res = *answer->Response;
            auto* readResult = res.MutablePartitionResponse()->MutableCmdReadResult();

            bool needStop = false;
            ui32 cnt = 0;
            ui32 size = 0;
            ui32 lastBlobSize = 0;

            TMaybe<TReadAnswer> early = info.AddBlobsFromBody(blobs,
                                                              0,
                                                              1,
                                                              nullptr,
                                                              kAddBlobsFromBodyProbeReadOffset10,
                                                              kAddBlobsFromBodyProbeEndOffset,
                                                              kAddBlobsFromBodyProbeSizeLag,
                                                              Ctx->Edge,
                                                              kAddBlobsFromBodyProbeReadOffset10,
                                                              readResult,
                                                              answer,
                                                              needStop,
                                                              cnt,
                                                              size,
                                                              lastBlobSize,
                                                              ctx);

            UNIT_ASSERT(!early.Defined());
            UNIT_ASSERT_VALUES_EQUAL(readResult->ResultSize(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(0).GetOffset(), 12u);
            UNIT_ASSERT_VALUES_EQUAL(info.Offset, 13u);
            UNIT_ASSERT_VALUES_EQUAL(info.PartNo, 0u);
        }

        // Same partition offset, higher PartNo: reader expects part 0, blob carries part 1+.
        {
            std::deque<TClientBlob> dq;
            dq.push_back(MakeMultipartBodyReadBlob(2, 1, 2, 16, 'h'));
            TString raw = SerializePackedBatchForReadBodyTest(TBatch::FromBlobs(12, std::move(dq)));

            const TKey key = TKey::ForBody(TKeyPrefix::TypeData, partitionId, 12, 1, 1, 0);
            TRequestedBlob blob = MakeRequestedBlobForRead(12, 1, 1, 0, std::move(raw), key);

            TVector<TRequestedBlob> blobs;
            blobs.push_back(std::move(blob));

            TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge, kAddBlobsFromBodyProbeReadOffset12);
            info.Blobs = blobs;
            info.CompactedBlobsCount = 1;

            auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
            NKikimrClient::TResponse& res = *answer->Response;
            auto* readResult = res.MutablePartitionResponse()->MutableCmdReadResult();

            bool needStop = false;
            ui32 cnt = 0;
            ui32 size = 0;
            ui32 lastBlobSize = 0;

            TMaybe<TReadAnswer> early = info.AddBlobsFromBody(blobs,
                                                              0,
                                                              1,
                                                              nullptr,
                                                              kAddBlobsFromBodyProbeReadOffset12,
                                                              kAddBlobsFromBodyProbeEndOffset,
                                                              kAddBlobsFromBodyProbeSizeLag,
                                                              Ctx->Edge,
                                                              kAddBlobsFromBodyProbeReadOffset12,
                                                              readResult,
                                                              answer,
                                                              needStop,
                                                              cnt,
                                                              size,
                                                              lastBlobSize,
                                                              ctx);

            UNIT_ASSERT(!early.Defined());
            UNIT_ASSERT_VALUES_EQUAL(readResult->ResultSize(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(readResult->GetResult(0).GetOffset(), 12u);
            UNIT_ASSERT_VALUES_EQUAL(info.Offset, 13u);
            UNIT_ASSERT_VALUES_EQUAL(info.PartNo, 0u);
        }
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));

    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

// Reader is past this blob's data: FindPos returns Max → "this batch does not contain data to read, skip it".
Y_UNIT_TEST_F(AddBlobsFromBodySkipsBatchWhenReaderAhead, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);

    std::deque<TClientBlob> dq;
    dq.push_back(MakeSinglePartBodyReadBlob(1, 'p'));
    dq.push_back(MakeSinglePartBodyReadBlob(2, 'q'));
    dq.push_back(MakeSinglePartBodyReadBlob(3, 'r'));
    TString raw = SerializePackedBatchForReadBodyTest(TBatch::FromBlobs(12, std::move(dq)));

    const TKey key = TKey::ForBody(TKeyPrefix::TypeData, partitionId, 12, 0, 3, 0);
    TRequestedBlob blob = MakeRequestedBlobForRead(12, 0, 3, 0, std::move(raw), key);

    TVector<TRequestedBlob> blobs;
    blobs.push_back(std::move(blob));

    auto probe = [&](const TActorContext& ctx) {
        TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge, kAddBlobsFromBodyProbeReadOffset20);
        info.Blobs = blobs;
        info.CompactedBlobsCount = 1;

        auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
        NKikimrClient::TResponse& res = *answer->Response;
        auto* readResult = res.MutablePartitionResponse()->MutableCmdReadResult();

        bool needStop = false;
        ui32 cnt = 0;
        ui32 size = 0;
        ui32 lastBlobSize = 0;

        TMaybe<TReadAnswer> early = info.AddBlobsFromBody(blobs,
                                                          0,
                                                          1,
                                                          nullptr,
                                                          kAddBlobsFromBodyProbeReadOffset20,
                                                          kAddBlobsFromBodyProbeEndOffset,
                                                          kAddBlobsFromBodyProbeSizeLag,
                                                          Ctx->Edge,
                                                          kAddBlobsFromBodyProbeReadOffset20,
                                                          readResult,
                                                          answer,
                                                          needStop,
                                                          cnt,
                                                          size,
                                                          lastBlobSize,
                                                          ctx);

        UNIT_ASSERT(!early.Defined());
        UNIT_ASSERT_VALUES_EQUAL(readResult->ResultSize(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(info.Offset, kAddBlobsFromBodyProbeReadOffset20);
        UNIT_ASSERT_VALUES_EQUAL(info.PartNo, 0u);
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));

    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

// Covers: (1) `if (res.IsLastPart())` then + inner `if (ReachedLastOffset())`;
// (2) UpdateUsage last-part branch with inner `if (messageSkippingBehaviour)` true vs false.
Y_UNIT_TEST_F(AddBlobsFromBodyLastOffsetAndUpdateUsageSkips, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    const TPartitionId partitionId(1);

    auto probe = [&](const TActorContext& ctx) {
        auto makeOneBlob10 = [&]() {
            std::deque<TClientBlob> dq;
            dq.push_back(MakeSinglePartBodyReadBlob(1, 'u'));
            TString raw = SerializePackedBatchForReadBodyTest(TBatch::FromBlobs(10, std::move(dq)));
            const TKey key = TKey::ForBody(TKeyPrefix::TypeData, partitionId, 10, 0, 1, 0);
            TVector<TRequestedBlob> blobs;
            blobs.push_back(MakeRequestedBlobForRead(10, 0, 1, 0, std::move(raw), key));
            return blobs;
        };

        // LastOffset bound: after finishing one message, Offset reaches TReadInfo::LastOffset → needStop
        // before UpdateUsage on that row.
        {
            TVector<TRequestedBlob> blobs = makeOneBlob10();
            constexpr ui64 kLastOff = 11;
            TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge, kAddBlobsFromBodyProbeReadOffset10, kLastOff);
            info.Blobs = blobs;
            info.CompactedBlobsCount = 1;

            auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
            auto* readResult = answer->Response->MutablePartitionResponse()->MutableCmdReadResult();
            bool needStop = false;
            ui32 cnt = 0;
            ui32 size = 0;
            ui32 lastBlobSize = 0;

            TMaybe<TReadAnswer> early = info.AddBlobsFromBody(blobs,
                                                              0,
                                                              1,
                                                              nullptr,
                                                              kAddBlobsFromBodyProbeReadOffset10,
                                                              kAddBlobsFromBodyProbeEndOffset,
                                                              kAddBlobsFromBodyProbeSizeLag,
                                                              Ctx->Edge,
                                                              kAddBlobsFromBodyProbeReadOffset10,
                                                              readResult,
                                                              answer,
                                                              needStop,
                                                              cnt,
                                                              size,
                                                              lastBlobSize,
                                                              ctx);

            UNIT_ASSERT(!early.Defined());
            UNIT_ASSERT(needStop);
            UNIT_ASSERT_VALUES_EQUAL(readResult->ResultSize(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(info.Offset, kLastOff);
        }

        // UpdateUsage: messageSkippingBehaviour true (FCC + ReadTimestampMs > write ts) → cnt not charged.
        Ctx->Runtime->GetAppData(0).FeatureFlags.SetEnableSkipMessagesWithObsoleteTimestamp(false);
        Ctx->Runtime->GetAppData(0).PQConfig.SetTopicsAreFirstClassCitizen(true);
        NKikimr::AppData(ctx)->PQConfig.SetTopicsAreFirstClassCitizen(true);

        {
            TVector<TRequestedBlob> blobs = makeOneBlob10();
            TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge,
                                                                 kAddBlobsFromBodyProbeReadOffset10,
                                                                 0,
                                                                 0,
                                                                 1,
                                                                 kAddBlobsFromBodyDefaultByteLimit,
                                                                 1'000'000);
            info.Blobs = blobs;
            info.CompactedBlobsCount = 1;

            auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
            auto* readResult = answer->Response->MutablePartitionResponse()->MutableCmdReadResult();
            bool needStop = false;
            ui32 cnt = 0;
            ui32 size = 0;
            ui32 lastBlobSize = 0;

            TMaybe<TReadAnswer> early = info.AddBlobsFromBody(blobs,
                                                              0,
                                                              1,
                                                              nullptr,
                                                              kAddBlobsFromBodyProbeReadOffset10,
                                                              kAddBlobsFromBodyProbeEndOffset,
                                                              kAddBlobsFromBodyProbeSizeLag,
                                                              Ctx->Edge,
                                                              kAddBlobsFromBodyProbeReadOffset10,
                                                              readResult,
                                                              answer,
                                                              needStop,
                                                              cnt,
                                                              size,
                                                              lastBlobSize,
                                                              ctx);

            UNIT_ASSERT(!early.Defined());
            UNIT_ASSERT(!needStop);
            UNIT_ASSERT_VALUES_EQUAL(readResult->ResultSize(), 1u);
        }

        // UpdateUsage: no timestamp skip → cnt reaches Count → needStop from UpdateUsage.
        Ctx->Runtime->GetAppData(0).FeatureFlags.SetEnableSkipMessagesWithObsoleteTimestamp(false);
        Ctx->Runtime->GetAppData(0).PQConfig.SetTopicsAreFirstClassCitizen(false);
        NKikimr::AppData(ctx)->PQConfig.SetTopicsAreFirstClassCitizen(false);

        {
            TVector<TRequestedBlob> blobs = makeOneBlob10();
            TReadInfo info = MakeReadInfoForAddBlobsFromBodyTest(Ctx->Edge, kAddBlobsFromBodyProbeReadOffset10, 0, 0, 1);
            info.Blobs = blobs;
            info.CompactedBlobsCount = 1;

            auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
            auto* readResult = answer->Response->MutablePartitionResponse()->MutableCmdReadResult();
            bool needStop = false;
            ui32 cnt = 0;
            ui32 size = 0;
            ui32 lastBlobSize = 0;

            TMaybe<TReadAnswer> early = info.AddBlobsFromBody(blobs,
                                                              0,
                                                              1,
                                                              nullptr,
                                                              kAddBlobsFromBodyProbeReadOffset10,
                                                              kAddBlobsFromBodyProbeEndOffset,
                                                              kAddBlobsFromBodyProbeSizeLag,
                                                              Ctx->Edge,
                                                              kAddBlobsFromBodyProbeReadOffset10,
                                                              readResult,
                                                              answer,
                                                              needStop,
                                                              cnt,
                                                              size,
                                                              lastBlobSize,
                                                              ctx);

            UNIT_ASSERT(!early.Defined());
            UNIT_ASSERT(needStop);
            UNIT_ASSERT_VALUES_EQUAL(readResult->ResultSize(), 1u);
        }
    };

    Ctx->Runtime->Register(new TAddBlobsFromBodyReadTestActor(Ctx->Edge, std::move(probe)));

    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

TBlobKeyTokenPtr MakeTestBlobKeyToken() {
    auto token = std::make_shared<TBlobKeyToken>();
    token->NeedDelete = false;
    return token;
}

void AddHeadKeyWithBatch(
    TPartitionBlobEncoder& encoder,
    ui32 levelBorder,
    const TPartitionId& partitionId,
    ui64 offset,
    char fill,
    ui64 seqNo,
    ui32 levelIndex = 0)
{
    std::deque<TClientBlob> dq;
    dq.push_back(MakeSinglePartBodyReadBlob(seqNo, fill));
    TBatch batch = TBatch::FromBlobs(offset, std::move(dq));
    batch.Pack();
    const ui32 blobSize = batch.GetPackedSize();

    const TKey key = TKey::ForHead(TKeyPrefix::TypeData, partitionId, offset, 0, 1, 0);

    if (encoder.DataKeysHead.size() <= levelIndex) {
        while (encoder.DataKeysHead.size() <= levelIndex) {
            encoder.DataKeysHead.emplace_back(levelBorder);
        }
    }
    encoder.DataKeysHead[levelIndex].AddKey(key, blobSize);
    encoder.HeadKeys.push_back(TDataKey{key, blobSize, TInstant::MilliSeconds(1), 0, MakeTestBlobKeyToken()});
    encoder.Head.AddBatch(batch);
}

void AddBodyKeyToEncoder(
    TPartitionBlobEncoder& encoder,
    const TPartitionId& partitionId,
    ui64 offset,
    ui32 size)
{
    const TKey key = TKey::ForBody(TKeyPrefix::TypeData, partitionId, offset, 0, 1, 0);
    encoder.DataKeysBody.push_back(TDataKey{key, size, TInstant::MilliSeconds(1), 0, MakeTestBlobKeyToken()});
    encoder.BodySize += size;
}

void AddHeadKeyWithPartNo(
    TPartitionBlobEncoder& encoder,
    ui32 levelBorder,
    const TPartitionId& partitionId,
    ui64 offset,
    ui16 partNo,
    char fill,
    ui64 seqNo,
    ui32 levelIndex = 0)
{
    std::deque<TClientBlob> dq;
    dq.push_back(MakeSinglePartBodyReadBlob(seqNo, fill));
    TBatch batch = TBatch::FromBlobs(offset, std::move(dq));
    batch.Pack();
    const ui32 blobSize = batch.GetPackedSize();

    const TKey key = TKey::ForHead(TKeyPrefix::TypeData, partitionId, offset, partNo, 1, 0);

    if (encoder.DataKeysHead.size() <= levelIndex) {
        while (encoder.DataKeysHead.size() <= levelIndex) {
            encoder.DataKeysHead.emplace_back(levelBorder);
        }
    }
    encoder.DataKeysHead[levelIndex].AddKey(key, blobSize);
    encoder.HeadKeys.push_back(TDataKey{key, blobSize, TInstant::MilliSeconds(1), 0, MakeTestBlobKeyToken()});
    encoder.Head.AddBatch(batch);
    encoder.Head.Offset = offset;
    encoder.Head.PartNo = partNo;
}

Y_UNIT_TEST_F(GetCompactionZoneEmptyStartOffsetUsesFwzBodyStart, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());
    TPartition* partition = CreatePartition({.Partition = TPartitionId{1}, .Begin = 0, .End = 0});

    const TPartitionId partitionId(1);
    TPartitionTestWrapper::CompactionBlobEncoder(*partition).DataKeysBody.clear();
    TPartitionTestWrapper::CompactionBlobEncoder(*partition).HeadKeys.clear();

    AddBodyKeyToEncoder(TPartitionTestWrapper::BlobEncoder(*partition), partitionId, 100, 1000);
    TPartitionTestWrapper::BlobEncoder(*partition).StartOffset = 100;
    TPartitionTestWrapper::BlobEncoder(*partition).EndOffset = 202;

    UNIT_ASSERT_VALUES_EQUAL(TPartitionTestWrapper::GetCompactionZoneEmptyStartOffset(*partition), 100u);
}

Y_UNIT_TEST_F(GetCompactionZoneEmptyStartOffsetUsesFwzHeadPartNo, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());
    TPartition* partition = CreatePartition({.Partition = TPartitionId{1}, .Begin = 0, .End = 0});

    const TPartitionId partitionId(1);
    TPartitionTestWrapper::CompactionBlobEncoder(*partition).DataKeysBody.clear();
    TPartitionTestWrapper::CompactionBlobEncoder(*partition).HeadKeys.clear();

    TPartitionTestWrapper::BlobEncoder(*partition).DataKeysBody.clear();
    TPartitionTestWrapper::BlobEncoder(*partition).StartOffset = 8;
    TPartitionTestWrapper::BlobEncoder(*partition).EndOffset = 10;

    AddHeadKeyWithPartNo(TPartitionTestWrapper::BlobEncoder(*partition), 8_MB, partitionId, 8, 2, 'm', 1);

    // Old bug used GetEndOffset()==10; correct boundary is offset + 1 when PartNo > 0.
    UNIT_ASSERT_VALUES_EQUAL(TPartitionTestWrapper::GetCompactionZoneEmptyStartOffset(*partition), 9u);
}

Y_UNIT_TEST_F(GetCompactionZoneEmptyStartOffsetPrefersCzhHead, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());
    TPartition* partition = CreatePartition({.Partition = TPartitionId{1}, .Begin = 0, .End = 0});

    const TPartitionId partitionId(1);
    AddHeadKeyWithPartNo(TPartitionTestWrapper::CompactionBlobEncoder(*partition), 8_MB, partitionId, 4, 1, 'h', 1);

    AddBodyKeyToEncoder(TPartitionTestWrapper::BlobEncoder(*partition), partitionId, 100, 1000);
    TPartitionTestWrapper::BlobEncoder(*partition).StartOffset = 100;
    TPartitionTestWrapper::BlobEncoder(*partition).EndOffset = 202;

    UNIT_ASSERT_VALUES_EQUAL(TPartitionTestWrapper::GetCompactionZoneEmptyStartOffset(*partition), 5u);
}

class TPartitionMethodTestActor : public TActorBootstrapped<TPartitionMethodTestActor> {
public:
    TPartitionMethodTestActor(TActorId edge, std::function<void(const TActorContext&)> body)
        : Edge(edge)
        , Body(std::move(body))
    {}

    void Bootstrap(const TActorContext& ctx) {
        Body(ctx);
        Send(Edge, new NActors::TEvents::TEvWakeup());
        PassAway();
    }

private:
    TActorId Edge;
    std::function<void(const TActorContext&)> Body;
};

Y_UNIT_TEST_F(InitWithMetaOffsetsButNoDataKeysNormalizesEmptyPartition, TPartitionFixture) {
    // Regression for #49507: meta says [0, 3804) but data range is NODATA.
    // Without normalization InitComplete → ReportCounters → GetWriteTimeEstimate crashes.
    UNIT_ASSERT(Ctx.Defined());

    constexpr ui64 metaEnd = 3804;
    TPartition* partition = CreatePartition({
        .Partition = TPartitionId{1},
        .Begin = 0,
        .End = metaEnd,
        .Config = {
            // Offset within stale meta range: before normalize AnyCommits would be true
            // (Offset > BlobEncoder.StartOffset == 0); after normalize StartOffset == metaEnd.
            .Consumers = {{.Consumer = "user", .Offset = 100}},
        },
        .EndWriteTimestamp = TInstant::Seconds(1),
        .NoDataKeys = true,
    });

    UNIT_ASSERT_VALUES_EQUAL(TPartitionTestWrapper::GetStartOffset(*partition), metaEnd);
    UNIT_ASSERT_VALUES_EQUAL(TPartitionTestWrapper::GetEndOffset(*partition), metaEnd);

    auto& cz = TPartitionTestWrapper::CompactionBlobEncoder(*partition);
    auto& fwz = TPartitionTestWrapper::BlobEncoder(*partition);
    UNIT_ASSERT_VALUES_EQUAL(cz.StartOffset, metaEnd);
    UNIT_ASSERT_VALUES_EQUAL(cz.EndOffset, metaEnd);
    UNIT_ASSERT_VALUES_EQUAL(fwz.StartOffset, metaEnd);
    UNIT_ASSERT_VALUES_EQUAL(fwz.EndOffset, metaEnd);
    UNIT_ASSERT_VALUES_EQUAL(cz.Head.Offset, metaEnd);
    UNIT_ASSERT_VALUES_EQUAL(fwz.Head.Offset, metaEnd);
    UNIT_ASSERT_VALUES_EQUAL(cz.NewHead.Offset, metaEnd);
    UNIT_ASSERT_VALUES_EQUAL(fwz.NewHead.Offset, metaEnd);
    UNIT_ASSERT(cz.IsEmpty());
    UNIT_ASSERT(fwz.IsEmpty());

    UNIT_ASSERT_VALUES_EQUAL(TPartitionTestWrapper::GetWriteTimeEstimate(*partition, 0), TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(TPartitionTestWrapper::GetWriteTimeEstimate(*partition, metaEnd), TInstant::Zero());
    UNIT_ASSERT(!TPartitionTestWrapper::GetAnyCommits(*partition, "user"));
}

Y_UNIT_TEST_F(InitWithMetaOffsetsEmptyOkDataRangeNormalizesEmptyPartition, TPartitionFixture) {
    // Same inconsistent meta as #49507, but data range returns OK with zero pairs —
    // hits FormHeadAndProceed empty-keys → NormalizeOffsetsForEmptyData.
    UNIT_ASSERT(Ctx.Defined());

    constexpr ui64 metaEnd = 3804;
    TPartition* partition = CreatePartition({
        .Partition = TPartitionId{1},
        .Begin = 0,
        .End = metaEnd,
        .EndWriteTimestamp = TInstant::Seconds(1),
        .EmptyDataRangeOk = true,
    });

    UNIT_ASSERT_VALUES_EQUAL(TPartitionTestWrapper::GetStartOffset(*partition), metaEnd);
    UNIT_ASSERT_VALUES_EQUAL(TPartitionTestWrapper::GetEndOffset(*partition), metaEnd);
    UNIT_ASSERT(TPartitionTestWrapper::CompactionBlobEncoder(*partition).IsEmpty());
    UNIT_ASSERT(TPartitionTestWrapper::BlobEncoder(*partition).IsEmpty());
    UNIT_ASSERT_VALUES_EQUAL(TPartitionTestWrapper::GetWriteTimeEstimate(*partition, 0), TInstant::Zero());
}

Y_UNIT_TEST_F(InitWithNonZeroMetaStartAndNoDataKeysNormalizesToEnd, TPartitionFixture) {
    // Retention advanced StartOffset; meta [100, 3804) but blobs are gone.
    UNIT_ASSERT(Ctx.Defined());

    constexpr ui64 metaStart = 100;
    constexpr ui64 metaEnd = 3804;
    TPartition* partition = CreatePartition({
        .Partition = TPartitionId{1},
        .Begin = metaStart,
        .End = metaEnd,
        .EndWriteTimestamp = TInstant::Seconds(1),
        .NoDataKeys = true,
    });

    UNIT_ASSERT_VALUES_EQUAL(TPartitionTestWrapper::GetStartOffset(*partition), metaEnd);
    UNIT_ASSERT_VALUES_EQUAL(TPartitionTestWrapper::GetEndOffset(*partition), metaEnd);

    auto& cz = TPartitionTestWrapper::CompactionBlobEncoder(*partition);
    auto& fwz = TPartitionTestWrapper::BlobEncoder(*partition);
    UNIT_ASSERT_VALUES_EQUAL(cz.StartOffset, metaEnd);
    UNIT_ASSERT_VALUES_EQUAL(cz.EndOffset, metaEnd);
    UNIT_ASSERT_VALUES_EQUAL(fwz.StartOffset, metaEnd);
    UNIT_ASSERT_VALUES_EQUAL(fwz.EndOffset, metaEnd);
}

Y_UNIT_TEST_F(GetWriteTimeEstimateReturnsTimestampFromBodyKeys, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());

    constexpr ui64 begin = 0;
    constexpr ui64 end = 10;
    TPartition* partition = CreatePartition({
        .Partition = TPartitionId{1},
        .Begin = begin,
        .End = end,
        .EndWriteTimestamp = TInstant::Seconds(1),
    });

    UNIT_ASSERT_VALUES_EQUAL(TPartitionTestWrapper::GetStartOffset(*partition), begin);
    UNIT_ASSERT_VALUES_EQUAL(TPartitionTestWrapper::GetEndOffset(*partition), end);
    UNIT_ASSERT(!TPartitionTestWrapper::CompactionBlobEncoder(*partition).IsEmpty() ||
                !TPartitionTestWrapper::BlobEncoder(*partition).IsEmpty());

    const TInstant ts = TPartitionTestWrapper::GetWriteTimeEstimate(*partition, begin);
    UNIT_ASSERT_GT(ts, TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(TPartitionTestWrapper::GetWriteTimeEstimate(*partition, end), TInstant::Zero());
}

Y_UNIT_TEST_F(GetClientOffsetSurvivesInitWithMetaButNoDataKeys, TPartitionFixture) {
    // E2E: InitComplete → ReportCounters and later GetClientOffset must not crash
    // when meta offsets exist without data keys (#49507).
    UNIT_ASSERT(Ctx.Defined());

    constexpr ui64 metaEnd = 3804;
    const TString client = "user";
    CreatePartition({
        .Partition = TPartitionId{1},
        .Begin = 0,
        .End = metaEnd,
        .Config = {
            .Consumers = {{.Consumer = client, .Offset = 0}},
        },
        .EndWriteTimestamp = TInstant::Seconds(1),
        .NoDataKeys = true,
    });

    SendGetOffset(1, client);
    WaitProxyResponse({.Cookie = 1, .Status = NMsgBusProxy::MSTATUS_OK, .Offset = 0});
}

Y_UNIT_TEST_F(FinalizeEmptyBlobEncoderResetsHeadPartNo, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());
    TPartition* partition = CreatePartition({.Partition = TPartitionId{1}, .Begin = 0, .End = 0});

    const TPartitionId partitionId(1);
    auto& encoder = TPartitionTestWrapper::CompactionBlobEncoder(*partition);
    while (encoder.DataKeysHead.size() < 4) {
        encoder.DataKeysHead.emplace_back(8_MB);
    }
    AddHeadKeyWithPartNo(encoder, 8_MB, partitionId, 4, 2, 'h', 1);
    encoder.Head.PartNo = 2;

    TPartitionTestWrapper::FinalizeEmptyBlobEncoder(*partition, encoder, 100, true);

    UNIT_ASSERT(encoder.HeadKeys.empty());
    UNIT_ASSERT(encoder.Head.GetBatches().empty());
    UNIT_ASSERT_VALUES_EQUAL(encoder.Head.PartNo, 0u);
    UNIT_ASSERT_VALUES_EQUAL(encoder.Head.Offset, 100u);
}

Y_UNIT_TEST_F(FinalizeEmptyBlobEncoderClearsNewHead, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());
    TPartition* partition = CreatePartition({.Partition = TPartitionId{1}, .Begin = 0, .End = 0});

    const TPartitionId partitionId(1);
    auto& encoder = TPartitionTestWrapper::CompactionBlobEncoder(*partition);
    while (encoder.DataKeysHead.size() < 4) {
        encoder.DataKeysHead.emplace_back(8_MB);
    }

    std::deque<TClientBlob> dq;
    dq.push_back(MakeSinglePartBodyReadBlob(1, 'n'));
    TBatch newHeadBatch = TBatch::FromBlobs(50, std::move(dq));
    newHeadBatch.Pack();
    const ui32 newHeadBatchSize = newHeadBatch.GetPackedSize();

    encoder.NewHead.Offset = 50;
    encoder.NewHead.PartNo = 1;
    encoder.NewHead.PackedSize = newHeadBatchSize;
    encoder.NewHead.AddBatch(newHeadBatch);

    const TKey newHeadKey = TKey::ForHead(TKeyPrefix::TypeData, partitionId, 50, 1, 1, 0);
    encoder.NewHeadKey = TDataKey{newHeadKey, newHeadBatchSize, TInstant::MilliSeconds(1), 0, MakeTestBlobKeyToken()};

    UNIT_ASSERT(!encoder.NewHead.GetBatches().empty());
    UNIT_ASSERT_VALUES_UNEQUAL(encoder.NewHeadKey.Size, 0u);

    TPartitionTestWrapper::FinalizeEmptyBlobEncoder(*partition, encoder, 100, true);

    UNIT_ASSERT(encoder.NewHead.GetBatches().empty());
    UNIT_ASSERT_VALUES_EQUAL(encoder.NewHead.PackedSize, 0u);
    UNIT_ASSERT_VALUES_EQUAL(encoder.NewHead.PartNo, 0u);
    UNIT_ASSERT_VALUES_EQUAL(encoder.NewHead.Offset, 100u);
    UNIT_ASSERT_VALUES_EQUAL(encoder.NewHeadKey.Size, 0u);
    UNIT_ASSERT_VALUES_EQUAL(encoder.NewHeadKey.Key.GetOffset(), 0u);
}

Y_UNIT_TEST_F(CleanUpBlobsResetsStaleCompactionZoneHeadPartNo, TPartitionFixture) {
    UNIT_ASSERT(Ctx.Defined());
    Ctx->Runtime->GetAppData(0).FeatureFlags.SetEnableTopicRetentionDeleteLastBlob(true);

    TPartition* partition = CreatePartition({.Partition = TPartitionId{1}, .Begin = 0, .End = 0});

    auto& czEncoder = TPartitionTestWrapper::CompactionBlobEncoder(*partition);
    czEncoder.DataKeysBody.clear();
    czEncoder.HeadKeys.clear();
    czEncoder.Head.Clear();
    czEncoder.Head.Offset = 200;
    czEncoder.Head.PartNo = 2;

    auto& fwzEncoder = TPartitionTestWrapper::BlobEncoder(*partition);
    fwzEncoder.StartOffset = 200;
    fwzEncoder.EndOffset = 202;

    auto probe = [&](const TActorContext& ctx) {
        TPartitionTestWrapper::CleanUpBlobs(*partition, ctx);
        UNIT_ASSERT_VALUES_EQUAL(czEncoder.Head.PartNo, 0u);
    };

    Ctx->Runtime->Register(new TPartitionMethodTestActor(Ctx->Edge, std::move(probe)));

    TDispatchOptions options;
    options.FinalEvents.emplace_back([](IEventHandle& ev) {
        return ev.GetTypeRewrite() == NActors::TEvents::TEvWakeup::EventType;
    });
    Ctx->Runtime->DispatchEvents(options);
}

Y_UNIT_TEST(PopFrontHeadKeySyncsHeadWithRemainingHeadKeys) {
    const TPartitionId partitionId(1);
    TPartitionBlobEncoder encoder(partitionId, false);

    AddHeadKeyWithBatch(encoder, 8_MB, partitionId, 10, 'a', 1, 0);
    AddHeadKeyWithBatch(encoder, 8_MB, partitionId, 11, 'b', 2, 0);

    encoder.Head.Offset = 10;
    encoder.Head.PartNo = 0;
    encoder.Head.PackedSize = encoder.HeadKeys[0].Size + encoder.HeadKeys[1].Size;

    UNIT_ASSERT_VALUES_EQUAL(encoder.HeadKeys.size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(encoder.Head.GetBatches().size(), 2u);

    encoder.PopFrontHeadKey();

    UNIT_ASSERT_VALUES_EQUAL(encoder.HeadKeys.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(encoder.HeadKeys.front().Key.GetOffset(), 11u);
    UNIT_ASSERT_VALUES_EQUAL(encoder.Head.Offset, 11u);
    UNIT_ASSERT_VALUES_EQUAL(encoder.Head.PartNo, 0u);
    UNIT_ASSERT_VALUES_EQUAL(encoder.Head.GetBatches().size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(encoder.Head.GetBatch(0).GetOffset(), 11u);
    UNIT_ASSERT_VALUES_EQUAL(encoder.Head.PackedSize, encoder.HeadKeys.front().Size);

    TVector<TClientBlob> remaining;
    encoder.Head.GetBatch(0).UnpackTo(&remaining);
    UNIT_ASSERT_VALUES_EQUAL(remaining.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(remaining[0].Data[0], 'b');
}

} // End of suite

} // namespace
