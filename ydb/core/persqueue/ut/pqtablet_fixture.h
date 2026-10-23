#pragma once

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

TWriteId MakeDeferredWriteId(ui64 intPublicationId, const TString& extPublicationId = "ext-publication");

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

constexpr ui32 WRITE_TX_COOKIE = 5; // TPersQueue::WRITE_TX_COOKIE

NKikimrPQ::TTabletTxInfo ParseTxWritesFromWriteTxRequest(const NKikimrClient::TKeyValueRequest& request);
THashSet<i64> CollectKafkaProducerIds(const NKikimrPQ::TTabletTxInfo& info);

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
    NKikimrPQ::TEvProposeTransactionResult::EStatus WaitProposeTransactionStatus(ui64 txId);

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
    void SendKafkaTxnWriteRequest(const NKafka::TProducerInstanceId& producerInstanceId, const TString& ownerCookie, const ui32 partitionId = 0,
                                  ui64 seqNo = 0, const TString& data = "123test123", ui64 cookie = 123, bool waitResponse = true,
                                  ui32 kafkaBatchSize = 0);
    void ProposeKafkaTransaction(NKafka::TProducerInstanceId producerInstanceId, ui64 txId, const std::vector<ui32>& partitionIds = {0});
    void WaitTransactionCompleted(ui64 txId, ui64 planStep);
    void CommitKafkaTransaction(NKafka::TProducerInstanceId producerInstanceId, ui64 txId, const std::vector<ui32>& partitionIds = {0},
                                ui64 planStep = 100);

    TString CreateSupportivePartitionForDeferredPublication(const TWriteId& writeId, ui32 partitionId = 0);
    void SendDeferredPublicationWriteRequest(const TWriteId& writeId, const TString& ownerCookie, ui32 partitionId = 0);
    void SendDeferredPublicationWriteRequestWithoutWait(const TWriteId& writeId, const TString& ownerCookie, ui32 partitionId = 0);
    void WaitDeferredPublicationWriteResponse();
    TVector<TString> ReadMainPartitionMessages(ui32 partitionId = 0, ui32 count = 10);
    NKikimrClient::TCmdReadResult CmdReadCapture(const TPQCmdReadSettings& settings);
    void CommitTopicTransaction(const TWriteId& writeId, ui32 supportivePartitionId, ui64 txId,
                                const std::vector<ui32>& partitionIds = {0}, ui64 planStep = 100);
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
        const std::vector<ui32>& partitionIds = {0},
        ui64 planStep = 100);
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

} // namespace NKikimr::NPQ
