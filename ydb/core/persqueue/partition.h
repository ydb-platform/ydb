#pragma once

#include "blob.h"
#include "header.h"
#include "key.h"
#include "partition_types.h"
#include "quota_tracker.h"
#include "sourceid.h"
#include "subscriber.h"
#include "user_info.h"
#include "utils.h"

#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/library/persqueue/counter_time_keeper/counter_time_keeper.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/sliding_window/sliding_window.h>

#include <util/generic/set.h>


namespace NKikimr::NPQ {

static const ui32 MAX_BLOB_PART_SIZE = 500_KB;

using TPartitionLabeledCounters = TProtobufTabletLabeledCounters<EPartitionLabeledCounters_descriptor>;

ui64 GetOffsetEstimate(const std::deque<TDataKey>& container, TInstant timestamp, ui64 headOffset);

void CalcTopicWriteQuotaParams(const NKikimrPQ::TPQConfig& pqConfig,
                               bool isLocalDC,
                               NPersQueue::TTopicConverterPtr topicConverter,
                               ui64 tabletId,
                               const TActorContext& ctx,
                               TString& topicWriteQuoterPath,
                               TString& topicWriteQuotaResourcePath);

class TKeyLevel;
struct TMirrorerInfo;

struct TTransaction {
    explicit TTransaction(TSimpleSharedPtr<TEvPQ::TEvTxCalcPredicate> tx,
                          TMaybe<bool> predicate = Nothing()) :
        Tx(tx),
        Predicate(predicate)
    {
        Y_VERIFY(Tx);
    }

    TTransaction(TSimpleSharedPtr<TEvPQ::TEvChangePartitionConfig> changeConfig,
                 bool sendReply) :
        ChangeConfig(changeConfig),
        SendReply(sendReply)
    {
        Y_VERIFY(ChangeConfig);
    }

    explicit TTransaction(TSimpleSharedPtr<TEvPQ::TEvProposePartitionConfig> proposeConfig) :
        ProposeConfig(proposeConfig)
    {
        Y_VERIFY(ProposeConfig);
    }

    TSimpleSharedPtr<TEvPQ::TEvTxCalcPredicate> Tx;
    TMaybe<bool> Predicate;

    TSimpleSharedPtr<TEvPQ::TEvChangePartitionConfig> ChangeConfig;
    bool SendReply;

    TSimpleSharedPtr<TEvPQ::TEvProposePartitionConfig> ProposeConfig;
};

class TPartition : public TActorBootstrapped<TPartition> {
private:
    static const ui32 MAX_ERRORS_COUNT_TO_STORE = 10;

private:
    struct THasDataReq;
    struct THasDataDeadline;

    void ReplyError(const TActorContext& ctx, const ui64 dst, NPersQueue::NErrorCode::EErrorCode errorCode, const TString& error);
    void ReplyPropose(const TActorContext& ctx, const NKikimrPQ::TEvProposeTransaction& event, NKikimrPQ::TEvProposeTransactionResult::EStatus statusCode);
    void ReplyErrorForStoredWrites(const TActorContext& ctx);

    void ReplyGetClientOffsetOk(const TActorContext& ctx, const ui64 dst, const i64 offset, const TInstant writeTimestamp, const TInstant createTimestamp);
    void ReplyOk(const TActorContext& ctx, const ui64 dst);
    void ReplyOwnerOk(const TActorContext& ctx, const ui64 dst, const TString& ownerCookie);

    void ReplyWrite(const TActorContext& ctx, ui64 dst, const TString& sourceId, ui64 seqNo, ui16 partNo, ui16 totalParts, ui64 offset, TInstant writeTimestamp, bool already, ui64 maxSeqNo, TDuration partitionQuotedTime, TDuration topicQuotedTime, TDuration queueTime, TDuration writeTime);

    void AddNewWriteBlob(std::pair<TKey, ui32>& res, TEvKeyValue::TEvRequest* request, bool headCleared, const TActorContext& ctx);
    void AnswerCurrentWrites(const TActorContext& ctx);
    void CancelAllWritesOnIdle(const TActorContext& ctx);
    void CancelAllWritesOnWrite(const TActorContext& ctx, TEvKeyValue::TEvRequest* request, const TString& errorStr, const TWriteMsg& p, TSourceIdWriter& sourceIdWriter, NPersQueue::NErrorCode::EErrorCode errorCode);
    void ClearOldHead(const ui64 offset, const ui16 partNo, TEvKeyValue::TEvRequest* request);
    void CreateMirrorerActor();
    void DoRead(TEvPQ::TEvRead::TPtr ev, TDuration waitQuotaTime, const TActorContext& ctx);
    void FailBadClient(const TActorContext& ctx);
    void FillBlobsMetaData(const NKikimrClient::TKeyValueResponse::TReadRangeResult& range, const TActorContext& ctx);
    void FillReadFromTimestamps(const NKikimrPQ::TPQTabletConfig& config, const TActorContext& ctx);
    void FilterDeadlinedWrites(const TActorContext& ctx);
    void FormHeadAndProceed(const TActorContext& ctx);

    void Handle(NReadSpeedLimiterEvents::TEvCounters::TPtr& ev, const TActorContext& ctx);
    void Handle(NReadSpeedLimiterEvents::TEvResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvBlobResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvChangeOwner::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvChangePartitionConfig::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvError::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvGetClientOffset::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvGetMaxSeqNoRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvGetPartitionClientInfo::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvHandleWriteResponse::TPtr&, const TActorContext& ctx);
    void Handle(TEvPQ::TEvMirrorerCounters::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvPartitionOffsets::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvPartitionStatus::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvPipeDisconnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvProxyResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvQuotaDeadlineCheck::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvRead::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvReadTimeout::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvReserveBytes::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvSetClientInfo::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvUpdateWriteTimestamp::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvHasDataInfo::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvProposeTransaction::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvTxCalcPredicate::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvTxCommit::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvTxRollback::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvReportPartitionError::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvQuota::TEvClearance::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvSubDomainStatus::TPtr& ev, const TActorContext& ctx);
    void HandleDataRangeRead(const NKikimrClient::TKeyValueResponse::TReadRangeResult& range, const TActorContext& ctx);
    void HandleDataRead(const NKikimrClient::TResponse& range, const TActorContext& ctx);
    void HandleGetDiskStatus(const NKikimrClient::TResponse& res, const TActorContext& ctx);
    void HandleInfoRangeRead(const NKikimrClient::TKeyValueResponse::TReadRangeResult& range, const TActorContext& ctx);
    void HandleMetaRead(const NKikimrClient::TResponse& response, const TActorContext& ctx);
    void HandleMonitoring(TEvPQ::TEvMonRequest::TPtr& ev, const TActorContext& ctx);
    void HandleOnIdle(TEvPQ::TEvDeregisterMessageGroup::TPtr& ev, const TActorContext& ctx);
    void HandleOnIdle(TEvPQ::TEvRegisterMessageGroup::TPtr& ev, const TActorContext& ctx);
    void HandleOnIdle(TEvPQ::TEvSplitMessageGroup::TPtr& ev, const TActorContext& ctx);
    void HandleOnIdle(TEvPQ::TEvUpdateAvailableSize::TPtr& ev, const TActorContext& ctx);
    void HandleOnIdle(TEvPQ::TEvWrite::TPtr& ev, const TActorContext& ctx);
    void HandleOnInit(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx);
    void HandleOnInit(TEvPQ::TEvPartitionOffsets::TPtr& ev, const TActorContext& ctx);
    void HandleOnInit(TEvPQ::TEvPartitionStatus::TPtr& ev, const TActorContext& ctx);
    void HandleOnWrite(TEvPQ::TEvDeregisterMessageGroup::TPtr& ev, const TActorContext& ctx);
    void HandleOnWrite(TEvPQ::TEvRegisterMessageGroup::TPtr& ev, const TActorContext& ctx);
    void HandleOnWrite(TEvPQ::TEvSplitMessageGroup::TPtr& ev, const TActorContext& ctx);
    void HandleOnWrite(TEvPQ::TEvUpdateAvailableSize::TPtr& ev, const TActorContext& ctx);
    void HandleOnWrite(TEvPQ::TEvWrite::TPtr& ev, const TActorContext& ctx);
    void HandleSetOffsetResponse(ui64 cookie, const TActorContext& ctx);
    void HandleWakeup(const TActorContext& ctx);
    void HandleWriteResponse(const TActorContext& ctx);

    void InitComplete(const TActorContext& ctx);
    void InitUserInfoForImportantClients(const TActorContext& ctx);

    void LogAndCollectError(NKikimrServices::EServiceKikimr service, const TString& msg, const TActorContext& ctx);
    void LogAndCollectError(const NKikimrPQ::TStatusResponse::TErrorMessage& error, const TActorContext& ctx);

    void OnReadRequestFinished(TReadInfo&& info, ui64 answerSize);

    void ProcessChangeOwnerRequest(TAutoPtr<TEvPQ::TEvChangeOwner> ev, const TActorContext& ctx);
    void ProcessChangeOwnerRequests(const TActorContext& ctx);
    void ProcessHasDataRequests(const TActorContext& ctx);
    void ProcessRead(const TActorContext& ctx, TReadInfo&& info, const ui64 cookie, bool subscription);
    void ProcessReserveRequests(const TActorContext& ctx);
    void ProcessTimestampRead(const TActorContext& ctx);
    void ProcessTimestampsForNewData(const ui64 prevEndOffset, const TActorContext& ctx);

    void ReadTimestampForOffset(const TString& user, TUserInfo& ui, const TActorContext& ctx);
    void ReportCounters(const TActorContext& ctx);
    void ScheduleUpdateAvailableSize(const TActorContext& ctx);
    void SetDeadlinesForWrites(const TActorContext& ctx);

    void SetupStreamCounters(const TActorContext& ctx);
    void SetupTopicCounters(const TActorContext& ctx);

    void SyncMemoryStateWithKVState(const TActorContext& ctx);
    void UpdateAvailableSize(const TActorContext& ctx);

    void AddMetaKey(TEvKeyValue::TEvRequest* request);
    void BecomeIdle(const TActorContext& ctx);
    void CheckHeadConsistency() const;
    void HandleWrites(const TActorContext& ctx);
    void RequestQuotaForWriteBlobRequest(size_t dataSize, ui64 cookie);
    void WriteBlobWithQuota(THolder<TEvKeyValue::TEvRequest>&& request);

    void UpdateUserInfoEndOffset(const TInstant& now);
    void UpdateWriteBufferIsFullState(const TInstant& now);

    TInstant GetWriteTimeEstimate(ui64 offset) const;
    bool AppendHeadWithNewWrites(TEvKeyValue::TEvRequest* request, const TActorContext& ctx, TSourceIdWriter& sourceIdWriter);
    bool CleanUp(TEvKeyValue::TEvRequest* request, bool hasWrites, const TActorContext& ctx);
    bool CleanUpBlobs(TEvKeyValue::TEvRequest *request, bool hasWrites, const TActorContext& ctx);
    bool IsQuotingEnabled() const;
    bool ProcessWrites(TEvKeyValue::TEvRequest* request, TInstant now, const TActorContext& ctx);
    bool WaitingForPreviousBlobQuota() const;
    bool WaitingForSubDomainQuota(const TActorContext& ctx, const ui64 withSize = 0) const;
    size_t GetQuotaRequestSize(const TEvKeyValue::TEvRequest& request);
    std::pair<TInstant, TInstant> GetTime(const TUserInfo& userInfo, ui64 offset) const;
    std::pair<TKey, ui32> Compact(const TKey& key, const ui32 size, bool headCleared);
    ui32 NextChannel(bool isHead, ui32 blobSize);
    ui64 GetSizeLag(i64 offset);
    std::pair<TKey, ui32> GetNewWriteKey(bool headCleared);
    THashMap<TString, TOwnerInfo>::iterator DropOwner(THashMap<TString, TOwnerInfo>::iterator& it,
                                                      const TActorContext& ctx);
    // will return rcount and rsize also
    TVector<TRequestedBlob> GetReadRequestFromBody(const ui64 startOffset, const ui16 partNo, const ui32 maxCount, const ui32 maxSize, ui32* rcount, ui32* rsize);
    TVector<TClientBlob>    GetReadRequestFromHead(const ui64 startOffset, const ui16 partNo, const ui32 maxCount, const ui32 maxSize, const ui64 readTimestampMs, ui32* rcount, ui32* rsize, ui64* insideHeadOffset);

    ui64 GetUsedStorage(const TActorContext& ctx);

    void ProcessTxsAndUserActs(const TActorContext& ctx);
    void ContinueProcessTxsAndUserActs(const TActorContext& ctx);

    void PushBackDistrTx(TSimpleSharedPtr<TEvPQ::TEvTxCalcPredicate> event);
    void PushBackDistrTx(TSimpleSharedPtr<TEvPQ::TEvChangePartitionConfig> event);
    void PushFrontDistrTx(TSimpleSharedPtr<TEvPQ::TEvChangePartitionConfig> event);
    void PushBackDistrTx(TSimpleSharedPtr<TEvPQ::TEvProposePartitionConfig> event);
    void RemoveDistrTx();
    void ProcessDistrTxs(const TActorContext& ctx);
    void ProcessDistrTx(const TActorContext& ctx);

    void AddImmediateTx(TSimpleSharedPtr<TEvPersQueue::TEvProposeTransaction> event);
    void RemoveImmediateTx();
    void ProcessImmediateTxs(const TActorContext& ctx);
    void ProcessImmediateTx(const NKikimrPQ::TEvProposeTransaction& tx,
                            const TActorContext& ctx);

    void AddUserAct(TSimpleSharedPtr<TEvPQ::TEvSetClientInfo> act);
    void RemoveUserAct();
    size_t GetUserActCount(const TString& consumer) const;

    void ProcessUserActs(const TActorContext& ctx);
    void ProcessUserAct(TEvPQ::TEvSetClientInfo& act,
                        const TActorContext& ctx);
    void EmulatePostProcessUserAct(const TEvPQ::TEvSetClientInfo& act,
                                   TUserInfoBase& userInfo,
                                   const TActorContext& ctx);

    void ScheduleReplyOk(const ui64 dst);
    void ScheduleReplyGetClientOffsetOk(const ui64 dst,
                                        const i64 offset,
                                        const TInstant writeTimestamp, const TInstant createTimestamp);
    void ScheduleReplyError(const ui64 dst,
                            NPersQueue::NErrorCode::EErrorCode errorCode,
                            const TString& error);
    void ScheduleReplyPropose(const NKikimrPQ::TEvProposeTransaction& event,
                              NKikimrPQ::TEvProposeTransactionResult::EStatus statusCode);
    void ScheduleReplyCommitDone(ui64 step, ui64 txId);
    void ScheduleDropPartitionLabeledCounters(const TString& group);
    void SchedulePartitionConfigChanged();

    void AddCmdWrite(NKikimrClient::TKeyValueRequest& request,
                     const TKeyPrefix& ikey, const TKeyPrefix& ikeyDeprecated,
                     ui64 offset, ui32 gen, ui32 step, const TString& session,
                     ui64 readOffsetRewindSum,
                     ui64 readRuleGeneration);
    void AddCmdWriteTxMeta(NKikimrClient::TKeyValueRequest& request,
                           ui64 step, ui64 txId);
    void AddCmdWriteUserInfos(NKikimrClient::TKeyValueRequest& request);
    void AddCmdWriteConfig(NKikimrClient::TKeyValueRequest& request);
    void AddCmdDeleteRange(NKikimrClient::TKeyValueRequest& request,
                           const TKeyPrefix& ikey, const TKeyPrefix& ikeyDeprecated);

    TUserInfoBase& GetOrCreatePendingUser(const TString& user, TMaybe<ui64> readRuleGeneration = {});
    TUserInfoBase* GetPendingUserIfExists(const TString& user);

    THolder<TEvPQ::TEvProxyResponse> MakeReplyOk(const ui64 dst);
    THolder<TEvPQ::TEvProxyResponse> MakeReplyGetClientOffsetOk(const ui64 dst,
                                                                const i64 offset,
                                                                const TInstant writeTimestamp, const TInstant createTimestamp);
    THolder<TEvPQ::TEvError> MakeReplyError(const ui64 dst,
                                            NPersQueue::NErrorCode::EErrorCode errorCode,
                                            const TString& error);
    THolder<TEvPersQueue::TEvProposeTransactionResult> MakeReplyPropose(const NKikimrPQ::TEvProposeTransaction& event,
                                                                        NKikimrPQ::TEvProposeTransactionResult::EStatus statusCode);
    THolder<TEvPQ::TEvTxCommitDone> MakeCommitDone(ui64 step, ui64 txId);

    bool BeginTransaction(const TEvPQ::TEvTxCalcPredicate& event,
                          const TActorContext& ctx);
    bool BeginTransaction(const TEvPQ::TEvProposePartitionConfig& event);
    void EndTransaction(const TEvPQ::TEvTxCommit& event,
                        const TActorContext& ctx);
    void EndTransaction(const TEvPQ::TEvTxRollback& event,
                        const TActorContext& ctx);

    void BeginChangePartitionConfig(const NKikimrPQ::TPQTabletConfig& config,
                                    const TActorContext& ctx);
    void EndChangePartitionConfig(const NKikimrPQ::TPQTabletConfig& config,
                                  NPersQueue::TTopicConverterPtr topicConverter,
                                  const TActorContext& ctx);
    TString GetKeyConfig() const;

    void InitPendingUserInfoForImportantClients(const NKikimrPQ::TPQTabletConfig& config,
                                                const TActorContext& ctx);

    void RequestConfig(const TActorContext& ctx);
    void HandleConfig(const NKikimrClient::TResponse& res, const TActorContext& ctx);
    void Initialize(const TActorContext& ctx);

    template <typename T>
    void EmplaceRequest(T&& body, const TActorContext& ctx) {
        Requests.emplace_back(body, WriteQuota->GetQuotedTime(ctx.Now()), ctx.Now() - TInstant::Zero());
    }
    void EmplaceResponse(TMessage&& message, const TActorContext& ctx);

    void Handle(TEvPQ::TEvProposePartitionConfig::TPtr& ev, const TActorContext& ctx);

    void HandleOnInit(TEvPQ::TEvTxCalcPredicate::TPtr& ev, const TActorContext& ctx);
    void HandleOnInit(TEvPQ::TEvTxCommit::TPtr& ev, const TActorContext& ctx);
    void HandleOnInit(TEvPQ::TEvTxRollback::TPtr& ev, const TActorContext& ctx);
    void HandleOnInit(TEvPQ::TEvProposePartitionConfig::TPtr& ev, const TActorContext& ctx);

    void ChangePlanStepAndTxId(ui64 step, ui64 txId);

    void ResendPendingEvents(const TActorContext& ctx);

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PERSQUEUE_PARTITION_ACTOR;
    }

    TPartition(ui64 tabletId, ui32 partition, const TActorId& tablet, const TActorId& blobCache,
               const NPersQueue::TTopicConverterPtr& topicConverter, bool isLocalDC, TString dcId, bool isServerless,
               const NKikimrPQ::TPQTabletConfig& config, const TTabletCountersBase& counters, bool SubDomainOutOfSpace,
               bool newPartition = false,
               TVector<TTransaction> distrTxs = {});

    void Bootstrap(const TActorContext& ctx);

    ui64 Size() const {
        return BodySize + Head.PackedSize;
    }

    ui64 MeteringDataSize(const TActorContext& ctx) const;

    ui64 UsedReserveSize(const TActorContext& ctx) const {
         return std::min<ui64>(MeteringDataSize(ctx), ReserveSize());
    }

    ui64 ReserveSize() const {
        return TopicPartitionReserveSize(Config);
    }


    //Bootstrap sends kvRead
    //Become StateInit
    //StateInit
    //wait for correct result, cache all
    //Become StateIdle
    //StateIdle
    //got read - make kvRead
    //got kvReadResult - answer read
    //got write - make kvWrite, Become StateWrite
    //StateWrite
    // got read - ...
    // got kwReadResult - ...
    //got write - store it inflight
    //got kwWriteResult - check it, become StateIdle of StateWrite(and write inflight)

private:
    template <typename TEv>
    TString EventStr(const char * func, const TEv& ev) {
        TStringStream ss;
        ss << func << " event# " << ev->GetTypeRewrite() << " (" << ev->GetTypeName() << "), Tablet " << Tablet << ", Partition " << Partition
           << ", Sender " << ev->Sender.ToString() << ", Recipient " << ev->Recipient.ToString() << ", Cookie: " << ev->Cookie;
        return ss.Str();
    }

    STFUNC(StateInit)
    {
        NPersQueue::TCounterTimeKeeper keeper(TabletCounters.Cumulative()[COUNTER_PQ_TABLET_CPU_USAGE]);

        LOG_TRACE_S(ctx, NKikimrServices::PERSQUEUE, EventStr("StateInit", ev));

        TRACE_EVENT(NKikimrServices::PERSQUEUE);
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            HFuncTraced(TEvKeyValue::TEvResponse, HandleOnInit); //result of reads
            HFuncTraced(TEvents::TEvPoisonPill, Handle);
            HFuncTraced(TEvPQ::TEvMonRequest, HandleMonitoring);
            HFuncTraced(TEvPQ::TEvChangePartitionConfig, Handle);
            HFuncTraced(TEvPQ::TEvPartitionOffsets, HandleOnInit);
            HFuncTraced(TEvPQ::TEvPartitionStatus, HandleOnInit);
            HFuncTraced(TEvPersQueue::TEvReportPartitionError, Handle);
            HFuncTraced(TEvPersQueue::TEvHasDataInfo, Handle);
            HFuncTraced(TEvPQ::TEvMirrorerCounters, Handle);
            HFuncTraced(NReadSpeedLimiterEvents::TEvCounters, Handle);
            HFuncTraced(TEvPQ::TEvGetPartitionClientInfo, Handle);
            HFuncTraced(TEvPQ::TEvTxCalcPredicate, HandleOnInit);
            HFuncTraced(TEvPQ::TEvProposePartitionConfig, HandleOnInit);
            HFuncTraced(TEvPQ::TEvTxCommit, HandleOnInit);
            HFuncTraced(TEvPQ::TEvTxRollback, HandleOnInit);
            HFuncTraced(TEvPQ::TEvSubDomainStatus, Handle);
        default:
            LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE, "Unexpected " << EventStr("StateInit", ev));
            break;
        };
    }

    STFUNC(StateIdle)
    {
        NPersQueue::TCounterTimeKeeper keeper(TabletCounters.Cumulative()[COUNTER_PQ_TABLET_CPU_USAGE]);

        LOG_TRACE_S(ctx, NKikimrServices::PERSQUEUE, EventStr("StateIdle", ev));

        TRACE_EVENT(NKikimrServices::PERSQUEUE);
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            HFuncTraced(TEvKeyValue::TEvResponse, Handle);
            HFuncTraced(TEvPQ::TEvBlobResponse, Handle);
            HFuncTraced(TEvPQ::TEvWrite, HandleOnIdle);
            HFuncTraced(TEvPQ::TEvRead, Handle);
            HFuncTraced(NReadSpeedLimiterEvents::TEvResponse, Handle);
            HFuncTraced(TEvPQ::TEvReadTimeout, Handle);
            HFuncTraced(TEvents::TEvPoisonPill, Handle);
            HFuncTraced(TEvPQ::TEvMonRequest, HandleMonitoring);
            HFuncTraced(TEvPQ::TEvGetMaxSeqNoRequest, Handle);
            HFuncTraced(TEvPQ::TEvChangePartitionConfig, Handle);
            HFuncTraced(TEvPQ::TEvGetClientOffset, Handle);
            HFuncTraced(TEvPQ::TEvUpdateWriteTimestamp, Handle);
            HFuncTraced(TEvPQ::TEvSetClientInfo, Handle);
            HFuncTraced(TEvPQ::TEvPartitionOffsets, Handle);
            HFuncTraced(TEvPQ::TEvPartitionStatus, Handle);
            HFuncTraced(TEvPersQueue::TEvReportPartitionError, Handle);
            HFuncTraced(TEvPQ::TEvChangeOwner, Handle);
            HFuncTraced(TEvPersQueue::TEvHasDataInfo, Handle);
            HFuncTraced(TEvPQ::TEvMirrorerCounters, Handle);
            HFuncTraced(NReadSpeedLimiterEvents::TEvCounters, Handle);
            HFuncTraced(TEvPQ::TEvProxyResponse, Handle);
            HFuncTraced(TEvPQ::TEvError, Handle);
            HFuncTraced(TEvPQ::TEvGetPartitionClientInfo, Handle);
            HFuncTraced(TEvPQ::TEvUpdateAvailableSize, HandleOnIdle);
            HFuncTraced(TEvPQ::TEvReserveBytes, Handle);
            HFuncTraced(TEvPQ::TEvPipeDisconnected, Handle);
            HFuncTraced(TEvQuota::TEvClearance, Handle);
            HFuncTraced(TEvPQ::TEvQuotaDeadlineCheck, Handle);
            HFuncTraced(TEvPQ::TEvRegisterMessageGroup, HandleOnIdle);
            HFuncTraced(TEvPQ::TEvDeregisterMessageGroup, HandleOnIdle);
            HFuncTraced(TEvPQ::TEvSplitMessageGroup, HandleOnIdle);
            HFuncTraced(TEvPersQueue::TEvProposeTransaction, Handle);
            HFuncTraced(TEvPQ::TEvTxCalcPredicate, Handle);
            HFuncTraced(TEvPQ::TEvProposePartitionConfig, Handle);
            HFuncTraced(TEvPQ::TEvTxCommit, Handle);
            HFuncTraced(TEvPQ::TEvTxRollback, Handle);
            HFuncTraced(TEvPQ::TEvSubDomainStatus, Handle);

        default:
            LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE, "Unexpected " << EventStr("StateIdle", ev));
            break;
        };
    }

    STFUNC(StateWrite)
    {
        NPersQueue::TCounterTimeKeeper keeper(TabletCounters.Cumulative()[COUNTER_PQ_TABLET_CPU_USAGE]);

        LOG_TRACE_S(ctx, NKikimrServices::PERSQUEUE, EventStr("StateWrite", ev));

        TRACE_EVENT(NKikimrServices::PERSQUEUE);
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            HFuncTraced(TEvKeyValue::TEvResponse, Handle);
            HFuncTraced(TEvPQ::TEvHandleWriteResponse, Handle);
            HFuncTraced(TEvPQ::TEvBlobResponse, Handle);
            HFuncTraced(TEvPQ::TEvWrite, HandleOnWrite);
            HFuncTraced(TEvPQ::TEvRead, Handle);
            HFuncTraced(NReadSpeedLimiterEvents::TEvResponse, Handle);
            HFuncTraced(TEvPQ::TEvReadTimeout, Handle);
            HFuncTraced(TEvents::TEvPoisonPill, Handle);
            HFuncTraced(TEvPQ::TEvMonRequest, HandleMonitoring);
            HFuncTraced(TEvPQ::TEvGetMaxSeqNoRequest, Handle);
            HFuncTraced(TEvPQ::TEvGetClientOffset, Handle);
            HFuncTraced(TEvPQ::TEvUpdateWriteTimestamp, Handle);
            HFuncTraced(TEvPQ::TEvSetClientInfo, Handle);
            HFuncTraced(TEvPQ::TEvPartitionOffsets, Handle);
            HFuncTraced(TEvPQ::TEvPartitionStatus, Handle);
            HFuncTraced(TEvPersQueue::TEvReportPartitionError, Handle);
            HFuncTraced(TEvPQ::TEvChangeOwner, Handle);
            HFuncTraced(TEvPQ::TEvChangePartitionConfig, Handle);
            HFuncTraced(TEvPersQueue::TEvHasDataInfo, Handle);
            HFuncTraced(TEvPQ::TEvMirrorerCounters, Handle);
            HFuncTraced(NReadSpeedLimiterEvents::TEvCounters, Handle);
            HFuncTraced(TEvPQ::TEvProxyResponse, Handle);
            HFuncTraced(TEvPQ::TEvError, Handle);
            HFuncTraced(TEvPQ::TEvReserveBytes, Handle);
            HFuncTraced(TEvPQ::TEvGetPartitionClientInfo, Handle);
            HFuncTraced(TEvPQ::TEvPipeDisconnected, Handle);
            HFuncTraced(TEvPQ::TEvUpdateAvailableSize, HandleOnWrite);
            HFuncTraced(TEvPQ::TEvQuotaDeadlineCheck, Handle);
            HFuncTraced(TEvQuota::TEvClearance, Handle);
            HFuncTraced(TEvPQ::TEvRegisterMessageGroup, HandleOnWrite);
            HFuncTraced(TEvPQ::TEvDeregisterMessageGroup, HandleOnWrite);
            HFuncTraced(TEvPQ::TEvSplitMessageGroup, HandleOnWrite);
            HFuncTraced(TEvPersQueue::TEvProposeTransaction, Handle);
            HFuncTraced(TEvPQ::TEvTxCalcPredicate, Handle);
            HFuncTraced(TEvPQ::TEvProposePartitionConfig, Handle);
            HFuncTraced(TEvPQ::TEvTxCommit, Handle);
            HFuncTraced(TEvPQ::TEvTxRollback, Handle);
            HFuncTraced(TEvPQ::TEvSubDomainStatus, Handle);

        default:
            LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE, "Unexpected " << EventStr("StateWrite", ev));
            break;
        };
    }
private:
    enum EInitState {
        WaitConfig,
        WaitDiskStatus,
        WaitInfoRange,
        WaitDataRange,
        WaitDataRead,
        WaitMetaRead,
        WaitTxInfo
    };

    ui64 TabletID;
    ui32 Partition;
    NKikimrPQ::TPQTabletConfig Config;
    NKikimrPQ::TPQTabletConfig TabletConfig;
    const TTabletCountersBase& Counters;
    NPersQueue::TTopicConverterPtr TopicConverter;
    bool IsLocalDC;
    TString DCId;

    ui32 MaxBlobSize;
    const ui32 TotalLevels = 4;
    TVector<ui32> CompactLevelBorder;
    ui32 TotalMaxCount;
    ui32 MaxSizeCheck;

//                           [ 8+Mb][ 8+Mb ][not compacted data    ] [ data sended to KV but not yet confirmed]
//ofsets in partition:       101 102|103 104|105 106 107 108 109 110|111 112 113
//                            ^               ^                       ^
//                          StartOffset     HeadOffset                EndOffset
//                          [DataKeysBody  ][DataKeysHead                      ]
    ui64 StartOffset;
    ui64 EndOffset;

    ui64 WriteInflightSize;
    TActorId Tablet;
    TActorId BlobCache;

    EInitState InitState;

    std::deque<TMessage> Requests;
    std::deque<TMessage> Responses;

    THead Head;
    THead NewHead;
    TPartitionedBlob PartitionedBlob;
    std::deque<std::pair<TKey, ui32>> CompactedKeys; //key and blob size
    TDataKey NewHeadKey;

    ui64 BodySize;
    ui32 MaxWriteResponsesSize;

    std::deque<TDataKey> DataKeysBody;
    TVector<TKeyLevel> DataKeysHead;
    std::deque<TDataKey> HeadKeys;

    std::deque<std::pair<ui64,ui64>> GapOffsets;
    ui64 GapSize;

    TString CloudId;
    TString DbId;
    TString DbPath;
    bool IsServerless;
    TString FolderId;

    TMaybe<TUsersInfoStorage> UsersInfoStorage;

    //
    // user actions and transactions
    //
    std::deque<TSimpleSharedPtr<TEvPQ::TEvSetClientInfo>> UserActs;
    std::deque<TSimpleSharedPtr<TEvPersQueue::TEvProposeTransaction>> ImmediateTxs;
    std::deque<TTransaction> DistrTxs;
    THashMap<TString, size_t> UserActCount;
    THashMap<TString, TUserInfoBase> PendingUsersInfo;
    TVector<std::pair<TActorId, std::unique_ptr<IEventBase>>> Replies;
    THashSet<TString> AffectedUsers;
    bool UsersInfoWriteInProgress = false;
    bool TxInProgress = false;
    TMaybe<ui64> PlanStep;
    TMaybe<ui64> TxId;
    bool TxIdHasChanged = false;
    TSimpleSharedPtr<TEvPQ::TEvChangePartitionConfig> ChangeConfig;
    bool SendChangeConfigReply = true;
    //
    //
    //

    std::deque<std::pair<TString, ui64>> UpdateUserInfoTimestamp;
    bool ReadingTimestamp;
    TString ReadingForUser;
    ui64 ReadingForUserReadRuleGeneration;
    ui64 ReadingForOffset; // log only

    THashMap<ui64, TReadInfo> ReadInfo;    // cookie -> {...}
    ui64 Cookie;
    TInstant CreationTime;
    TDuration InitDuration;
    bool InitDone;
    const bool NewPartition;

    THashMap<TString, NKikimr::NPQ::TOwnerInfo> Owners;
    THashSet<TActorId> OwnerPipes;

    TSourceIdStorage SourceIdStorage;

    std::deque<THolder<TEvPQ::TEvChangeOwner>> WaitToChangeOwner;

    TTabletCountersBase TabletCounters;
    THolder<TPartitionLabeledCounters> PartitionCountersLabeled;

    TSubscriber Subscriber;

    TInstant WriteCycleStartTime;
    ui32 WriteCycleSize;
    ui32 WriteNewSize;
    ui32 WriteNewSizeInternal;
    ui64 WriteNewSizeUncompressed;
    ui32 WriteNewMessages;
    ui32 WriteNewMessagesInternal;

    TInstant CurrentTimestamp;

    bool DiskIsFull;
    bool SubDomainOutOfSpace;

    TSet<THasDataReq> HasDataRequests;
    TSet<THasDataDeadline> HasDataDeadlines;
    ui64 HasDataReqNum;

    TMaybe<TQuotaTracker> WriteQuota;
    THolder<TPercentileCounter> PartitionWriteQuotaWaitCounter;
    TInstant QuotaDeadline = TInstant::Zero();

    TVector<NSlidingWindow::TSlidingWindow<NSlidingWindow::TSumOperation<ui64>>> AvgWriteBytes;
    TVector<NSlidingWindow::TSlidingWindow<NSlidingWindow::TSumOperation<ui64>>> AvgQuotaBytes;

    ui64 ReservedSize;
    std::deque<THolder<TEvPQ::TEvReserveBytes>> ReserveRequests;

    ui32 Channel;
    TVector<ui32> TotalChannelWritesByHead;

    TWorkingTimeCounter WriteBufferIsFullCounter;

    TInstant WriteTimestamp;
    TInstant WriteTimestampEstimate;
    bool ManageWriteTimestampEstimate = true;
    NSlidingWindow::TSlidingWindow<NSlidingWindow::TMaxOperation<ui64>> WriteLagMs;
    THolder<TPercentileCounter> InputTimeLag;
    THolder<TPercentileCounter> MessageSize;
    TPercentileCounter WriteLatency;

    NKikimr::NPQ::TMultiCounter SLIBigLatency;
    NKikimr::NPQ::TMultiCounter WritesTotal;
    NKikimr::NPQ::TMultiCounter BytesWrittenTotal;
    NKikimr::NPQ::TMultiCounter BytesWrittenGrpc;
    NKikimr::NPQ::TMultiCounter BytesWrittenUncompressed;
    NKikimr::NPQ::TMultiCounter BytesWrittenComp;
    NKikimr::NPQ::TMultiCounter MsgsWrittenTotal;
    NKikimr::NPQ::TMultiCounter MsgsWrittenGrpc;;

    // Writing blob with topic quota variables
    ui64 TopicQuotaRequestCookie = 0;

    // Wait topic quota metrics
    THolder<TPercentileCounter> TopicWriteQuotaWaitCounter;
    TInstant StartTopicQuotaWaitTimeForCurrentBlob;
    TInstant WriteStartTime;
    TDuration TopicQuotaWaitTimeForCurrentBlob;

    // Topic quota parameters
    TString TopicWriteQuoterPath;
    TString TopicWriteQuotaResourcePath;
    ui64 NextTopicWriteQuotaRequestCookie = 1;

    TDeque<NKikimrPQ::TStatusResponse::TErrorMessage> Errors;

    THolder<TMirrorerInfo> Mirrorer;

    TInstant LastUsedStorageMeterTimestamp;

    TDeque<std::unique_ptr<IEventBase>> PendingEvents;
};

} // namespace NKikimr::NPQ
