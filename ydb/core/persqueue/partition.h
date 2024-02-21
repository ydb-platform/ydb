#pragma once

#include "blob.h"
#include "header.h"
#include "key.h"
#include "partition_init.h"
#include "partition_sourcemanager.h"
#include "partition_types.h"
#include "quota_tracker.h"
#include "sourceid.h"
#include "subscriber.h"
#include "user_info.h"
#include "utils.h"
#include "read_quoter.h"

#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/library/persqueue/counter_time_keeper/counter_time_keeper.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/sliding_window/sliding_window.h>

#include <util/generic/set.h>

#include <variant>

namespace NKikimr::NPQ {

static const ui32 MAX_BLOB_PART_SIZE = 500_KB;

using TPartitionLabeledCounters = TProtobufTabletLabeledCounters<EPartitionLabeledCounters_descriptor>;

ui64 GetOffsetEstimate(const std::deque<TDataKey>& container, TInstant timestamp, ui64 headOffset);


class TKeyLevel;
struct TMirrorerInfo;

struct TTransaction {
    explicit TTransaction(TSimpleSharedPtr<TEvPQ::TEvTxCalcPredicate> tx,
                          TMaybe<bool> predicate = Nothing()) :
        Tx(tx),
        Predicate(predicate)
    {
        Y_ABORT_UNLESS(Tx);
    }

    TTransaction(TSimpleSharedPtr<TEvPQ::TEvChangePartitionConfig> changeConfig,
                 bool sendReply) :
        ChangeConfig(changeConfig),
        SendReply(sendReply)
    {
        Y_ABORT_UNLESS(ChangeConfig);
    }

    explicit TTransaction(TSimpleSharedPtr<TEvPQ::TEvProposePartitionConfig> proposeConfig) :
        ProposeConfig(proposeConfig)
    {
        Y_ABORT_UNLESS(ProposeConfig);
    }

    TSimpleSharedPtr<TEvPQ::TEvTxCalcPredicate> Tx;
    TMaybe<bool> Predicate;

    TSimpleSharedPtr<TEvPQ::TEvChangePartitionConfig> ChangeConfig;
    bool SendReply;

    TSimpleSharedPtr<TEvPQ::TEvProposePartitionConfig> ProposeConfig;
};

class TPartition : public TActorBootstrapped<TPartition> {
    friend TInitializer;
    friend TInitializerStep;
    friend TInitConfigStep;
    friend TInitInternalFieldsStep;
    friend TInitDiskStatusStep;
    friend TInitMetaStep;
    friend TInitInfoRangeStep;
    friend TInitDataRangeStep;
    friend TInitDataStep;

    friend TPartitionSourceManager;

public:
    const TString& TopicName() const;

private:
    static const ui32 MAX_ERRORS_COUNT_TO_STORE = 10;

private:
    struct THasDataReq;
    struct THasDataDeadline;

    bool CanWrite() const;
    bool CanEnqueue() const;

    void ReplyError(const TActorContext& ctx, const ui64 dst, NPersQueue::NErrorCode::EErrorCode errorCode, const TString& error);
    void ReplyPropose(const TActorContext& ctx, const NKikimrPQ::TEvProposeTransaction& event, NKikimrPQ::TEvProposeTransactionResult::EStatus statusCode);
    void ReplyErrorForStoredWrites(const TActorContext& ctx);

    void ReplyGetClientOffsetOk(const TActorContext& ctx, const ui64 dst, const i64 offset, const TInstant writeTimestamp, const TInstant createTimestamp);
    void ReplyOk(const TActorContext& ctx, const ui64 dst);
    void ReplyOwnerOk(const TActorContext& ctx, const ui64 dst, const TString& ownerCookie, ui64 seqNo);

    void ReplyWrite(const TActorContext& ctx, ui64 dst, const TString& sourceId, ui64 seqNo, ui16 partNo, ui16 totalParts, ui64 offset, TInstant writeTimestamp, bool already, ui64 maxSeqNo, TDuration partitionQuotedTime, TDuration topicQuotedTime, TDuration queueTime, TDuration writeTime);

    void AddNewWriteBlob(std::pair<TKey, ui32>& res, TEvKeyValue::TEvRequest* request, bool headCleared, const TActorContext& ctx);
    void AnswerCurrentWrites(const TActorContext& ctx);
    void CancelAllWritesOnIdle(const TActorContext& ctx);
    void CancelAllWritesOnWrite(const TActorContext& ctx, TEvKeyValue::TEvRequest* request, const TString& errorStr, const TWriteMsg& p, TPartitionSourceManager::TModificationBatch& sourceIdBatch, NPersQueue::NErrorCode::EErrorCode errorCode = NPersQueue::NErrorCode::BAD_REQUEST);
    void ClearOldHead(const ui64 offset, const ui16 partNo, TEvKeyValue::TEvRequest* request);
    void CreateMirrorerActor();
    void DoRead(TEvPQ::TEvRead::TPtr&& ev, TDuration waitQuotaTime, const TActorContext& ctx);
    void FailBadClient(const TActorContext& ctx);
    void FillReadFromTimestamps(const NKikimrPQ::TPQTabletConfig& config, const TActorContext& ctx);
    void FilterDeadlinedWrites(const TActorContext& ctx);

    void Handle(NReadQuoterEvents::TEvAccountQuotaCountersUpdated::TPtr& ev, const TActorContext& ctx);
    void Handle(NReadQuoterEvents::TEvQuotaCountersUpdated::TPtr& ev, const TActorContext& ctx);
    void Handle(NReadQuoterEvents::TEvQuotaUpdated::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvApproveReadQuota::TPtr& ev, const TActorContext& ctx);
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
    void Handle(TEvPQ::TEvGetWriteInfoRequest::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvTxCalcPredicate::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvTxCommit::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvTxRollback::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvReportPartitionError::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvApproveWriteQuota::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvSubDomainStatus::TPtr& ev, const TActorContext& ctx);
    void HandleMonitoring(TEvPQ::TEvMonRequest::TPtr& ev, const TActorContext& ctx);
    void HandleOnIdle(TEvPQ::TEvDeregisterMessageGroup::TPtr& ev, const TActorContext& ctx);
    void HandleOnIdle(TEvPQ::TEvRegisterMessageGroup::TPtr& ev, const TActorContext& ctx);
    void HandleOnIdle(TEvPQ::TEvSplitMessageGroup::TPtr& ev, const TActorContext& ctx);
    void HandleOnIdle(TEvPQ::TEvUpdateAvailableSize::TPtr& ev, const TActorContext& ctx);
    void HandleOnIdle(TEvPQ::TEvWrite::TPtr& ev, const TActorContext& ctx);
    void HandleOnInit(TEvPQ::TEvPartitionOffsets::TPtr& ev, const TActorContext& ctx);
    void HandleOnInit(TEvPQ::TEvPartitionStatus::TPtr& ev, const TActorContext& ctx);
    void HandleOnWrite(TEvPQ::TEvDeregisterMessageGroup::TPtr& ev, const TActorContext& ctx);
    void HandleOnWrite(TEvPQ::TEvRegisterMessageGroup::TPtr& ev, const TActorContext& ctx);
    void HandleOnWrite(TEvPQ::TEvSplitMessageGroup::TPtr& ev, const TActorContext& ctx);
    void HandleOnWrite(TEvPQ::TEvUpdateAvailableSize::TPtr& ev, const TActorContext& ctx);
    void HandleOnWrite(TEvPQ::TEvWrite::TPtr& ev, const TActorContext& ctx);
    void HandleWakeup(const TActorContext& ctx);
    void HandleWriteResponse(const TActorContext& ctx);

    void InitComplete(const TActorContext& ctx);
    void InitUserInfoForImportantClients(const TActorContext& ctx);

    void LogAndCollectError(NKikimrServices::EServiceKikimr service, const TString& msg, const TActorContext& ctx);
    void LogAndCollectError(const NKikimrPQ::TStatusResponse::TErrorMessage& error, const TActorContext& ctx);

    void OnReadRequestFinished(ui64 cookie, ui64 answerSize, const TString& consumer, const TActorContext& ctx);

    void ProcessChangeOwnerRequest(TAutoPtr<TEvPQ::TEvChangeOwner> ev, const TActorContext& ctx);
    void ProcessChangeOwnerRequests(const TActorContext& ctx);
    void ProcessHasDataRequests(const TActorContext& ctx);
    void ProcessRead(const TActorContext& ctx, TReadInfo&& info, const ui64 cookie, bool subscription);
    void ProcessReserveRequests(const TActorContext& ctx);
    void ProcessTimestampRead(const TActorContext& ctx);
    void ProcessTimestampsForNewData(const ui64 prevEndOffset, const TActorContext& ctx);

    void ProcessMaxSeqNoRequest(const TActorContext& ctx);

    void ReadTimestampForOffset(const TString& user, TUserInfo& ui, const TActorContext& ctx);
    void ReportCounters(const TActorContext& ctx, bool force = false);
    bool UpdateCounters(const TActorContext& ctx, bool force = false);
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
    void RequestBlobQuota();
    void WritePendingBlob();

    void UpdateUserInfoEndOffset(const TInstant& now);
    void UpdateWriteBufferIsFullState(const TInstant& now);

    TInstant GetWriteTimeEstimate(ui64 offset) const;
    bool AppendHeadWithNewWrites(TEvKeyValue::TEvRequest* request, const TActorContext& ctx,
        TPartitionSourceManager::TModificationBatch& sourceIdBatch);
    bool CleanUp(TEvKeyValue::TEvRequest* request, const TActorContext& ctx);

    // Removes blobs that are no longer required. Blobs are no longer required if the storage time of all messages
    // stored in this blob has expired and they have been read by all important consumers.
    bool CleanUpBlobs(TEvKeyValue::TEvRequest *request, const TActorContext& ctx);
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
    TVector<TRequestedBlob> GetReadRequestFromBody(const ui64 startOffset, const ui16 partNo, const ui32 maxCount,
                                                   const ui32 maxSize, ui32* rcount, ui32* rsize, ui64 lastOffset);
    TVector<TClientBlob>    GetReadRequestFromHead(const ui64 startOffset, const ui16 partNo, const ui32 maxCount,
                                                   const ui32 maxSize, const ui64 readTimestampMs, ui32* rcount,
                                                   ui32* rsize, ui64* insideHeadOffset, ui64 lastOffset);

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
    void OnProcessTxsAndUserActsWriteComplete(ui64 cookie, const TActorContext& ctx);
    void EndChangePartitionConfig(const NKikimrPQ::TPQTabletConfig& config,
                                  NPersQueue::TTopicConverterPtr topicConverter,
                                  const TActorContext& ctx);
    TString GetKeyConfig() const;

    void InitPendingUserInfoForImportantClients(const NKikimrPQ::TPQTabletConfig& config,
                                                const TActorContext& ctx);

    void Initialize(const TActorContext& ctx);

    template <typename T>
    void EmplaceRequest(T&& body, const TActorContext& ctx) {
        const auto now = ctx.Now();
        Requests.emplace_back(body, now - TInstant::Zero());
    }
    void EmplaceResponse(TMessage&& message, const TActorContext& ctx);

    void Handle(TEvPQ::TEvProposePartitionConfig::TPtr& ev, const TActorContext& ctx);

    void HandleOnInit(TEvPQ::TEvTxCalcPredicate::TPtr& ev, const TActorContext& ctx);
    void HandleOnInit(TEvPQ::TEvTxCommit::TPtr& ev, const TActorContext& ctx);
    void HandleOnInit(TEvPQ::TEvTxRollback::TPtr& ev, const TActorContext& ctx);
    void HandleOnInit(TEvPQ::TEvProposePartitionConfig::TPtr& ev, const TActorContext& ctx);

    void ChangePlanStepAndTxId(ui64 step, ui64 txId);

    void ResendPendingEvents(const TActorContext& ctx);
    void SendReadPreparedProxyResponse(const TReadAnswer& answer, const TReadInfo& readInfo, TUserInfo& user);

    void CheckIfSessionExists(TUserInfoBase& userInfo, const TActorId& newPipe);
    // void DestroyReadSession(const TReadSessionKey& key);

    void Handle(TEvPQ::TEvCheckPartitionStatusRequest::TPtr& ev, const TActorContext& ctx);

    TString LogPrefix() const;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PERSQUEUE_PARTITION_ACTOR;
    }

    TPartition(ui64 tabletId, const TPartitionId& partition, const TActorId& tablet, ui32 tabletGeneration, const TActorId& blobCache,
               const NPersQueue::TTopicConverterPtr& topicConverter, TString dcId, bool isServerless,
               const NKikimrPQ::TPQTabletConfig& config, const TTabletCountersBase& counters, bool SubDomainOutOfSpace, ui32 numChannels,
               bool newPartition = false,
               TVector<TTransaction> distrTxs = {}, const TActorId& writeQuoterActorId = {});

    void Bootstrap(const TActorContext& ctx);

    ui64 Size() const {
        return BodySize + Head.PackedSize;
    }

    // The size of the data realy was persisted in the storage by the partition
    ui64 MeteringDataSize(const TActorContext& ctx) const;
    // The size of the storage that was reserved by the partition
    ui64 ReserveSize() const;
    // The size of the storage that usud by the partition. That included combination of the reserver and realy persisted data.
    ui64 StorageSize(const TActorContext& ctx) const;
    ui64 UsedReserveSize(const TActorContext& ctx) const;
    // Minimal offset, the data from which cannot be deleted, because it is required by an important consumer
    ui64 ImportantClientsMinOffset() const;


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

    TInitializer Initializer;

    STFUNC(StateInit)
    {
        NPersQueue::TCounterTimeKeeper keeper(TabletCounters.Cumulative()[COUNTER_PQ_TABLET_CPU_USAGE]);

        ALOG_TRACE(NKikimrServices::PERSQUEUE, EventStr("StateInit", ev));

        TRACE_EVENT(NKikimrServices::PERSQUEUE);
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            HFuncTraced(TEvents::TEvPoisonPill, Handle);
            HFuncTraced(TEvPQ::TEvMonRequest, HandleMonitoring);
            HFuncTraced(TEvPQ::TEvChangePartitionConfig, Handle);
            HFuncTraced(TEvPQ::TEvPartitionOffsets, HandleOnInit);
            HFuncTraced(TEvPQ::TEvPartitionStatus, HandleOnInit);
            HFuncTraced(TEvPersQueue::TEvReportPartitionError, Handle);
            HFuncTraced(TEvPersQueue::TEvHasDataInfo, Handle);
            HFuncTraced(TEvPQ::TEvMirrorerCounters, Handle);
            HFuncTraced(TEvPQ::TEvGetPartitionClientInfo, Handle);
            HFuncTraced(TEvPQ::TEvTxCalcPredicate, HandleOnInit);
            HFuncTraced(TEvPQ::TEvProposePartitionConfig, HandleOnInit);
            HFuncTraced(TEvPQ::TEvTxCommit, HandleOnInit);
            HFuncTraced(TEvPQ::TEvTxRollback, HandleOnInit);
            HFuncTraced(TEvPQ::TEvSubDomainStatus, Handle);
            HFuncTraced(NReadQuoterEvents::TEvQuotaUpdated, Handle);
            HFuncTraced(NReadQuoterEvents::TEvAccountQuotaCountersUpdated, Handle);
            HFuncTraced(NReadQuoterEvents::TEvQuotaCountersUpdated, Handle);
            HFuncTraced(TEvPQ::TEvGetWriteInfoRequest, Handle);
        default:
            if (!Initializer.Handle(ev)) {
                ALOG_ERROR(NKikimrServices::PERSQUEUE, "Unexpected " << EventStr("StateInit", ev));
            }
            break;
        };
    }

    STFUNC(StateIdle)
    {
        NPersQueue::TCounterTimeKeeper keeper(TabletCounters.Cumulative()[COUNTER_PQ_TABLET_CPU_USAGE]);

        ALOG_TRACE(NKikimrServices::PERSQUEUE, EventStr("StateIdle", ev));

        TRACE_EVENT(NKikimrServices::PERSQUEUE);
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            HFuncTraced(TEvKeyValue::TEvResponse, Handle);
            HFuncTraced(TEvPQ::TEvBlobResponse, Handle);
            HFuncTraced(TEvPQ::TEvWrite, HandleOnIdle);
            HFuncTraced(TEvPQ::TEvRead, Handle);
            HFuncTraced(TEvPQ::TEvApproveReadQuota, Handle);
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
            HFuncTraced(TEvPQ::TEvProxyResponse, Handle);
            HFuncTraced(TEvPQ::TEvError, Handle);
            HFuncTraced(TEvPQ::TEvGetPartitionClientInfo, Handle);
            HFuncTraced(TEvPQ::TEvUpdateAvailableSize, HandleOnIdle);
            HFuncTraced(TEvPQ::TEvReserveBytes, Handle);
            HFuncTraced(TEvPQ::TEvPipeDisconnected, Handle);
            HFuncTraced(TEvPQ::TEvApproveWriteQuota, Handle);
            HFuncTraced(TEvPQ::TEvQuotaDeadlineCheck, Handle);
            HFuncTraced(TEvPQ::TEvRegisterMessageGroup, HandleOnIdle);
            HFuncTraced(TEvPQ::TEvDeregisterMessageGroup, HandleOnIdle);
            HFuncTraced(TEvPQ::TEvSplitMessageGroup, HandleOnIdle);
            HFuncTraced(TEvPersQueue::TEvProposeTransaction, Handle);
            HFuncTraced(TEvPQ::TEvTxCalcPredicate, Handle);
            HFuncTraced(TEvPQ::TEvGetWriteInfoRequest, Handle);
            HFuncTraced(TEvPQ::TEvProposePartitionConfig, Handle);
            HFuncTraced(TEvPQ::TEvTxCommit, Handle);
            HFuncTraced(TEvPQ::TEvTxRollback, Handle);
            HFuncTraced(TEvPQ::TEvSubDomainStatus, Handle);
            HFuncTraced(TEvPQ::TEvCheckPartitionStatusRequest, Handle);
            HFuncTraced(NReadQuoterEvents::TEvQuotaUpdated, Handle);
            HFuncTraced(NReadQuoterEvents::TEvAccountQuotaCountersUpdated, Handle);
            HFuncTraced(NReadQuoterEvents::TEvQuotaCountersUpdated, Handle);
        default:
            ALOG_ERROR(NKikimrServices::PERSQUEUE, "Unexpected " << EventStr("StateIdle", ev));
            break;
        };
    }

    STFUNC(StateWrite)
    {
        NPersQueue::TCounterTimeKeeper keeper(TabletCounters.Cumulative()[COUNTER_PQ_TABLET_CPU_USAGE]);

        ALOG_TRACE(NKikimrServices::PERSQUEUE, EventStr("StateWrite", ev));

        TRACE_EVENT(NKikimrServices::PERSQUEUE);
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            HFuncTraced(TEvKeyValue::TEvResponse, Handle);
            HFuncTraced(TEvPQ::TEvHandleWriteResponse, Handle);
            HFuncTraced(TEvPQ::TEvBlobResponse, Handle);
            HFuncTraced(TEvPQ::TEvWrite, HandleOnWrite);
            HFuncTraced(TEvPQ::TEvRead, Handle);
            HFuncTraced(TEvPQ::TEvApproveReadQuota, Handle);
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
            HFuncTraced(TEvPQ::TEvProxyResponse, Handle);
            HFuncTraced(TEvPQ::TEvError, Handle);
            HFuncTraced(TEvPQ::TEvReserveBytes, Handle);
            HFuncTraced(TEvPQ::TEvGetPartitionClientInfo, Handle);
            HFuncTraced(TEvPQ::TEvPipeDisconnected, Handle);
            HFuncTraced(TEvPQ::TEvUpdateAvailableSize, HandleOnWrite);
            HFuncTraced(TEvPQ::TEvQuotaDeadlineCheck, Handle);
            HFuncTraced(TEvPQ::TEvApproveWriteQuota, Handle);
            HFuncTraced(TEvPQ::TEvRegisterMessageGroup, HandleOnWrite);
            HFuncTraced(TEvPQ::TEvDeregisterMessageGroup, HandleOnWrite);
            HFuncTraced(TEvPQ::TEvSplitMessageGroup, HandleOnWrite);
            HFuncTraced(TEvPersQueue::TEvProposeTransaction, Handle);
            HFuncTraced(TEvPQ::TEvTxCalcPredicate, Handle);
            HFuncTraced(TEvPQ::TEvGetWriteInfoRequest, Handle);
            HFuncTraced(TEvPQ::TEvProposePartitionConfig, Handle);
            HFuncTraced(TEvPQ::TEvTxCommit, Handle);
            HFuncTraced(TEvPQ::TEvTxRollback, Handle);
            HFuncTraced(TEvPQ::TEvSubDomainStatus, Handle);
            HFuncTraced(TEvPQ::TEvCheckPartitionStatusRequest, Handle);
            HFuncTraced(NReadQuoterEvents::TEvQuotaUpdated, Handle);
            HFuncTraced(NReadQuoterEvents::TEvAccountQuotaCountersUpdated, Handle);
            HFuncTraced(NReadQuoterEvents::TEvQuotaCountersUpdated, Handle);
        default:
            ALOG_ERROR(NKikimrServices::PERSQUEUE, "Unexpected " << EventStr("StateWrite", ev));
            break;
        };
    }

private:
    enum class ProcessResult {
        Continue,
        Abort,
        Break
    };

    struct ProcessParameters {
        ProcessParameters(TPartitionSourceManager::TModificationBatch& sourceIdBatch)
                : SourceIdBatch(sourceIdBatch) {
            }

        TPartitionSourceManager::TModificationBatch& SourceIdBatch;

        ui64 CurOffset;
        bool OldPartsCleared;
        bool HeadCleared;
    };

    ProcessResult ProcessRequest(TRegisterMessageGroupMsg& msg, ProcessParameters& parameters);
    ProcessResult ProcessRequest(TDeregisterMessageGroupMsg& msg, ProcessParameters& parameters);
    ProcessResult ProcessRequest(TSplitMessageGroupMsg& msg, ProcessParameters& parameters);
    ProcessResult ProcessRequest(TWriteMsg& msg, ProcessParameters& parameters, TEvKeyValue::TEvRequest* request, const TActorContext& ctx);

private:
    ui64 TabletID;
    ui32 TabletGeneration;
    TPartitionId Partition;
    NKikimrPQ::TPQTabletConfig Config;
    NKikimrPQ::TPQTabletConfig TabletConfig;
    const NKikimrPQ::TPQTabletConfig::TPartition* PartitionConfig = nullptr;
    const NKikimrPQ::TPQTabletConfig::TPartition* PendingPartitionConfig = nullptr;

    const TTabletCountersBase& Counters;
    NPersQueue::TTopicConverterPtr TopicConverter;
    bool IsLocalDC;
    TString DCId;

    TPartitionGraph PartitionGraph;
    TPartitionSourceManager SourceManager;

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

    template <class T> T& GetUserActionAndTransactionEventsFront();
    template <class T> bool UserActionAndTransactionEventsFrontIs() const;

    bool ProcessUserActionOrTransaction(TEvPQ::TEvSetClientInfo& event, const TActorContext& ctx);
    bool ProcessUserActionOrTransaction(const TEvPersQueue::TEvProposeTransaction& event, const TActorContext& ctx);
    bool ProcessUserActionOrTransaction(TTransaction& tx, const TActorContext& ctx);

    //
    // user actions and transactions
    //
    using TUserActionAndTransactionEvent =
        std::variant<TSimpleSharedPtr<TEvPQ::TEvSetClientInfo>,             // user actions
                     TSimpleSharedPtr<TEvPersQueue::TEvProposeTransaction>, // immediate transaction
                     TSimpleSharedPtr<TTransaction>>;                       // distributed transaction or update config
    std::deque<TUserActionAndTransactionEvent> UserActionAndTransactionEvents;
    size_t ImmediateTxCount = 0;
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
    bool NewPartition;

    THashMap<TString, NKikimr::NPQ::TOwnerInfo> Owners;
    THashSet<TActorId> OwnerPipes;

    TSourceIdStorage SourceIdStorage;

    std::deque<THolder<TEvPQ::TEvChangeOwner>> WaitToChangeOwner;

    TTabletCountersBase TabletCounters;
    THolder<TPartitionLabeledCounters> PartitionCountersLabeled;
    TInstant LastCountersUpdate;

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

    TActorId ReadQuotaTrackerActor;
    TActorId WriteQuotaTrackerActor;
    THolder<TPercentileCounter> PartitionWriteQuotaWaitCounter;
    TInstant QuotaDeadline = TInstant::Zero();

    TVector<NSlidingWindow::TSlidingWindow<NSlidingWindow::TSumOperation<ui64>>> AvgWriteBytes;
    NSlidingWindow::TSlidingWindow<NSlidingWindow::TSumOperation<ui64>> AvgReadBytes;
    TVector<NSlidingWindow::TSlidingWindow<NSlidingWindow::TSumOperation<ui64>>> AvgQuotaBytes;

    ui64 ReservedSize;
    std::deque<THolder<TEvPQ::TEvReserveBytes>> ReserveRequests;

    ui32 Channel;
    ui32 NumChannels;
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
    ui64 NextTopicWriteQuotaRequestCookie = 1;
    ui64 TopicQuotaConsumedCookie = 0;

    // Wait topic quota metrics
    ui64 TotalPartitionWriteSpeed = 0;
    THolder<TPercentileCounter> TopicWriteQuotaWaitCounter;
    TInstant WriteStartTime;
    TDuration TopicQuotaWaitTimeForCurrentBlob;
    TDuration PartitionQuotaWaitTimeForCurrentBlob;

    //Pending request
    THolder<TEvKeyValue::TEvRequest> PendingWriteRequest;


    TDeque<NKikimrPQ::TStatusResponse::TErrorMessage> Errors;

    THolder<TMirrorerInfo> Mirrorer;

    TInstant LastUsedStorageMeterTimestamp;

    TDeque<std::unique_ptr<IEventBase>> PendingEvents;
    TRowVersion LastEmittedHeartbeat;

    const NKikimrPQ::TPQTabletConfig::TPartition* GetPartitionConfig(const NKikimrPQ::TPQTabletConfig& config);

    bool ClosedInternalPartition = false;
};

} // namespace NKikimr::NPQ

