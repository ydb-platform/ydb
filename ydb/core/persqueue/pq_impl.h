#pragma once

#include "percentile_counter.h"
#include "metering_sink.h"
#include "transaction.h"

#include <ydb/core/keyvalue/keyvalue_flat_impl.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/core/tablet/tablet_pipe_client_cache.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/time_cast/time_cast.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/tx/long_tx_service/public/events.h>

#include <ydb/library/actors/interconnect/interconnect.h>

namespace NKikimr {
namespace NPQ {

struct TPartitionInfo;
struct TChangeNotification;

class TResponseBuilder;
class TPartition;

struct TTransaction;

//USES MAIN chanel for big blobs, INLINE or EXTRA for ZK-like load, EXTRA2 for small blob for logging (VDISK of type LOG is ok with EXTRA2)

class TPersQueue : public NKeyValue::TKeyValueFlat {
    enum ECookie : ui64 {
        WRITE_CONFIG_COOKIE = 2,
        READ_CONFIG_COOKIE  = 3,
        WRITE_STATE_COOKIE  = 4,
        WRITE_TX_COOKIE = 5,
        READ_TXS_COOKIE = 6,
    };

    void CreatedHook(const TActorContext& ctx) override;
    bool HandleHook(STFUNC_SIG) override;

    void ReplyError(const TActorContext& ctx, const ui64 responseCookie, NPersQueue::NErrorCode::EErrorCode errorCode, const TString& error);

    void HandleWakeup(const TActorContext&);

    void Handle(TEvPersQueue::TEvProposeTransaction::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTxProcessing::TEvPlanStep::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTxProcessing::TEvReadSet::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTxProcessing::TEvReadSetAck::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvTxCalcPredicateResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvTxCommitDone::TPtr& ev, const TActorContext& ctx);

    void InitResponseBuilder(const ui64 responseCookie, const ui32 count, const ui32 counterId);
    void Handle(TEvPQ::TEvError::TPtr& ev, const TActorContext&);
    void Handle(TEvPQ::TEvProxyResponse::TPtr& ev, const TActorContext&);
    void FinishResponse(THashMap<ui64, TAutoPtr<TResponseBuilder>>::iterator it);

    void Handle(TEvInterconnect::TEvNodeInfo::TPtr& ev, const TActorContext&);

    void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev, const TActorContext&);
    void Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev, const TActorContext&);

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext&);
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext&);

    //when partition is ready it's sends event to tablet
    void Handle(TEvPQ::TEvInitComplete::TPtr& ev, const TActorContext&);

    //partitions will send some times it's counters
    void Handle(TEvPQ::TEvPartitionCounters::TPtr& ev, const TActorContext&);

    void Handle(TEvPQ::TEvMetering::TPtr& ev, const TActorContext&);

    void Handle(TEvPQ::TEvPartitionLabeledCounters::TPtr& ev, const TActorContext&);
    void Handle(TEvPQ::TEvPartitionLabeledCountersDrop::TPtr& ev, const TActorContext&);
    void AggregateAndSendLabeledCountersFor(const TString& group, const TActorContext&);

    void Handle(TEvPQ::TEvTabletCacheCounters::TPtr& ev, const TActorContext&);
    void SetCacheCounters(TEvPQ::TEvTabletCacheCounters::TCacheCounters& cacheCounters);

    //client requests
    void Handle(TEvPersQueue::TEvUpdateConfig::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvPartitionConfigChanged::TPtr& ev, const TActorContext& ctx);
    void ProcessUpdateConfigRequest(TAutoPtr<TEvPersQueue::TEvUpdateConfig> ev, const TActorId& sender, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvOffsets::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvStatus::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvDropTablet::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvHasDataInfo::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvPartitionClientInfo::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvSubDomainStatus::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvReadingPartitionStatusRequest::TPtr& ev, const TActorContext& ctx);

    bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx) override;

    void HandleDie(const TActorContext& ctx) override;

    //response from KV on READ or WRITE config request
    void Handle(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx);
    void HandleConfigReadResponse(NKikimrClient::TResponse&& resp, const TActorContext& ctx);
    void HandleTransactionsReadResponse(NKikimrClient::TResponse&& resp, const TActorContext& ctx);
    void ApplyNewConfigAndReply(const TActorContext& ctx);
    void ApplyNewConfig(const NKikimrPQ::TPQTabletConfig& newConfig,
                        const TActorContext& ctx);
    void HandleStateWriteResponse(const NKikimrClient::TResponse& resp, const TActorContext& ctx);

    void ReadTxInfo(const NKikimrClient::TKeyValueResponse::TReadResult& read,
                    const TActorContext& ctx);
    void ReadTxWrites(const NKikimrClient::TKeyValueResponse::TReadResult& read,
                      const TActorContext& ctx);
    void ReadConfig(const NKikimrClient::TKeyValueResponse::TReadResult& read,
                    const TVector<NKikimrClient::TKeyValueResponse::TReadRangeResult>& readRanges,
                    const TActorContext& ctx);
    void ReadState(const NKikimrClient::TKeyValueResponse::TReadResult& read, const TActorContext& ctx);

    void InitializeMeteringSink(const TActorContext& ctx);
    void ProcessReadRequestImpl(const ui64 responseCookie, const TActorId& partActor,
                                const NKikimrClient::TPersQueuePartitionRequest& req, bool doPrepare, ui32 readId,
                                const TActorContext& ctx);

    TMaybe<TEvPQ::TEvRegisterMessageGroup::TBody> MakeRegisterMessageGroup(
        const NKikimrClient::TPersQueuePartitionRequest::TCmdRegisterMessageGroup& cmd,
        NPersQueue::NErrorCode::EErrorCode& code, TString& error) const;

    TMaybe<TEvPQ::TEvDeregisterMessageGroup::TBody> MakeDeregisterMessageGroup(
        const NKikimrClient::TPersQueuePartitionRequest::TCmdDeregisterMessageGroup& cmd,
        NPersQueue::NErrorCode::EErrorCode& code, TString& error) const;

    void TrySendUpdateConfigResponses(const TActorContext& ctx);
    static void CreateTopicConverter(const NKikimrPQ::TPQTabletConfig& config,
                                     NPersQueue::TConverterFactoryPtr& converterFactory,
                                     NPersQueue::TTopicConverterPtr& topicConverter,
                                     const TActorContext& ctx);

    //client request
    void Handle(TEvPersQueue::TEvRequest::TPtr& ev, const TActorContext& ctx);
#define DESCRIBE_HANDLE(A) void A(const ui64 responseCookie, const TActorId& partActor, \
                                  const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx);
    DESCRIBE_HANDLE(HandleGetMaxSeqNoRequest)
    DESCRIBE_HANDLE(HandleSetClientOffsetRequest)
    DESCRIBE_HANDLE(HandleGetClientOffsetRequest)
    DESCRIBE_HANDLE(HandleWriteRequest)
    DESCRIBE_HANDLE(HandleUpdateWriteTimestampRequest)
    DESCRIBE_HANDLE(HandleRegisterMessageGroupRequest)
    DESCRIBE_HANDLE(HandleDeregisterMessageGroupRequest)
    DESCRIBE_HANDLE(HandleSplitMessageGroupRequest)
#undef DESCRIBE_HANDLE

#define DESCRIBE_HANDLE_WITH_SENDER(A) void A(const ui64 responseCookie, const TActorId& partActor, \
                                  const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx,\
                                  const TActorId& pipeClient, const TActorId& sender);

    DESCRIBE_HANDLE_WITH_SENDER(HandleCreateSessionRequest)
    DESCRIBE_HANDLE_WITH_SENDER(HandleDeleteSessionRequest)
    DESCRIBE_HANDLE_WITH_SENDER(HandleReadRequest)
    DESCRIBE_HANDLE_WITH_SENDER(HandlePublishReadRequest)
    DESCRIBE_HANDLE_WITH_SENDER(HandleForgetReadRequest)
    DESCRIBE_HANDLE_WITH_SENDER(HandleGetOwnershipRequest)
    DESCRIBE_HANDLE_WITH_SENDER(HandleReserveBytesRequest)
#undef DESCRIBE_HANDLE_WITH_SENDER

    bool ChangingState() const { return !TabletStateRequests.empty(); }
    void TryReturnTabletStateAll(const TActorContext& ctx, NKikimrProto::EReplyStatus status = NKikimrProto::OK);
    void ReturnTabletState(const TActorContext& ctx, const TChangeNotification& req, NKikimrProto::EReplyStatus status);

    void SchedulePlanStepAck(ui64 step,
                             const THashMap<TActorId, TVector<ui64>>& txAcks);
    void SchedulePlanStepAccepted(const TActorId& target,
                                  ui64 step);

    ui64 GetAllowedStep() const;

    void Handle(TEvPQ::TEvCheckPartitionStatusRequest::TPtr& ev, const TActorContext& ctx);
    void ProcessCheckPartitionStatusRequests(const TPartitionId& partitionId);

    void Handle(TEvPQ::TEvPartitionScaleStatusChanged::TPtr& ev, const TActorContext& ctx);

    TString LogPrefix() const;

    static constexpr const char * KeyConfig() { return "_config"; }
    static constexpr const char * KeyState() { return "_state"; }
    static constexpr const char * KeyTxInfo() { return "_txinfo"; }

    static NTabletPipe::TClientConfig GetPipeClientConfig();

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PERSQUEUE_ACTOR;
    }

    TPersQueue(const TActorId& tablet, TTabletStorageInfo *info);

private:
    bool ConfigInited;
    ui32 PartitionsInited;
    ui32 OriginalPartitionsCount;
    bool InitCompleted = false;
    THashMap<TPartitionId, TPartitionInfo> Partitions;
    THashMap<TString, TIntrusivePtr<TEvTabletCounters::TInFlightCookie>> CounterEventsInflight;

    struct TTxWriteInfo {
        THashMap<ui32, TPartitionId> Partitions;
        TMaybe<ui64> TxId;
        NKikimrLongTxService::TEvLockStatus::EStatus LongTxSubscriptionStatus = NKikimrLongTxService::TEvLockStatus::STATUS_UNSPECIFIED;
        bool Deleting = false;
    };

    THashMap<TWriteId, TTxWriteInfo> TxWrites, NewTxWrites;
    bool TxWritesChanged = false;
    ui32 NextSupportivePartitionId = 100'000;

    TActorId CacheActor;
    TActorId ReadBalancerActorId;

    TSet<TChangeNotification> ChangeConfigNotification;
    NKikimrPQ::TPQTabletConfig NewConfig;
    bool NewConfigShouldBeApplied;
    size_t ChangePartitionConfigInflight = 0;

    TString TopicName;
    TString TopicPath;
    NPersQueue::TConverterFactoryPtr TopicConverterFactory;
    NPersQueue::TTopicConverterPtr TopicConverter;
    bool IsLocalDC = false;
    TString DCId;
    bool IsServerless = false;
    TVector<NScheme::TTypeInfo> KeySchema;
    NKikimrPQ::TPQTabletConfig Config;

    NKikimrPQ::ETabletState TabletState;
    TSet<TChangeNotification> TabletStateRequests;

    TAutoPtr<TTabletCountersBase> Counters;
    TEvPQ::TEvTabletCacheCounters::TCacheCounters CacheCounters;
    TMap<TString, NKikimr::NPQ::TMultiCounter> BytesWrittenFromDC;


    THashMap<TString, TTabletLabeledCountersBase> LabeledCounters;

    TVector<TAutoPtr<TEvPersQueue::TEvHasDataInfo>> HasDataRequests;
    TVector<std::pair<TAutoPtr<TEvPersQueue::TEvUpdateConfig>, TActorId> > UpdateConfigRequests;

public:
    struct TPipeInfo {
        TActorId PartActor;
        TString Owner;
        ui32 ServerActors = 0;
        TString ClientId;
        TString SessionId;
        ui64 PartitionSessionId = 0;
        TPipeInfo() = default;
        static TPipeInfo ForOwner(const TActorId& partActor, const TString& owner, ui32 serverActors) {
            TPipeInfo res;
            res.Owner = owner;
            res.PartActor = partActor;
            res.ServerActors = serverActors;
            return res;
        }
    };

private:
    THashMap<TActorId, TPipeInfo> PipesInfo;

    ui64 NextResponseCookie;
    THashMap<ui64, TAutoPtr<TResponseBuilder>> ResponseProxy;

    NMetrics::TResourceMetrics *ResourceMetrics;

    TMeteringSink MeteringSink;

    //
    // транзакции
    //
    THashMap<ui64, TDistributedTransaction> Txs;
    TQueue<std::pair<ui64, ui64>> TxQueue;
    ui64 PlanStep = 0;
    ui64 PlanTxId = 0;
    ui64 ExecStep = 0;
    ui64 ExecTxId = 0;

    TDeque<std::unique_ptr<TEvPersQueue::TEvProposeTransaction>> EvProposeTransactionQueue;
    TDeque<std::pair<TActorId, std::unique_ptr<TEvTxProcessing::TEvPlanStep>>> EvPlanStepQueue;
    THashMap<ui64, NKikimrPQ::TTransaction::EState> WriteTxs;
    THashSet<ui64> DeleteTxs;
    THashSet<ui64> ChangedTxs;
    TMaybe<NKikimrPQ::TPQTabletConfig> TabletConfigTx;
    TMaybe<NKikimrPQ::TBootstrapConfig> BootstrapConfigTx;
    bool WriteTxsInProgress = false;

    struct TReplyToActor;

    TVector<TReplyToActor> RepliesToActor;

    TIntrusivePtr<NTabletPipe::TBoundedClientCacheConfig> PipeClientCacheConfig;
    THolder<NTabletPipe::IClientCache> PipeClientCache;
    TMap<ui64, TActorId> PartitionWriteQuoters;

    bool SubDomainOutOfSpace = false;

    void BeginWriteTxs(const TActorContext& ctx);
    void EndWriteTxs(const NKikimrClient::TResponse& resp,
                     const TActorContext& ctx);
    void TryWriteTxs(const TActorContext& ctx);

    void ProcessProposeTransactionQueue(const TActorContext& ctx);
    void ProcessPlanStepQueue(const TActorContext& ctx);
    void ProcessWriteTxs(const TActorContext& ctx,
                         NKikimrClient::TKeyValueRequest& request);
    void ProcessDeleteTxs(const TActorContext& ctx,
                          NKikimrClient::TKeyValueRequest& request);
    void ProcessConfigTx(const TActorContext& ctx,
                         TEvKeyValue::TEvRequest* request);
    void AddCmdWriteTabletTxInfo(NKikimrClient::TKeyValueRequest& request);

    void ScheduleProposeTransactionResult(const TDistributedTransaction& tx);

    void SendEvReadSetToReceivers(const TActorContext& ctx,
                                  TDistributedTransaction& tx);
    void SendEvReadSetAckToSenders(const TActorContext& ctx,
                                   TDistributedTransaction& tx);
    void SendEvTxCalcPredicateToPartitions(const TActorContext& ctx,
                                           TDistributedTransaction& tx);
    void SendEvTxCommitToPartitions(const TActorContext& ctx,
                                    TDistributedTransaction& tx);
    void SendEvTxRollbackToPartitions(const TActorContext& ctx,
                                      TDistributedTransaction& tx);
    void SendEvProposeTransactionResult(const TActorContext& ctx,
                                        TDistributedTransaction& tx);

    TDistributedTransaction* GetTransaction(const TActorContext& ctx,
                                            ui64 txId);

    void CheckTxState(const TActorContext& ctx,
                      TDistributedTransaction& tx);

    void WriteTx(TDistributedTransaction& tx, NKikimrPQ::TTransaction::EState state);
    void DeleteTx(TDistributedTransaction& tx);

    void SendReplies(const TActorContext& ctx);
    void CheckChangedTxStates(const TActorContext& ctx);

    bool AllTransactionsHaveBeenProcessed() const;

    void BeginWriteTabletState(const TActorContext& ctx, NKikimrPQ::ETabletState state);
    void EndWriteTabletState(const NKikimrClient::TResponse& resp,
                             const TActorContext& ctx);

    void SendProposeTransactionAbort(const TActorId& target,
                                     ui64 txId,
                                     NKikimrPQ::TError::EKind kind,
                                     const TString& reason,
                                     const TActorContext& ctx);

    void Handle(TEvPQ::TEvProposePartitionConfigResult::TPtr& ev, const TActorContext& ctx);
    void HandleDataTransaction(TAutoPtr<TEvPersQueue::TEvProposeTransaction> event,
                               const TActorContext& ctx);
    void HandleConfigTransaction(TAutoPtr<TEvPersQueue::TEvProposeTransaction> event,
                                 const TActorContext& ctx);

    void SendEvProposePartitionConfig(const TActorContext& ctx,
                                      TDistributedTransaction& tx);

    TActorId GetPartitionQuoter(const TPartitionId& partitionId);

    TPartition* CreatePartitionActor(const TPartitionId& partitionId,
                                     const NPersQueue::TTopicConverterPtr topicConverter,
                                     const NKikimrPQ::TPQTabletConfig& config,
                                     bool newPartition,
                                     const TActorContext& ctx);
    void CreateNewPartitions(NKikimrPQ::TPQTabletConfig& config,
                             NPersQueue::TTopicConverterPtr topicConverter,
                             const TActorContext& ctx);
    void CreateOriginalPartition(const NKikimrPQ::TPQTabletConfig& config,
                                 const NKikimrPQ::TPQTabletConfig::TPartition& partition,
                                 NPersQueue::TTopicConverterPtr topicConverter,
                                 const TPartitionId& partitionId,
                                 bool newPartition,
                                 const TActorContext& ctx);
    void EnsurePartitionsAreNotDeleted(const NKikimrPQ::TPQTabletConfig& config) const;

    void BeginWriteConfig(const NKikimrPQ::TPQTabletConfig& cfg,
                          const NKikimrPQ::TBootstrapConfig& bootstrapCfg,
                          const TActorContext& ctx);
    void EndWriteConfig(const NKikimrClient::TResponse& resp,
                        const TActorContext& ctx);
    void AddCmdWriteConfig(TEvKeyValue::TEvRequest* request,
                           const NKikimrPQ::TPQTabletConfig& cfg,
                           const NKikimrPQ::TBootstrapConfig& bootstrapCfg,
                           const TActorContext& ctx);

    void ClearNewConfig();

    void SendToPipe(ui64 tabletId,
                    TDistributedTransaction& tx,
                    std::unique_ptr<IEventBase> event,
                    const TActorContext& ctx);

    void InitTransactions(const NKikimrClient::TKeyValueResponse::TReadRangeResult& readRange,
                          THashMap<ui32, TVector<TTransaction>>& partitionTxs);
    void TryStartTransaction(const TActorContext& ctx);
    void OnInitComplete(const TActorContext& ctx);

    void RestartPipe(ui64 tabletId, const TActorContext& ctx);

    void BindTxToPipe(ui64 tabletId, ui64 txId);
    void UnbindTxFromPipe(ui64 tabletId, ui64 txId);
    const THashSet<ui64>& GetBindedTxs(ui64 tabletId);

    THashMap<ui64, THashSet<ui64>> BindedTxs;

    void InitProcessingParams(const TActorContext& ctx);
    void InitMediatorTimeCast(const TActorContext& ctx);

    TMaybe<NKikimrSubDomains::TProcessingParams> ProcessingParams;

    void Handle(TEvPersQueue::TEvProposeTransactionAttach::TPtr& ev, const TActorContext& ctx);

    void StartWatchingTenantPathId(const TActorContext& ctx);
    void StopWatchingTenantPathId(const TActorContext& ctx);
    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr& ev, const TActorContext& ctx);

    void MediatorTimeCastRegisterTablet(const TActorContext& ctx);
    void MediatorTimeCastUnregisterTablet(const TActorContext& ctx);
    void Handle(TEvMediatorTimecast::TEvRegisterTabletResult::TPtr& ev, const TActorContext& ctx);

    TIntrusivePtr<TMediatorTimecastEntry> MediatorTimeCastEntry;

    void DeleteExpiredTransactions(const TActorContext& ctx);
    void Handle(TEvPersQueue::TEvCancelTransactionProposal::TPtr& ev, const TActorContext& ctx);

    bool CanProcessProposeTransactionQueue() const;
    bool CanProcessPlanStepQueue() const;
    bool CanProcessWriteTxs() const;
    bool CanProcessDeleteTxs() const;
    bool CanProcessTxWrites() const;

    ui64 GetGeneration();
    void DestroySession(TPipeInfo& pipeInfo);
    bool UseMediatorTimeCast = true;

    TVector<TEvPersQueue::TEvStatus::TPtr> StatusRequests;
    void ProcessStatusRequests(const TActorContext &ctx);

    THashMap<ui32, TVector<TEvPQ::TEvCheckPartitionStatusRequest::TPtr>> CheckPartitionStatusRequests;
    TMaybe<ui64> TabletGeneration;

    TMaybe<TPartitionId> FindPartitionId(const NKikimrPQ::TDataTransaction& txBody) const;

    void InitPlanStep(const NKikimrPQ::TTabletTxInfo& info = {});
    void SavePlanStep(NKikimrPQ::TTabletTxInfo& info);

    void InitTxWrites(const NKikimrPQ::TTabletTxInfo& info, const TActorContext& ctx);
    void SaveTxWrites(NKikimrPQ::TTabletTxInfo& info);

    void HandleEventForSupportivePartition(const ui64 responseCookie,
                                           TEvPersQueue::TEvRequest::TPtr& event,
                                           const TActorId& sender,
                                           const TActorContext& ctx);
    void HandleEventForSupportivePartition(const ui64 responseCookie,
                                           const NKikimrClient::TPersQueuePartitionRequest& req,
                                           const TActorId& sender,
                                           const TActorContext& ctx);
    void HandleGetOwnershipRequestForSupportivePartition(const ui64 responseCookie,
                                                         const NKikimrClient::TPersQueuePartitionRequest& req,
                                                         const TActorId& sender,
                                                         const TActorContext& ctx);
    void HandleReserveBytesRequestForSupportivePartition(const ui64 responseCookie,
                                                         const NKikimrClient::TPersQueuePartitionRequest& req,
                                                         const TActorId& sender,
                                                         const TActorContext& ctx);
    void HandleWriteRequestForSupportivePartition(const ui64 responseCookie,
                                                  const NKikimrClient::TPersQueuePartitionRequest& req,
                                                  const TActorContext& ctx);

    void ForwardGetOwnershipToSupportivePartitions(const TActorContext& ctx);

    //
    // list of supporive partitions created before writing
    //
    THashSet<TPartitionId> NewSupportivePartitions;
    //
    // list of supportive partitions for which actors should be created
    //
    THashSet<TPartitionId> PendingSupportivePartitions;

    TPartitionInfo& GetPartitionInfo(const TPartitionId& partitionId);
    const TPartitionInfo& GetPartitionInfo(const NKikimrClient::TPersQueuePartitionRequest& request) const;
    void AddSupportivePartition(const TPartitionId& shadowPartitionId);
    void CreateSupportivePartitionActors(const TActorContext& ctx);
    void CreateSupportivePartitionActor(const TPartitionId& shadowPartitionId, const TActorContext& ctx);
    NKikimrPQ::TPQTabletConfig MakeSupportivePartitionConfig() const;
    void SubscribeWriteId(const TWriteId& writeId, const TActorContext& ctx);
    void UnsubscribeWriteId(const TWriteId& writeId, const TActorContext& ctx);

    bool AllOriginalPartitionsInited() const;

    void Handle(NLongTxService::TEvLongTxService::TEvLockStatus::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvDeletePartitionDone::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQ::TEvTransactionCompleted::TPtr& ev, const TActorContext& ctx);

    void BeginDeleteTx(const TDistributedTransaction& tx);
    void BeginDeletePartitions(TTxWriteInfo& writeInfo);

    bool CheckTxWriteOperation(const NKikimrPQ::TPartitionOperation& operation,
                               const TWriteId& writeId) const;
    bool CheckTxWriteOperations(const NKikimrPQ::TDataTransaction& txBody) const;

    void MoveTopTxToCalculating(TDistributedTransaction& tx, const TActorContext& ctx);
    void DeletePartition(const TPartitionId& partitionId, const TActorContext& ctx);

    std::deque<std::pair<ui64, ui64>> PlannedTxs;

    void BeginInitTransactions();
    void EndInitTransactions();

    void EndReadConfig(const TActorContext& ctx);

    void AddCmdReadTransactionRange(TEvKeyValue::TEvRequest& request,
                                    const TString& fromKey, bool includeFrom);

    NKikimrClient::TResponse ConfigReadResponse;
    TVector<NKikimrClient::TKeyValueResponse::TReadRangeResult> TransactionsReadResults;

    void SendTransactionsReadRequest(const TString& fromKey, bool includeFrom,
                                     const TActorContext& ctx);
};


}// NPQ
}// NKikimr
