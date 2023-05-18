#pragma once

#include "percentile_counter.h"
#include "metering_sink.h"
#include "transaction.h"

#include <ydb/core/keyvalue/keyvalue_flat_impl.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/core/tablet/tablet_pipe_client_cache.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/tx/tx_processing.h>

#include <library/cpp/actors/interconnect/interconnect.h>

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

    void DefaultSignalTabletActive(const TActorContext&) override;

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

    bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx) override;

    void HandleDie(const TActorContext& ctx) override;

    //response from KV on READ or WRITE config request
    void Handle(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx);
    void HandleConfigReadResponse(const NKikimrClient::TResponse& resp, const TActorContext& ctx);
    void ApplyNewConfigAndReply(const TActorContext& ctx);
    void ApplyNewConfig(const NKikimrPQ::TPQTabletConfig& newConfig,
                        const TActorContext& ctx);
    void HandleStateWriteResponse(const NKikimrClient::TResponse& resp, const TActorContext& ctx);

    void ReadTxInfo(const NKikimrClient::TKeyValueResponse::TReadResult& read,
                    const TActorContext& ctx);
    void ReadConfig(const NKikimrClient::TKeyValueResponse::TReadResult& read,
                    const NKikimrClient::TKeyValueResponse::TReadRangeResult& readRange,
                    const TActorContext& ctx);
    void ReadState(const NKikimrClient::TKeyValueResponse::TReadResult& read, const TActorContext& ctx);

    void InitializeMeteringSink(const TActorContext& ctx);

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
    DESCRIBE_HANDLE(HandleDeleteSessionRequest)
    DESCRIBE_HANDLE(HandleCreateSessionRequest)
    DESCRIBE_HANDLE(HandleSetClientOffsetRequest)
    DESCRIBE_HANDLE(HandleGetClientOffsetRequest)
    DESCRIBE_HANDLE(HandleWriteRequest)
    DESCRIBE_HANDLE(HandleUpdateWriteTimestampRequest)
    DESCRIBE_HANDLE(HandleReadRequest)
    DESCRIBE_HANDLE(HandleRegisterMessageGroupRequest)
    DESCRIBE_HANDLE(HandleDeregisterMessageGroupRequest)
    DESCRIBE_HANDLE(HandleSplitMessageGroupRequest)
#undef DESCRIBE_HANDLE
#define DESCRIBE_HANDLE_WITH_SENDER(A) void A(const ui64 responseCookie, const TActorId& partActor, \
                                  const NKikimrClient::TPersQueuePartitionRequest& req, const TActorContext& ctx,\
                                  const TActorId& pipeClient, const TActorId& sender);
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
    THashMap<ui32, TPartitionInfo> Partitions;
    THashMap<TString, TIntrusivePtr<TEvTabletCounters::TInFlightCookie>> CounterEventsInflight;

    TActorId CacheActor;

    TSet<TChangeNotification> ChangeConfigNotification;
    NKikimrPQ::TPQTabletConfig NewConfig;
    bool NewConfigShouldBeApplied;
    size_t ChangePartitionConfigInflight = 0;

    TString TopicName;
    TString TopicPath;
    NPersQueue::TConverterFactoryPtr TopicConverterFactory;
    NPersQueue::TTopicConverterPtr TopicConverter;
    bool IsLocalDC;
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

    struct TPipeInfo {
        TActorId PartActor;
        TString Owner;
        ui32 ServerActors;
    };

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
    ui64 LastStep = 0;
    ui64 LastTxId = 0;

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
                                     const TActorContext& ctx);

    void Handle(TEvPQ::TEvProposePartitionConfigResult::TPtr& ev, const TActorContext& ctx);
    void HandleDataTransaction(TAutoPtr<TEvPersQueue::TEvProposeTransaction> event,
                               const TActorContext& ctx);
    void HandleConfigTransaction(TAutoPtr<TEvPersQueue::TEvProposeTransaction> event,
                                 const TActorContext& ctx);

    void SendEvProposePartitionConfig(const TActorContext& ctx,
                                      TDistributedTransaction& tx);

    TPartition* CreatePartitionActor(ui32 partitionId,
                                     const NPersQueue::TTopicConverterPtr topicConverter,
                                     const NKikimrPQ::TPQTabletConfig& config,
                                     bool newPartition,
                                     const TActorContext& ctx);
    void CreateNewPartitions(NKikimrPQ::TPQTabletConfig& config,
                             NPersQueue::TTopicConverterPtr topicConverter,
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

    TMaybe<NKikimrSubDomains::TProcessingParams> ProcessingParams;

    void Handle(TEvPersQueue::TEvProposeTransactionAttach::TPtr& ev, const TActorContext& ctx);
};


}// NPQ
}// NKikimr
