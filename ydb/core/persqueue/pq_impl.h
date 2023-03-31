#pragma once

#include "percentile_counter.h"
#include "metering_sink.h"
#include <ydb/core/keyvalue/keyvalue_flat_impl.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/persqueue/events/internal.h>

#include <library/cpp/actors/interconnect/interconnect.h>

namespace NKikimr {
namespace NPQ {

struct TPartitionInfo;
struct TChangeNotification;

class TResponseBuilder;

//USES MAIN chanel for big blobs, INLINE or EXTRA for ZK-like load, EXTRA2 for small blob for logging (VDISK of type LOG is ok with EXTRA2)

class TPersQueue : public NKeyValue::TKeyValueFlat {
    enum ECookie : ui64 {
        WRITE_CONFIG_COOKIE = 2,
        READ_CONFIG_COOKIE  = 3,
        WRITE_STATE_COOKIE  = 4
    };

    void CreatedHook(const TActorContext& ctx) override;
    bool HandleHook(STFUNC_SIG) override;

//    void ReplyError(const TActorContext& ctx, const TActorId& dst, NPersQueue::NErrorCode::EErrorCode errorCode, const TString& error);
    void ReplyError(const TActorContext& ctx, const ui64 responseCookie, NPersQueue::NErrorCode::EErrorCode errorCode, const TString& error);

    void HandleWakeup(const TActorContext&);

    void Handle(TEvPersQueue::TEvProposeTransaction::TPtr& ev, const TActorContext&);

    void InitResponseBuilder(const ui64 responseCookie, const ui32 count, const ui32 counterId);
    void Handle(TEvPQ::TEvError::TPtr& ev, const TActorContext&);
    void Handle(TEvPQ::TEvProxyResponse::TPtr& ev, const TActorContext&);
    void FinishResponse(THashMap<ui64, TAutoPtr<TResponseBuilder>>::iterator it);

    void Handle(TEvInterconnect::TEvNodeInfo::TPtr& ev, const TActorContext&);

    void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev, const TActorContext&);
    void Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev, const TActorContext&);

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
    void HandleConfigWriteResponse(const NKikimrClient::TResponse& resp, const TActorContext& ctx);
    void HandleConfigReadResponse(const NKikimrClient::TResponse& resp, const TActorContext& ctx);
    void ApplyNewConfigAndReply(const TActorContext& ctx);
    void HandleStateWriteResponse(const NKikimrClient::TResponse& resp, const TActorContext& ctx);

    void ReadConfig(const NKikimrClient::TKeyValueResponse::TReadResult& read, const TActorContext& ctx);
    void ReadState(const NKikimrClient::TKeyValueResponse::TReadResult& read, const TActorContext& ctx);

    void InitializeMeteringSink(const TActorContext& ctx);

    TMaybe<TEvPQ::TEvRegisterMessageGroup::TBody> MakeRegisterMessageGroup(
        const NKikimrClient::TPersQueuePartitionRequest::TCmdRegisterMessageGroup& cmd,
        NPersQueue::NErrorCode::EErrorCode& code, TString& error) const;

    TMaybe<TEvPQ::TEvDeregisterMessageGroup::TBody> MakeDeregisterMessageGroup(
        const NKikimrClient::TPersQueuePartitionRequest::TCmdDeregisterMessageGroup& cmd,
        NPersQueue::NErrorCode::EErrorCode& code, TString& error) const;

    void OnAllPartitionConfigChanged(const TActorContext& ctx);

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
    void ReturnTabletStateAll(const TActorContext& ctx, NKikimrProto::EReplyStatus status = NKikimrProto::OK);
    void ReturnTabletState(const TActorContext& ctx, const TChangeNotification& req, NKikimrProto::EReplyStatus status);

    static constexpr const char * KeyConfig() { return "_config"; }
    static constexpr const char * KeyState() { return "_state"; }

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

    bool SubDomainOutOfSpace = false;
};


}// NPQ
}// NKikimr
