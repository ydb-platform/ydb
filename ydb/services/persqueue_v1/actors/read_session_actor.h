#pragma once

#include "events.h"
#include "persqueue_utils.h"

#include "partition_actor.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/containers/disjoint_interval_tree/disjoint_interval_tree.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/persqueue/events/global.h>

#include <util/generic/guid.h>
#include <util/system/compiler.h>


namespace NKikimr::NGRpcProxy::V1 {

inline TActorId GetPQReadServiceActorID() {
    return TActorId(0, "PQReadSvc");
}

class TReadSessionActor : public TActorBootstrapped<TReadSessionActor> {
using IContext = NGRpcServer::IGRpcStreamingContext<PersQueue::V1::MigrationStreamingReadClientMessage, PersQueue::V1::MigrationStreamingReadServerMessage>;
public:
     TReadSessionActor(NKikimr::NGRpcService::TEvStreamPQReadRequest* request, const ui64 cookie,
                       const NActors::TActorId& schemeCache, const NActors::TActorId& newSchemeCache,
                       TIntrusivePtr<NMonitoring::TDynamicCounters> counters, const TMaybe<TString> clientDC,
                       const NPersQueue::TTopicsListController& topicsHandler);
    ~TReadSessionActor();

    void Bootstrap(const NActors::TActorContext& ctx);

    void Die(const NActors::TActorContext& ctx) override;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::FRONT_PQ_READ; }


private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleWakeup)

            HFunc(IContext::TEvReadFinished, Handle);
            HFunc(IContext::TEvWriteFinished, Handle);
            CFunc(IContext::TEvNotifiedWhenDone::EventType, HandleDone);
            HFunc(NGRpcService::TGRpcRequestProxy::TEvRefreshTokenResponse, Handle);

            HFunc(TEvPQProxy::TEvAuthResultOk, Handle); // form auth actor

            HFunc(TEvPQProxy::TEvDieCommand, HandlePoison)
            HFunc(TEvPQProxy::TEvReadInit,  Handle) //from gRPC
            HFunc(TEvPQProxy::TEvReadSessionStatus, Handle) // from read sessions info builder proxy
            HFunc(TEvPQProxy::TEvRead, Handle) //from gRPC
            HFunc(TEvPQProxy::TEvDone, Handle) //from gRPC
            HFunc(TEvPQProxy::TEvCloseSession, Handle) //from partitionActor
            HFunc(TEvPQProxy::TEvPartitionReady, Handle) //from partitionActor
            HFunc(TEvPQProxy::TEvPartitionReleased, Handle) //from partitionActor

            HFunc(TEvPQProxy::TEvReadResponse, Handle) //from partitionActor
            HFunc(TEvPQProxy::TEvCommitCookie, Handle) //from gRPC
            HFunc(TEvPQProxy::TEvCommitRange, Handle) //from gRPC
            HFunc(TEvPQProxy::TEvStartRead, Handle) //from gRPC
            HFunc(TEvPQProxy::TEvReleased, Handle) //from gRPC
            HFunc(TEvPQProxy::TEvGetStatus, Handle) //from gRPC
            HFunc(TEvPQProxy::TEvAuth, Handle) //from gRPC

            HFunc(TEvPQProxy::TEvCommitDone, Handle) //from PartitionActor
            HFunc(TEvPQProxy::TEvPartitionStatus, Handle) //from partitionActor

            HFunc(TEvPersQueue::TEvLockPartition, Handle) //from Balancer
            HFunc(TEvPersQueue::TEvReleasePartition, Handle) //from Balancer
            HFunc(TEvPersQueue::TEvError, Handle) //from Balancer

            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);

        default:
            break;
        };
    }

    bool WriteResponse(PersQueue::V1::MigrationStreamingReadServerMessage&& response, bool finish = false);

    void Handle(IContext::TEvReadFinished::TPtr& ev, const TActorContext &ctx);
    void Handle(IContext::TEvWriteFinished::TPtr& ev, const TActorContext &ctx);
    void HandleDone(const TActorContext &ctx);

    void Handle(NGRpcService::TGRpcRequestProxy::TEvRefreshTokenResponse::TPtr& ev, const TActorContext &ctx);


    void Handle(TEvPQProxy::TEvReadInit::TPtr& ev,  const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvReadSessionStatus::TPtr& ev,  const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvRead::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvReadResponse::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvDone::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvCloseSession::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvPartitionReady::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvPartitionReleased::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvCommitCookie::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvCommitRange::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvStartRead::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvReleased::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvGetStatus::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvAuth::TPtr& ev, const NActors::TActorContext& ctx);
    void ProcessAuth(const TString& auth, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvCommitDone::TPtr& ev, const NActors::TActorContext& ctx);

    void Handle(TEvPQProxy::TEvPartitionStatus::TPtr& ev, const NActors::TActorContext& ctx);

    void Handle(TEvPersQueue::TEvLockPartition::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPersQueue::TEvReleasePartition::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPersQueue::TEvError::TPtr& ev, const NActors::TActorContext& ctx);

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const NActors::TActorContext& ctx);
    [[nodiscard]] bool ProcessBalancerDead(const ui64 tabletId, const NActors::TActorContext& ctx); // returns false if actor died

    void HandlePoison(TEvPQProxy::TEvDieCommand::TPtr& ev, const NActors::TActorContext& ctx);
    void HandleWakeup(const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvAuthResultOk::TPtr& ev, const NActors::TActorContext& ctx);

    void CloseSession(const TString& errorReason, const PersQueue::ErrorCode::ErrorCode errorCode,
                      const NActors::TActorContext& ctx);

    void SetupCounters();
    void SetupTopicCounters(const NPersQueue::TTopicConverterPtr& topic);
    void SetupTopicCounters(const NPersQueue::TTopicConverterPtr& topic, const TString& cloudId, const TString& dbId,
                            const TString& folderId);

    void ProcessReads(const NActors::TActorContext& ctx); // returns false if actor died
    struct TFormedReadResponse;
    void ProcessAnswer(const NActors::TActorContext& ctx, TIntrusivePtr<TFormedReadResponse> formedResponse); // returns false if actor died

    void RegisterSessions(const NActors::TActorContext& ctx);
    void RegisterSession(const TActorId& pipe, const TString& topic, const TVector<ui32>& groups, const TActorContext& ctx);

    struct TPartitionActorInfo;
    void DropPartition(THashMap<ui64, TPartitionActorInfo>::iterator it, const TActorContext& ctx);

    bool ActualPartitionActor(const TActorId& part);
    void ReleasePartition(const THashMap<ui64, TPartitionActorInfo>::iterator& it,
                        bool couldBeReads, const TActorContext& ctx); // returns false if actor died

    void SendReleaseSignalToClient(const THashMap<ui64, TPartitionActorInfo>::iterator& it, bool kill, const TActorContext& ctx);

    void InformBalancerAboutRelease(const THashMap<ui64, TPartitionActorInfo>::iterator& it, const TActorContext& ctx);

    static ui32 NormalizeMaxReadMessagesCount(ui32 sourceValue);
    static ui32 NormalizeMaxReadSize(ui32 sourceValue);

private:
    std::unique_ptr<NKikimr::NGRpcService::TEvStreamPQReadRequest> Request;

    const TString ClientDC;

    const TInstant StartTimestamp;

    TActorId SchemeCache;
    TActorId NewSchemeCache;

    TActorId AuthInitActor;
    TIntrusivePtr<NACLib::TUserToken> Token;

    TString ClientId;
    TString ClientPath;
    TString Session;
    TString PeerName;

    bool CommitsDisabled;
    bool BalancersInitStarted;

    bool InitDone;
    bool RangesMode = false;

    ui32 MaxReadMessagesCount;
    ui32 MaxReadSize;
    ui32 MaxTimeLagMs;
    ui64 ReadTimestampMs;

    TString Auth;

    bool ForceACLCheck;
    bool RequestNotChecked;
    TInstant LastACLCheckTimestamp;

    struct TPartitionActorInfo {
        TActorId Actor;
        const TPartitionId Partition;
        std::deque<ui64> Commits;
        bool Reading;
        bool Releasing;
        bool Released;
        bool LockSent;
        bool ReleaseSent;

        ui64 ReadIdToResponse;
        ui64 ReadIdCommitted;
        TSet<ui64> NextCommits;
        TDisjointIntervalTree<ui64> NextRanges;

        ui64 Offset;

        TInstant AssignTimestamp;

	    NPersQueue::TTopicConverterPtr Topic;

        TPartitionActorInfo(const TActorId& actor, const TPartitionId& partition,
                            const NPersQueue::TTopicConverterPtr& topic, const TActorContext& ctx)
            : Actor(actor)
            , Partition(partition)
            , Reading(false)
            , Releasing(false)
            , Released(false)
            , LockSent(false)
            , ReleaseSent(false)
            , ReadIdToResponse(1)
            , ReadIdCommitted(0)
            , Offset(0)
            , AssignTimestamp(ctx.Now())
            , Topic(topic)
        { }

        void MakeCommit(const TActorContext& ctx);
    };


    THashSet<TActorId> ActualPartitionActors;
    THashMap<ui64, std::pair<ui32, ui64>> BalancerGeneration;
    ui64 NextAssignId;
    THashMap<ui64, TPartitionActorInfo> Partitions; //assignId -> info

    THashMap<TString, TTopicHolder> Topics; // topic -> info
    THashMap<TString, NPersQueue::TTopicConverterPtr> FullPathToConverter; // PrimaryFullPath -> Converter, for balancer replies matching
    THashSet<TString> TopicsToResolve;
    THashMap<TString, TVector<ui32>> TopicGroups;
    THashMap<TString, ui64> ReadFromTimestamp;

    bool ReadOnlyLocal;
    TDuration CommitInterval;

    struct TPartitionInfo {
        ui64 AssignId;
        ui64 WTime;
        ui64 SizeLag;
        ui64 MsgLag;
        bool operator < (const TPartitionInfo& rhs) const {
            return std::tie(WTime, AssignId) < std::tie(rhs.WTime, rhs.AssignId);
        }
    };

    TSet<TPartitionInfo> AvailablePartitions;

    struct TFormedReadResponse: public TSimpleRefCount<TFormedReadResponse> {
        using TPtr = TIntrusivePtr<TFormedReadResponse>;

        TFormedReadResponse(const TString& guid, const TInstant start)
            : Guid(guid)
            , Start(start)
            , FromDisk(false)
        {
        }

        PersQueue::V1::MigrationStreamingReadServerMessage Response;
        ui32 RequestsInfly = 0;
        i64 ByteSize = 0;
        ui64 RequestedBytes = 0;

        //returns byteSize diff
        i64 ApplyResponse(PersQueue::V1::MigrationStreamingReadServerMessage&& resp);

        THashSet<TActorId> PartitionsTookPartInRead;
        TSet<TPartitionId> PartitionsTookPartInControlMessages;

        TSet<TPartitionInfo> PartitionsBecameAvailable; // Partitions that became available during this read request execution.

                                                        // These partitions are bringed back to AvailablePartitions after reply to this read request.

        const TString Guid;
        TInstant Start;
        bool FromDisk;
        TDuration WaitQuotaTime;
    };

    THashMap<TActorId, TFormedReadResponse::TPtr> PartitionToReadResponse; // Partition actor -> TFormedReadResponse answer that has this partition.
                                                                           // PartitionsTookPartInRead in formed read response contain this actor id.

    struct TControlMessages {
        TVector<PersQueue::V1::MigrationStreamingReadServerMessage> ControlMessages;
        ui32 Infly = 0;
    };

    TMap<TPartitionId, TControlMessages> PartitionToControlMessages;


    std::deque<THolder<TEvPQProxy::TEvRead>> Reads;

    ui64 Cookie;

    struct TCommitInfo {
        ui64 StartReadId;
        ui32 Partitions;
    };

    TMap<ui64, TCommitInfo> Commits; //readid->TCommitInfo

    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;

    NMonitoring::TDynamicCounters::TCounterPtr SessionsCreated;
    NMonitoring::TDynamicCounters::TCounterPtr SessionsActive;

    NMonitoring::TDynamicCounters::TCounterPtr Errors;
    NMonitoring::TDynamicCounters::TCounterPtr PipeReconnects;
    NMonitoring::TDynamicCounters::TCounterPtr BytesInflight;
    ui64 BytesInflight_;
    ui64 RequestedBytes;
    ui32 ReadsInfly;
    std::queue<ui64> ActiveWrites;

    NKikimr::NPQ::TPercentileCounter PartsPerSession;

    THashMap<TString, TTopicCounters> TopicCounters;
    THashMap<TString, ui32> NumPartitionsFromTopic;

    TVector<NPersQueue::TPQLabelsInfo> Aggr;
    NKikimr::NPQ::TMultiCounter SLITotal;
    NKikimr::NPQ::TMultiCounter SLIErrors;
    TInstant StartTime;
    NKikimr::NPQ::TPercentileCounter InitLatency;
    NKikimr::NPQ::TPercentileCounter ReadLatency;
    NKikimr::NPQ::TPercentileCounter ReadLatencyFromDisk;
    NKikimr::NPQ::TPercentileCounter CommitLatency;
    NKikimr::NPQ::TMultiCounter SLIBigLatency;
    NKikimr::NPQ::TMultiCounter SLIBigReadLatency;
    NKikimr::NPQ::TMultiCounter ReadsTotal;

    NPersQueue::TTopicsListController TopicsHandler;
    NPersQueue::TTopicsToConverter TopicsList;
};

}
