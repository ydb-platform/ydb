#pragma once

#include "actors.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/fetch_request_actor.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/services/persqueue_v1/actors/read_init_auth_actor.h>


namespace NKafka {
    using namespace NKikimr;

    /*
     * Pipeline:
     *
     * client                                 server
     *           JOIN_GROUP request(topics)
     *           ---------------->
     *           JOIN_GROUP response()
     *           <----------------
     *
     *           SYNC_GROUP request()
     *           ---------------->
     *           SYNC_GROUP response(partitions to read)
     *           <----------------
     *
     *           HEARTBEAT request()
     *           ---------------->
     *           HEARTBEAT response(status = OK)
     *           <---------------- 
     *
     *           HEARTBEAT request()
     *           ---------------->
     *           HEARTBEAT response(status = REBALANCE_IN_PROGRESS) //if partitions to read list changes
     *           <---------------- 
     *
     *           JOIN_GROUP request(topics) //client send again, because REBALANCE_IN_PROGRESS in heartbeat response
     *           ---------------->
     *
     *           ...
     *           ...
     *           ...
     *
     *           LEAVE_GROUP request()
     *           ---------------->
     *           LEAVE_GROUP response()
     *           <----------------   
     */

class TKafkaReadSessionActor: public NActors::TActorBootstrapped<TKafkaReadSessionActor> {

enum EReadSessionSteps {
    WAIT_JOIN_GROUP,
    WAIT_SYNC_GROUP,
    READING
};

struct TPartitionsInfo {
    THashSet<ui64> ReadingNow;
    THashSet<ui64> ToRelease;
    THashSet<ui64> ToLock;
};

struct TNewPartitionToLockInfo {
    ui64 PartitionId;
    TInstant LockOn;
};

struct TNextRequestError {
    EKafkaErrors Code = EKafkaErrors::NONE_ERROR;
    TString Message = "";
};

public:
    using TBase = NActors::TActorBootstrapped<TKafkaReadSessionActor>;
    TKafkaReadSessionActor(const TContext::TPtr context, ui64 cookie)
        : Context(context),
          Cookie(cookie)
    {}

    void Bootstrap(const NActors::TActorContext& ctx);

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::KAFKA_READ_SESSION_ACTOR; }

private:
    using TActorContext = NActors::TActorContext;

    TString LogPrefix();

    void Die(const TActorContext& ctx) override;

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {

            // from client
            HFunc(TEvKafka::TEvJoinGroupRequest, HandleJoinGroup);
            HFunc(TEvKafka::TEvSyncGroupRequest, HandleSyncGroup);
            HFunc(TEvKafka::TEvLeaveGroupRequest, HandleLeaveGroup);
            HFunc(TEvKafka::TEvHeartbeatRequest, HandleHeartbeat);

            // from TReadInitAndAuthActor
            HFunc(NGRpcProxy::V1::TEvPQProxy::TEvAuthResultOk, HandleAuthOk);
            HFunc(NGRpcProxy::V1::TEvPQProxy::TEvCloseSession, HandleAuthCloseSession);

            // from PQRB
            HFunc(TEvPersQueue::TEvLockPartition, HandleLockPartition);
            HFunc(TEvPersQueue::TEvReleasePartition, HandleReleasePartition);
            HFunc(TEvPersQueue::TEvError, HandleBalancerError);
            
            // from Pipe
            HFunc(TEvTabletPipe::TEvClientConnected, HandlePipeConnected);
            HFunc(TEvTabletPipe::TEvClientDestroyed, HandlePipeDestroyed);
            
            // others
            HFunc(TEvKafka::TEvWakeup, HandleWakeup);
            SFunc(TEvents::TEvPoison, Die);
        }
    }

    void HandleJoinGroup(TEvKafka::TEvJoinGroupRequest::TPtr ev, const TActorContext& ctx);
    void HandleSyncGroup(TEvKafka::TEvSyncGroupRequest::TPtr ev, const TActorContext& ctx);
    void HandleLeaveGroup(TEvKafka::TEvLeaveGroupRequest::TPtr ev, const TActorContext& ctx);
    void HandleHeartbeat(TEvKafka::TEvHeartbeatRequest::TPtr ev, const TActorContext& ctx);
    void HandlePipeConnected(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext&);
    void HandlePipeDestroyed(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx);
    void HandleLockPartition(TEvPersQueue::TEvLockPartition::TPtr& ev, const TActorContext&);
    void HandleReleasePartition(TEvPersQueue::TEvReleasePartition::TPtr& ev, const TActorContext&);
    void HandleBalancerError(TEvPersQueue::TEvError::TPtr& ev, const TActorContext&);
    void HandleWakeup(TEvKafka::TEvWakeup::TPtr, const TActorContext& ctx);
    void HandleAuthOk(NGRpcProxy::V1::TEvPQProxy::TEvAuthResultOk::TPtr& ev, const TActorContext& ctx);
    void HandleAuthCloseSession(NGRpcProxy::V1::TEvPQProxy::TEvCloseSession::TPtr& ev, const TActorContext& ctx);

    void SendJoinGroupResponseOk(const TActorContext&, ui64 corellationId);
    void SendJoinGroupResponseFail(const TActorContext&, ui64 corellationId, EKafkaErrors error, TString message = "");
    void SendSyncGroupResponseOk(const TActorContext& ctx, ui64 corellationId);
    void SendSyncGroupResponseFail(const TActorContext&, ui64 corellationId, EKafkaErrors error, TString message = "");
    void SendHeartbeatResponseOk(const TActorContext&, ui64 corellationId, EKafkaErrors error);
    void SendHeartbeatResponseFail(const TActorContext&, ui64 corellationId, EKafkaErrors error, TString message = "");
    void SendLeaveGroupResponseOk(const TActorContext& ctx, ui64 corellationId);
    void SendLeaveGroupResponseFail(const TActorContext&, ui64 corellationId, EKafkaErrors error, TString message = "");
    void CloseReadSession(const TActorContext& ctx);

    void AuthAndFindBalancers(const TActorContext& ctx);
    void RegisterBalancerSession(const TString& topic, const TActorId& pipe, const TVector<ui32>& groups, const TActorContext& ctx);
    TConsumerProtocolAssignment BuildAssignmentAndInformBalancerIfRelease(const TActorContext& ctx);
    void InformBalancerAboutPartitionRelease(const TString& topic, ui64 partition, const TActorContext& ctx);
    void ProcessBalancerDead(ui64 tabletId, const TActorContext& ctx);
    TActorId CreatePipeClient(ui64 tabletId, const TActorContext& ctx);
    bool CheckHeartbeatIsExpired();
    bool CheckTopicsListAreChanged(const TMessagePtr<TJoinGroupRequestData> joinGroupRequestData);
    bool TryFillTopicsToRead(const TMessagePtr<TJoinGroupRequestData> joinGroupRequestData, THashSet<TString>& topics);
    void FillTopicsFromJoinGroupMetadata(TKafkaBytes& metadata, THashSet<TString>& topics);

private:
    const TContext::TPtr Context;
    TString GroupId;
    TString GroupName;
    TString Session;
    TString AssignProtocolName;
    i64 GenerationId = 0;
    ui64 JoinGroupCorellationId = 0;
    ui64 Cookie;
    bool NeedRebalance = false;
    TInstant LastHeartbeatTime = TInstant::Now();
    TDuration MaxHeartbeatTimeoutMs = TDuration::Seconds(10);
    EReadSessionSteps ReadStep = EReadSessionSteps::WAIT_JOIN_GROUP;
    TNextRequestError NextRequestError;

    THashMap<TString, NGRpcProxy::TTopicHolder> TopicsInfo; // topic -> info
    NPersQueue::TTopicsToConverter TopicsToConverter;
    THashSet<TString> TopicsToReadNames;
    THashMap<TString, TString> OriginalTopicNames;
    THashMap<TString, TPartitionsInfo> TopicPartitions;
    THashMap<TString, NPersQueue::TTopicConverterPtr> FullPathToConverter; // PrimaryFullPath -> Converter, for balancer replies matching
    THashMap<TString, TVector<TNewPartitionToLockInfo>> NewPartitionsToLockOnTime; // Topic -> PartitionsToLock

    static constexpr NTabletPipe::TClientRetryPolicy RetryPolicyForPipes = {
        .RetryLimitCount = 21,
        .MinRetryTime = TDuration::MilliSeconds(10),
        .MaxRetryTime = TDuration::Seconds(5),
        .BackoffMultiplier = 2,
        .DoFirstRetryInstantly = true
    };
};

} // namespace NKafka
