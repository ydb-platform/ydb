#pragma once

#include "actors.h"
#include "kqp_balance_transaction.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/fetch_request_actor.h>
#include <ydb/core/protos/kafka.pb.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/services/metadata/service.h>
#include <ydb/services/persqueue_v1/actors/read_init_auth_actor.h>
#include <util/datetime/base.h>

namespace NKafka {
using namespace NKikimr;

extern const TString INSERT_NEW_GROUP;
extern const TString UPDATE_GROUP;
extern const TString UPDATE_GROUP_STATE_AND_PROTOCOL;
extern const TString INSERT_MEMBER;
extern const TString UPDATE_GROUPS_AND_SELECT_WORKER_STATES;
extern const TString SELECT_WORKER_STATE_QUERY;
extern const TString SELECT_MASTER;
extern const TString UPSERT_ASSIGNMENTS_AND_SET_WORKING_STATE;
extern const TString CHECK_GROUP_STATE;
extern const TString FETCH_ASSIGNMENTS;
extern const TString CHECK_DEAD_MEMBERS;
extern const TString UPDATE_TTLS;
extern const TString UPDATE_TTL_LEAVE_GROUP;

class TKafkaBalancerActor : public NActors::TActorBootstrapped<TKafkaBalancerActor> {
public:
    using TBase = NActors::TActorBootstrapped<TKafkaBalancerActor>;

    enum EBalancerStep : ui32 {
        STEP_NONE = 0,

        JOIN_TX0_0_BEGIN_TX,
        JOIN_TX0_1_CHECK_STATE_AND_GENERATION,
        JOIN_TX0_2_INSERT_NEW_GROUP,
        JOIN_TX0_2_UPDATE_GROUP_STATE_AND_GENERATION,
        JOIN_TX0_2_SKIP,
        JOIN_TX0_3_INSERT_MEMBER,
        JOIN_TX0_4_COMMIT_TX,
        JOIN_TX0_5_WAIT,

        JOIN_TX1_0_BEGIN_TX,
        JOIN_TX1_1_CHECK_STATE_AND_GENERATION,
        JOIN_TX1_2_GET_MEMBERS_AND_SET_STATE_SYNC,
        JOIN_TX1_3_COMMIT_TX,

        SYNC_TX0_0_BEGIN_TX,
        SYNC_TX0_1_SELECT_MASTER,
        SYNC_TX0_2_CHECK_STATE_AND_GENERATION,
        SYNC_TX0_3_SET_ASSIGNMENTS_AND_SET_WORKING_STATE,
        SYNC_TX0_4_COMMIT_TX,

        SYNC_TX1_0_BEGIN_TX,
        SYNC_TX1_1_CHECK_STATE,
        SYNC_TX1_2_FETCH_ASSIGNMENTS,
        SYNC_TX1_3_COMMIT_TX,

        HEARTBEAT_TX0_0_BEGIN_TX,
        HEARTBEAT_TX0_1_CHECK_DEAD_MEMBERS,
        HEARTBEAT_TX0_2_COMMIT_TX,

        HEARTBEAT_TX1_0_BEGIN_TX,
        HEARTBEAT_TX1_1_CHECK_GEN_AND_STATE,
        HEARTBEAT_TX1_2_UPDATE_TTL,
        HEARTBEAT_TX1_3_COMMIT_TX,

        LEAVE_TX0_0_BEGIN_TX,
        LEAVE_TX0_1_UPDATE_TTL,
        LEAVE_TX0_2_COMMIT_TX,
    };

    TKafkaBalancerActor(const TContext::TPtr context, ui64 cookie, ui64 corellationId, TMessagePtr<TJoinGroupRequestData> message)
        : Context(context)
        , CorrelationId(corellationId)
        , Cookie(cookie)
        , JoinGroupRequestData(message)
        , SyncGroupRequestData(
            std::shared_ptr<TBuffer>(),
        std::shared_ptr<TApiMessage>())
        , HeartbeatGroupRequestData(
            std::shared_ptr<TBuffer>(),
        std::shared_ptr<TApiMessage>())
        , LeaveGroupRequestData(
            std::shared_ptr<TBuffer>(),
        std::shared_ptr<TApiMessage>())
    {
        KAFKA_LOG_D("HandleJoinGroup request");

        RequestType   = JOIN_GROUP;
        CurrentStep   = STEP_NONE;

        GroupId  = JoinGroupRequestData->GroupId.value();
    }

    TKafkaBalancerActor(const TContext::TPtr context, ui64 cookie, ui64 corellationId, TMessagePtr<TSyncGroupRequestData> message)
        : Context(context)
        , CorrelationId(corellationId)
        , Cookie(cookie)
        , JoinGroupRequestData(
            std::shared_ptr<TBuffer>(),
        std::shared_ptr<TApiMessage>())
        , SyncGroupRequestData(message)
        , HeartbeatGroupRequestData(
            std::shared_ptr<TBuffer>(),
        std::shared_ptr<TApiMessage>())
        , LeaveGroupRequestData(
            std::shared_ptr<TBuffer>(),
        std::shared_ptr<TApiMessage>())
    {
        KAFKA_LOG_D("HandleSyncGroup request");

        RequestType   = SYNC_GROUP;
        CurrentStep   = STEP_NONE;

        GroupId  = SyncGroupRequestData->GroupId.value();
        MemberId = SyncGroupRequestData->MemberId.value();
        GenerationId = SyncGroupRequestData->GenerationId;
    }

    TKafkaBalancerActor(const TContext::TPtr context, ui64 cookie, ui64 corellationId, TMessagePtr<THeartbeatRequestData> message)
        : Context(context)
        , CorrelationId(corellationId)
        , Cookie(cookie)
        , JoinGroupRequestData(
            std::shared_ptr<TBuffer>(),
        std::shared_ptr<TApiMessage>())
        , SyncGroupRequestData(
            std::shared_ptr<TBuffer>(),
        std::shared_ptr<TApiMessage>())
        , HeartbeatGroupRequestData(message)
        , LeaveGroupRequestData(
            std::shared_ptr<TBuffer>(),
        std::shared_ptr<TApiMessage>())
    {
        KAFKA_LOG_D("HandleHeartbeat request");

        RequestType   = HEARTBEAT;
        CurrentStep   = STEP_NONE;

        GroupId  = HeartbeatGroupRequestData->GroupId.value();
        MemberId = HeartbeatGroupRequestData->MemberId.value();
        GenerationId = HeartbeatGroupRequestData->GenerationId;
    }

    TKafkaBalancerActor(const TContext::TPtr context, ui64 cookie, ui64 corellationId, TMessagePtr<TLeaveGroupRequestData> message)
        : Context(context)
        , CorrelationId(corellationId)
        , Cookie(cookie)
        , JoinGroupRequestData(
            std::shared_ptr<TBuffer>(),
        std::shared_ptr<TApiMessage>())
        , SyncGroupRequestData(
            std::shared_ptr<TBuffer>(),
        std::shared_ptr<TApiMessage>())
        , HeartbeatGroupRequestData(
            std::shared_ptr<TBuffer>(),
        std::shared_ptr<TApiMessage>())
        , LeaveGroupRequestData(message)
    {
        KAFKA_LOG_D("HandleLeaveGroup request");
        RequestType   = LEAVE_GROUP;
        CurrentStep   = STEP_NONE;

        GroupId  = LeaveGroupRequestData->GroupId.value();
        MemberId = LeaveGroupRequestData->MemberId.value();
    }

    void Bootstrap(const NActors::TActorContext& ctx);

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KAFKA_READ_SESSION_ACTOR;
    }

private:
    using TActorContext = NActors::TActorContext;

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NMetadata::NProvider::TEvManagerPrepared, Handle);
            HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle);
            HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);

            HFunc(TEvents::TEvWakeup, Handle);
            SFunc(TEvents::TEvPoison, Die);
        }
    }

    void Handle(NMetadata::NProvider::TEvManagerPrepared::TPtr&, const TActorContext& ctx);
    void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx);

    void HandleJoinGroupResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void HandleSyncGroupResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void HandleLeaveGroupResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void HandleHeartbeatResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);

    void Die(const TActorContext& ctx) override;

    void SendJoinGroupResponseOk(const TActorContext&, ui64 corellationId);
    void SendJoinGroupResponseFail(const TActorContext&, ui64 corellationId,
                                   EKafkaErrors error, TString message = "");
    void SendSyncGroupResponseOk(const TActorContext& ctx, ui64 corellationId);
    void SendSyncGroupResponseFail(const TActorContext&, ui64 corellationId,
                                   EKafkaErrors error, TString message = "");
    void SendHeartbeatResponseOk(const TActorContext&, ui64 corellationId, EKafkaErrors error);
    void SendHeartbeatResponseFail(const TActorContext&, ui64 corellationId,
                                   EKafkaErrors error, TString message = "");
    void SendLeaveGroupResponseOk(const TActorContext& ctx, ui64 corellationId);
    void SendLeaveGroupResponseFail(const TActorContext&, ui64 corellationId,
                                    EKafkaErrors error, TString message = "");

    bool ParseCheckStateAndGeneration(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, bool& outGroupExists, ui64& outGeneration, ui64& outState, TString& outMasterId, TInstant& outTtl);
    bool ParseAssignments(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, TString& assignments);
    bool ParseWorkerStatesAndChooseProtocol(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, std::unordered_map<TString, TString>& workerStates, TString& chosenProtocol);
    bool ParseDeadCount(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, ui64& outCount);

private:
    enum EGroupState : ui32 {
        GROUP_STATE_JOIN = 0,
        GROUP_STATE_JOINED,
        GROUP_STATE_SYNC,
        GROUP_STATE_WORKING
    };

private:
    const TContext::TPtr Context;
    NKafka::EApiKey RequestType;

    ui8 TablesInited = 0;
    TString GroupId;
    TString GroupName;
    TString MemberId;
    TString AssignProtocolName;
    ui64 GenerationId = 0;
    ui64 CorrelationId = 0;
    ui64 Cookie = 0;
    ui64 KqpReqCookie = 0;
    ui8 WaitingWorkingStateRetries = 0;

    TString Assignments;
    std::unordered_map<TString, TString> WorkerStates;
    TString Protocol;
    TString Master;

    bool IsMaster = false;

    EBalancerStep CurrentStep = STEP_NONE;

    std::unique_ptr<NKikimr::NGRpcProxy::V1::TKqpTxHelper> Kqp;

    TMessagePtr<TJoinGroupRequestData> JoinGroupRequestData;
    TMessagePtr<TSyncGroupRequestData> SyncGroupRequestData;
    TMessagePtr<THeartbeatRequestData> HeartbeatGroupRequestData;
    TMessagePtr<TLeaveGroupRequestData> LeaveGroupRequestData;

};

} // namespace NKafka
