#pragma once

#include "actors.h"
#include "kafka_consumer_groups_metadata_initializers.h"
#include "kafka_consumer_members_metadata_initializers.h"
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

constexpr ui32 DEFAULT_REBALANCE_TIMEOUT_MS = 45000;
constexpr ui32 MIN_REBALANCE_TIMEOUT_MS = 3000;
constexpr ui32 MAX_REBALANCE_TIMEOUT_MS = 300000;

constexpr ui32 DEFAULT_SESSION_TIMEOUT_MS = 45000;
constexpr ui32 MIN_SESSION_TIMEOUT_MS = 3000;
constexpr ui32 MAX_SESSION_TIMEOUT_MS = 300000;

constexpr ui32 MAX_GROUPS_COUNT = 1000;
constexpr ui32 LIMIT_MEMBERS_PER_REQUEST = 999;

constexpr ui8 MASTER_WAIT_JOINS_DELAY_SECONDS = 5;
constexpr ui8 WAIT_FOR_MASTER_DELAY_SECONDS = 1;
constexpr ui8 TX_ABORT_MAX_RETRY_COUNT = 5;
constexpr ui8 TABLES_TO_INIT_COUNT = 2;

extern const TString INSERT_NEW_GROUP;
extern const TString UPDATE_GROUP;
extern const TString UPDATE_GROUP_STATE_AND_PROTOCOL;
extern const TString INSERT_MEMBER;
extern const TString SELECT_WORKER_STATES;
extern const TString SELECT_WORKER_STATE_QUERY;
extern const TString SELECT_MASTER;
extern const TString UPSERT_ASSIGNMENTS_AND_SET_WORKING_STATE;
extern const TString CHECK_GROUP_STATE;
extern const TString FETCH_ASSIGNMENTS;
extern const TString CHECK_DEAD_MEMBERS;
extern const TString UPDATE_LASTHEARTBEATS;
extern const TString UPDATE_LASTHEARTBEAT_TO_LEAVE_GROUP;
extern const TString CHECK_GROUPS_COUNT;

struct TGroupStatus {
    bool Exists;
    ui64 Generation;
    ui64 State;
    TString MasterId;
    TInstant LastHeartbeat;
    TString ProtocolName;
    TString ProtocolType;
    ui64 RebalanceTimeoutMs;
};

class TKafkaBalancerActor : public NActors::TActorBootstrapped<TKafkaBalancerActor> {
public:
    using TBase = NActors::TActorBootstrapped<TKafkaBalancerActor>;

    enum EBalancerStep : ui8 {
        STEP_NONE = 0,

        JOIN_TX0_0_BEGIN_TX,
        JOIN_TX0_1_CHECK_STATE_AND_GENERATION,
        JOIN_TX0_2_CHECK_GROUPS_COUNT,
        JOIN_TX0_2_ADD_GROUP_OR_UPDATE_EXISTS,
        JOIN_TX0_2_SKIP,
        JOIN_TX0_3_INSERT_MEMBER,
        JOIN_TX0_4_COMMIT_TX,
        JOIN_TX0_5_WAIT,

        JOIN_TX1_0_BEGIN_TX,
        JOIN_TX1_1_CHECK_STATE_AND_GENERATION,
        JOIN_TX1_2_GET_MEMBERS,
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

    TKafkaBalancerActor(const TContext::TPtr context, ui64 cookie, ui64 corellationId, TMessagePtr<TJoinGroupRequestData> message, ui8 retryNum = 0)
        : Context(context)
        , CorrelationId(corellationId)
        , Cookie(cookie)
        , CurrentRetryNumber(retryNum)
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

        GroupId  = JoinGroupRequestData->GroupId.value_or("");
        ProtocolType = JoinGroupRequestData->ProtocolType.value_or("");

        if (JoinGroupRequestData->SessionTimeoutMs) {
            SessionTimeoutMs = JoinGroupRequestData->SessionTimeoutMs;
        }

        if (JoinGroupRequestData->RebalanceTimeoutMs) {
            RebalanceTimeoutMs = JoinGroupRequestData->RebalanceTimeoutMs;
            WaitingMasterMaxRetryCount = JoinGroupRequestData->RebalanceTimeoutMs / WAIT_FOR_MASTER_DELAY_SECONDS * 1000;
        }
    }

    TKafkaBalancerActor(const TContext::TPtr context, ui64 cookie, ui64 corellationId, TMessagePtr<TSyncGroupRequestData> message, ui8 retryNum = 0)
        : Context(context)
        , CorrelationId(corellationId)
        , Cookie(cookie)
        , CurrentRetryNumber(retryNum)
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

        GroupId  = SyncGroupRequestData->GroupId.value_or("");
        MemberId = SyncGroupRequestData->MemberId.value_or("");
        GenerationId = SyncGroupRequestData->GenerationId;
    }

    TKafkaBalancerActor(const TContext::TPtr context, ui64 cookie, ui64 corellationId, TMessagePtr<THeartbeatRequestData> message, ui8 retryNum = 0)
        : Context(context)
        , CorrelationId(corellationId)
        , Cookie(cookie)
        , CurrentRetryNumber(retryNum)
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

        GroupId  = HeartbeatGroupRequestData->GroupId.value_or("");
        MemberId = HeartbeatGroupRequestData->MemberId.value_or("");
        GenerationId = HeartbeatGroupRequestData->GenerationId;
    }

    TKafkaBalancerActor(const TContext::TPtr context, ui64 cookie, ui64 corellationId, TMessagePtr<TLeaveGroupRequestData> message, ui8 retryNum = 0)
        : Context(context)
        , CorrelationId(corellationId)
        , Cookie(cookie)
        , CurrentRetryNumber(retryNum)
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

        GroupId  = LeaveGroupRequestData->GroupId.value_or("");
        MemberId = LeaveGroupRequestData->MemberId.value_or("");
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

    void HandleResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);

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

    TString LogPrefix();
    void SendResponseFail(const TActorContext& ctx, EKafkaErrors error, const TString& message);

    std::optional<TGroupStatus> ParseCheckStateAndGeneration(NKqp::TEvKqp::TEvQueryResponse::TPtr ev);
    bool ParseAssignments(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, TString& assignments);
    bool ParseWorkerStates(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, std::unordered_map<TString, NKafka::TWorkerState>& workerStates, TString& lastMemberId);
    bool ParseDeadCountAndSessionTimeout(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, ui64& outCount, ui32& outSessionTimeoutMs);
    bool ParseGroupsCount(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, ui64& groupsCount);
    bool ChooseProtocolAndFillStates();

    NYdb::TParamsBuilder BuildCheckGroupStateParams();
    NYdb::TParamsBuilder BuildUpdateOrInsertNewGroupParams();
    NYdb::TParamsBuilder BuildInsertMemberParams();
    NYdb::TParamsBuilder BuildAssignmentsParams();
    NYdb::TParamsBuilder BuildSelectWorkerStatesParams();
    NYdb::TParamsBuilder BuildUpdateGroupStateAndProtocolParams();
    NYdb::TParamsBuilder BuildFetchAssignmentsParams();
    NYdb::TParamsBuilder BuildLeaveGroupParams();
    NYdb::TParamsBuilder BuildUpdateLastHeartbeatsParams();
    NYdb::TParamsBuilder BuildCheckDeadsParams();

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
    ui32 SessionTimeoutMs = DEFAULT_SESSION_TIMEOUT_MS;
    ui32 RebalanceTimeoutMs = DEFAULT_REBALANCE_TIMEOUT_MS;

    ui64 GenerationId = 0;
    ui64 CorrelationId = 0;
    ui64 Cookie = 0;
    ui64 KqpReqCookie = 0;
    ui8 WaitingMasterRetryCount = 0;
    ui8 WaitingMasterMaxRetryCount = 10;

    TString WorkerStatesPaginationMemberId = "";

    TString Assignments;
    std::unordered_map<TString, TString> WorkerStates;
    std::unordered_map<TString, NKafka::TWorkerState> AllWorkerStates;
    TString Protocol;
    TString ProtocolType;
    TString Master;
    ui8 CurrentRetryNumber;

    bool IsMaster = false;

    EBalancerStep CurrentStep = STEP_NONE;

    std::unique_ptr<NKikimr::NGRpcProxy::V1::TKqpTxHelper> Kqp;

    TMessagePtr<TJoinGroupRequestData> JoinGroupRequestData;
    TMessagePtr<TSyncGroupRequestData> SyncGroupRequestData;
    TMessagePtr<THeartbeatRequestData> HeartbeatGroupRequestData;
    TMessagePtr<TLeaveGroupRequestData> LeaveGroupRequestData;

};

} // namespace NKafka
