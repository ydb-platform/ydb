#pragma once

#include "actors.h"
#include "../kafka_consumer_groups_metadata_initializers.h"
#include "../kafka_consumer_members_metadata_initializers.h"
#include "../kqp_balance_transaction.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/fetch_request_actor.h>
#include <ydb/core/protos/kafka.pb.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/services/metadata/service.h>
#include <ydb/services/persqueue_v1/actors/read_init_auth_actor.h>
#include <util/datetime/base.h>

namespace NKafka {
using namespace NKikimr;

constexpr ui32 DEFAULT_REBALANCE_TIMEOUT_MS = 5000;
constexpr ui32 MIN_REBALANCE_TIMEOUT_MS = 5000;
constexpr ui32 MAX_REBALANCE_TIMEOUT_MS = 300000;

constexpr ui32 DEFAULT_SESSION_TIMEOUT_MS = 45000;
constexpr ui32 MIN_SESSION_TIMEOUT_MS = 5000;
constexpr ui32 MAX_SESSION_TIMEOUT_MS = 300000;

constexpr ui32 MAX_GROUPS_COUNT = 1000;
constexpr ui32 LIMIT_MEMBERS_PER_REQUEST = 999;

constexpr ui32 MASTER_WAIT_JOINS_DELAY_SECONDS = 3;
constexpr ui32 WAIT_MASTER_ASSIGNMENTS_PER_RETRY_SECONDS = 2;
constexpr ui32 WAIT_FOR_MASTER_ASSIGNMENTS_MAX_RETRY_COUNT = 15;
constexpr ui32 TX_ABORT_RETRY_MAX_COUNT = 3;
constexpr ui32 TABLES_TO_INIT_COUNT = 2;

constexpr TKafkaUint16 ASSIGNMENT_VERSION = 3;

extern const TString INSERT_NEW_GROUP;
extern const TString UPDATE_GROUP;
extern const TString UPDATE_GROUP_STATE_AND_PROTOCOL;
extern const TString INSERT_MEMBER;
extern const TString SELECT_WORKER_STATES;
extern const TString SELECT_ALIVE_MEMBERS;
extern const TString SELECT_WORKER_STATE_QUERY;
extern const TString SELECT_MASTER;
extern const TString UPSERT_ASSIGNMENTS_AND_SET_WORKING_STATE;
extern const TString CHECK_GROUP_STATE;
extern const TString FETCH_ASSIGNMENTS;
extern const TString CHECK_DEAD_MEMBERS;
extern const TString UPDATE_LAST_MEMBER_AND_GROUP_HEARTBEATS;
extern const TString UPDATE_LAST_MEMBER_HEARTBEAT;
extern const TString UPDATE_LAST_HEARTBEAT_AND_STATE_TO_LEAVE_GROUP;
extern const TString GET_GENERATION_BY_MEMBER;
extern const TString CHECK_GROUPS_COUNT;
extern const TString UPDATE_GROUP_STATE;
extern const TString CHECK_MASTER_ALIVE;

struct TGroupStatus {
    bool Exists;
    ui64 Generation;
    ui64 LastSuccessGeneration;
    ui64 State;
    TString MasterId;
    TInstant LastHeartbeat;
    TString ProtocolName;
    TString ProtocolType;
};

class TKafkaBalancerActor : public NActors::TActorBootstrapped<TKafkaBalancerActor> {
public:
    using TBase = NActors::TActorBootstrapped<TKafkaBalancerActor>;

    enum EBalancerStep : ui8 {
        STEP_NONE = 0,

        JOIN_CHECK_STATE_AND_GENERATION,
        JOIN_CHECK_GROUPS_COUNT,
        JOIN_GROUP_OR_UPDATE_EXISTS,
        JOIN_INSERT_MEMBER,
        JOIN_WAIT_JOINS,

        JOIN_BEGIN_TX,
        JOIN_RE_CHECK_STATE_AND_GENERATION,
        JOIN_SET_MASTER_DEAD,
        JOIN_GET_PREV_MEMBERS,
        JOIN_GET_CUR_MEMBERS,
        JOIN_UPDATE_MASTER_HEARTBEAT_AND_WAIT_JOINS,
        JOIN_UPDATE_GROUP_STATE_AND_PROTOCOL,

        SYNC_SELECT_MASTER, // 13
        SYNC_CHECK_STATE_AND_GENERATION,
        SYNC_SET_ASSIGNMENTS_AND_SET_WORKING_STATE,
        SYNC_WAIT_ASSIGNMENTS,
        SYNC_COMMIT_TX,

        SYNC_BEGIN_TX,
        SYNC_RE_CHECK_STATE_AND_GENERATION,
        SYNC_FETCH_ASSIGNMENTS,
        SYNC_COMMIT_FINAL_TX,

        HEARTBEAT_CHECK_DEAD_MEMBERS,
        HEARTBEAT_COMMIT_TX,

        HEARTBEAT_BEGIN_TX,
        HEARTBEAT_CHECK_STATE_AND_GENERATION,
        HEARTBEAT_UPDATE_DEADLINES,

        LEAVE_GET_LAST_GENERATION,
        LEAVE_SET_DEAD
    };

    TKafkaBalancerActor(const TContext::TPtr context, ui64 cookie, ui64 corellationId, TMessagePtr<TJoinGroupRequestData> message, ui8 retryNum = 0)
        : Context(context)
        , CorrelationId(corellationId)
        , Cookie(cookie)
        , CurrentTxAbortRetryNumber(retryNum)
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
        RequestType = JOIN_GROUP;
        CurrentStep = STEP_NONE;

        GroupId = JoinGroupRequestData->GroupId.value_or("");
        ProtocolType = JoinGroupRequestData->ProtocolType.value_or("");
        InstanceId = JoinGroupRequestData->GroupInstanceId.value_or("");
        MemberId = JoinGroupRequestData->MemberId.value_or("");

        KAFKA_LOG_ERROR(TStringBuilder() << "JOIN_GROUP request. MemberId# " << MemberId);

        if (JoinGroupRequestData->SessionTimeoutMs) {
            SessionTimeoutMs = JoinGroupRequestData->SessionTimeoutMs;
        }

        if (JoinGroupRequestData->RebalanceTimeoutMs) {
            RebalanceTimeoutMs = JoinGroupRequestData->RebalanceTimeoutMs;
        }

        if (RebalanceTimeoutMs > MAX_REBALANCE_TIMEOUT_MS || RebalanceTimeoutMs < MIN_REBALANCE_TIMEOUT_MS) {
            RebalanceTimeoutMs = DEFAULT_REBALANCE_TIMEOUT_MS;
        }
    }

    TKafkaBalancerActor(const TContext::TPtr context, ui64 cookie, ui64 corellationId, TMessagePtr<TSyncGroupRequestData> message, ui8 retryNum = 0)
        : Context(context)
        , CorrelationId(corellationId)
        , Cookie(cookie)
        , CurrentTxAbortRetryNumber(retryNum)
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
        RequestType = SYNC_GROUP;
        CurrentStep = STEP_NONE;

        GroupId = SyncGroupRequestData->GroupId.value_or("");
        MemberId = SyncGroupRequestData->MemberId.value_or("");
        GenerationId = SyncGroupRequestData->GenerationId;
    }

    TKafkaBalancerActor(const TContext::TPtr context, ui64 cookie, ui64 corellationId, TMessagePtr<THeartbeatRequestData> message, ui8 retryNum = 0)
        : Context(context)
        , CorrelationId(corellationId)
        , Cookie(cookie)
        , CurrentTxAbortRetryNumber(retryNum)
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
        RequestType = HEARTBEAT;
        CurrentStep = STEP_NONE;

        GroupId = HeartbeatGroupRequestData->GroupId.value_or("");
        MemberId = HeartbeatGroupRequestData->MemberId.value_or("");
        GenerationId = HeartbeatGroupRequestData->GenerationId;
    }

    TKafkaBalancerActor(const TContext::TPtr context, ui64 cookie, ui64 corellationId, TMessagePtr<TLeaveGroupRequestData> message, ui8 retryNum = 0)
        : Context(context)
        , CorrelationId(corellationId)
        , Cookie(cookie)
        , CurrentTxAbortRetryNumber(retryNum)
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
        RequestType = LEAVE_GROUP;
        CurrentStep = STEP_NONE;

        GroupId = LeaveGroupRequestData->GroupId.value_or("");
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

    void RequestFullRetry();

    void Handle(NMetadata::NProvider::TEvManagerPrepared::TPtr&, const TActorContext& ctx);
    void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx);

    void JoinGroupNextStep(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void SyncGroupNextStep(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void LeaveGroupStep(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void HeartbeatNextStep(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);

    void JoinStepCheckGroupState(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void JoinStepCreateNewOrJoinGroup(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void JoinStepCheckGroupsCount(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void JoinStepInsertNewMember(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void JoinStepWaitJoinsIfMaster(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void JoinStepCheckMasterAlive(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void JoinStepReCheckGroupState(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void JoinStepSelectWorkerStates(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void JoinStepSelectPrevMembers(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void JoinStepWaitProtocolChoosing(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void JoinStepCheckPrevGenerationMembers(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void JoinStepWaitMembersAndChooseProtocol(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);

    void SyncStepCheckGroupState(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void SyncStepBuildAssignmentsIfMaster(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void SyncStepCommitTx(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void SyncStepReCheckGroupState(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void SyncStepWaitAssignments(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void SyncStepGetAssignments(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);

    void LeaveStepGetMemberGeneration(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void LeaveStepLeaveGroup(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void LeaveStepCommitTx(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);

    void HeartbeatStepCheckDeadMembers(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void HeartbeatStepCommitTx(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void HeartbeatStepBeginTx(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void HeartbeatStepCheckGroupState(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);
    void HeartbeatStepUpdateHeartbeatDeadlines(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx);

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

    std::optional<TGroupStatus> ParseGroupState(NKqp::TEvKqp::TEvQueryResponse::TPtr ev);
    bool ParseAssignments(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, TString& assignments);
    bool ParseWorkerStates(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, std::unordered_map<TString, NKafka::TWorkerState>& workerStates, TString& outLastMemberId);
    bool ParseMembersAndRebalanceTimeouts(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, std::unordered_map<TString, ui32>& membersAndRebalanceTimeouts, TString& lastMemberId);
    bool ParseDeadsAndSessionTimeout(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, ui64& deadsCount, ui32& outSessionTimeoutMs);
    bool ParseGroupsCount(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, ui64& groupsCount);
    bool ParseMemberGeneration(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, ui64& generation);
    bool ParseMasterAlive(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, bool& alive);
    bool ChooseProtocolAndFillStates();

    NYdb::TParamsBuilder BuildCheckGroupStateParams();
    NYdb::TParamsBuilder BuildUpdateOrInsertNewGroupParams();
    NYdb::TParamsBuilder BuildInsertMemberParams();
    NYdb::TParamsBuilder BuildAssignmentsParams();
    NYdb::TParamsBuilder BuildSelectMembersParams(ui64 generationId);
    NYdb::TParamsBuilder BuildUpdateGroupStateAndProtocolParams();
    NYdb::TParamsBuilder BuildFetchAssignmentsParams();
    NYdb::TParamsBuilder BuildLeaveGroupParams();
    NYdb::TParamsBuilder BuildUpdateLastHeartbeatsParams();
    NYdb::TParamsBuilder BuildCheckDeadsParams();
    NYdb::TParamsBuilder BuildSetMasterDeadParams();
    NYdb::TParamsBuilder BuildGetMemberParams();
    NYdb::TParamsBuilder BuildCheckMasterAlive();
    NYdb::TParamsBuilder BuildCheckGroupsCountParams();

private:
    enum EGroupState : ui32 {
        GROUP_STATE_JOIN = 0,
        GROUP_STATE_SYNC,
        GROUP_STATE_WORKING,
        GROUP_STATE_MASTER_IS_DEAD
    };

private:
    const TContext::TPtr Context;
    NKafka::EApiKey RequestType;
    EBalancerStep CurrentStep = STEP_NONE;

    ui8 TablesInited = 0;
    TString GroupId;
    TString MemberId;
    TString InstanceId;
    ui64 GenerationId = 0;
    ui64 CorrelationId = 0;
    ui64 Cookie = 0;
    ui64 KqpReqCookie = 0;

    ui32 SessionTimeoutMs = DEFAULT_SESSION_TIMEOUT_MS;
    ui32 RebalanceTimeoutMs = DEFAULT_REBALANCE_TIMEOUT_MS;

    ui8 WaitingMasterRetryCount = 0;
    ui64 LastSuccessGeneration = 0;
    TString WorkerStatesPaginationMemberId = "";

    TString Assignments;
    std::unordered_map<TString, TString> WorkerStates;
    std::unordered_map<TString, NKafka::TWorkerState> AllWorkerStates;
    std::unordered_map<TString, ui32> WaitedMemberIdsAndTimeouts;
    TInstant RebalanceStartTime = TInstant::Now();
    TString Protocol;
    TString ProtocolType;
    TString Master;
    ui8 CurrentTxAbortRetryNumber;

    bool IsMaster = false;

    std::unique_ptr<NKafka::TKqpTxHelper> Kqp;

    TMessagePtr<TJoinGroupRequestData> JoinGroupRequestData;
    TMessagePtr<TSyncGroupRequestData> SyncGroupRequestData;
    TMessagePtr<THeartbeatRequestData> HeartbeatGroupRequestData;
    TMessagePtr<TLeaveGroupRequestData> LeaveGroupRequestData;

};

} // namespace NKafka
