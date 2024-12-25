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
#include <ydb/library/aclib/aclib.h>
#include <ydb/services/persqueue_v1/actors/read_init_auth_actor.h>

namespace NKafka {
using namespace NKikimr;

extern const TString SELECT_STATE_AND_GENERATION;
extern const TString INSERT_NEW_GROUP;
extern const TString UPDATE_GROUP;
extern const TString INSERT_MEMBER_AND_SELECT_MASTER_QUERY;
extern const TString UPDATE_GROUPS_AND_SELECT_WORKER_STATES;
extern const TString SELECT_WORKER_STATE_QUERY;
extern const TString SELECT_MASTER;
extern const TString UPSERT_ASSIGNMENTS_AND_SET_WORKING_STATE;
extern const TString CHECK_GROUP_STATE;
extern const TString FETCH_ASSIGNMENT;

class TKafkaBalancerActor : public NActors::TActorBootstrapped<TKafkaBalancerActor> {
public:
    using TBase = NActors::TActorBootstrapped<TKafkaBalancerActor>;

    TKafkaBalancerActor(const TContext::TPtr context, ui64 cookie)
        : Context(context)
        , Cookie(cookie)
    {}

    void Bootstrap(const NActors::TActorContext& ctx);

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KAFKA_READ_SESSION_ACTOR;
    }

private:
    using TActorContext = NActors::TActorContext;

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            // from client
            HFunc(TEvKafka::TEvJoinGroupRequest, HandleJoinGroup);
            HFunc(TEvKafka::TEvSyncGroupRequest, HandleSyncGroup);
            HFunc(TEvKafka::TEvLeaveGroupRequest, HandleLeaveGroup);
            HFunc(TEvKafka::TEvHeartbeatRequest, HandleHeartbeat);

            // from KQP
            HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle);
            HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);

            // internal
            HFunc(TEvents::TEvWakeup, Handle);

            // die
            SFunc(TEvents::TEvPoison, Die);
        }
    }

    void HandleJoinGroup(TEvKafka::TEvJoinGroupRequest::TPtr ev, const TActorContext& ctx);
    void HandleSyncGroup(TEvKafka::TEvSyncGroupRequest::TPtr ev, const TActorContext& ctx);
    void HandleLeaveGroup(TEvKafka::TEvLeaveGroupRequest::TPtr ev, const TActorContext& ctx);
    void HandleHeartbeat(TEvKafka::TEvHeartbeatRequest::TPtr ev, const TActorContext& ctx);
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


    bool ParseCheckStateAndGeneration(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, bool& outGroupExists, ui64& outGenaration, ui64& outState);
    bool ParseMaster(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, TString& outMasterId, ui64 resultIndex);
    bool ParseAssignments(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, std::vector<TString>& assignments);
    bool ParseWorkerStates(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, std::vector<TString>& workerStates);

private:
    enum EBalancerStep : ui32 {
        STEP_NONE = 0,

        JOIN_TX0_0_BEGIN_TX,
        JOIN_TX0_1_CHECK_STATE_AND_GENERATION,
        JOIN_TX0_2_INSERT_NEW_GROUP,
        JOIN_TX0_2_UPDATE_GROUP_STATE_AND_GENERATION,
        JOIN_TX0_3_INSERT_MEMBER_AND_CHECK_MASTER,
        JOIN_TX0_4_COMMIT_TX,

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
    };

    enum EGroupState : ui32 {
        GROUP_STATE_JOIN = 0,
        GROUP_STATE_SYNC,
        GROUP_STATE_WORKING
    };

private:
    const TContext::TPtr Context;
    NKafka::EApiKey RequestType;

    TString GroupId;
    TString GroupName;
    TString MemberId;
    TString AssignProtocolName;
    ui64 GenerationId = 0;
    ui64 CorellationId = 0;
    ui64 Cookie = 0;
    ui64 KqpReqCookie = 0;
    ui8 WaitingWorkingStateRetries = 0;

    std::vector<TString> Assignments;
    std::vector<TString> WorkerStates;

    bool IsMaster = false;

    EBalancerStep CurrentStep = STEP_NONE;

    std::unique_ptr<NKikimr::NGRpcProxy::V1::TKqpTxHelper> Kqp;
};

} // namespace NKafka
