#include "kafka_balancer_actor.h"
#include "kqp_balance_transaction.h"
#include "ydb/core/kqp/common/simple/services.h"

namespace NKafka {

using namespace NKikimr;
using namespace NKikimr::NGRpcProxy::V1;

static EKafkaErrors KqpStatusToKafkaError(Ydb::StatusIds::StatusCode status) {
    // savnik finish it
    if (status == Ydb::StatusIds::SUCCESS) {
        return EKafkaErrors::NONE_ERROR;
    }
    return EKafkaErrors::UNKNOWN_SERVER_ERROR;
}

static constexpr ui8 WAKE_UP_DELAY_SECONDS = 5;
static constexpr ui8 WAIT_WORKING_STATE_MAX_RETRY_COUNT = 5;

void TKafkaBalancerActor::Bootstrap(const NActors::TActorContext& /*ctx*/) {
    Kqp = std::make_unique<TKqpTxHelper>("Database"); // savnik get database
    Become(&TKafkaBalancerActor::StateWork);
}

void TKafkaBalancerActor::HandleJoinGroup(TEvKafka::TEvJoinGroupRequest::TPtr ev, const TActorContext& ctx) {
    KAFKA_LOG_D("HandleJoinGroup request");

    auto joinGroupRequest = ev->Get()->Request;

    RequestType   = JOIN_GROUP;
    CurrentStep   = STEP_NONE;
    CorellationId = ev->Get()->CorrelationId;

    GroupId  = joinGroupRequest->GroupId.value();
    MemberId = SelfId().ToString();

    Kqp->SendCreateSessionRequest(ctx);
}

void TKafkaBalancerActor::HandleSyncGroup(TEvKafka::TEvSyncGroupRequest::TPtr ev, const TActorContext& ctx) {
    KAFKA_LOG_D("HandleSyncGroup request");

    auto syncGroupRequest = ev->Get()->Request;

    RequestType   = SYNC_GROUP;
    CurrentStep   = STEP_NONE;
    CorellationId = ev->Get()->CorrelationId;

    GroupId  = syncGroupRequest->GroupId.value();
    MemberId = syncGroupRequest->MemberId.value();

    Kqp->SendCreateSessionRequest(ctx);
}

void TKafkaBalancerActor::HandleLeaveGroup(TEvKafka::TEvLeaveGroupRequest::TPtr ev, const TActorContext& ctx) {
    KAFKA_LOG_D("HandleLeaveGroup request");

    auto leaveGroupRequest = ev->Get()->Request;

    RequestType   = LEAVE_GROUP;
    CurrentStep   = STEP_NONE;
    CorellationId = ev->Get()->CorrelationId;

    GroupId  = leaveGroupRequest->GroupId.value();
    MemberId = leaveGroupRequest->MemberId.value_or(SelfId().ToString());

    Kqp->SendCreateSessionRequest(ctx);
}

void TKafkaBalancerActor::HandleHeartbeat(TEvKafka::TEvHeartbeatRequest::TPtr ev, const TActorContext& ctx) {
    KAFKA_LOG_D("HandleHeartbeat request");

    auto heartbeatRequest = ev->Get()->Request;

    RequestType   = HEARTBEAT;
    CurrentStep   = STEP_NONE;
    CorellationId = ev->Get()->CorrelationId;

    GroupId  = heartbeatRequest->GroupId.value();
    MemberId = heartbeatRequest->MemberId.value();

    Kqp->SendCreateSessionRequest(ctx);
}


void TKafkaBalancerActor::Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx) {
    const TString createSessionError = "Failed to create KQP session";
    if (!Kqp->HandleCreateSessionResponse(ev, ctx)) {
        switch (RequestType) {
            case JOIN_GROUP:
                SendJoinGroupResponseFail(ctx, CorellationId,
                                          EKafkaErrors::UNKNOWN_SERVER_ERROR,
                                          createSessionError);
                break;
            case SYNC_GROUP:
                SendSyncGroupResponseFail(ctx, CorellationId,
                                          EKafkaErrors::UNKNOWN_SERVER_ERROR,
                                          createSessionError);
                break;
            case LEAVE_GROUP:
                SendLeaveGroupResponseFail(ctx, CorellationId,
                                           EKafkaErrors::UNKNOWN_SERVER_ERROR,
                                           createSessionError);
                break;
            case HEARTBEAT:
                SendHeartbeatResponseFail(ctx, CorellationId,
                                          EKafkaErrors::UNKNOWN_SERVER_ERROR,
                                          createSessionError);
                break;
            default:
                break;
        }
        PassAway();
        return;
    }

    switch (RequestType) {
        case JOIN_GROUP:
            HandleJoinGroupResponse(nullptr, ctx);
            break;
        case SYNC_GROUP:
            HandleSyncGroupResponse(nullptr, ctx);
            break;
        case LEAVE_GROUP:
            HandleLeaveGroupResponse(nullptr, ctx);
            break;
        case HEARTBEAT:
            HandleHeartbeatResponse(nullptr, ctx);
            break;
        default:
            KAFKA_LOG_CRIT("Unknown RequestType in TEvCreateSessionResponse");
            PassAway();
            break;
    }
}

void TKafkaBalancerActor::Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
    const TString kqpQueryError = "KQP query error";
    if (ev->Cookie != KqpReqCookie) {
        KAFKA_LOG_CRIT("Unexpected cookie in TEvQueryResponse");
        return;
    }

    const auto& record = ev->Get()->Record;
    auto status   = record.GetYdbStatus();
    auto kafkaErr = KqpStatusToKafkaError(status);

    if (kafkaErr != EKafkaErrors::NONE_ERROR) {
        switch (RequestType) {
            case JOIN_GROUP:
                SendJoinGroupResponseFail(ctx, CorellationId, kafkaErr, kqpQueryError);
                break;
            case SYNC_GROUP:
                SendSyncGroupResponseFail(ctx, CorellationId, kafkaErr, kqpQueryError);
                break;
            case LEAVE_GROUP:
                SendLeaveGroupResponseFail(ctx, CorellationId, kafkaErr, kqpQueryError);
                break;
            case HEARTBEAT:
                SendHeartbeatResponseFail(ctx, CorellationId, kafkaErr, kqpQueryError);
                break;
            default:
                break;
        }
        PassAway();
        return;
    }

    switch (RequestType) {
        case JOIN_GROUP:
            HandleJoinGroupResponse(ev, ctx);
            break;
        case SYNC_GROUP:
            HandleSyncGroupResponse(ev, ctx);
            break;
        case LEAVE_GROUP:
            HandleLeaveGroupResponse(ev, ctx);
            break;
        case HEARTBEAT:
            HandleHeartbeatResponse(ev, ctx);
            break;
        default:
            KAFKA_LOG_CRIT("Unknown RequestType in TEvCreateSessionResponse");
            PassAway();
            break;
    }
}

bool TKafkaBalancerActor::ParseCheckStateAndGeneration(
    NKqp::TEvKqp::TEvQueryResponse::TPtr ev,
    bool& outGroupExists,
    ui64& outGenaration,
    ui64& outState
) {
    if (!ev) {
        return false;
    }

    auto& record = ev->Get()->Record;
    if (record.GetResponse().GetYdbResults().empty()) {
        outGroupExists = false;
        outGenaration = 0;
        outState = 0;
        return true;
    }

    NYdb::TResultSetParser parser(record.GetResponse().GetYdbResults(0));
    if (!parser.TryNextRow()) {
        outGroupExists = false;
        outGenaration = 0;
        outState = 0;
        return true;
    }

    outState = parser.ColumnParser("state").GetOptionalUint64().GetOrElse(0);
    outGenaration = parser.ColumnParser("generation").GetOptionalUint64().GetOrElse(0);
    outGroupExists = true;

    if (parser.TryNextRow()) {
        return false;
    }

    return true;
}

bool TKafkaBalancerActor::ParseMaster(
    NKqp::TEvKqp::TEvQueryResponse::TPtr ev,
    TString& outMasterId,
    ui64 resultIndex
) {
    if (!ev) {
        return false;
    }

    auto& record = ev->Get()->Record;
    auto& resp = record.GetResponse();

    if (resp.GetYdbResults().empty()) {
        return false;
    }

    const auto& masterRs = resp.GetYdbResults(resultIndex);
    NYdb::TResultSetParser parser(masterRs);

    if (!parser.TryNextRow()) {
        return false;
    }

    outMasterId = parser.ColumnParser("master_id").GetOptionalString().GetOrElse("");

    if (parser.TryNextRow()) {
        return false;
    }

    return true;
}

bool TKafkaBalancerActor::ParseAssignments(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, std::vector<TString>& assignments) {
    if (!ev) {
        return false;
    }

    auto& record = ev->Get()->Record;
    if (record.GetResponse().GetYdbResults().empty()) {
        return false;
    }

    NYdb::TResultSetParser parser(record.GetResponse().GetYdbResults(0));
    assignments.clear();

    while (parser.TryNextRow()) {
        TString assignment = parser.ColumnParser("assignment").GetOptionalString().GetOrElse("");
        assignments.push_back(assignment);
    }

    return !assignments.empty();
}

bool TKafkaBalancerActor::ParseWorkerStates(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, std::vector<TString>& workerStates) {
    if (!ev) {
        return false;
    }

    auto& record = ev->Get()->Record;
    if (record.GetResponse().GetYdbResults().empty()) {
        return false;
    }

    NYdb::TResultSetParser parser(record.GetResponse().GetYdbResults(0));
    workerStates.clear();

    while (parser.TryNextRow()) {
        TString workerState = parser.ColumnParser("worker_state").GetOptionalString().GetOrElse("");
        workerStates.push_back(workerState);
    }

    return !workerStates.empty();
}

void TKafkaBalancerActor::HandleJoinGroupResponse(
    NKqp::TEvKqp::TEvQueryResponse::TPtr ev,
    const TActorContext& ctx
) {

    switch (CurrentStep) {
        case STEP_NONE: {
            CurrentStep   = JOIN_TX0_0_BEGIN_TX;
            KqpReqCookie++;
            Kqp->BeginTransaction(KqpReqCookie, ctx);
            break;
        }

        case JOIN_TX0_0_BEGIN_TX: {
            CurrentStep  = JOIN_TX0_1_CHECK_STATE_AND_GENERATION;
            KqpReqCookie++;
            NYdb::TParamsBuilder params;
            params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();

            // savnik разберись, как оптимизировать компиляцию yql

            Kqp->SendYqlRequest(SELECT_STATE_AND_GENERATION, params.Build(), KqpReqCookie, ctx);
            break;
        }

        case JOIN_TX0_1_CHECK_STATE_AND_GENERATION: {
            bool groupExists   = false;
            ui64 oldGeneration = 0;
            ui64 state         = 0;

            if (!ParseCheckStateAndGeneration(ev, groupExists, oldGeneration, state)) {
                SendJoinGroupResponseFail(ctx, CorellationId,
                                            EKafkaErrors::UNKNOWN_SERVER_ERROR,
                                            "Error"); // savnik error
                PassAway();
                return;
            }

            if (!groupExists) {
                GenerationId = 0;
                CurrentStep = JOIN_TX0_2_INSERT_NEW_GROUP;
                KqpReqCookie++;

                NYdb::TParamsBuilder params;
                params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
                params.AddParam("$Generation").Uint64(GenerationId).Build();
                params.AddParam("$State").Uint64(GROUP_STATE_JOIN).Build();

                Kqp->SendYqlRequest(INSERT_NEW_GROUP, params.Build(), KqpReqCookie, ctx);
            } else {
                GenerationId = oldGeneration + 1;
                CurrentStep = JOIN_TX0_2_UPDATE_GROUP_STATE_AND_GENERATION;
                KqpReqCookie++;

                NYdb::TParamsBuilder params;
                params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
                params.AddParam("$NewState").Uint64(GROUP_STATE_JOIN).Build();
                params.AddParam("$OldGeneration").Uint64(oldGeneration).Build();

                Kqp->SendYqlRequest(UPDATE_GROUP, params.Build(), KqpReqCookie, ctx);
            }
            break;
        }

        case JOIN_TX0_2_INSERT_NEW_GROUP:
        case JOIN_TX0_2_UPDATE_GROUP_STATE_AND_GENERATION: {
            CurrentStep  = JOIN_TX0_3_INSERT_MEMBER_AND_CHECK_MASTER;
            KqpReqCookie++;

            NYdb::TParamsBuilder params;
            params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
            params.AddParam("$Generation").Uint64(GenerationId).Build();
            params.AddParam("$MemberId").Utf8(MemberId).Build();

            // savnik get worker state from request
            params.AddParam("$WorkerState").String("worker_state_bytes").Build();

            Kqp->SendYqlRequest(INSERT_MEMBER_AND_SELECT_MASTER_QUERY, params.Build(), KqpReqCookie, ctx);
            break;
        }

        case JOIN_TX0_3_INSERT_MEMBER_AND_CHECK_MASTER: {
            TString masterId;
            if (!ParseMaster(ev, masterId, 1)) {
                SendJoinGroupResponseFail(ctx, CorellationId,
                                            EKafkaErrors::UNKNOWN_SERVER_ERROR,
                                            "Error");
                PassAway();
                return;
            }

            IsMaster = (masterId == MemberId);
            CurrentStep  = JOIN_TX0_4_COMMIT_TX;
            KqpReqCookie++;
            Kqp->CommitTx(KqpReqCookie, ctx);
            break;
        }

        case JOIN_TX0_4_COMMIT_TX: {
            if (IsMaster) {
                auto wakeup = std::make_unique<TEvents::TEvWakeup>(0);
                ctx.ActorSystem()->Schedule(
                    TDuration::Seconds(WAKE_UP_DELAY_SECONDS),
                    new IEventHandle(SelfId(), SelfId(), wakeup.release())
                );
            } else {
                SendJoinGroupResponseOk(ctx, CorellationId);
                PassAway();
            }
            break;
        }

        case JOIN_TX1_0_BEGIN_TX: {
            CurrentStep = JOIN_TX1_1_CHECK_STATE_AND_GENERATION;
            KqpReqCookie++;

            NYdb::TParamsBuilder params;
            params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();

            Kqp->SendYqlRequest(
                SELECT_STATE_AND_GENERATION,
                params.Build(),
                KqpReqCookie,
                ctx
            );
            break;
        }

        case JOIN_TX1_1_CHECK_STATE_AND_GENERATION: {
            bool groupExists = false;
            ui64 generation  = 0;
            ui64 state       = 0;

            if (!ParseCheckStateAndGeneration(ev, groupExists, generation, state) || !groupExists || state != GROUP_STATE_JOIN || generation != GenerationId) {
                SendJoinGroupResponseFail(ctx, CorellationId,
                            EKafkaErrors::REBALANCE_IN_PROGRESS, // savnik change to JOIN state in table
                            "Error");
                PassAway();
                return;
            }

            CurrentStep  = JOIN_TX1_2_GET_MEMBERS_AND_SET_STATE_SYNC;
            KqpReqCookie++;
            NYdb::TParamsBuilder params;
            params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
            params.AddParam("$State").Uint64(GROUP_STATE_SYNC).Build();

            Kqp->SendYqlRequest(UPDATE_GROUPS_AND_SELECT_WORKER_STATES, params.Build(), KqpReqCookie, ctx);
            break;
        }

        case JOIN_TX1_2_GET_MEMBERS_AND_SET_STATE_SYNC: {

            if (!ParseWorkerStates(ev, WorkerStates)) {
                SendJoinGroupResponseFail(ctx, CorellationId,
            EKafkaErrors::UNKNOWN_SERVER_ERROR,
            "Error");
                PassAway();
                return;
            }

            CurrentStep  = JOIN_TX1_3_COMMIT_TX;
            KqpReqCookie++;
            Kqp->CommitTx(KqpReqCookie, ctx);
            break;
        }

        case JOIN_TX1_3_COMMIT_TX: {
            SendJoinGroupResponseOk(ctx, CorellationId);
            PassAway();
            break;
        }

        default:
            KAFKA_LOG_CRIT("JOIN_GROUP: Unexpected step " << CurrentStep);
                            SendJoinGroupResponseFail(ctx, CorellationId,
            EKafkaErrors::UNKNOWN_SERVER_ERROR,
            "Error");
            PassAway();
            return;
            break;
    } // switch (CurrentStep)
}

void TKafkaBalancerActor::HandleSyncGroupResponse(
    NKqp::TEvKqp::TEvQueryResponse::TPtr ev,
    const TActorContext& ctx
) {
    switch (CurrentStep) {
        case STEP_NONE: {
            CurrentStep  = SYNC_TX0_0_BEGIN_TX;
            KqpReqCookie++;
            Kqp->BeginTransaction(KqpReqCookie, ctx);
            break;
        }

        case SYNC_TX0_0_BEGIN_TX: {
            CurrentStep  = SYNC_TX0_1_SELECT_MASTER;
            KqpReqCookie++;

            NYdb::TParamsBuilder params;
            params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
            params.AddParam("$Generation").Uint64(GenerationId).Build();

            Kqp->SendYqlRequest(SELECT_MASTER, params.Build(), KqpReqCookie, ctx);
            break;
        }

        case SYNC_TX0_1_SELECT_MASTER: {
            TString masterId;
            if (!ParseMaster(ev, masterId, 0)) {
                SendSyncGroupResponseFail(ctx, CorellationId,
                                            EKafkaErrors::UNKNOWN_SERVER_ERROR,
                                            "Failed to determine master");
                PassAway();
                return;
            }

            IsMaster = (masterId == MemberId);
            if (!IsMaster) {
                CurrentStep = SYNC_TX0_4_COMMIT_TX; // savnik abort TX
                HandleSyncGroupResponse(ev, ctx);
                return;
            }

            CurrentStep = SYNC_TX0_2_CHECK_STATE_AND_GENERATION;
            KqpReqCookie++;

            NYdb::TParamsBuilder params;
            params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
            Kqp->SendYqlRequest(SELECT_STATE_AND_GENERATION, params.Build(), KqpReqCookie, ctx);
            break;
        }

    case SYNC_TX0_2_CHECK_STATE_AND_GENERATION: {
        bool groupExists = false;
        ui64 generation = 0;
        ui64 state = 0;

        if (!ParseCheckStateAndGeneration(ev, groupExists, generation, state) ||
            !groupExists || generation != GenerationId || state != GROUP_STATE_SYNC) {
            SendSyncGroupResponseFail(ctx, CorellationId,
                                    EKafkaErrors::UNKNOWN_SERVER_ERROR,
                                    "Group state or generation mismatch");
            PassAway();
            return;
        }

        CurrentStep = SYNC_TX0_3_SET_ASSIGNMENTS_AND_SET_WORKING_STATE;
        KqpReqCookie++;

        NYdb::TParamsBuilder params;
        params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();

        std::unordered_map<TString, TString> assignmentsMap; // savnik get assignment from request

        auto& assignmentList = params.AddParam("$Assignments").BeginList();
        for (const auto& [memberId, assignment] : assignmentsMap) {
            assignmentList.AddListItem()
                .BeginStruct()
                    .AddMember("MemberId").Utf8(memberId)
                    .AddMember("Assignment").String(assignment)
                .EndStruct();
        }
        assignmentList.EndList();

        Kqp->SendYqlRequest(UPSERT_ASSIGNMENTS_AND_SET_WORKING_STATE, params.Build(), KqpReqCookie, ctx);
        break;
    }

        case SYNC_TX0_3_SET_ASSIGNMENTS_AND_SET_WORKING_STATE: {
            // savnik проверять, что транзакция реал выполнилась
            CurrentStep  = SYNC_TX0_4_COMMIT_TX;
            KqpReqCookie++;
            Kqp->CommitTx(KqpReqCookie, ctx);
            break;
        }

        case SYNC_TX0_4_COMMIT_TX: {
            CurrentStep  = SYNC_TX1_0_BEGIN_TX;
            KqpReqCookie++;
            Kqp->BeginTransaction(KqpReqCookie, ctx);
            break;
        }

        case SYNC_TX1_0_BEGIN_TX: {
            CurrentStep  = SYNC_TX1_1_CHECK_STATE;
            KqpReqCookie++;

            NYdb::TParamsBuilder params;
            params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
            Kqp->SendYqlRequest(CHECK_GROUP_STATE, params.Build(), KqpReqCookie, ctx);
            break;
        }

        case SYNC_TX1_1_CHECK_STATE: {
            bool groupExists = false;
            ui64 generation  = 0;
            ui64 state       = 0;

            if (!ParseCheckStateAndGeneration(ev, groupExists, generation, state) || !groupExists || generation != GenerationId) {
                SendSyncGroupResponseFail(ctx, CorellationId,
                                            EKafkaErrors::UNKNOWN_SERVER_ERROR,
                                            "Group state or generation mismatch");
                PassAway();
                return;
            }

            if (state != GROUP_STATE_WORKING) {
                if (WaitingWorkingStateRetries == WAIT_WORKING_STATE_MAX_RETRY_COUNT) {
                    SendSyncGroupResponseFail(ctx, CorellationId, REBALANCE_IN_PROGRESS);
                    PassAway();
                    return;
                }

                auto wakeup = std::make_unique<TEvents::TEvWakeup>(1);
                ctx.ActorSystem()->Schedule(
                    TDuration::Seconds(WAKE_UP_DELAY_SECONDS),
                    new IEventHandle(SelfId(), SelfId(), wakeup.release())
                );
                WaitingWorkingStateRetries++;
                return;
            }

            CurrentStep  = SYNC_TX1_2_FETCH_ASSIGNMENTS;
            KqpReqCookie++;

            NYdb::TParamsBuilder params;
            params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
            params.AddParam("$Generation").Uint64(GenerationId).Build();
            params.AddParam("$MemberId").Utf8(MemberId).Build();

            Kqp->SendYqlRequest(FETCH_ASSIGNMENT, params.Build(), KqpReqCookie, ctx);
            break;
        }

        case SYNC_TX1_2_FETCH_ASSIGNMENTS: {
            if (!ParseAssignments(ev, Assignments)) {
                SendSyncGroupResponseFail(ctx, CorellationId,
                                            EKafkaErrors::UNKNOWN_SERVER_ERROR,
                                            "Failed to get assignments from master");
                PassAway();
                return;
            }

            CurrentStep  = SYNC_TX1_3_COMMIT_TX;
            KqpReqCookie++;
            Kqp->CommitTx(KqpReqCookie, ctx);
            break;
        }

        case SYNC_TX1_3_COMMIT_TX: {
            SendSyncGroupResponseOk(ctx, CorellationId);
            PassAway();
            break;
        }

        default: {
            KAFKA_LOG_CRIT("SYNC_GROUP: Unexpected step in HandleSyncGroupResponse: " << CurrentStep);
            PassAway();
            break;
        }
    } // switch (CurrentStep)
}

void TKafkaBalancerActor::HandleLeaveGroupResponse(
    NKqp::TEvKqp::TEvQueryResponse::TPtr ev,
    const TActorContext& ctx
) {
    // do some
}

void TKafkaBalancerActor::HandleHeartbeatResponse(
    NKqp::TEvKqp::TEvQueryResponse::TPtr ev,
    const TActorContext& ctx
) {
    // do some
}

void TKafkaBalancerActor::Handle(TEvents::TEvWakeup::TPtr& /*ev*/, const TActorContext& ctx) {
    if (RequestType == JOIN_GROUP) {
        KqpReqCookie++;
        CurrentStep = JOIN_TX1_0_BEGIN_TX;
        Kqp->BeginTransaction(KqpReqCookie, ctx); // savnik maybe move to main method
    } else if (RequestType == SYNC_GROUP) {
        CurrentStep = SYNC_TX1_0_BEGIN_TX;
        HandleSyncGroupResponse(nullptr, ctx);
    }
}

void TKafkaBalancerActor::Die(const TActorContext& ctx) {
    KAFKA_LOG_D("TKafkaBalancerActor pass away");
    TBase::Die(ctx);
}

void TKafkaBalancerActor::SendJoinGroupResponseOk(const TActorContext& /*ctx*/, ui64 corellationId) {
    auto response = std::make_shared<TJoinGroupResponseData>();
    response->ProtocolType = "connect";
    response->ProtocolName = "roundrobin";
    response->ErrorCode    = EKafkaErrors::NONE_ERROR;
    response->GenerationId = GenerationId;
    response->MemberId     = MemberId;

    // savnik set workes states

    Send(Context->ConnectionId, new TEvKafka::TEvResponse(corellationId, response, EKafkaErrors::NONE_ERROR));
}

void TKafkaBalancerActor::SendJoinGroupResponseFail(const TActorContext&,
                                                    ui64 corellationId,
                                                    EKafkaErrors error,
                                                    TString message) {
    KAFKA_LOG_CRIT("JOIN_GROUP failed. reason# " << message);
    auto response = std::make_shared<TJoinGroupResponseData>();
    response->ErrorCode = error;
    Send(Context->ConnectionId, new TEvKafka::TEvResponse(corellationId, response, error));
}

void TKafkaBalancerActor::SendSyncGroupResponseOk(const TActorContext& ctx, ui64 corellationId) {
    auto response = std::make_shared<TSyncGroupResponseData>();
    response->ProtocolType = "connect";
    response->ProtocolName = "roundrobin";
    response->ErrorCode    = EKafkaErrors::NONE_ERROR;

    // savnik set assignments

    Send(Context->ConnectionId, new TEvKafka::TEvResponse(corellationId, response, EKafkaErrors::NONE_ERROR));
}

void TKafkaBalancerActor::SendSyncGroupResponseFail(const TActorContext&,
                                                    ui64 corellationId,
                                                    EKafkaErrors error,
                                                    TString message) {
    KAFKA_LOG_CRIT("SYNC_GROUP failed. reason# " << message);
    auto response = std::make_shared<TSyncGroupResponseData>();
    response->ErrorCode = error;
    Send(Context->ConnectionId, new TEvKafka::TEvResponse(corellationId, response, error));
}

void TKafkaBalancerActor::SendLeaveGroupResponseOk(const TActorContext&, ui64 corellationId) {
    auto response = std::make_shared<TLeaveGroupResponseData>();
    response->ErrorCode = EKafkaErrors::NONE_ERROR;
    Send(Context->ConnectionId, new TEvKafka::TEvResponse(corellationId, response, EKafkaErrors::NONE_ERROR));
}

void TKafkaBalancerActor::SendLeaveGroupResponseFail(const TActorContext&,
                                                     ui64 corellationId,
                                                     EKafkaErrors error,
                                                     TString message) {
    KAFKA_LOG_CRIT("LEAVE_GROUP failed. reason# " << message);
    auto response = std::make_shared<TLeaveGroupResponseData>();
    response->ErrorCode = error;
    Send(Context->ConnectionId, new TEvKafka::TEvResponse(corellationId, response, error));
}

void TKafkaBalancerActor::SendHeartbeatResponseOk(const TActorContext&,
                                                  ui64 corellationId,
                                                  EKafkaErrors error) {
    auto response = std::make_shared<THeartbeatResponseData>();
    response->ErrorCode = error;
    Send(Context->ConnectionId, new TEvKafka::TEvResponse(corellationId, response, error));
}

void TKafkaBalancerActor::SendHeartbeatResponseFail(const TActorContext&,
                                                    ui64 corellationId,
                                                    EKafkaErrors error,
                                                    TString message) {
    KAFKA_LOG_CRIT("HEARTBEAT failed. reason# " << message);
    auto response = std::make_shared<THeartbeatResponseData>();
    response->ErrorCode = error;
    Send(Context->ConnectionId, new TEvKafka::TEvResponse(corellationId, response, error));
}

} // namespace NKafka
