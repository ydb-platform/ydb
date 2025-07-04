#include "kafka_balancer_actor.h"

namespace NKafka {

using namespace NKikimr;
using namespace NKikimr::NGRpcProxy::V1;

void TKafkaBalancerActor::Bootstrap(const NActors::TActorContext& ctx) {
    Kqp = std::make_unique<TKqpTxHelper>(Context->DatabasePath);

    if (RequestType == JOIN_GROUP && MemberId.empty()) {
        MemberId = SelfId().ToString();
    }

    if (GroupId.empty()) {
        SendResponseFail(ctx, EKafkaErrors::INVALID_GROUP_ID, TStringBuilder() << "Empty GroupId.");
        return;
    }

    if (MemberId.empty()) {
        SendResponseFail(ctx, EKafkaErrors::MEMBER_ID_REQUIRED, TStringBuilder() << "Empty MemberId.");
        return;
    }
    Kqp->SendInitTableRequest(ctx, NKikimr::NGRpcProxy::V1::TKafkaConsumerGroupsMetaInitManager::GetInstant());
    Kqp->SendInitTableRequest(ctx, NKikimr::NGRpcProxy::V1::TKafkaConsumerMembersMetaInitManager::GetInstant());
    Become(&TKafkaBalancerActor::StateWork);
}

void TKafkaBalancerActor::Handle(NMetadata::NProvider::TEvManagerPrepared::TPtr&, const TActorContext& ctx) {
    TablesInited++;
    if (TablesInited == TABLES_TO_INIT_COUNT) {
        Kqp->SendCreateSessionRequest(ctx);
    }
}

void TKafkaBalancerActor::Die(const TActorContext& ctx) {
    KAFKA_LOG_D("Pass away.");
    if (Kqp) {
        Kqp->CloseKqpSession(ctx);
    }
    TBase::Die(ctx);
}

static EKafkaErrors KqpStatusToKafkaError(Ydb::StatusIds::StatusCode status) {
    if (status == Ydb::StatusIds::SUCCESS) {
        return EKafkaErrors::NONE_ERROR;
    } else if (status == Ydb::StatusIds::ABORTED) {
        return EKafkaErrors::BROKER_NOT_AVAILABLE;
    } else if (status == Ydb::StatusIds::PRECONDITION_FAILED) {
        return EKafkaErrors::REBALANCE_IN_PROGRESS;
    }
    return EKafkaErrors::INVALID_REQUEST;
}

void TKafkaBalancerActor::Handle(TEvents::TEvWakeup::TPtr&, const TActorContext& ctx) {
    if (RequestType == JOIN_GROUP) {
        JoinGroupNextStep(nullptr, ctx);
    } else if (RequestType == SYNC_GROUP) {
        SyncGroupNextStep(nullptr, ctx);
    }
}

void TKafkaBalancerActor::Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx) {
    if (!Kqp->HandleCreateSessionResponse(ev, ctx)) {
        SendResponseFail(ctx, EKafkaErrors::UNKNOWN_SERVER_ERROR, "Failed to create KQP session");
        return;
    }

    HandleResponse(nullptr, ctx);
}

void TKafkaBalancerActor::RequestFullRetry() {
    CurrentTxAbortRetryNumber++;
    switch (RequestType) {
        case JOIN_GROUP:
            JoinGroupRequestData->MemberId = MemberId;
            Register(new TKafkaBalancerActor(Context, Cookie, CorrelationId, JoinGroupRequestData, CurrentTxAbortRetryNumber));
            break;
        case SYNC_GROUP:
            Register(new TKafkaBalancerActor(Context, Cookie, CorrelationId, SyncGroupRequestData, CurrentTxAbortRetryNumber));
            break;
        case LEAVE_GROUP:
            Register(new TKafkaBalancerActor(Context, Cookie, CorrelationId, LeaveGroupRequestData, CurrentTxAbortRetryNumber));
            break;
        case HEARTBEAT:
            Register(new TKafkaBalancerActor(Context, Cookie, CorrelationId, HeartbeatGroupRequestData, CurrentTxAbortRetryNumber));
            break;
        default:
            break;
    }
}

void TKafkaBalancerActor::Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
    if (ev->Cookie != KqpReqCookie) {
        KAFKA_LOG_CRIT("Unexpected cookie in TEvQueryResponse.");
        return;
    }

    const auto& record = ev->Get()->Record;
    auto status = record.GetYdbStatus();
    if (status == Ydb::StatusIds::ABORTED) {
        if ((CurrentStep == JOIN_UPDATE_MASTER_HEARTBEAT_AND_WAIT_JOINS || CurrentStep == JOIN_UPDATE_GROUP_STATE_AND_PROTOCOL) && CurrentTxAbortRetryNumber < TX_ABORT_RETRY_MAX_COUNT) {
            CurrentTxAbortRetryNumber++;
            CurrentStep = JOIN_WAIT_JOINS;
            JoinGroupNextStep(nullptr, ctx);
            return;
        }

        if (CurrentTxAbortRetryNumber < TX_ABORT_RETRY_MAX_COUNT) {
            KAFKA_LOG_ERROR(TStringBuilder() << "Retry after tx aborted. CurrentTxAbortRetryNumber# " << static_cast<int>(CurrentTxAbortRetryNumber));
            RequestFullRetry();
            Die(ctx);
            return;
        }
    }

    auto kafkaErr = KqpStatusToKafkaError(status);

    if (kafkaErr != EKafkaErrors::NONE_ERROR) {
        auto kqpQueryError = TStringBuilder() <<" Kqp error. Status# " << status << ", ";

        NYql::TIssues issues;
        NYql::IssuesFromMessage(record.GetResponse().GetQueryIssues(), issues);
        kqpQueryError << issues.ToString();

        SendResponseFail(ctx, kafkaErr, kqpQueryError);
        return;
    }

    HandleResponse(ev, ctx);
}

void TKafkaBalancerActor::HandleResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx) {
    KAFKA_LOG_I(TStringBuilder() << "Handle kqp response");

    switch (RequestType) {
        case JOIN_GROUP:
            JoinGroupNextStep(ev, ctx);
            break;
        case SYNC_GROUP:
            SyncGroupNextStep(ev, ctx);
            break;
        case LEAVE_GROUP:
            LeaveGroupStep(ev, ctx);
            break;
        case HEARTBEAT:
            HeartbeatNextStep(ev, ctx);
            break;
        default:
            KAFKA_LOG_ERROR("Unknown RequestType in TEvCreateSessionResponse");
            Die(ctx);
            break;
    }
}

void TKafkaBalancerActor::JoinGroupNextStep(
    NKqp::TEvKqp::TEvQueryResponse::TPtr ev,
    const TActorContext& ctx
) {
    switch (CurrentStep) {
        case STEP_NONE: {
            JoinStepCheckGroupState(ev, ctx);
            break;
        }
        case JOIN_CHECK_STATE_AND_GENERATION: {
            JoinStepCreateNewOrJoinGroup(ev, ctx);
            break;
        }
        case JOIN_CHECK_GROUPS_COUNT: {
            JoinStepCheckGroupsCount(ev, ctx);
            break;
        }
        case JOIN_GROUP_OR_UPDATE_EXISTS: {
            JoinStepInsertNewMember(ev, ctx);
            break;
        }
        case JOIN_UPDATE_MASTER_HEARTBEAT_AND_WAIT_JOINS:
        case JOIN_INSERT_MEMBER: {
            JoinStepWaitJoinsIfMaster(ev, ctx);
            break;
        }
        case JOIN_WAIT_JOINS: {
            JoinStepCheckMasterAlive(ev, ctx);
            break;
        }
        case JOIN_BEGIN_TX: {
            JoinStepReCheckGroupState(ev, ctx);
            break;
        }
        case JOIN_RE_CHECK_STATE_AND_GENERATION: {
            if (IsMaster) {
                if (LastSuccessGeneration != std::numeric_limits<ui64>::max()) {
                    JoinStepSelectPrevMembers(ev, ctx);
                } else {
                    JoinStepSelectWorkerStates(ev, ctx);
                }
            } else {
                JoinStepWaitProtocolChoosing(ev, ctx);
            }
            break;
        }
        case JOIN_SET_MASTER_DEAD: {
            SendJoinGroupResponseFail(ctx, CorrelationId, REBALANCE_IN_PROGRESS, "Group leader is not available #1");
            break;
        }
        case JOIN_GET_PREV_MEMBERS: {
            JoinStepCheckPrevGenerationMembers(ev, ctx);
            break;
        }
        case JOIN_GET_CUR_MEMBERS: {
            JoinStepWaitMembersAndChooseProtocol(ev, ctx);
            break;
        }
        case JOIN_UPDATE_GROUP_STATE_AND_PROTOCOL:
        {
            SendJoinGroupResponseOk(ctx, CorrelationId);
            break;
        }
        default:
            KAFKA_LOG_CRIT("JOIN_GROUP: Unexpected step" );
            SendJoinGroupResponseFail(ctx, CorrelationId, EKafkaErrors::UNKNOWN_SERVER_ERROR, "Unexpected step");
            break;
    }
}

void TKafkaBalancerActor::SyncGroupNextStep(
    NKqp::TEvKqp::TEvQueryResponse::TPtr ev,
    const TActorContext& ctx
) {
    switch (CurrentStep) {
        case STEP_NONE: {
            SyncStepCheckGroupState(ev, ctx);
            break;
        }

        case SYNC_CHECK_STATE_AND_GENERATION: {
            SyncStepBuildAssignmentsIfMaster(ev, ctx);
            break;
        }

        case SYNC_WAIT_ASSIGNMENTS:
        case SYNC_SET_ASSIGNMENTS_AND_SET_WORKING_STATE: {
            SyncStepCommitTx(ev, ctx);
            break;
        }

        case SYNC_COMMIT_TX: {
            SyncStepReCheckGroupState(ev, ctx);
            break;
        }

        case SYNC_RE_CHECK_STATE_AND_GENERATION: {
            SyncStepWaitAssignments(ev, ctx);
            break;
        }

        case SYNC_FETCH_ASSIGNMENTS: {
            SyncStepGetAssignments(ev, ctx);
            break;
        }

        case SYNC_COMMIT_FINAL_TX: {
            SendSyncGroupResponseOk(ctx, CorrelationId);
            break;
        }

        default: {
            KAFKA_LOG_CRIT("SYNC_GROUP: Unexpected step");
            SendSyncGroupResponseFail(ctx, CorrelationId, EKafkaErrors::UNKNOWN_SERVER_ERROR, "Failed to get assignments from master");
            break;
        }
    }
}

void TKafkaBalancerActor::HeartbeatNextStep(
    NKqp::TEvKqp::TEvQueryResponse::TPtr ev,
    const TActorContext& ctx
) {
    switch (CurrentStep) {
        case STEP_NONE: {
            HeartbeatStepCheckDeadMembers(ev, ctx);
            break;
        }

        case HEARTBEAT_CHECK_DEAD_MEMBERS: {
            HeartbeatStepCommitTx(ev, ctx);
            break;
        }

        case HEARTBEAT_COMMIT_TX: {
            HeartbeatStepBeginTx(ev, ctx);
            break;
        }

        case HEARTBEAT_BEGIN_TX: {
            HeartbeatStepCheckGroupState(ev, ctx);
            break;
        }

        case HEARTBEAT_CHECK_STATE_AND_GENERATION: {
            HeartbeatStepUpdateHeartbeatDeadlines(ev, ctx);
            break;
        }

        case HEARTBEAT_UPDATE_DEADLINES: {
            SendHeartbeatResponseOk(ctx, CorrelationId, EKafkaErrors::NONE_ERROR);
            break;
        }

        default: {
            KAFKA_LOG_CRIT("HEARTBEAT: Unexpected step");
            SendHeartbeatResponseFail(ctx, CorrelationId, EKafkaErrors::UNKNOWN_SERVER_ERROR, "Unexpected step");
            break;
        }
    }
}

void TKafkaBalancerActor::LeaveGroupStep(
    NKqp::TEvKqp::TEvQueryResponse::TPtr ev,
    const TActorContext& ctx
) {
    switch (CurrentStep) {
        case STEP_NONE: {
            LeaveStepGetMemberGeneration(ev, ctx);
            break;
        }

        case LEAVE_GET_LAST_GENERATION: {
            LeaveStepLeaveGroup(ev, ctx);
            break;
        }

        case LEAVE_SET_DEAD: {
            SendLeaveGroupResponseOk(ctx, CorrelationId);
            break;
        }

        default: {
            KAFKA_LOG_CRIT("LEAVE_GROUP: Unexpected step");
            SendLeaveGroupResponseFail(ctx, CorrelationId, EKafkaErrors::UNKNOWN_SERVER_ERROR, "Unexpected step");
            break;
        }
    }
}

void TKafkaBalancerActor::JoinStepCheckGroupState(NKqp::TEvKqp::TEvQueryResponse::TPtr, const TActorContext& ctx) {
    if (SessionTimeoutMs > MAX_SESSION_TIMEOUT_MS || SessionTimeoutMs < MIN_SESSION_TIMEOUT_MS) {
        SendJoinGroupResponseFail(ctx, CorrelationId,
            EKafkaErrors::INVALID_SESSION_TIMEOUT,
            "Invalid session timeout");
        return;
    }

    KqpReqCookie++;
    CurrentStep = JOIN_CHECK_STATE_AND_GENERATION;
    NYdb::TParamsBuilder params = BuildCheckGroupStateParams();
    Kqp->SendYqlRequest(Sprintf(CHECK_GROUP_STATE.c_str(), TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx);
}

void TKafkaBalancerActor::JoinStepCreateNewOrJoinGroup(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx) {
    Kqp->SetTxId(ev->Get()->Record.GetResponse().GetTxMeta().id());
    auto groupStatus = ParseGroupState(ev);

    if (!groupStatus) {
        SendJoinGroupResponseFail(ctx, CorrelationId,
                                    EKafkaErrors::UNKNOWN_SERVER_ERROR,
                                    "Can't get group state");
        return;
    }

    if (groupStatus->Exists && groupStatus->ProtocolType != ProtocolType) {
        SendJoinGroupResponseFail(ctx, CorrelationId,
                                    EKafkaErrors::INCONSISTENT_GROUP_PROTOCOL,
                                    "The group already exists and has a different protocol type");
        return;
    }

    if (groupStatus->Exists) {
        KAFKA_LOG_I(TStringBuilder() << "Check group before join status."
            "\n memberId: " << MemberId <<
            "\n instanceId: " << InstanceId <<
            "\n group: " << GroupId <<
            "\n exists: " << groupStatus->Exists <<
            "\n protocolType: " << groupStatus->ProtocolType <<
            "\n protocolName: " << groupStatus->ProtocolName <<
            "\n master: " << groupStatus->MasterId <<
            "\n generation: " << groupStatus->Generation <<
            "\n lastSuccessGeneration: " << groupStatus->LastSuccessGeneration <<
            "\n state: " << groupStatus->State);
    }

    if (!groupStatus->Exists) {
        KqpReqCookie++;
        IsMaster = true;
        Master = MemberId;
        GenerationId = 0;
        CurrentStep = JOIN_CHECK_GROUPS_COUNT;
        LastSuccessGeneration = std::numeric_limits<ui64>::max();
        NYdb::TParamsBuilder params = BuildCheckGroupsCountParams();
        Kqp->SendYqlRequest(Sprintf(CHECK_GROUPS_COUNT.c_str(), TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx);
    } else if (groupStatus->State != GROUP_STATE_JOIN) {
        KqpReqCookie++;
        IsMaster = true;
        Master = MemberId;
        CurrentStep = JOIN_GROUP_OR_UPDATE_EXISTS;
        GenerationId = groupStatus->Generation + 1;
        NYdb::TParamsBuilder params = BuildUpdateOrInsertNewGroupParams();
        Kqp->SendYqlRequest(Sprintf(UPDATE_GROUP.c_str(), TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx);
    } else {
        IsMaster = MemberId == groupStatus->MasterId;
        Master = groupStatus->MasterId;
        CurrentStep = JOIN_GROUP_OR_UPDATE_EXISTS;
        GenerationId = groupStatus->Generation;
        JoinGroupNextStep(ev, ctx);
    }
}

void TKafkaBalancerActor::JoinStepCheckGroupsCount(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx) {
    ui64 groupsCount;
    if (!ParseGroupsCount(ev, groupsCount)) {
        SendJoinGroupResponseFail(ctx, CorrelationId, EKafkaErrors::UNKNOWN_SERVER_ERROR, "Can't get groups count");
        return;
    }

    if (groupsCount > MAX_GROUPS_COUNT) {
        SendJoinGroupResponseFail(ctx, CorrelationId, EKafkaErrors::INVALID_REQUEST, "Сan't create a new group. The maximum number of groups has been exceeded.");
        return;
    }

    KqpReqCookie++;
    CurrentStep = JOIN_GROUP_OR_UPDATE_EXISTS;

    NYdb::TParamsBuilder params = BuildUpdateOrInsertNewGroupParams();
    Kqp->SendYqlRequest(Sprintf(INSERT_NEW_GROUP.c_str(), TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx);
}

void TKafkaBalancerActor::JoinStepInsertNewMember(NKqp::TEvKqp::TEvQueryResponse::TPtr, const TActorContext& ctx) {
    KqpReqCookie++;
    CurrentStep = JOIN_INSERT_MEMBER;

    NYdb::TParamsBuilder params = BuildInsertMemberParams();
    Kqp->SendYqlRequest(Sprintf(INSERT_MEMBER.c_str(), TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx, true);
}

void TKafkaBalancerActor::JoinStepWaitJoinsIfMaster(NKqp::TEvKqp::TEvQueryResponse::TPtr, const TActorContext& ctx) {
    CurrentStep = JOIN_WAIT_JOINS;

    if (IsMaster) {
        auto wakeup = std::make_unique<TEvents::TEvWakeup>(0);
        ctx.ActorSystem()->Schedule(
            TDuration::Seconds(MASTER_WAIT_JOINS_DELAY_SECONDS),
            new IEventHandle(SelfId(), SelfId(), wakeup.release())
        );
    } else {
        JoinGroupNextStep(nullptr, ctx);
    }
}

void TKafkaBalancerActor::JoinStepCheckMasterAlive(NKqp::TEvKqp::TEvQueryResponse::TPtr, const TActorContext& ctx) {
    CurrentStep = JOIN_BEGIN_TX;
    KqpReqCookie++;
    if (!IsMaster) {
        Kqp->ResetTxId();
        NYdb::TParamsBuilder params = BuildCheckMasterAlive();
        Kqp->SendYqlRequest(Sprintf(CHECK_MASTER_ALIVE.c_str(), TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx);
    } else {
        Kqp->BeginTransaction(KqpReqCookie, ctx);
    }
}

void TKafkaBalancerActor::JoinStepReCheckGroupState(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx) {
    Kqp->SetTxId(ev->Get()->Record.GetResponse().GetTxMeta().id());

    if (!IsMaster) {
        bool masterAlive;
        if (!ParseMasterAlive(ev, masterAlive)) {
            SendJoinGroupResponseFail(ctx, CorrelationId, EKafkaErrors::UNKNOWN_SERVER_ERROR, "Can't check master alive");
            return;
        }

        if (!masterAlive) {
            CurrentStep = JOIN_SET_MASTER_DEAD;
            KqpReqCookie++;
            NYdb::TParamsBuilder params = BuildSetMasterDeadParams();
            Kqp->SendYqlRequest(Sprintf(UPDATE_GROUP_STATE.c_str(), TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx, true);
            return;
        }
    }

    CurrentStep = JOIN_RE_CHECK_STATE_AND_GENERATION;
    KqpReqCookie++;

    NYdb::TParamsBuilder params = BuildCheckGroupStateParams();
    Kqp->SendYqlRequest(Sprintf(CHECK_GROUP_STATE.c_str(), TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx, !IsMaster);
}

void TKafkaBalancerActor::JoinStepSelectPrevMembers(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx) {
    auto groupStatus = ParseGroupState(ev);

    if (!groupStatus) {
        SendJoinGroupResponseFail(ctx, CorrelationId,
                    EKafkaErrors::UNKNOWN_SERVER_ERROR,
                    "Can't get group state");
        return;
    }

    if (!groupStatus->Exists || groupStatus->State != GROUP_STATE_JOIN || groupStatus->Generation != GenerationId) {
        SendJoinGroupResponseFail(ctx, CorrelationId, EKafkaErrors::REBALANCE_IN_PROGRESS, "Group status changed #1. Need rebalance");
        return;
    }

    Master = groupStatus->MasterId;

    KqpReqCookie++;
    CurrentStep = JOIN_GET_PREV_MEMBERS;
    NYdb::TParamsBuilder params = BuildSelectMembersParams(LastSuccessGeneration);
    Kqp->SendYqlRequest(Sprintf(SELECT_ALIVE_MEMBERS.c_str(), TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx);
}

void TKafkaBalancerActor::JoinStepSelectWorkerStates(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx) {
    auto groupStatus = ParseGroupState(ev);

    if (!groupStatus) {
        SendJoinGroupResponseFail(ctx, CorrelationId,
                    EKafkaErrors::UNKNOWN_SERVER_ERROR,
                    "Can't get group state");
        return;
    }

    if (!groupStatus->Exists || groupStatus->State != GROUP_STATE_JOIN || groupStatus->Generation != GenerationId) {
        SendJoinGroupResponseFail(ctx, CorrelationId, EKafkaErrors::REBALANCE_IN_PROGRESS, "Group status changed #2. Need rebalance");
        return;
    }

    KqpReqCookie++;
    CurrentStep  = JOIN_GET_CUR_MEMBERS;
    NYdb::TParamsBuilder params = BuildSelectMembersParams(GenerationId);
    auto sql = Sprintf(SELECT_WORKER_STATES.c_str(), TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath().c_str());
    Kqp->SendYqlRequest(sql, params.Build(), KqpReqCookie, ctx);
}

void TKafkaBalancerActor::JoinStepWaitProtocolChoosing(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx) {
    auto groupStatus = ParseGroupState(ev);

    if (!groupStatus) {
        SendJoinGroupResponseFail(ctx, CorrelationId,
                    EKafkaErrors::UNKNOWN_SERVER_ERROR,
                    "Can't get group state");
        return;
    }

    if (!groupStatus->Exists || groupStatus->Generation != GenerationId) {
        SendJoinGroupResponseFail(ctx, CorrelationId, EKafkaErrors::REBALANCE_IN_PROGRESS, TStringBuilder() << "Group status changed #3. Need rebalance. Expect generation: " << GenerationId << ", actual generation: " << groupStatus->Generation);
        return;
    }

    if (groupStatus->State < GROUP_STATE_SYNC) {
        CurrentStep = JOIN_WAIT_JOINS;
        auto wakeup = std::make_unique<TEvents::TEvWakeup>(1);
        ctx.ActorSystem()->Schedule(
            TDuration::Seconds(MASTER_WAIT_JOINS_DELAY_SECONDS),
            new IEventHandle(SelfId(), SelfId(), wakeup.release())
        );
        return;
    }

    Protocol = groupStatus->ProtocolName;
    SendJoinGroupResponseOk(ctx, CorrelationId);
}

void TKafkaBalancerActor::JoinStepCheckPrevGenerationMembers(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx) {
    auto membersCount = WaitedMemberIdsAndTimeouts.size();
    if (!ParseMembersAndRebalanceTimeouts(ev, WaitedMemberIdsAndTimeouts, WorkerStatesPaginationMemberId)) {
        SendJoinGroupResponseFail(ctx, CorrelationId, EKafkaErrors::INVALID_REQUEST, "Can't get prev generation members");
        return;
    }

    KqpReqCookie++;
    // if pagination ended
    if (WaitedMemberIdsAndTimeouts.size() == membersCount) {
        WorkerStatesPaginationMemberId = "";
        CurrentStep  = JOIN_GET_CUR_MEMBERS;
        NYdb::TParamsBuilder params = BuildSelectMembersParams(GenerationId);
        auto sql = Sprintf(SELECT_WORKER_STATES.c_str(), TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath().c_str());
        Kqp->SendYqlRequest(sql, params.Build(), KqpReqCookie, ctx);
    } else {
        NYdb::TParamsBuilder params = BuildSelectMembersParams(LastSuccessGeneration);
        auto sql = Sprintf(SELECT_ALIVE_MEMBERS.c_str(), TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath().c_str());
        Kqp->SendYqlRequest(sql, params.Build(), KqpReqCookie, ctx);
    }
}

void TKafkaBalancerActor::JoinStepWaitMembersAndChooseProtocol(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx) {
    auto statesCount = AllWorkerStates.size();
    if (!ParseWorkerStates(ev, AllWorkerStates, WorkerStatesPaginationMemberId)) {
        SendJoinGroupResponseFail(ctx, CorrelationId, EKafkaErrors::INVALID_REQUEST, "Can't get workers state");
        return;
    }

    KqpReqCookie++;
    // if pagination ended
    if (AllWorkerStates.size() == statesCount) {
        auto now = TInstant::Now();

        // check all clients have joined or their timeout has expired
        for (auto prevGenerationMembersAndTimeoutsIt = WaitedMemberIdsAndTimeouts.begin(); prevGenerationMembersAndTimeoutsIt != WaitedMemberIdsAndTimeouts.end();) {
            ui32 memberRebalanceTimeoutMs = prevGenerationMembersAndTimeoutsIt->second.RebalanceTimeoutMs;
            const TInstant& memberHeartbeatDeadline = prevGenerationMembersAndTimeoutsIt->second.HeartbeatDeadline;
            if (AllWorkerStates.count(prevGenerationMembersAndTimeoutsIt->first) == 1) {
                KAFKA_LOG_D(TStringBuilder() << "Waited member connected: " << prevGenerationMembersAndTimeoutsIt->first);
                prevGenerationMembersAndTimeoutsIt = WaitedMemberIdsAndTimeouts.erase(prevGenerationMembersAndTimeoutsIt);
            } else if ((RebalanceStartTime + TDuration::MilliSeconds(memberRebalanceTimeoutMs)) < now) {
                KAFKA_LOG_D(TStringBuilder() << "Rebalance deadline: " << prevGenerationMembersAndTimeoutsIt->first);
                prevGenerationMembersAndTimeoutsIt = WaitedMemberIdsAndTimeouts.erase(prevGenerationMembersAndTimeoutsIt);
            } else if (memberHeartbeatDeadline < now) {
                KAFKA_LOG_D(TStringBuilder() << "Waited member connect session deadline: " << prevGenerationMembersAndTimeoutsIt->first);
                prevGenerationMembersAndTimeoutsIt = WaitedMemberIdsAndTimeouts.erase(prevGenerationMembersAndTimeoutsIt);
            } else {
                ++prevGenerationMembersAndTimeoutsIt;
            }
        }

        if (WaitedMemberIdsAndTimeouts.size() != 0) {
            KAFKA_LOG_D(TStringBuilder() << "Members waited count# : " << WaitedMemberIdsAndTimeouts.size());
            WaitedMemberIdsAndTimeouts.clear();
            AllWorkerStates.clear();
            WorkerStatesPaginationMemberId = "";

            CurrentStep = JOIN_UPDATE_MASTER_HEARTBEAT_AND_WAIT_JOINS;
            NYdb::TParamsBuilder params = BuildUpdateLastHeartbeatsParams();
            Kqp->SendYqlRequest(Sprintf(UPDATE_LAST_MEMBER_HEARTBEAT.c_str(), TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx, true);
            return;
        }

        if (!ChooseProtocolAndFillStates()) {
            SendJoinGroupResponseFail(ctx, CorrelationId, EKafkaErrors::INCONSISTENT_GROUP_PROTOCOL, "Can't choose protocol");
            return;
        }

        NYdb::TParamsBuilder params = BuildUpdateGroupStateAndProtocolParams();
        CurrentStep  = JOIN_UPDATE_GROUP_STATE_AND_PROTOCOL;
        Kqp->SendYqlRequest(Sprintf(UPDATE_GROUP_STATE_AND_PROTOCOL.c_str(), TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx, true);
    } else {
        NYdb::TParamsBuilder params = BuildSelectMembersParams(GenerationId);
        auto sql = Sprintf(SELECT_WORKER_STATES.c_str(), TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath().c_str());
        Kqp->SendYqlRequest(sql, params.Build(), KqpReqCookie, ctx);
    }
}

void TKafkaBalancerActor::SyncStepCheckGroupState(NKqp::TEvKqp::TEvQueryResponse::TPtr, const TActorContext& ctx) {
    CurrentStep = SYNC_CHECK_STATE_AND_GENERATION;
    KqpReqCookie++;

    NYdb::TParamsBuilder params = BuildCheckGroupStateParams();
    Kqp->SendYqlRequest(Sprintf(CHECK_GROUP_STATE.c_str(), TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx);
}

void TKafkaBalancerActor::SyncStepBuildAssignmentsIfMaster(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx) {
    Kqp->SetTxId(ev->Get()->Record.GetResponse().GetTxMeta().id());
    auto groupStatus = ParseGroupState(ev);

    if (!groupStatus) {
        SendSyncGroupResponseFail(ctx, CorrelationId,
                                EKafkaErrors::UNKNOWN_SERVER_ERROR,
                                "Can't get group state");
        return;
    }

    if (!groupStatus->Exists) {
        SendSyncGroupResponseFail(ctx, CorrelationId,
                                EKafkaErrors::GROUP_ID_NOT_FOUND,
                                "Unknown group# " + GroupId);
        return;
    }

    if (groupStatus->Generation != GenerationId) {
        SendSyncGroupResponseFail(ctx, CorrelationId,
                                EKafkaErrors::ILLEGAL_GENERATION,
                                TStringBuilder() << "Old or unknown group generation# " << GenerationId);
        return;
    }

    if (groupStatus->State < GROUP_STATE_SYNC) {
        SendSyncGroupResponseFail(ctx, CorrelationId,
                                EKafkaErrors::INVALID_REQUEST,
                                TStringBuilder() << "Unexpected group state# " << groupStatus->State);
        return;
    }

    ProtocolType = groupStatus->ProtocolType;
    Master = groupStatus->MasterId;

    IsMaster = MemberId == groupStatus->MasterId;

    CurrentStep = SYNC_SET_ASSIGNMENTS_AND_SET_WORKING_STATE;

    if (IsMaster) {
        if (SyncGroupRequestData->Assignments.size() == 0) {
            SendSyncGroupResponseFail(ctx, CorrelationId, EKafkaErrors::INVALID_REQUEST);
            return;
        }

        KqpReqCookie++;
        NYdb::TParamsBuilder params = BuildAssignmentsParams();
        Kqp->SendYqlRequest(Sprintf(UPSERT_ASSIGNMENTS_AND_SET_WORKING_STATE.c_str(),  TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath().c_str(), TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx);
    } else {
        SyncGroupNextStep(nullptr, ctx);
    }
}

void TKafkaBalancerActor::SyncStepCommitTx(NKqp::TEvKqp::TEvQueryResponse::TPtr, const TActorContext& ctx) {
    CurrentStep  = SYNC_COMMIT_TX;
    KqpReqCookie++;
    Kqp->CommitTx(KqpReqCookie, ctx);
}

void TKafkaBalancerActor::SyncStepReCheckGroupState(NKqp::TEvKqp::TEvQueryResponse::TPtr, const TActorContext& ctx) {
    Kqp->ResetTxId();
    CurrentStep  = SYNC_RE_CHECK_STATE_AND_GENERATION;
    KqpReqCookie++;

    NYdb::TParamsBuilder params = BuildCheckGroupStateParams();
    Kqp->SendYqlRequest(Sprintf(CHECK_GROUP_STATE.c_str(), TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx);
}

void TKafkaBalancerActor::SyncStepWaitAssignments(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx) {
    Kqp->SetTxId(ev->Get()->Record.GetResponse().GetTxMeta().id());
    auto groupStatus = ParseGroupState(ev);

    if (!groupStatus) {
        SendSyncGroupResponseFail(ctx, CorrelationId,
                                EKafkaErrors::UNKNOWN_SERVER_ERROR,
                                "Can't get group state");
        return;
    }

    if (!groupStatus->Exists) {
        SendSyncGroupResponseFail(ctx, CorrelationId,
                                EKafkaErrors::GROUP_ID_NOT_FOUND,
                                "Unknown group# " + GroupId);
        return;
    }

    if (groupStatus->Generation != GenerationId) {
        SendSyncGroupResponseFail(ctx, CorrelationId,
                                EKafkaErrors::ILLEGAL_GENERATION,
                                TStringBuilder() << "Old or unknown group generation# " << GenerationId);
        return;
    }

    if (groupStatus->State != GROUP_STATE_WORKING) {
        if (WaitingMasterRetryCount == WAIT_FOR_MASTER_ASSIGNMENTS_MAX_RETRY_COUNT) {
            SendSyncGroupResponseFail(ctx, CorrelationId, REBALANCE_IN_PROGRESS, "Group leader is not available #2");
            return;
        }

        CurrentStep = SYNC_WAIT_ASSIGNMENTS;
        auto wakeup = std::make_unique<TEvents::TEvWakeup>(2);
        ctx.ActorSystem()->Schedule(
            TDuration::Seconds(WAIT_MASTER_ASSIGNMENTS_PER_RETRY_SECONDS),
            new IEventHandle(SelfId(), SelfId(), wakeup.release())
        );
        WaitingMasterRetryCount++;
        return;
    }

    CurrentStep = SYNC_FETCH_ASSIGNMENTS;
    KqpReqCookie++;

    NYdb::TParamsBuilder params = BuildFetchAssignmentsParams();
    Kqp->SendYqlRequest(Sprintf(FETCH_ASSIGNMENTS.c_str(), TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx);
}

void TKafkaBalancerActor::SyncStepGetAssignments(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx) {
    if (!ParseAssignments(ev, Assignments)) {
        SendSyncGroupResponseFail(ctx, CorrelationId,
                                    EKafkaErrors::INVALID_REQUEST,
                                    "Failed to get assignments from master");
        return;
    }

    CurrentStep = SYNC_COMMIT_FINAL_TX;
    KqpReqCookie++;
    Kqp->CommitTx(KqpReqCookie, ctx);
}

void TKafkaBalancerActor::HeartbeatStepCheckDeadMembers(NKqp::TEvKqp::TEvQueryResponse::TPtr, const TActorContext& ctx) {
    CurrentStep  = HEARTBEAT_CHECK_DEAD_MEMBERS;
    KqpReqCookie++;

    NYdb::TParamsBuilder params = BuildCheckDeadsParams();
    Kqp->SendYqlRequest(Sprintf(CHECK_DEAD_MEMBERS.c_str(), TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath().c_str(), TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx);
}

void TKafkaBalancerActor::HeartbeatStepCommitTx(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx) {
    Kqp->SetTxId(ev->Get()->Record.GetResponse().GetTxMeta().id());
    ui64 deadsCount = 0;
    if (!ParseDeadsAndSessionTimeout(ev, deadsCount, SessionTimeoutMs)) {
        SendHeartbeatResponseFail(ctx, CorrelationId, EKafkaErrors::UNKNOWN_SERVER_ERROR, "Can't parse dead members count");
        return;
    }

    if (deadsCount > 0) {
        SendHeartbeatResponseFail(ctx, CorrelationId, EKafkaErrors::REBALANCE_IN_PROGRESS, "Deads members found. Rejoin required.");
        return;
    }

    CurrentStep = HEARTBEAT_COMMIT_TX;
    KqpReqCookie++;
    Kqp->CommitTx(KqpReqCookie, ctx);
}

void TKafkaBalancerActor::HeartbeatStepBeginTx(NKqp::TEvKqp::TEvQueryResponse::TPtr, const TActorContext& ctx) {
    CurrentStep  = HEARTBEAT_BEGIN_TX;
    KqpReqCookie++;
    Kqp->BeginTransaction(KqpReqCookie, ctx);
}

void TKafkaBalancerActor::HeartbeatStepCheckGroupState(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx) {
    Kqp->SetTxId(ev->Get()->Record.GetResponse().GetTxMeta().id());
    CurrentStep = HEARTBEAT_CHECK_STATE_AND_GENERATION;
    KqpReqCookie++;

    NYdb::TParamsBuilder params = BuildCheckGroupStateParams();
    Kqp->SendYqlRequest(Sprintf(CHECK_GROUP_STATE.c_str(), TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx, true);
}

void TKafkaBalancerActor::HeartbeatStepUpdateHeartbeatDeadlines(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx) {
    Kqp->ResetTxId();
    auto groupStatus = ParseGroupState(ev);

    if (!groupStatus) {
        SendHeartbeatResponseFail(ctx, CorrelationId,
                                EKafkaErrors::UNKNOWN_SERVER_ERROR,
                                "Can't get group state");
        return;
    }

    if (!groupStatus->Exists || groupStatus->Generation != GenerationId || groupStatus->State != GROUP_STATE_WORKING) {
        SendHeartbeatResponseFail(ctx, CorrelationId, EKafkaErrors::REBALANCE_IN_PROGRESS, "Group state changed. Rejoin required");
        return;
    }

    ProtocolType = groupStatus->ProtocolType;
    IsMaster = (groupStatus->MasterId == MemberId);
    CurrentStep = HEARTBEAT_UPDATE_DEADLINES;
    KqpReqCookie++;

    NYdb::TParamsBuilder params = BuildUpdateLastHeartbeatsParams();
    if (IsMaster) {
        Kqp->SendYqlRequest(Sprintf(UPDATE_LAST_MEMBER_AND_GROUP_HEARTBEATS.c_str(), TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str(), TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx, true);
    } else {
        Kqp->SendYqlRequest(Sprintf(UPDATE_LAST_MEMBER_HEARTBEAT.c_str(), TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx, true);
    }
}

void TKafkaBalancerActor::LeaveStepGetMemberGeneration(NKqp::TEvKqp::TEvQueryResponse::TPtr, const TActorContext& ctx) {
    CurrentStep = LEAVE_GET_LAST_GENERATION;
    KqpReqCookie++;

    NYdb::TParamsBuilder params = BuildGetMemberParams();
    Kqp->SendYqlRequest(Sprintf(GET_GENERATION_BY_MEMBER.c_str(), TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx, true);
}

void TKafkaBalancerActor::LeaveStepLeaveGroup(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, const TActorContext& ctx) {
    if (!ParseMemberGeneration(ev, GenerationId)) {
        SendLeaveGroupResponseFail(ctx, CorrelationId, EKafkaErrors::UNKNOWN_MEMBER_ID, "Can't found memberId");
        return;
    }

    CurrentStep = LEAVE_SET_DEAD;
    KqpReqCookie++;

    NYdb::TParamsBuilder params = BuildLeaveGroupParams();
    Kqp->SendYqlRequest(Sprintf(UPDATE_LAST_HEARTBEAT_AND_STATE_TO_LEAVE_GROUP.c_str(), TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath().c_str(), TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx, true);
}

std::optional<TGroupStatus> TKafkaBalancerActor::ParseGroupState(
    NKqp::TEvKqp::TEvQueryResponse::TPtr ev
) {
    if (!ev) {
        return std::nullopt;
    }

    TGroupStatus result;

    auto& record = ev->Get()->Record;
    auto& resp = record.GetResponse();
    if (resp.GetYdbResults().empty()) {
        result.LastSuccessGeneration = std::numeric_limits<ui64>::max();
        result.Exists = false;
        return result;
    }

    NYdb::TResultSetParser parser(resp.GetYdbResults(0));
    if (!parser.TryNextRow()) {
        result.Exists = false;
        return result;
    }

    result.State = parser.ColumnParser("state").GetUint64();;
    result.Generation = parser.ColumnParser("generation").GetUint64();
    result.LastSuccessGeneration = parser.ColumnParser("last_success_generation").GetOptionalUint64().value_or(std::numeric_limits<ui64>::max());
    result.MasterId = parser.ColumnParser("master").GetUtf8();
    result.LastHeartbeat = parser.ColumnParser("last_heartbeat_time").GetDatetime();
    result.ProtocolName = parser.ColumnParser("protocol").GetOptionalUtf8().value_or("");
    result.ProtocolType = parser.ColumnParser("protocol_type").GetUtf8();
    result.Exists = true;

    if (parser.TryNextRow()) {
        return std::nullopt;
    }

    return result;
}

bool TKafkaBalancerActor::ParseAssignments(
    NKqp::TEvKqp::TEvQueryResponse::TPtr ev,
    TString& assignments)
{
    if (!ev) {
        return false;
    }

    auto& record = ev->Get()->Record;
    if (record.GetResponse().GetYdbResults().empty()) {
        return false;
    }

    NYdb::TResultSetParser parser(record.GetResponse().GetYdbResults(0));
    assignments.clear();

    if (!parser.TryNextRow()) {
        return false;
    }

    assignments = parser.ColumnParser("assignment").GetOptionalString().value_or("");

    if (parser.TryNextRow()) {
        return false;
    }

    return !assignments.empty();
}

bool TKafkaBalancerActor::ParseMembersAndRebalanceTimeouts(
    NKqp::TEvKqp::TEvQueryResponse::TPtr ev,
    std::unordered_map<TString, MemberTimeoutsMs>& membersAndTimeouts,
    TString& lastMemberId)
{
    if (!ev) {
        return false;
    }

    auto& record = ev->Get()->Record;
    if (record.GetResponse().GetYdbResults().empty()) {
        return false;
    }

    NYdb::TResultSetParser parser(record.GetResponse().GetYdbResults(0));

    while (parser.TryNextRow()) {
        TString memberId = TString(parser.ColumnParser("member_id").GetUtf8());
        TString instanceId = parser.ColumnParser("instance_id").GetOptionalUtf8().value_or("");
        ui32 rebalanceTimeoutMs = parser.ColumnParser("rebalance_timeout_ms").GetOptionalUint32().value_or(DEFAULT_REBALANCE_TIMEOUT_MS);
        ui32 sessionTimeoutMs = parser.ColumnParser("session_timeout_ms").GetOptionalUint32().value_or(DEFAULT_SESSION_TIMEOUT_MS);
        TInstant heartbeatDeadline = parser.ColumnParser("heartbeat_deadline").GetOptionalDatetime().value_or(TInstant::Now() + TDuration::MilliSeconds(sessionTimeoutMs));
        membersAndTimeouts[memberId] = {rebalanceTimeoutMs, heartbeatDeadline};

        lastMemberId = memberId;
    }

    return true;
}

bool TKafkaBalancerActor::ParseMasterAlive(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, bool& alive)
{
    if (!ev) {
        return false;
    }

    auto& record = ev->Get()->Record;
    if (record.GetResponse().GetYdbResults().empty()) {
        return false;
    }

    NYdb::TResultSetParser parser(record.GetResponse().GetYdbResults(0));

    if (!parser.TryNextRow()) {
        return false;
    }

    alive = parser.ColumnParser("allive").GetUint64() == 1;

    if (parser.TryNextRow()) {
        return false;
    }

    return true;
}

bool TKafkaBalancerActor::ParseMemberGeneration(NKqp::TEvKqp::TEvQueryResponse::TPtr ev, ui64& generation)
{
    if (!ev) {
        return false;
    }

    auto& record = ev->Get()->Record;
    if (record.GetResponse().GetYdbResults().empty()) {
        return false;
    }

    NYdb::TResultSetParser parser(record.GetResponse().GetYdbResults(0));

    if (!parser.TryNextRow()) {
        return false;
    }

    generation = parser.ColumnParser("generation").GetUint64();

    if (parser.TryNextRow()) {
        return false;
    }

    return true;
}

bool TKafkaBalancerActor::ParseWorkerStates(
    NKqp::TEvKqp::TEvQueryResponse::TPtr ev,
    std::unordered_map<TString, NKafka::TWorkerState>& workerStates,
    TString& lastMemberId)
{
    if (!ev) {
        return false;
    }

    auto& record = ev->Get()->Record;
    if (record.GetResponse().GetYdbResults().empty()) {
        return false;
    }

    NYdb::TResultSetParser parser(record.GetResponse().GetYdbResults(0));

    while (parser.TryNextRow()) {
        TString protoStr = parser.ColumnParser("worker_state_proto").GetOptionalString().value_or("");
        TString memberId = TString(parser.ColumnParser("member_id").GetUtf8());

        NKafka::TWorkerState workerState;
        if (!protoStr.empty() && !workerState.ParseFromString(protoStr)) {
            return false;
        }

        workerStates[memberId] = workerState;
        lastMemberId = memberId;
    }

    return true;
}

bool TKafkaBalancerActor::ParseDeadsAndSessionTimeout(
    NKqp::TEvKqp::TEvQueryResponse::TPtr ev,
    ui64& deadsCount,
    ui32& sessionTimeoutMs
) {
    if (!ev) {
        return false;
    }

    auto& record = ev->Get()->Record;
    auto& resp = record.GetResponse();

    if (resp.GetYdbResults().size() < 2) {
        return false;
    }

    {
        NYdb::TResultSetParser parser(resp.GetYdbResults(0));
        if (!parser.TryNextRow()) {
            return false;
        }

        deadsCount = parser.ColumnParser("deads_cnt").GetUint64();
        if (parser.TryNextRow()) {
            return false;
        }
    }

    {
        NYdb::TResultSetParser parser(resp.GetYdbResults(1));
        if (!parser.TryNextRow()) {
            return false;
        }
        sessionTimeoutMs = parser.ColumnParser("session_timeout_ms").GetOptionalUint32().value_or(DEFAULT_SESSION_TIMEOUT_MS);
        if (parser.TryNextRow()) {
            return false;
        }
    }

    return true;
}

bool TKafkaBalancerActor::ParseGroupsCount(
    NKqp::TEvKqp::TEvQueryResponse::TPtr ev,
    ui64& groupsCount
) {
    if (!ev) {
        return false;
    }

    auto& record = ev->Get()->Record;
    auto& resp = record.GetResponse();

    if (record.GetResponse().GetYdbResults().empty()) {
        return false;
    }

    NYdb::TResultSetParser parser(resp.GetYdbResults(0));
    if (!parser.TryNextRow()) {
        return false;
    }

    groupsCount = parser.ColumnParser("groups_count").GetUint64();
    if (parser.TryNextRow()) {
        return false;
    }

    return true;
}

bool TKafkaBalancerActor::ChooseProtocolAndFillStates() {
    if (AllWorkerStates.empty()) {
        return false;
    }

    std::unordered_map<TString, int> sumRanks;
    std::unordered_map<TString, int> counts;

    for (const auto& kv : AllWorkerStates) {
        const auto& protocols = kv.second.protocols();

        std::unordered_set<TString> seen;
        seen.reserve(protocols.size());

        for (int rank = 0; rank < protocols.size(); rank++) {
            const auto& name = protocols[rank].protocol_name();
            if (!seen.insert(name).second) {
                continue;
            }
            sumRanks[name] += rank;
            counts[name] += 1;
        }
    }

    TString bestProtocol;
    int bestSum = std::numeric_limits<int>::max();

    for (const auto& kv : sumRanks) {
        const auto& protocolName = kv.first;
        int totalRank = kv.second;

        if (counts[protocolName] == static_cast<int>(AllWorkerStates.size())) {
            if (totalRank < bestSum) {
                bestSum = totalRank;
                bestProtocol = protocolName;
            }
        }
    }

    if (bestProtocol.empty()) {
        return false;
    }

    Protocol = bestProtocol;

    for (const auto& kv : AllWorkerStates) {
        const auto& memberId = kv.first;
        const auto& protocols = kv.second.protocols();

        for (const auto& p : protocols) {
            if (p.protocol_name() == Protocol) {
                WorkerStates[memberId] = p.metadata();
                break;
            }
        }
    }

    return true;
}

NYdb::TParamsBuilder TKafkaBalancerActor::BuildCheckGroupStateParams() {
    NYdb::TParamsBuilder params;
    params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
    params.AddParam("$Database").Utf8(Kqp->DataBase).Build();

    return params;
}

NYdb::TParamsBuilder TKafkaBalancerActor::BuildSetMasterDeadParams() {
    NYdb::TParamsBuilder params;
    params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
    params.AddParam("$Database").Utf8(Kqp->DataBase).Build();
    params.AddParam("$State").Uint64(GROUP_STATE_MASTER_IS_DEAD).Build();
    params.AddParam("$Generation").Uint64(GenerationId).Build();

    return params;
}

NYdb::TParamsBuilder TKafkaBalancerActor::BuildUpdateOrInsertNewGroupParams() {
    NYdb::TParamsBuilder params;
    params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
    params.AddParam("$Generation").Uint64(GenerationId).Build();
    params.AddParam("$State").Uint64(GROUP_STATE_JOIN).Build();
    params.AddParam("$Database").Utf8(Kqp->DataBase).Build();
    params.AddParam("$Master").Utf8(MemberId).Build();
    params.AddParam("$LastMasterHeartbeat").Datetime(TInstant::Now()).Build();
    params.AddParam("$ProtocolType").Utf8(ProtocolType).Build();
    params.AddParam("$GroupsCountCheckDeadline").Datetime(TInstant::Now() - TDuration::MilliSeconds(MAX_SESSION_TIMEOUT_MS)).Build();

    return params;
}

NYdb::TParamsBuilder TKafkaBalancerActor::BuildCheckGroupsCountParams() {
    NYdb::TParamsBuilder params;
    params.AddParam("$GroupsCountCheckDeadline").Datetime(TInstant::Now() - TDuration::MilliSeconds(MAX_SESSION_TIMEOUT_MS)).Build();

    return params;
}

NYdb::TParamsBuilder TKafkaBalancerActor::BuildInsertMemberParams() {
    NYdb::TParamsBuilder params;
    params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
    params.AddParam("$Generation").Uint64(GenerationId).Build();
    params.AddParam("$MemberId").Utf8(MemberId).Build();
    params.AddParam("$InstanceId").Utf8(InstanceId).Build();
    params.AddParam("$Database").Utf8(Kqp->DataBase).Build();
    params.AddParam("$HeartbeatDeadline").Datetime(TInstant::Now() + TDuration::MilliSeconds(SessionTimeoutMs)).Build();
    params.AddParam("$SessionTimeoutMs").Uint32(SessionTimeoutMs).Build();
    params.AddParam("$RebalanceTimeoutMs").Uint32(RebalanceTimeoutMs).Build();

    NKafka::TWorkerState workerState;
    for (const auto& protocol : JoinGroupRequestData->Protocols) {
        auto* item = workerState.add_protocols();
        item->set_protocol_name(protocol.Name.value());

        auto dataRef = protocol.Metadata.value();
        item->mutable_metadata()->assign(dataRef.data(), dataRef.size());
    }

    TString serializedWorkerState = workerState.SerializeAsString();
    params.AddParam("$WorkerStateProto").String(serializedWorkerState).Build();

    return params;
}

NYdb::TParamsBuilder TKafkaBalancerActor::BuildAssignmentsParams() {
    NYdb::TParamsBuilder params;
    params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
    params.AddParam("$Database").Utf8(Kqp->DataBase).Build();
    params.AddParam("$Generation").Uint64(GenerationId).Build();
    params.AddParam("$State").Uint64(GROUP_STATE_WORKING).Build();
    params.AddParam("$LastMasterHeartbeat").Datetime(TInstant::Now()).Build();

    auto& assignmentList = params.AddParam("$Assignments").BeginList();

    KAFKA_LOG_D(TStringBuilder() << "Assignments count: " << SyncGroupRequestData->Assignments.size());

    for (auto& assignment: SyncGroupRequestData->Assignments) {
        assignmentList.AddListItem()
            .BeginStruct()
                .AddMember("MemberId").Utf8(assignment.MemberId.value())
                .AddMember("Assignment").String(TString(assignment.Assignment.value().data(),
                        assignment.Assignment.value().size()))
            .EndStruct();
    }
    assignmentList.EndList().Build();

    return params;
}

NYdb::TParamsBuilder TKafkaBalancerActor::BuildSelectMembersParams(ui64 generationId) {
    NYdb::TParamsBuilder params;
    params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
    params.AddParam("$Database").Utf8(Kqp->DataBase).Build();
    params.AddParam("$Generation").Uint64(generationId).Build();
    params.AddParam("$Limit").Uint64(LIMIT_MEMBERS_PER_REQUEST).Build();
    params.AddParam("$PaginationMemberId").Utf8(WorkerStatesPaginationMemberId).Build();

    return params;
}

NYdb::TParamsBuilder TKafkaBalancerActor::BuildUpdateGroupStateAndProtocolParams() {
    NYdb::TParamsBuilder params;
    params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
    params.AddParam("$State").Uint64(GROUP_STATE_SYNC).Build();
    params.AddParam("$Database").Utf8(Kqp->DataBase).Build();
    params.AddParam("$Protocol").Utf8(Protocol).Build();
    params.AddParam("$LastMasterHeartbeat").Datetime(TInstant::Now()).Build();

    return params;
}

NYdb::TParamsBuilder TKafkaBalancerActor::BuildFetchAssignmentsParams() {
    NYdb::TParamsBuilder params;
    params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
    params.AddParam("$Generation").Uint64(GenerationId).Build();
    params.AddParam("$MemberId").Utf8(MemberId).Build();
    params.AddParam("$Database").Utf8(Kqp->DataBase).Build();

    return params;
}

NYdb::TParamsBuilder TKafkaBalancerActor::BuildLeaveGroupParams() {
    NYdb::TParamsBuilder params;
    params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
    params.AddParam("$Generation").Uint64(GenerationId).Build();
    params.AddParam("$MemberId").Utf8(MemberId).Build();
    params.AddParam("$Database").Utf8(Kqp->DataBase).Build();
    params.AddParam("$LastMasterHeartbeat").Datetime(TInstant::Now() - TDuration::Seconds(1)).Build();
    params.AddParam("$Database").Utf8(Kqp->DataBase).Build();
    params.AddParam("$State").Uint64(GROUP_STATE_MASTER_IS_DEAD).Build();

    if (IsMaster) {
        params.AddParam("$UpdateState").Bool(true).Build();
    } else {
        params.AddParam("$UpdateState").Bool(false).Build();
    }

    return params;
}

NYdb::TParamsBuilder TKafkaBalancerActor::BuildGetMemberParams() {
    NYdb::TParamsBuilder params;
    params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
    params.AddParam("$MemberId").Utf8(MemberId).Build();
    params.AddParam("$Database").Utf8(Kqp->DataBase).Build();

    return params;
}

NYdb::TParamsBuilder TKafkaBalancerActor::BuildUpdateLastHeartbeatsParams() {
    NYdb::TParamsBuilder params;
    auto now = TInstant::Now();
    params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
    params.AddParam("$Generation").Uint64(GenerationId).Build();
    params.AddParam("$MemberId").Utf8(MemberId).Build();
    params.AddParam("$Database").Utf8(Kqp->DataBase).Build();
    params.AddParam("$LastMasterHeartbeat").Datetime(now).Build();
    params.AddParam("$HeartbeatDeadline").Datetime(now + TDuration::MilliSeconds(SessionTimeoutMs)).Build();

    if (IsMaster) {
        params.AddParam("$UpdateGroupHeartbeat").Bool(true).Build();
    } else {
        params.AddParam("$UpdateGroupHeartbeat").Bool(false).Build();
    }

    return params;
}

NYdb::TParamsBuilder TKafkaBalancerActor::BuildCheckDeadsParams() {
    NYdb::TParamsBuilder params;
    params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
    params.AddParam("$Generation").Uint64(GenerationId).Build();
    params.AddParam("$Database").Utf8(Kqp->DataBase).Build();
    params.AddParam("$MemberId").Utf8(MemberId).Build();
    params.AddParam("$Now").Datetime(TInstant::Now()).Build();

    return params;
}

NYdb::TParamsBuilder TKafkaBalancerActor::BuildCheckMasterAlive() {
    NYdb::TParamsBuilder params;
    params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
    params.AddParam("$MasterId").Utf8(Master).Build();
    params.AddParam("$Database").Utf8(Kqp->DataBase).Build();
    params.AddParam("$Generation").Uint64(GenerationId).Build();
    params.AddParam("$Now").Datetime(TInstant::Now()).Build();

    return params;
}

void TKafkaBalancerActor::SendResponseFail(const TActorContext& ctx, EKafkaErrors error, const TString& message) {
    switch (RequestType) {
        case JOIN_GROUP:
            SendJoinGroupResponseFail(ctx, CorrelationId, error, message);
            break;
        case SYNC_GROUP:
            SendSyncGroupResponseFail(ctx, CorrelationId, error, message);
            break;
        case LEAVE_GROUP:
            SendLeaveGroupResponseFail(ctx, CorrelationId, error, message);
            break;
        case HEARTBEAT:
            SendHeartbeatResponseFail(ctx, CorrelationId, error, message);
            break;
        default:
            break;
    }
}

void TKafkaBalancerActor::SendJoinGroupResponseOk(const TActorContext& ctx, ui64 correlationId) {
    KAFKA_LOG_I(TStringBuilder() << "JOIN_GROUP success.");
    auto response = std::make_shared<TJoinGroupResponseData>();

    response->ProtocolType = ProtocolType;
    response->ProtocolName = Protocol;
    response->ErrorCode    = EKafkaErrors::NONE_ERROR;
    response->GenerationId = GenerationId;
    response->MemberId     = MemberId;

    response->Leader = Master;

    if (IsMaster) {
        response->Members.reserve(WorkerStates.size());
        for (const auto& [mId, meta] : WorkerStates) {
            TJoinGroupResponseData::TJoinGroupResponseMember member;
            member.MemberId = mId;
            member.MetaStr = meta;
            member.Metadata = member.MetaStr;
            response->Members.push_back(std::move(member));
        }
    }

    Send(Context->ConnectionId, new TEvKafka::TEvResponse(correlationId, response, EKafkaErrors::NONE_ERROR));
    Die(ctx);
}

void TKafkaBalancerActor::SendSyncGroupResponseOk(const TActorContext& ctx, ui64 correlationId) {
    KAFKA_LOG_I(TStringBuilder() << "SYNC_GROUP success.");
    auto response = std::make_shared<TSyncGroupResponseData>();
    response->ProtocolType = ProtocolType;
    response->ProtocolName = Protocol;
    response->ErrorCode = EKafkaErrors::NONE_ERROR;
    response->AssignmentStr = TString(Assignments.data(), Assignments.size());
    response->Assignment = response->AssignmentStr;
    Send(Context->ConnectionId, new TEvKafka::TEvResponse(correlationId, response, EKafkaErrors::NONE_ERROR));
    Die(ctx);
}

void TKafkaBalancerActor::SendLeaveGroupResponseOk(const TActorContext& ctx, ui64 corellationId) {
    KAFKA_LOG_I(TStringBuilder() << "LEAVE_GROUP success.");
    auto response = std::make_shared<TLeaveGroupResponseData>();
    response->ErrorCode = EKafkaErrors::NONE_ERROR;
    Send(Context->ConnectionId, new TEvKafka::TEvResponse(corellationId, response, EKafkaErrors::NONE_ERROR));
    Die(ctx);
}

void TKafkaBalancerActor::SendHeartbeatResponseOk(const TActorContext& ctx,
                                                  ui64 corellationId,
                                                  EKafkaErrors error) {
    KAFKA_LOG_I(TStringBuilder() << "HEARTBEAT success.");
    auto response = std::make_shared<THeartbeatResponseData>();
    response->ErrorCode = error;
    Send(Context->ConnectionId, new TEvKafka::TEvResponse(corellationId, response, error));
    Die(ctx);
}


void TKafkaBalancerActor::SendJoinGroupResponseFail(const TActorContext& ctx,
                                                    ui64 corellationId,
                                                    EKafkaErrors error,
                                                    TString message) {
    KAFKA_LOG_ERROR(TStringBuilder() << "JOIN_GROUP failed. reason# " << message);
    auto response = std::make_shared<TJoinGroupResponseData>();
    response->ErrorCode = error;
    Send(Context->ConnectionId, new TEvKafka::TEvResponse(corellationId, response, error));
    Die(ctx);
}

void TKafkaBalancerActor::SendSyncGroupResponseFail(const TActorContext& ctx,
                                                    ui64 corellationId,
                                                    EKafkaErrors error,
                                                    TString message) {
    KAFKA_LOG_ERROR(TStringBuilder() << "SYNC_GROUP failed. reason# " << message);
    auto response = std::make_shared<TSyncGroupResponseData>();
    response->ErrorCode = error;

    response->AssignmentStr = "";
    response->Assignment = response->AssignmentStr;

    Send(Context->ConnectionId, new TEvKafka::TEvResponse(corellationId, response, error));
    Die(ctx);
}

void TKafkaBalancerActor::SendLeaveGroupResponseFail(const TActorContext& ctx,
                                                     ui64 corellationId,
                                                     EKafkaErrors error,
                                                     TString message) {
    KAFKA_LOG_ERROR(TStringBuilder() << "LEAVE_GROUP failed. reason# " << message);
    auto response = std::make_shared<TLeaveGroupResponseData>();
    response->ErrorCode = error;
    Send(Context->ConnectionId, new TEvKafka::TEvResponse(corellationId, response, error));
    Die(ctx);
}

void TKafkaBalancerActor::SendHeartbeatResponseFail(const TActorContext& ctx,
                                                    ui64 corellationId,
                                                    EKafkaErrors error,
                                                    TString message) {
    KAFKA_LOG_ERROR(TStringBuilder() << "HEARTBEAT failed. reason# " << message);
    auto response = std::make_shared<THeartbeatResponseData>();
    response->ErrorCode = error;
    Send(Context->ConnectionId, new TEvKafka::TEvResponse(corellationId, response, error));
    Die(ctx);
}

TString TKafkaBalancerActor::LogPrefix() {
    TStringBuilder sb;
    sb << "TKafkaBalancerActor: GroupId# " << GroupId << ", MemberId# " << MemberId << ", CurrentStep# " << static_cast<int>(CurrentStep) << ". ";
    return sb;
}

} // namespace NKafka
