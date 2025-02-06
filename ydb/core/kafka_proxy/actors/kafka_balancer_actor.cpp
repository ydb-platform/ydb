#include "kafka_balancer_actor.h"
#include "kqp_balance_transaction.h"

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
static constexpr ui8 WAIT_STATE_MAX_RETRY_COUNT = 6;

void TKafkaBalancerActor::Bootstrap(const NActors::TActorContext& ctx) {
    Kqp = std::make_unique<TKqpTxHelper>(Context->DatabasePath);
    Kqp->SendInitTablesRequest(ctx);
    Become(&TKafkaBalancerActor::StateWork);
    if (!MemberId) {
        MemberId = SelfId().ToString();
    }
}

void TKafkaBalancerActor::Handle(NMetadata::NProvider::TEvManagerPrepared::TPtr&, const TActorContext& ctx) {
    TablesInited++;
    if (TablesInited == 2) {
        Kqp->SendCreateSessionRequest(ctx);
    }
}

void TKafkaBalancerActor::Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx) {
    Cookie = 0;
    const TString createSessionError = "Failed to create KQP session";
    if (!Kqp->HandleCreateSessionResponse(ev, ctx)) {
        switch (RequestType) {
            case JOIN_GROUP:
                SendJoinGroupResponseFail(ctx, CorrelationId,
                                          EKafkaErrors::UNKNOWN_SERVER_ERROR,
                                          createSessionError);
                break;
            case SYNC_GROUP:
                SendSyncGroupResponseFail(ctx, CorrelationId,
                                          EKafkaErrors::UNKNOWN_SERVER_ERROR,
                                          createSessionError);
                break;
            case LEAVE_GROUP:
                SendLeaveGroupResponseFail(ctx, CorrelationId,
                                           EKafkaErrors::UNKNOWN_SERVER_ERROR,
                                           createSessionError);
                break;
            case HEARTBEAT:
                SendHeartbeatResponseFail(ctx, CorrelationId,
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
    if (status == ::Ydb::StatusIds_StatusCode::StatusIds_StatusCode_ABORTED) {
        switch (RequestType) {
            case JOIN_GROUP:
                Register(new TKafkaBalancerActor(Context, Cookie, CorrelationId, JoinGroupRequestData));
                break;
            case SYNC_GROUP:
                Register(new TKafkaBalancerActor(Context, Cookie, CorrelationId, SyncGroupRequestData));
                break;
            case LEAVE_GROUP:
                Register(new TKafkaBalancerActor(Context, Cookie, CorrelationId, LeaveGroupRequestData));
                break;
            case HEARTBEAT:
                Register(new TKafkaBalancerActor(Context, Cookie, CorrelationId, HeartbeatGroupRequestData));
                break;
            default:
                break;
        }

        PassAway();
        return;
    }

    auto kafkaErr = KqpStatusToKafkaError(status);

    if (kafkaErr != EKafkaErrors::NONE_ERROR) {
        switch (RequestType) {
            case JOIN_GROUP:
                SendJoinGroupResponseFail(ctx, CorrelationId, kafkaErr, kqpQueryError);
                break;
            case SYNC_GROUP:
                SendSyncGroupResponseFail(ctx, CorrelationId, kafkaErr, kqpQueryError);
                break;
            case LEAVE_GROUP:
                SendLeaveGroupResponseFail(ctx, CorrelationId, kafkaErr, kqpQueryError);
                break;
            case HEARTBEAT:
                SendHeartbeatResponseFail(ctx, CorrelationId, kafkaErr, kqpQueryError);
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
    ui64& outGeneration,
    ui64& outState,
    TString& outMasterId,
    TInstant& outTtl
) {
    if (!ev) {
        return false;
    }

    auto& record = ev->Get()->Record;
    auto& resp = record.GetResponse();
    if (resp.GetYdbResults().empty()) {
        outGroupExists = false;
        return true;
    }

    NYdb::TResultSetParser parser(resp.GetYdbResults(0));
    if (!parser.TryNextRow()) {
        outGroupExists = false;
        return true;
    }

    outState = parser.ColumnParser("state").GetOptionalUint64().GetOrElse(0);
    outGeneration = parser.ColumnParser("generation").GetOptionalUint64().GetOrElse(0);
    outMasterId = parser.ColumnParser("master").GetOptionalUtf8().GetOrElse("");
    outTtl = parser.ColumnParser("last_heartbeat_time").GetOptionalDatetime().GetOrElse(TInstant::Zero());

    outGroupExists = true;
    if (parser.TryNextRow()) {
        return false;
    }

    return true;
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

    assignments = parser.ColumnParser("assignment").GetOptionalString().GetOrElse("");

    if (parser.TryNextRow()) {
        return false;
    }

    return !assignments.empty();
}


bool TKafkaBalancerActor::ParseWorkerStatesAndChooseProtocol(
    NKqp::TEvKqp::TEvQueryResponse::TPtr ev,
    std::unordered_map<TString, TString>& workerStates,
    TString& chosenProtocol)
{
    if (!ev) {
        return false;
    }

    auto& record = ev->Get()->Record;
    if (record.GetResponse().GetYdbResults().empty()) {
        return false;
    }

    NYdb::TResultSetParser parser(record.GetResponse().GetYdbResults(0));

    struct TParsedState {
        TString MemberId;
        NKafka::TWorkerState WorkerState;
    };

    std::vector<TParsedState> states;
    while (parser.TryNextRow()) {
        TString protoStr = parser.ColumnParser("worker_state_proto").GetOptionalString().GetOrElse("");
        TString memberId = parser.ColumnParser("member_id").GetOptionalUtf8().GetOrElse("");

        NKafka::TWorkerState workerState;
        if (!protoStr.empty() && !workerState.ParseFromString(protoStr)) {
            return false;
        }

        TParsedState st;
        st.MemberId = memberId;
        st.WorkerState = std::move(workerState);
        states.push_back(std::move(st));
    }

    if (states.empty()) {
        return false;
    }

    const auto& firstWorkerProtocols = states.front().WorkerState.protocols();
    for (const auto& protocol : firstWorkerProtocols) {
        const TString& candidateName = protocol.protocol_name();
        bool allHaveCandidate = true;

        for (size_t i = 1; i < states.size(); ++i) {
            const auto& otherWorkerProtocols = states[i].WorkerState.protocols();
            bool foundThisCandidate = false;
            for (const auto& otherProtocol : otherWorkerProtocols) {
                if (otherProtocol.protocol_name() == candidateName) {
                    foundThisCandidate = true;
                    break;
                }
            }

            if (!foundThisCandidate) {
                allHaveCandidate = false;
                break;
            }
        }

        if (allHaveCandidate) {
            chosenProtocol = candidateName;
            break;
        }
    }

    if (chosenProtocol.empty()) {
        return false;
    }

    for (const auto& st : states) {
        const auto& protos = st.WorkerState.protocols();
        for (const auto& pr : protos) {
            if (pr.protocol_name() == chosenProtocol) {
                workerStates[st.MemberId] = pr.metadata();
                break;
            }
        }
    }

    return true;
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
            Kqp->TxId = ev->Get()->Record.GetResponse().GetTxMeta().id();
            CurrentStep  = JOIN_TX0_1_CHECK_STATE_AND_GENERATION;
            KqpReqCookie++;

            // savnik yql compile?
            NYdb::TParamsBuilder params;
            params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
            params.AddParam("$Database").Utf8(Kqp->DataBase).Build();

            Kqp->SendYqlRequest(Sprintf(CHECK_GROUP_STATE.c_str(), TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx);
            break;
        }

        case JOIN_TX0_1_CHECK_STATE_AND_GENERATION: {
            bool groupExists   = false;
            ui64 oldGeneration = 0;
            ui64 state         = 0;
            TString masterId = "";
            TInstant lastHeartbeatTime;


            if (!ParseCheckStateAndGeneration(ev, groupExists, oldGeneration, state, masterId, lastHeartbeatTime)) {
                SendJoinGroupResponseFail(ctx, CorrelationId,
                                            EKafkaErrors::GROUP_ID_NOT_FOUND,
                                            "Can't get group state");
                PassAway();
                return;
            }

            TString yqlRequest;
            EBalancerStep nextStep;

            ui64 newGeneration = 0;
            if (!groupExists) {
                nextStep = JOIN_TX0_2_INSERT_NEW_GROUP;
                yqlRequest = INSERT_NEW_GROUP;
            } else if (state != GROUP_STATE_JOIN) {
                newGeneration = oldGeneration + 1;
                nextStep = JOIN_TX0_2_UPDATE_GROUP_STATE_AND_GENERATION;
                yqlRequest = UPDATE_GROUP;
            } else {
                IsMaster = false;
                CurrentStep = JOIN_TX0_2_SKIP;
                GenerationId = oldGeneration;
                HandleJoinGroupResponse(ev, ctx);
                return;
            }

            GenerationId = newGeneration;
            CurrentStep = nextStep;
            KqpReqCookie++;

            NYdb::TParamsBuilder params;
            params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
            params.AddParam("$Generation").Uint64(GenerationId).Build();
            params.AddParam("$State").Uint64(GROUP_STATE_JOIN).Build();
            params.AddParam("$Database").Utf8(Kqp->DataBase).Build();
            params.AddParam("$Master").Utf8(MemberId).Build();
            params.AddParam("$LastHeartbeat").Datetime(TInstant::Now()).Build();

            Kqp->SendYqlRequest(Sprintf(yqlRequest.c_str(), TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx);

            break;
        }

        case JOIN_TX0_2_SKIP:
        case JOIN_TX0_2_INSERT_NEW_GROUP:
        case JOIN_TX0_2_UPDATE_GROUP_STATE_AND_GENERATION: {
            if (CurrentStep != JOIN_TX0_2_SKIP) {
                IsMaster = true;
                Master = SelfId().ToString();
            }

            CurrentStep  = JOIN_TX0_3_INSERT_MEMBER;
            KqpReqCookie++;


            NYdb::TParamsBuilder params;
            params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
            params.AddParam("$Generation").Uint64(GenerationId).Build();
            params.AddParam("$MemberId").Utf8(MemberId).Build();
            params.AddParam("$Database").Utf8(Kqp->DataBase).Build();
            params.AddParam("$LastHeartbeat").Datetime(TInstant::Now()).Build();

            NKafka::TWorkerState workerState;
            for (const auto& protocol : JoinGroupRequestData->Protocols) {
                auto* item = workerState.add_protocols();
                item->set_protocol_name(protocol.Name.value());

                auto dataRef = protocol.Metadata.value();
                item->mutable_metadata()->assign(dataRef.data(), dataRef.size());
            }

            TString serializedWorkerState = workerState.SerializeAsString();

            params.AddParam("$WorkerStateProto").String(serializedWorkerState).Build();

            Kqp->SendYqlRequest(Sprintf(INSERT_MEMBER.c_str(), TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx);
            break;
        }


        case JOIN_TX0_3_INSERT_MEMBER: {
            CurrentStep  = JOIN_TX0_4_COMMIT_TX;
            KqpReqCookie++;
            Kqp->CommitTx(KqpReqCookie, ctx);
            break;
        }

        case JOIN_TX0_4_COMMIT_TX: {
            CurrentStep = JOIN_TX0_5_WAIT;
            auto wakeup = std::make_unique<TEvents::TEvWakeup>(0);
            ctx.ActorSystem()->Schedule(
                TDuration::Seconds(WAKE_UP_DELAY_SECONDS),
                new IEventHandle(SelfId(), SelfId(), wakeup.release())
            );
            break;
        }

        case JOIN_TX0_5_WAIT: {
            KqpReqCookie++;
            CurrentStep = JOIN_TX1_0_BEGIN_TX;
            Kqp->BeginTransaction(KqpReqCookie, ctx);
            break;
        }

        case JOIN_TX1_0_BEGIN_TX: {
            CurrentStep = JOIN_TX1_1_CHECK_STATE_AND_GENERATION;
            KqpReqCookie++;
            Kqp->TxId = ev->Get()->Record.GetResponse().GetTxMeta().id();

            NYdb::TParamsBuilder params;
            params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
            params.AddParam("$Database").Utf8(Kqp->DataBase).Build();

            Kqp->SendYqlRequest(Sprintf(CHECK_GROUP_STATE.c_str(), TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx);
            break;
        }

        case JOIN_TX1_1_CHECK_STATE_AND_GENERATION: {
            bool groupExists = false;
            ui64 generation  = 0;
            ui64 state       = 0;
            TString masterId;
            TInstant groupTtl;

            if (!ParseCheckStateAndGeneration(ev, groupExists, generation, state, masterId, groupTtl)) {//her
                SendJoinGroupResponseFail(ctx, CorrelationId,
                            EKafkaErrors::REBALANCE_IN_PROGRESS,
                            "Rebalance");
                PassAway();
                return;
            }

            if (IsMaster) {
                if (!groupExists || state != GROUP_STATE_JOIN || generation != GenerationId) {
                    SendJoinGroupResponseFail(ctx, CorrelationId,
                    EKafkaErrors::REBALANCE_IN_PROGRESS,
                    "Rebalance");
                    PassAway();
                    return;
                }

                CurrentStep  = JOIN_TX1_2_GET_MEMBERS_AND_SET_STATE_SYNC;
                KqpReqCookie++;
                NYdb::TParamsBuilder params;
                params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
                params.AddParam("$State").Uint64(GROUP_STATE_JOINED).Build();
                params.AddParam("$Generation").Uint64(GenerationId).Build();
                params.AddParam("$Database").Utf8(Kqp->DataBase).Build();
                params.AddParam("$LastHeartbeat").Datetime(TInstant::Now()).Build();

                auto sql = Sprintf(UPDATE_GROUPS_AND_SELECT_WORKER_STATES.c_str(), TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str(), TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath().c_str());

                Kqp->SendYqlRequest(sql, params.Build(), KqpReqCookie, ctx);
            } else {
                if (!groupExists || generation != GenerationId) {
                    SendJoinGroupResponseFail(ctx, CorrelationId,
                    EKafkaErrors::REBALANCE_IN_PROGRESS,
                    "Rebalance");
                    PassAway();
                    return;
                }

                if (state != GROUP_STATE_SYNC) {
                    if (WaitingWorkingStateRetries == WAIT_STATE_MAX_RETRY_COUNT) {
                        SendJoinGroupResponseFail(ctx, CorrelationId, REBALANCE_IN_PROGRESS);
                        PassAway();
                        return;
                    }

                    CurrentStep = JOIN_TX0_5_WAIT;
                    auto wakeup = std::make_unique<TEvents::TEvWakeup>(1);
                    ctx.ActorSystem()->Schedule(
                        TDuration::Seconds(WAKE_UP_DELAY_SECONDS),
                        new IEventHandle(SelfId(), SelfId(), wakeup.release())
                    );
                    WaitingWorkingStateRetries++;
                    return;
                }

                CurrentStep = JOIN_TX1_2_GET_MEMBERS_AND_SET_STATE_SYNC;
                HandleJoinGroupResponse(ev, ctx);
                return;
            }
            break;
        }

        case JOIN_TX1_2_GET_MEMBERS_AND_SET_STATE_SYNC: {
            if (IsMaster) {
                if (!ParseWorkerStatesAndChooseProtocol(ev, WorkerStates, Protocol)) {
                    SendJoinGroupResponseFail(ctx, CorrelationId,
                EKafkaErrors::INVALID_REQUEST,
                "Can't get workers state");
                    PassAway();
                    return;
                }

                KqpReqCookie++;
                NYdb::TParamsBuilder params;
                params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
                params.AddParam("$State").Uint64(GROUP_STATE_SYNC).Build();
                params.AddParam("$Database").Utf8(Kqp->DataBase).Build();
                params.AddParam("$Protocol").Utf8(Protocol).Build();
                params.AddParam("$LastHeartbeat").Datetime(TInstant::Now()).Build();

                Kqp->SendYqlRequest(Sprintf(UPDATE_GROUP_STATE_AND_PROTOCOL.c_str(), TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx, true);
            } else {
                KqpReqCookie++;
                Kqp->CommitTx(KqpReqCookie, ctx);
            }

            CurrentStep  = JOIN_TX1_3_COMMIT_TX;

            break;
        }

        case JOIN_TX1_3_COMMIT_TX: {
            SendJoinGroupResponseOk(ctx, CorrelationId);
            PassAway();
            break;
        }

        default:
            KAFKA_LOG_CRIT("JOIN_GROUP: Unexpected step" );
                            SendJoinGroupResponseFail(ctx, CorrelationId,
            EKafkaErrors::UNKNOWN_SERVER_ERROR,
            "Unexpected step");
            PassAway();
            return;
            break;
    } // switch (CurrentStep)
}

bool TKafkaBalancerActor::ParseDeadCount(
    NKqp::TEvKqp::TEvQueryResponse::TPtr ev,
    ui64& deadCount
) {
    if (!ev) {
        return false;
    }

    auto& record = ev->Get()->Record;
    auto& resp = record.GetResponse();
    if (resp.GetYdbResults().empty()) {
        return false;
    }

    NYdb::TResultSetParser parser(resp.GetYdbResults(0));
    if (!parser.TryNextRow()) {
        return false;
    }

    deadCount = parser.ColumnParser(0).GetUint64();
    if (parser.TryNextRow()) {
        return false;
    }

    return true;
}

void TKafkaBalancerActor::Handle(TEvents::TEvWakeup::TPtr&, const TActorContext& ctx) {
    if (RequestType == JOIN_GROUP) {
        HandleJoinGroupResponse(nullptr, ctx);
    } else if (RequestType == SYNC_GROUP) {
        HandleSyncGroupResponse(nullptr, ctx);
    }
}

void TKafkaBalancerActor::Die(const TActorContext& ctx) {
    KAFKA_LOG_D("TKafkaBalancerActor pass away");
    TBase::Die(ctx);
}

void TKafkaBalancerActor::SendJoinGroupResponseOk(const TActorContext& /*ctx*/, ui64 correlationId) {
    auto response = std::make_shared<TJoinGroupResponseData>();

    response->ProtocolType = "consumer";  //savnik
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
            TBuffer buffer(member.Metadata.value().data() + sizeof(TKafkaVersion), member.Metadata.value().size_bytes() - sizeof(TKafkaVersion));
            response->Members.push_back(std::move(member));
        }
    }

    Send(Context->ConnectionId, new TEvKafka::TEvResponse(correlationId, response, EKafkaErrors::NONE_ERROR));
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
            CurrentStep = SYNC_TX0_2_CHECK_STATE_AND_GENERATION;
            KqpReqCookie++;
            Kqp->TxId = ev->Get()->Record.GetResponse().GetTxMeta().id();

            NYdb::TParamsBuilder params;
            params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
            params.AddParam("$Database").Utf8(Kqp->DataBase).Build();
            Kqp->SendYqlRequest(Sprintf(CHECK_GROUP_STATE.c_str(), TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx);
            break;
        }

        case SYNC_TX0_2_CHECK_STATE_AND_GENERATION: {
            bool groupExists = false;
            ui64 generation  = 0;
            ui64 state       = 0;
            TString masterId;
            TInstant groupTtl;

            if (!ParseCheckStateAndGeneration(ev, groupExists, generation, state, masterId, groupTtl) ||
                !groupExists || generation != GenerationId || state != GROUP_STATE_SYNC) { //
                SendSyncGroupResponseFail(ctx, CorrelationId,
                                        EKafkaErrors::UNKNOWN_SERVER_ERROR,
                                        "Group state or generation mismatch");
                PassAway();
                return;
            }

            Master = masterId;

            if (MemberId == masterId) {
                IsMaster = true;
            }

            CurrentStep = SYNC_TX0_3_SET_ASSIGNMENTS_AND_SET_WORKING_STATE;

            if (IsMaster) {
                KqpReqCookie++;
                NYdb::TParamsBuilder params;
                params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
                params.AddParam("$Database").Utf8(Kqp->DataBase).Build();
                params.AddParam("$Generation").Uint64(GenerationId).Build();
                params.AddParam("$State").Uint64(GROUP_STATE_WORKING).Build();
                params.AddParam("$LastHeartbeat").Datetime(TInstant::Now()).Build();

                if (SyncGroupRequestData->Assignments.size() == 0) {
                    SendSyncGroupResponseFail(ctx, CorrelationId, EKafkaErrors::INVALID_REQUEST);
                    PassAway();
                    return;
                }

                auto& assignmentList = params.AddParam("$Assignments").BeginList();
                for (auto& assignment: SyncGroupRequestData->Assignments) {
                    assignmentList.AddListItem()
                        .BeginStruct()
                            .AddMember("MemberId").Utf8(assignment.MemberId.value())
                            .AddMember("Assignment").String(TString(assignment.Assignment.value().data(),
                                    assignment.Assignment.value().size()))
                        .EndStruct();
                }
                assignmentList.EndList().Build();
                Kqp->SendYqlRequest(Sprintf(UPSERT_ASSIGNMENTS_AND_SET_WORKING_STATE.c_str(),  TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath().c_str(), TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx);
            } else {
                HandleSyncGroupResponse(nullptr, ctx);
            }
            break;
        }

        case SYNC_TX0_3_SET_ASSIGNMENTS_AND_SET_WORKING_STATE: {
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

            Kqp->TxId = ev->Get()->Record.GetResponse().GetTxMeta().id();

            NYdb::TParamsBuilder params;
            params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
            params.AddParam("$Database").Utf8(Kqp->DataBase).Build();

            Kqp->SendYqlRequest(Sprintf(CHECK_GROUP_STATE.c_str(), TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx);
            break;
        }

        case SYNC_TX1_1_CHECK_STATE: {
            bool groupExists = false;
            ui64 generation  = 0;
            ui64 state       = 0;
            TString masterId;
            TInstant groupTtl;

            if (!ParseCheckStateAndGeneration(ev, groupExists, generation, state, masterId, groupTtl) || !groupExists || generation != GenerationId) {
                SendSyncGroupResponseFail(ctx, CorrelationId,
                                            EKafkaErrors::GROUP_ID_NOT_FOUND,
                                            "Group state or generation mismatch");
                PassAway();
                return;
            }

            if (state != GROUP_STATE_WORKING) {
                if (WaitingWorkingStateRetries == WAIT_STATE_MAX_RETRY_COUNT) {
                    SendSyncGroupResponseFail(ctx, CorrelationId, REBALANCE_IN_PROGRESS);
                    PassAway();
                    return;
                }
                CurrentStep = SYNC_TX0_4_COMMIT_TX;
                auto wakeup = std::make_unique<TEvents::TEvWakeup>(2);
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
            params.AddParam("$Database").Utf8(Kqp->DataBase).Build();

            Kqp->SendYqlRequest(Sprintf(FETCH_ASSIGNMENTS.c_str(), TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx);
            break;
        }

        case SYNC_TX1_2_FETCH_ASSIGNMENTS: {
            if (!ParseAssignments(ev, Assignments)) {
                SendSyncGroupResponseFail(ctx, CorrelationId,
                                            EKafkaErrors::INVALID_REQUEST,
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
            SendSyncGroupResponseOk(ctx, CorrelationId);
            PassAway();
            break;
        }

        default: {
            KAFKA_LOG_CRIT("SYNC_GROUP: Unexpected step in HandleSyncGroupResponse");
            PassAway();
            break;
        }
    } // switch (CurrentStep)
}

void TKafkaBalancerActor::HandleLeaveGroupResponse(
    NKqp::TEvKqp::TEvQueryResponse::TPtr ev,
    const TActorContext& ctx
) {
    switch (CurrentStep) {
        case STEP_NONE: {
            CurrentStep  = LEAVE_TX0_0_BEGIN_TX;
            KqpReqCookie++;
            Kqp->BeginTransaction(KqpReqCookie, ctx);
            break;
        }

        case LEAVE_TX0_0_BEGIN_TX: {
            CurrentStep = LEAVE_TX0_1_UPDATE_TTL;
            KqpReqCookie++;
            Kqp->TxId = ev->Get()->Record.GetResponse().GetTxMeta().id();

            NYdb::TParamsBuilder params;
            params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
            params.AddParam("$MemberId").Utf8(MemberId).Build();
            params.AddParam("$Database").Utf8(Kqp->DataBase).Build();
            params.AddParam("$LastHeartbeat").Datetime(TInstant::Now() - TDuration::Hours(1)).Build();

            Kqp->SendYqlRequest(Sprintf(UPDATE_TTL_LEAVE_GROUP.c_str(), TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx);
            break;
        }

        case LEAVE_TX0_1_UPDATE_TTL: {
            if (ev->Get()->Record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
                SendLeaveGroupResponseFail(ctx, CorrelationId, EKafkaErrors::UNKNOWN_SERVER_ERROR, "Leave group update failed");
                PassAway();
                return;
            }

            CurrentStep  = LEAVE_TX0_2_COMMIT_TX;
            KqpReqCookie++;
            Kqp->CommitTx(KqpReqCookie, ctx);
            break;
        }

        case LEAVE_TX0_2_COMMIT_TX: {
            if (ev->Get()->Record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
                SendLeaveGroupResponseFail(ctx, CorrelationId, EKafkaErrors::UNKNOWN_SERVER_ERROR, "Leave group commit failed");
            } else {
                SendLeaveGroupResponseOk(ctx, CorrelationId);
            }
            PassAway();
            break;
        }

        default: {
            KAFKA_LOG_CRIT("LEAVE_GROUP: Unexpected step");
            SendLeaveGroupResponseFail(ctx, CorrelationId, EKafkaErrors::UNKNOWN_SERVER_ERROR, "Unexpected step");
            PassAway();
            break;
        }
    }
}

void TKafkaBalancerActor::HandleHeartbeatResponse(
    NKqp::TEvKqp::TEvQueryResponse::TPtr ev,
    const TActorContext& ctx
) {
    switch (CurrentStep) {
        case STEP_NONE: {
            CurrentStep  = HEARTBEAT_TX0_0_BEGIN_TX;
            KqpReqCookie++;
            Kqp->BeginTransaction(KqpReqCookie, ctx);
            break;
        }

        case HEARTBEAT_TX0_0_BEGIN_TX: {
            CurrentStep  = HEARTBEAT_TX0_1_CHECK_DEAD_MEMBERS;
            KqpReqCookie++;
            Kqp->TxId = ev->Get()->Record.GetResponse().GetTxMeta().id();

            NYdb::TParamsBuilder params;
            params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
            params.AddParam("$Generation").Uint64(GenerationId).Build();
            params.AddParam("$Database").Utf8(Kqp->DataBase).Build();
            params.AddParam("$Deadline").Datetime(TInstant::Now() - TDuration::Seconds(30)).Build();

            Kqp->SendYqlRequest(Sprintf(CHECK_DEAD_MEMBERS.c_str(), TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx);
            break;
        }

        case HEARTBEAT_TX0_1_CHECK_DEAD_MEMBERS: {
            ui64 deadCount = 0;
            if (!ParseDeadCount(ev, deadCount)) {
                SendHeartbeatResponseFail(ctx, CorrelationId, EKafkaErrors::UNKNOWN_SERVER_ERROR, "Can't parse dead members count");
                PassAway();
                return;
            }

            if (deadCount > 0) {
                SendHeartbeatResponseFail(ctx, CorrelationId, EKafkaErrors::REBALANCE_IN_PROGRESS, "Rejoin required");
                PassAway();
                return;
            }

            CurrentStep  = HEARTBEAT_TX0_2_COMMIT_TX;
            KqpReqCookie++;
            Kqp->CommitTx(KqpReqCookie, ctx);
            break;
        }

        case HEARTBEAT_TX0_2_COMMIT_TX: {
            CurrentStep  = HEARTBEAT_TX1_0_BEGIN_TX;
            KqpReqCookie++;
            Kqp->BeginTransaction(KqpReqCookie, ctx);
            break;
        }

        case HEARTBEAT_TX1_0_BEGIN_TX: {
            CurrentStep  = HEARTBEAT_TX1_1_CHECK_GEN_AND_STATE;
            KqpReqCookie++;
            Kqp->TxId = ev->Get()->Record.GetResponse().GetTxMeta().id();

            NYdb::TParamsBuilder params;
            params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
            params.AddParam("$Database").Utf8(Kqp->DataBase).Build();

            Kqp->SendYqlRequest(Sprintf(CHECK_GROUP_STATE.c_str(), TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx);
            break;
        }

        case HEARTBEAT_TX1_1_CHECK_GEN_AND_STATE: {
            bool groupExists = false;
            ui64 generation  = 0;
            ui64 state       = 0;
            TString masterId;
            TInstant groupTtl;

            if (!ParseCheckStateAndGeneration(ev, groupExists, generation, state, masterId, groupTtl) || !groupExists) {
                SendHeartbeatResponseFail(ctx, CorrelationId, EKafkaErrors::REBALANCE_IN_PROGRESS, "Rejoin required");
                PassAway();
                return;
            }

            if (generation != GenerationId || state != GROUP_STATE_WORKING) {
                SendHeartbeatResponseFail(ctx, CorrelationId, EKafkaErrors::REBALANCE_IN_PROGRESS, "Rejoin required");
                PassAway();
                return;
            }

            IsMaster = (masterId == MemberId);
            CurrentStep = HEARTBEAT_TX1_2_UPDATE_TTL;
            KqpReqCookie++;

            NYdb::TParamsBuilder params;
            params.AddParam("$ConsumerGroup").Utf8(GroupId).Build();
            params.AddParam("$Generation").Uint64(GenerationId).Build();
            params.AddParam("$MemberId").Utf8(MemberId).Build();
            params.AddParam("$Database").Utf8(Kqp->DataBase).Build();
            params.AddParam("$LastHeartbeat").Datetime(TInstant::Now()).Build();

            if (IsMaster) {
                params.AddParam("$UpdateGroupHeartbeat").Bool(true).Build();
            } else {
                params.AddParam("$UpdateGroupHeartbeat").Bool(false).Build();
            }

            Kqp->SendYqlRequest(Sprintf(UPDATE_TTLS.c_str(), TKafkaConsumerGroupsMetaInitManager::GetInstant()->GetStorageTablePath().c_str(), TKafkaConsumerMembersMetaInitManager::GetInstant()->GetStorageTablePath().c_str()), params.Build(), KqpReqCookie, ctx);
            break;
        }

        case HEARTBEAT_TX1_2_UPDATE_TTL: {
            CurrentStep  = HEARTBEAT_TX1_3_COMMIT_TX;
            KqpReqCookie++;
            Kqp->CommitTx(KqpReqCookie, ctx);
            break;
        }

        case HEARTBEAT_TX1_3_COMMIT_TX: {
            SendHeartbeatResponseOk(ctx, CorrelationId, EKafkaErrors::NONE_ERROR);
            PassAway();
            break;
        }

        default: {
            KAFKA_LOG_CRIT("HEARTBEAT: Unexpected step");
            SendHeartbeatResponseFail(ctx, CorrelationId, EKafkaErrors::UNKNOWN_SERVER_ERROR, "Unexpected step");
            PassAway();
            break;
        }
    }
}

void TKafkaBalancerActor::SendSyncGroupResponseOk(const TActorContext&, ui64 correlationId) {
    auto response = std::make_shared<TSyncGroupResponseData>();
    response->ProtocolType = "consumer"; // savnik get from db
    response->ProtocolName = Protocol;
    response->ErrorCode = EKafkaErrors::NONE_ERROR;

    TBuffer buf(Assignments.data(), Assignments.size());
    TKafkaReadable readable(buf);
    TKafkaVersion version;
    readable >> version;
    TConsumerProtocolAssignment assignment;
    assignment.Read(readable, version);
    response->Assignment = std::move(assignment);

    Send(Context->ConnectionId, new TEvKafka::TEvResponse(correlationId, response, EKafkaErrors::NONE_ERROR));
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
