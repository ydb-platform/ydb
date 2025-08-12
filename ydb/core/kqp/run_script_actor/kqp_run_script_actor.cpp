#include "kqp_run_script_actor.h"

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/kqp_timeouts.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/kqp/proxy_service/kqp_script_executions.h>
#include <ydb/core/kqp/proxy_service/proto/result_set_meta.pb.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/library/ydb_issue/proto/issue_id.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/generic/size_literals.h>

#include <forward_list>

#define LOG_T(stream) LOG_TRACE_S(NActors::TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, "TRunScriptActor " << SelfId() << ". Ctx: " << *UserRequestContext << ". LeaseGeneration: " << LeaseGeneration << ". RunState: " << static_cast<ui64>(RunState) << ". " << stream);
#define LOG_D(stream) LOG_DEBUG_S(NActors::TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, "TRunScriptActor " << SelfId() << ". Ctx: " << *UserRequestContext << ". LeaseGeneration: " << LeaseGeneration << ". RunState: " << static_cast<ui64>(RunState) << ". " << stream);
#define LOG_I(stream) LOG_INFO_S(NActors::TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, "TRunScriptActor " << SelfId() << ". Ctx: " << *UserRequestContext << ". LeaseGeneration: " << LeaseGeneration << ". RunState: " << static_cast<ui64>(RunState) << ". " << stream);
#define LOG_N(stream) LOG_NOTICE_S(NActors::TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, "TRunScriptActor " << SelfId() << ". Ctx: " << *UserRequestContext << ". LeaseGeneration: " << LeaseGeneration << ". RunState: " << static_cast<ui64>(RunState) << ". " << stream);
#define LOG_W(stream) LOG_WARN_S(NActors::TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, "TRunScriptActor " << SelfId() << ". Ctx: " << *UserRequestContext << ". LeaseGeneration: " << LeaseGeneration << ". RunState: " << static_cast<ui64>(RunState) << ". " << stream);
#define LOG_E(stream) LOG_ERROR_S(NActors::TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, "TRunScriptActor " << SelfId() << ". Ctx: " << *UserRequestContext << ". LeaseGeneration: " << LeaseGeneration << ". RunState: " << static_cast<ui64>(RunState) << ". " << stream);

namespace NKikimr::NKqp {

namespace {

constexpr ui32 LEASE_UPDATE_FREQUENCY = 2;

constexpr ui64 MIN_SAVE_RESULT_BATCH_SIZE = 5_MB;
constexpr i32 MIN_SAVE_RESULT_BATCH_ROWS = 5000;
constexpr ui64 RUN_SCRIPT_ACTOR_BUFFER_SIZE = 40_MB;

NYql::TIssues AddRootIssue(const TString& message, const NYql::TIssues& issues, bool addEmptyRoot = false) {
    if (!issues && !addEmptyRoot) {
        return {};
    }

    NYql::TIssue rootIssue(message);
    for (const auto& issue : issues) {
        rootIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
    }

    return {rootIssue};
}

struct TProducerState {
    TMaybe<ui64> LastSeqNo;
    i64 AckedFreeSpaceBytes = 0;
    TActorId ActorId;
    ui64 ChannelId = 0;
    bool Enough = false;

    void SendAck(const NActors::TActorIdentity& actor) const {
        auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>(*LastSeqNo, ChannelId);
        resp->Record.SetFreeSpace(AckedFreeSpaceBytes);
        resp->Record.SetEnough(Enough);
        actor.Send(ActorId, resp.Release());
    }

    bool ResumeIfStopped(const NActors::TActorIdentity& actor, i64 freeSpaceBytes) {
        if (LastSeqNo && AckedFreeSpaceBytes <= 0) {
            AckedFreeSpaceBytes = freeSpaceBytes;
            SendAck(actor);
            return true;
        }
        return false;
    }
};

class TRunScriptActor : public NActors::TActorBootstrapped<TRunScriptActor> {
    enum class ERunState {
        Created,
        Running,
        Cancelling,
        Cancelled,
        Finishing,
        Finished,
    };

    enum EWakeUp {
        RunEvent,
        UpdateLeaseEvent,
    };

    class TResultSetMeta {
    public:
        Ydb::Query::Internal::ResultSetMeta& MutableMeta() {
            JsonMeta = std::nullopt;
            return Meta;
        }

        const NJson::TJsonValue& GetJsonMeta() {
            if (!JsonMeta) {
                JsonMeta = NJson::TJsonValue();
                NProtobufJson::Proto2Json(Meta, *JsonMeta, NProtobufJson::TProto2JsonConfig());
            }
            return *JsonMeta;
        }

    private:
        Ydb::Query::Internal::ResultSetMeta Meta;
        std::optional<NJson::TJsonValue> JsonMeta;
    };

    struct TResultSetInfo {
        bool NewResultSet = true;
        bool Truncated = false;
        bool Finished = false;
        ui64 RowCount = 0;
        ui64 ByteCount = 0;
        TResultSetMeta Meta;

        ui64 FirstRowId = 0;
        ui64 AccumulatedSize = 0;
        Ydb::ResultSet PendingResult;
    };

public:
    TRunScriptActor(const NKikimrKqp::TEvQueryRequest& request, TKqpRunScriptActorSettings&& settings, NKikimrConfig::TQueryServiceConfig&& queryServiceConfig)
        : ExecutionId(std::move(settings.ExecutionId))
        , Request(request)
        , Database(std::move(settings.Database))
        , LeaseGeneration(settings.LeaseGeneration)
        , LeaseDuration(settings.LeaseDuration)
        , ResultsTtl(settings.ResultsTtl)
        , ProgressStatsPeriod(settings.ProgressStatsPeriod)
        , QueryServiceConfig(std::move(queryServiceConfig))
        , SaveQueryPhysicalGraph(settings.SaveQueryPhysicalGraph)
        , DisableDefaultTimeout(settings.DisableDefaultTimeout)
        , RequestActorId(settings.RequestActorId)
        , PhysicalGraph(std::move(settings.PhysicalGraph))
        , Counters(settings.Counters)
    {}

    static constexpr char ActorName[] = "KQP_RUN_SCRIPT_ACTOR";

    void Bootstrap() {
        const auto& traceId = Request.GetTraceId();
        UserRequestContext = MakeIntrusive<TUserRequestContext>(
            traceId,
            Database,
            /* SessionId*/ "",
            ExecutionId,
            /* CustomerSuppliedId */ traceId,
            SelfId()
        );

        LOG_I("Bootstrap");

        Become(&TRunScriptActor::StateFunc);
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(NActors::TEvents::TEvWakeup, Handle);
        hFunc(NActors::TEvents::TEvPoison, Handle);
        hFunc(TEvKqpExecuter::TEvStreamData, Handle);
        hFunc(TEvKqpExecuter::TEvExecuterProgress, Handle);
        hFunc(TEvSaveScriptExternalEffectRequest, Handle);
        hFunc(TEvSaveScriptPhysicalGraphRequest, Handle);
        hFunc(TEvSaveScriptPhysicalGraphResponse, Handle);
        hFunc(TEvKqp::TEvQueryResponse, Handle);
        hFunc(TEvKqp::TEvCreateSessionResponse, Handle);
        IgnoreFunc(TEvKqp::TEvCloseSessionResponse);
        hFunc(TEvKqp::TEvCancelQueryResponse, Handle);
        hFunc(TEvKqp::TEvCancelScriptExecutionRequest, Handle);
        hFunc(TEvScriptExecutionFinished, Handle);
        hFunc(TEvScriptLeaseUpdateResponse, Handle);
        hFunc(TEvSaveScriptResultMetaFinished, Handle);
        hFunc(TEvSaveScriptResultFinished, Handle);
        hFunc(TEvCheckAliveRequest, Handle);
    )

    void SendToKqpProxy(THolder<NActors::IEventBase> ev) {
        if (!Send(MakeKqpProxyID(SelfId().NodeId()), ev.Release())) {
            Issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Internal error, send to kqp proxy failed"));
            Finish(Ydb::StatusIds::INTERNAL_ERROR);
        }
    }

    void CreateSession() {
        auto ev = MakeHolder<TEvKqp::TEvCreateSessionRequest>();
        ev->Record.MutableRequest()->SetDatabase(Request.GetRequest().GetDatabase());

        LOG_D("Create new session");
        SendToKqpProxy(std::move(ev));
    }

    void Handle(TEvKqp::TEvCreateSessionResponse::TPtr& ev) {
        const auto& resp = ev->Get()->Record;
        if (resp.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
            LOG_E("Create new session failed: " << resp.GetYdbStatus() << ", ResourceExhausted: " << resp.GetResourceExhausted());
            auto error = TStringBuilder() << "Create new session failed with " << resp.GetYdbStatus();
            if (resp.GetResourceExhausted()) {
                Issues.AddIssue(error << " (resource exhausted)");
                Finish(Ydb::StatusIds::OVERLOADED);
            } else {
                Issues.AddIssue(error);
                Finish(Ydb::StatusIds::INTERNAL_ERROR);
            }
            return;
        }

        const auto& session = resp.GetResponse();
        SessionId = session.GetSessionId();
        UserRequestContext->SessionId = SessionId;

        if (session.GetNodeId() != SelfId().NodeId()) {
            LOG_E("New session started on unexpected node: " << session.GetNodeId());
            Issues.AddIssue(TStringBuilder() << "Session created on wrong node " << session.GetNodeId() << ", expected local session on node " << SelfId().NodeId());
            Finish(Ydb::StatusIds::INTERNAL_ERROR);
            return;
        }

        LOG_D("New session created");

        if (RunState == ERunState::Running) {
            Start();
        } else {
            CloseSession();
        }
    }

    void CloseSession() {
        if (SessionId) {
            LOG_D("Close session");

            auto ev = MakeHolder<TEvKqp::TEvCloseSessionRequest>();
            ev->Record.MutableRequest()->SetSessionId(SessionId);

            Send(MakeKqpProxyID(SelfId().NodeId()), ev.Release());
            SessionId.clear();
        }
    }

    void Start() {
        LOG_I("Start script execution, has PhysicalGraph: " << PhysicalGraph.has_value() << ", SaveQueryPhysicalGraph: " << SaveQueryPhysicalGraph);

        // Expecting that session actor and executor started on the same node with run script actor
        auto ev = MakeHolder<TEvKqp::TEvQueryRequest>();
        ev->Record = Request;
        ev->Record.MutableRequest()->SetSessionId(SessionId);
        ev->SetSaveQueryPhysicalGraph(SaveQueryPhysicalGraph);
        ev->SetDisableDefaultTimeout(DisableDefaultTimeout);
        ev->SetUserRequestContext(UserRequestContext);
        if (PhysicalGraph) {
            ev->SetQueryPhysicalGraph(std::move(*PhysicalGraph));
        }
        if (ev->Record.GetRequest().GetCollectStats() >= Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL) {
            ev->SetProgressStatsPeriod(ProgressStatsPeriod ? ProgressStatsPeriod : TDuration::MilliSeconds(QueryServiceConfig.GetProgressStatsPeriodMs()));
        }

        NActors::ActorIdToProto(SelfId(), ev->Record.MutableRequestActorId());

        SendToKqpProxy(std::move(ev));

        if (!SaveQueryPhysicalGraph) {
            UpdateScriptProgress("{}");
        }
    }

    void UpdateScriptProgress(const TString& plan) {
        const auto& updaterId = Register(CreateScriptProgressActor(ExecutionId, Database, plan, LeaseGeneration));
        LOG_T("Start TScriptProgressActor " << updaterId);
    }

    void Handle(TEvCheckAliveRequest::TPtr& ev) {
        LOG_W("Lease was expired in database"
            << ", LeaseUpdateScheduleTime: " << LeaseUpdateScheduleTime
            << ", LeaseUpdateQueryRunning: " << LeaseUpdateQueryRunning
            << ", FinalStatusIsSaved: " << FinalStatusIsSaved
            << ", FinishAfterLeaseUpdate: " << FinishAfterLeaseUpdate
            << ", WaitFinalizationRequest: " << WaitFinalizationRequest
            << ", CancelRequestsCount: " << CancelRequestsCount
            << ", Status: " << Status
            << ", Issues: " << Issues.ToOneLineString()
            << ", SaveResultInflight: " << SaveResultInflight
            << ", SaveResultMetaInflight: " << SaveResultMetaInflight);

        Send(ev->Sender, new TEvCheckAliveResponse());
    }

    void RunLeaseUpdater() {
        const auto& updaterId = Register(CreateScriptLeaseUpdateActor(SelfId(), Database, ExecutionId, LeaseDuration, LeaseGeneration, Counters));
        LOG_D("Run lease updater " << updaterId);

        LeaseUpdateQueryRunning = true;
        Counters->ReportRunActorLeaseUpdateBacklog(TInstant::Now() - LeaseUpdateScheduleTime);
    }

    bool LeaseUpdateRequired() const {
        return IsExecuting() || SaveResultMetaInflight || SaveResultInflight;
    }

    // TODO: remove this after there will be a normal way to store results and generate execution id
    void Handle(NActors::TEvents::TEvWakeup::TPtr& ev) {
        switch (ev->Get()->Tag) {
        case EWakeUp::RunEvent:
            LOG_D("Script execution entry saved");
            LeaseUpdateScheduleTime = TInstant::Now();
            Schedule(LeaseDuration / LEASE_UPDATE_FREQUENCY, new NActors::TEvents::TEvWakeup(EWakeUp::UpdateLeaseEvent));
            RunState = ERunState::Running;
            CreateSession();
            break;

        case EWakeUp::UpdateLeaseEvent:
            LOG_D("Try to start lease update");
            if (LeaseUpdateRequired() && !FinalStatusIsSaved) {
                RunLeaseUpdater();
            }
            break;
        }
    }

    void TerminateActorExecution(Ydb::StatusIds::StatusCode replyStatus, const NYql::TIssues& replyIssues) {
        LOG_I("Script execution finalized, cancel response status: " << replyStatus << ", issues: " << replyIssues.ToOneLineString());
        for (auto& req : CancelRequests) {
            Send(req->Sender, new TEvKqp::TEvCancelScriptExecutionResponse(replyStatus, replyIssues));
        }
        PassAway();
    }

    void RunScriptExecutionFinisher() {
        if (!FinalStatusIsSaved) {
            FinalStatusIsSaved = true;
            WaitFinalizationRequest = true;
            RunState = IsExecuting() ? ERunState::Finishing : RunState;

            if (RunState == ERunState::Cancelling) {
                NYql::TIssue cancelIssue("Request was canceled by user");
                cancelIssue.SetCode(NYql::DEFAULT_ERROR, NYql::TSeverityIds::S_INFO);
                Issues.AddIssue(std::move(cancelIssue));
            }

            LOG_I("Start script execution finalization");

            auto scriptFinalizeRequest = std::make_unique<TEvScriptFinalizeRequest>(
                GetFinalizationStatusFromRunState(), ExecutionId, Database, Status, GetExecStatusFromStatusCode(Status),
                Issues, std::move(QueryStats), std::move(QueryPlan), std::move(QueryAst), LeaseGeneration
            );
            Send(MakeKqpFinalizeScriptServiceId(SelfId().NodeId()), scriptFinalizeRequest.release());
        } else {
            LOG_N("Script final status is already saved, WaitFinalizationRequest: " << WaitFinalizationRequest);
        }

        if (!RequestActorNotified) {
            RequestActorNotified = true;
            Send(RequestActorId, new TEvScriptExecutionProgress(Status, /* stateSaved */ false, Issues));
        }

        if (!WaitFinalizationRequest && RunState != ERunState::Cancelled && RunState != ERunState::Finished) {
            RunState = ERunState::Finished;
            TerminateActorExecution(Ydb::StatusIds::PRECONDITION_FAILED, { NYql::TIssue("Already finished") });
        }
    }

    void Handle(TEvScriptLeaseUpdateResponse::TPtr& ev) {
        LeaseUpdateQueryRunning = false;

        const auto status = ev->Get()->Status;
        const auto& issues = ev->Get()->Issues;
        if (status != Ydb::StatusIds::SUCCESS) {
            LOG_E("Lease update " << ev->Sender << " failed " << status << ", Issues: " << issues.ToOneLineString() << ", ExecutionEntryExists: " << ev->Get()->ExecutionEntryExists);
        } else {
            LOG_D("Lease updated by " << ev->Sender << ", CurrentDeadline: " << ev->Get()->CurrentDeadline << ", ExecutionEntryExists: " << ev->Get()->ExecutionEntryExists);
        }

        if (!ev->Get()->ExecutionEntryExists) {
            LOG_E("Script execution entry was lost");

            FinalStatusIsSaved = true;
            if (RunState == ERunState::Running) {
                CancelRunningQuery();
            }
        }

        if (FinishAfterLeaseUpdate) {
            RunScriptExecutionFinisher();
        } else if (IsExecuting() && status != Ydb::StatusIds::SUCCESS) {
            Issues.AddIssues(AddRootIssue("Internal error. Failed to update lease state", issues, true));
            Finish(status);
        }

        if (LeaseUpdateRequired()) {
            TInstant leaseUpdateTime = ev->Get()->CurrentDeadline - LeaseDuration / LEASE_UPDATE_FREQUENCY;
            LOG_D("Schedule lease update on " << leaseUpdateTime);

            LeaseUpdateScheduleTime = TInstant::Now();
            if (TInstant::Now() >= leaseUpdateTime) {
                RunLeaseUpdater();
            } else {
                Schedule(leaseUpdateTime, new NActors::TEvents::TEvWakeup(EWakeUp::UpdateLeaseEvent));
            }
        }
    }

    // Event in case of error in registering script in database
    // Just pass away, because we have not started execution.
    void Handle(NActors::TEvents::TEvPoison::TPtr&) {
        Y_ABORT_UNLESS(RunState == ERunState::Created);
        LOG_E("Failed to save script execution entry");
        PassAway();
    }

    bool ShouldSaveResult(size_t resultSetId) const {
        if (SaveResultInflight) {
            return false;
        }

        const TResultSetInfo& resultInfo = ResultSetInfos[resultSetId];
        if (!resultInfo.PendingResult.rows_size()) {
            return false;
        }
        if (resultInfo.Truncated || !IsExecuting()) {
            return true;
        }
        return resultInfo.PendingResult.rows_size() >= MIN_SAVE_RESULT_BATCH_ROWS || resultInfo.ByteCount - resultInfo.AccumulatedSize >= MIN_SAVE_RESULT_BATCH_SIZE;
    }

    size_t GetBytesToSave(size_t resultSetId) const {
        const TResultSetInfo& resultInfo = ResultSetInfos[resultSetId];
        if (!resultInfo.PendingResult.rows_size()) {
            return 0;
        }
        return resultInfo.ByteCount - resultInfo.AccumulatedSize;
    }

    void SaveResult(size_t resultSetId) {
        if (!ExpireAt && ResultsTtl > TDuration::Zero()) {
            ExpireAt = TInstant::Now() + ResultsTtl;
        }

        auto& resultSetInfo = ResultSetInfos[resultSetId];
        const auto& saverId = Register(CreateSaveScriptExecutionResultActor(SelfId(), Database, ExecutionId, resultSetId, ExpireAt, resultSetInfo.FirstRowId, resultSetInfo.AccumulatedSize, std::move(resultSetInfo.PendingResult)));
        LOG_D("Save part for result set #" << resultSetId << ", saver id: " << saverId);

        SaveResultInflight++;
        const ui64 bytes = resultSetInfo.ByteCount - resultSetInfo.AccumulatedSize;
        PendingResultSetsSize -= bytes;
        SaveResultInflightBytes = bytes;
        resultSetInfo.FirstRowId = resultSetInfo.RowCount;
        resultSetInfo.AccumulatedSize = resultSetInfo.ByteCount;
        resultSetInfo.PendingResult = Ydb::ResultSet();
    }

    void SaveResult() {
        for (size_t resultSetId = 0; resultSetId < ResultSetInfos.size(); ++resultSetId) {
            if (ShouldSaveResult(resultSetId)) {
                SaveResult(resultSetId);
                break;
            }
        }
    }

    void Handle(TEvKqpExecuter::TEvStreamData::TPtr& ev) {
        if (RunState != ERunState::Running) {
            return;
        }

        LOG_D("Compute stream data"
            << ", seqNo: " << ev->Get()->Record.GetSeqNo()
            << ", queryResultIndex: " << ev->Get()->Record.GetQueryResultIndex()
            << ", rowsCount: " << ev->Get()->Record.GetResultSet().rows_size()
            << ", from: " << ev->Sender);

        auto& streamData = ev->Get()->Record;
        auto resultSetIndex = streamData.GetQueryResultIndex();

        if (resultSetIndex >= ResultSetInfos.size()) {
            // we don't know result set count, so just accept all of them
            // it's possible to have several result sets per script
            // they can arrive in any order and may be missed for some indices
            ResultSetInfos.resize(resultSetIndex + 1);
        }

        bool savedResult = false;
        auto& resultSetInfo = ResultSetInfos[resultSetIndex];
        if (IsExecuting() && !resultSetInfo.Truncated) {
            auto& rowCount = resultSetInfo.RowCount;
            auto& byteCount = resultSetInfo.ByteCount;

            for (auto& row : *streamData.MutableResultSet()->mutable_rows()) {
                if (QueryServiceConfig.GetScriptResultRowsLimit() && rowCount + 1 > QueryServiceConfig.GetScriptResultRowsLimit()) {
                    resultSetInfo.Truncated = true;
                    break;
                }

                auto serializedSize = row.ByteSizeLong();
                if (QueryServiceConfig.GetScriptResultSizeLimit() && byteCount + serializedSize > QueryServiceConfig.GetScriptResultSizeLimit()) {
                    resultSetInfo.Truncated = true;
                    break;
                }

                rowCount++;
                byteCount += serializedSize;
                PendingResultSetsSize += serializedSize;
                *resultSetInfo.PendingResult.add_rows() = std::move(row);
            }

            const bool newResultSet = std::exchange(resultSetInfo.NewResultSet, false);
            resultSetInfo.Finished = streamData.GetFinished();
            if (newResultSet || resultSetInfo.Truncated) {
                auto& meta = resultSetInfo.Meta.MutableMeta();
                if (newResultSet) {
                    meta.set_enabled_runtime_results(true);
                    *meta.mutable_columns() = streamData.GetResultSet().columns();

                    if (const auto& issues = NKikimr::NKqp::ValidateResultSetColumns(meta.columns())) {
                        NYql::TIssue rootIssue(TStringBuilder() << "Invalid result set " << resultSetIndex << " columns, please contact internal support");
                        for (const NYql::TIssue& issue : issues) {
                            rootIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
                        }
                        Issues.AddIssue(rootIssue);
                        meta.clear_columns();
                        Finish(Ydb::StatusIds::INTERNAL_ERROR);
                        return;
                    }
                }
                if (resultSetInfo.Truncated) {
                    meta.set_truncated(true);
                }
                SaveResultMeta();
            }

            if (ShouldSaveResult(resultSetIndex)) {
                savedResult = true;
                SaveResult(resultSetIndex);
            }
        }

        const i64 freeSpaceBytes = GetFreeSpaceBytes();
        const ui32 channelId = streamData.GetChannelId();
        const ui64 seqNo = streamData.GetSeqNo();
        auto& channel = StreamChannels[channelId];
        channel.ActorId = ev->Sender;
        channel.LastSeqNo = seqNo;
        channel.AckedFreeSpaceBytes = freeSpaceBytes;
        channel.ChannelId = channelId;
        channel.Enough = resultSetInfo.Truncated;
        channel.SendAck(SelfId());

        if (!savedResult && SaveResultInflight == 0) {
            CheckInflight();
        }
    }

    void SaveResultMeta() {
        // can't save meta when previous request is not completed for TLI reasons
        if (SaveResultMetaInflight) {
            PendingResultMeta = true;
            return;
        } 
        SaveResultMetaInflight++;

        NJson::TJsonValue resultSetMetas;
        resultSetMetas.SetType(NJson::JSON_ARRAY);
        for (size_t i = 0; auto& resultSetInfo : ResultSetInfos) {
            resultSetMetas[i++] = resultSetInfo.Meta.GetJsonMeta();
        }

        NJsonWriter::TBuf sout;
        sout.WriteJsonValue(&resultSetMetas);
        const auto& saverId = Register(
            CreateSaveScriptExecutionResultMetaActor(SelfId(), Database, ExecutionId, sout.Str(), LeaseGeneration)
        );

        LOG_D("Save result meta for result sets #" << ResultSetInfos.size() << ", saver id: " << saverId);
    }

    void Handle(TEvKqpExecuter::TEvExecuterProgress::TPtr& ev) {
        LOG_T("Got script progress from " << ev->Sender);
        UpdateScriptProgress(ev->Get()->Record.GetQueryPlan());
    }

    void Handle(TEvSaveScriptExternalEffectRequest::TPtr& ev) {
        LOG_I("Got save script external effect request from " << ev->Sender);

        if (RunState != ERunState::Running) {
            Send(ev->Sender, new TEvSaveScriptPhysicalGraphResponse(Ydb::StatusIds::INTERNAL_ERROR, {NYql::TIssue("Script execution is not running")}));
            return;
        }

        auto& sinks = ev->Get()->Description.Sinks;
        sinks = FilterExternalSinksWithEffects(sinks);

        if (!sinks.empty()) {
            const auto& saverId = Register(CreateSaveScriptExternalEffectActor(std::move(ev), LeaseGeneration));
            LOG_D("Save external effect, saver id: " << saverId);
        } else {
            Send(ev->Sender, new TEvSaveScriptExternalEffectResponse(Ydb::StatusIds::SUCCESS, {}));
        }
    }

    void Handle(TEvSaveScriptPhysicalGraphRequest::TPtr& ev) {
        LOG_I("Got save script physical graph request from " << ev->Sender);

        if (RunState != ERunState::Running) {
            Send(ev->Sender, new TEvSaveScriptPhysicalGraphResponse(Ydb::StatusIds::INTERNAL_ERROR, {NYql::TIssue("Script execution is not running")}));
            return;
        }

        if (PhysicalGraphSender) {
            Send(ev->Sender, new TEvSaveScriptPhysicalGraphResponse(Ydb::StatusIds::INTERNAL_ERROR, {NYql::TIssue("Can not save graph twice")}));
            return;
        }

        PhysicalGraphSender = ev->Sender;
        const auto& saverId = Register(CreateSaveScriptExecutionPhysicalGraphActor(SelfId(), Database, ExecutionId, std::move(ev->Get()->PhysicalGraph), LeaseGeneration, QueryServiceConfig));
        LOG_D("Save script physical graph, saver id: " << saverId);
    }

    void Handle(TEvSaveScriptPhysicalGraphResponse::TPtr& ev) {
        const auto status = ev->Get()->Status;
        const auto& issues = ev->Get()->Issues;
        LOG_D("Script physical graph saved " << ev->Sender << ", Status: " << status << ", Issues: " << issues.ToOneLineString());

        if (!RequestActorNotified) {
            RequestActorNotified = true;
            Send(RequestActorId, new TEvScriptExecutionProgress(status, /* stateSaved */ true, issues));
        }

        Y_ABORT_UNLESS(PhysicalGraphSender);
        Forward(ev, *PhysicalGraphSender);
        UpdateScriptProgress("{}");
    }

    void Handle(TEvKqp::TEvQueryResponse::TPtr& ev) {
        if (RunState != ERunState::Running) {
            LOG_N("Script query finished from " << ev->Sender << " after finalization");
            return;
        }

        auto& record = ev->Get()->Record;

        const auto& issueMessage = record.GetResponse().GetQueryIssues();
        NYql::TIssues issues;
        NYql::IssuesFromMessage(issueMessage, issues);
        Issues.AddIssues(TruncateIssues(issues));

        LOG_I("Script query finished from " << ev->Sender << " " << record.GetYdbStatus() << ", Issues: " << Issues.ToOneLineString());

        if (record.GetYdbStatus() == Ydb::StatusIds::TIMEOUT) {
            const TDuration timeout = GetQueryTimeout(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT, Request.GetRequest().GetTimeoutMs(), {}, QueryServiceConfig);
            NYql::TIssue timeoutIssue(TStringBuilder() << "Current request timeout is " << timeout.MilliSeconds() << "ms");
            timeoutIssue.SetCode(NYql::DEFAULT_ERROR, NYql::TSeverityIds::S_INFO);
            Issues.AddIssue(std::move(timeoutIssue));
        }

        if (record.GetResponse().HasQueryPlan()) {
            QueryPlan = record.GetResponse().GetQueryPlan();
        }

        if (record.GetResponse().HasQueryStats()) {
            QueryStats = record.GetResponse().GetQueryStats();
        }

        if (record.GetResponse().HasQueryAst()) {
            QueryAst = record.GetResponse().GetQueryAst();
        }

        Finish(record.GetYdbStatus());
    }

    void Handle(TEvKqp::TEvCancelScriptExecutionRequest::TPtr& ev) {
        CancelRequestsCount++;
        LOG_I("Cancel script execution request #" << CancelRequestsCount << " from " << ev->Sender << ", FinalStatusIsSaved: " << FinalStatusIsSaved << ", WaitFinalizationRequest: " << WaitFinalizationRequest);

        switch (RunState) {
        case ERunState::Created:
            CancelRequests.emplace_front(std::move(ev));
            Finish(Ydb::StatusIds::CANCELLED, ERunState::Cancelling);
            break;
        case ERunState::Running:
            CancelRequests.emplace_front(std::move(ev));
            CancelRunningQuery();
            break;
        case ERunState::Cancelling:
            CancelRequests.emplace_front(std::move(ev));
            break;
        case ERunState::Cancelled:
            Send(ev->Sender, new TEvKqp::TEvCancelScriptExecutionResponse(Ydb::StatusIds::PRECONDITION_FAILED, "Already cancelled"));
            break;
        case ERunState::Finishing:
            CancelRequests.emplace_front(std::move(ev));
            break;
        case ERunState::Finished:
            Send(ev->Sender, new TEvKqp::TEvCancelScriptExecutionResponse(Ydb::StatusIds::PRECONDITION_FAILED, "Already finished"));
            break;
        }
    }

    void CancelRunningQuery() {
        if (SessionId.empty()) {
            Finish(Ydb::StatusIds::CANCELLED, ERunState::Cancelling);
        } else {
            LOG_I("Cancel running query");

            RunState = ERunState::Cancelling;
            auto ev = MakeHolder<TEvKqp::TEvCancelQueryRequest>();
            ev->Record.MutableRequest()->SetSessionId(SessionId);

            Send(MakeKqpProxyID(SelfId().NodeId()), ev.Release());
        }
    }

    void Handle(TEvKqp::TEvCancelQueryResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (const auto status = record.GetStatus(); ev->Get()->Record.GetStatus() != Ydb::StatusIds::SUCCESS) {
            const auto& issueMessage = record.GetIssues();
            NYql::TIssues issues;
            NYql::IssuesFromMessage(issueMessage, issues);

            LOG_E("Failed to cancel query " << status << ", Issues: " << issues.ToOneLineString() << ", response from: " << ev->Sender);

            Issues.AddIssues(AddRootIssue(TStringBuilder() << "Failed to cancel query (" << status << ")", issues, true));
        } else {
            LOG_I("Query cancelled, response from: " << ev->Sender);
        }

        Finish(Ydb::StatusIds::CANCELLED, ERunState::Cancelling);
    }

    void Handle(TEvScriptExecutionFinished::TPtr& ev) {
        LOG_I("Script execution final status saved by " << ev->Sender
            << ", Status: " << ev->Get()->Status
            << ", Issues: " << ev->Get()->Issues.ToOneLineString()
            << ", OperationAlreadyFinalized: " << ev->Get()->OperationAlreadyFinalized
            << ", WaitingRetry: " << ev->Get()->WaitingRetry);

        WaitFinalizationRequest = false;

        if (RunState == ERunState::Cancelling) {
            RunState = ERunState::Cancelled;
        }

        if (RunState == ERunState::Finishing) {
            RunState = ERunState::Finished;
        }

        Ydb::StatusIds::StatusCode status = ev->Get()->Status;
        NYql::TIssues issues = std::move(ev->Get()->Issues);
        if (RunState == ERunState::Finished) {
            status = Ydb::StatusIds::PRECONDITION_FAILED;
            issues.Clear();
            issues.AddIssue("Already finished");
        }
        TerminateActorExecution(status, issues);
    }

    void Handle(TEvSaveScriptResultMetaFinished::TPtr& ev) {
        const auto status = ev->Get()->Status;
        const auto& issues = ev->Get()->Issues;
        if (status != Ydb::StatusIds::SUCCESS) {
            LOG_E("Save result meta " << ev->Sender << " failed " << status << ", Issues: " << issues.ToOneLineString());
        } else {
            LOG_D("Save result meta " << ev->Sender << " finished");
        }

        SaveResultMetaInflight--;

        if (PendingResultMeta) {
            PendingResultMeta = false;
            SaveResultMeta();
            return;
        }

        if (status != Ydb::StatusIds::SUCCESS && (Status == Ydb::StatusIds::SUCCESS || Status == Ydb::StatusIds::STATUS_CODE_UNSPECIFIED)) {
            Status = status;
            Issues.AddIssues(AddRootIssue("Failed to save result set meta", ev->Get()->Issues, true));
        }
        CheckInflight();
    }

    void Handle(TEvSaveScriptResultFinished::TPtr& ev) {
        SaveResultInflight--;
        SaveResultInflightBytes = 0;

        const auto status = ev->Get()->Status;
        const auto resultSetId = ev->Get()->ResultSetId;
        if (status == Ydb::StatusIds::SUCCESS) {
            LOG_D("Save result set #" << resultSetId << " " << ev->Sender << " finished");

            if (resultSetId >= ResultSetInfos.size()) {
                Issues.AddIssue(TStringBuilder() << "ResultSetId " << resultSetId << " is out of range [0; " << ResultSetInfos.size() << ")");
                Finish(Ydb::StatusIds::INTERNAL_ERROR);
                return;
            }

            auto& resultSetInfo = ResultSetInfos[resultSetId];
            auto& meta = resultSetInfo.Meta.MutableMeta();
            meta.set_number_rows(resultSetInfo.RowCount);
            if (resultSetInfo.PendingResult.rows().empty() && (resultSetInfo.Truncated || resultSetInfo.Finished)) {
                meta.set_finished(true);
            }

            SaveResultMeta();
        } else {
            LOG_E("Save result set #" << resultSetId << " " << ev->Sender << " failed " << status << ", Issues: " << ev->Get()->Issues.ToOneLineString());
        }

        if (Status == Ydb::StatusIds::SUCCESS || Status == Ydb::StatusIds::STATUS_CODE_UNSPECIFIED) {
            if (status != Ydb::StatusIds::SUCCESS) {
                Status = status;
                Issues.AddIssues(AddRootIssue(TStringBuilder() << "Failed to save result set " << resultSetId, ev->Get()->Issues, true));
            } else {
                SaveResult();
            }
        }

        const i64 freeSpaceBytes = GetFreeSpaceBytes();
        if (freeSpaceBytes > 0 && IsExecuting()) {
            for (auto& [channelId, channel] : StreamChannels) {
                if (channel.ResumeIfStopped(SelfId(), freeSpaceBytes)) {
                    LOG_D("Resume execution, "
                        << ", channel: " << channelId
                        << ", seqNo: " << channel.LastSeqNo
                        << ", freeSpace: " << freeSpaceBytes);
                }
            }
        }

        CheckInflight();
    }

    static Ydb::Query::ExecStatus GetExecStatusFromStatusCode(Ydb::StatusIds::StatusCode status) {
        if (status == Ydb::StatusIds::SUCCESS) {
            return Ydb::Query::EXEC_STATUS_COMPLETED;
        } else if (status == Ydb::StatusIds::CANCELLED) {
            return Ydb::Query::EXEC_STATUS_CANCELLED;
        } else {
            return Ydb::Query::EXEC_STATUS_FAILED;
        }
    }

    EFinalizationStatus GetFinalizationStatusFromRunState() const {
        if ((Status == Ydb::StatusIds::SUCCESS || Status == Ydb::StatusIds::CANCELLED) && !IsExecuting()) {
            return EFinalizationStatus::FS_COMMIT;
        }
        return EFinalizationStatus::FS_ROLLBACK;
    }

    void CheckInflight() {
        if (SaveResultMetaInflight || SaveResultInflight) {
            return;
        }

        const int freeSpaceBytes = GetFreeSpaceBytes();
        if (freeSpaceBytes < 0 && IsExecuting()) {
            // try to free the space
            size_t maxBytesToSave = 0;
            size_t maxResultSet = 0;
            for (size_t resultSetId = 0; resultSetId < ResultSetInfos.size(); ++resultSetId) {
                if (size_t bytesToSave = GetBytesToSave(resultSetId); bytesToSave > maxBytesToSave) {
                    maxBytesToSave = bytesToSave;
                    maxResultSet = resultSetId;
                }
            }
            SaveResult(maxResultSet);
        } else {
            if (Status == Ydb::StatusIds::STATUS_CODE_UNSPECIFIED || IsExecuting()) {
                // waiting for script completion
                return;
            }

            if (PendingResultSetsSize) {
                // Complete results saving
                SaveResult();
                return;
            }

            if (!LeaseUpdateQueryRunning) {
                RunScriptExecutionFinisher();
            } else {
                FinishAfterLeaseUpdate = true;
            }
        }
    }

    void Finish(Ydb::StatusIds::StatusCode status, ERunState runState = ERunState::Finishing) {
        LOG_I("Finish script execution, status: " << status << ", new run state: " << static_cast<ui64>(runState));

        RunState = runState;
        Status = status;

        // if query has no results, save empty json array
        if (ResultSetInfos.empty()) {
            SaveResultMeta();
        } else {
            CheckInflight();
        }
    }

    void PassAway() override {
        CloseSession();
        NActors::TActorBootstrapped<TRunScriptActor>::PassAway();
    }

    bool IsExecuting() const {
        return RunState != ERunState::Finished
            && RunState != ERunState::Finishing
            && RunState != ERunState::Cancelled
            && RunState != ERunState::Cancelling;
    }

    i64 GetFreeSpaceBytes() const {
        return static_cast<i64>(RUN_SCRIPT_ACTOR_BUFFER_SIZE) - static_cast<i64>(PendingResultSetsSize) - static_cast<i64>(SaveResultInflightBytes);
    }

private:
    const TString ExecutionId;
    NKikimrKqp::TEvQueryRequest Request;
    const TString Database;
    const i64 LeaseGeneration;
    const TDuration LeaseDuration;
    const TDuration ResultsTtl;
    const TDuration ProgressStatsPeriod;
    const NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    const bool SaveQueryPhysicalGraph = false;
    const bool DisableDefaultTimeout = false;
    const TActorId RequestActorId;
    bool RequestActorNotified = false;
    std::optional<NKikimrKqp::TQueryPhysicalGraph> PhysicalGraph;
    std::optional<TActorId> PhysicalGraphSender;
    TIntrusivePtr<TKqpCounters> Counters;
    TString SessionId;
    TInstant LeaseUpdateScheduleTime;
    bool LeaseUpdateQueryRunning = false;
    bool FinalStatusIsSaved = false;
    bool FinishAfterLeaseUpdate = false;
    bool WaitFinalizationRequest = false;
    ERunState RunState = ERunState::Created;
    std::forward_list<TEvKqp::TEvCancelScriptExecutionRequest::TPtr> CancelRequests;
    ui64 CancelRequestsCount = 0;

    // Result info
    NYql::TIssues Issues;
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;

    // Result data
    std::vector<TResultSetInfo> ResultSetInfos;
    TMap<ui64, TProducerState> StreamChannels;
    std::optional<TInstant> ExpireAt;
    ui32 SaveResultInflight = 0;
    ui64 SaveResultInflightBytes = 0;
    ui32 SaveResultMetaInflight = 0;
    bool PendingResultMeta = false;
    ui64 PendingResultSetsSize = 0;
    std::optional<TString> QueryPlan;
    std::optional<TString> QueryAst;
    std::optional<NKqpProto::TKqpStatsQuery> QueryStats;
    TIntrusivePtr<TUserRequestContext> UserRequestContext;
};

} // namespace

NActors::IActor* CreateRunScriptActor(const NKikimrKqp::TEvQueryRequest& request, TKqpRunScriptActorSettings&& settings, NKikimrConfig::TQueryServiceConfig queryServiceConfig) {
    return new TRunScriptActor(request, std::move(settings), std::move(queryServiceConfig));
}

} // namespace NKikimr::NKqp
