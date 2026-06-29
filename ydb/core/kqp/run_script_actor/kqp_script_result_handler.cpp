#include "kqp_run_script_actor_impl.h"

#include <ydb/core/fq/libs/checkpointing/events/events.h>
#include <ydb/core/fq/libs/common/util.h>
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/kqp/proxy_service/kqp_script_executions.h>
#include <ydb/core/kqp/proxy_service/proto/result_set_meta.pb.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/system/types.h>

#include <algorithm>
#include <exception>
#include <queue>
#include <utility>
#include <vector>

namespace NKikimr::NKqp::NPrivate {

namespace {

using namespace NActors;

#define LOG_T(stream) LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, LogPrefix() << stream);
#define LOG_D(stream) LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, LogPrefix() << stream);
#define LOG_I(stream) LOG_INFO_S(TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, LogPrefix() << stream);
#define LOG_N(stream) LOG_NOTICE_S(TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, LogPrefix() << stream);
#define LOG_W(stream) LOG_WARN_S(TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, LogPrefix() << stream);
#define LOG_E(stream) LOG_ERROR_S(TActivationContext::AsActorContext(), NKikimrServices::KQP_EXECUTER, LogPrefix() << stream);

class TScriptResultHandlerActor final : public TActorBootstrapped<TScriptResultHandlerActor>, IActorExceptionHandler {
    using TBase = TActorBootstrapped<TScriptResultHandlerActor>;

    static constexpr ui64 RUN_SCRIPT_ACTOR_BUFFER_SIZE = 40_MB;
    static constexpr ui64 MIN_SAVE_RESULT_BATCH_SIZE = 5_MB;
    static constexpr i32 MIN_SAVE_RESULT_BATCH_ROWS = 5000;

    struct TSaveProgressState {
        bool WaitSave = false;
        bool QueryStatsChanged = true; // We should update status once execution was started
        bool AstSaved = false;

        void UpdateChanged(const std::optional<TString>& from, const TString& to) {
            if (QueryStatsChanged) {
                return;
            }

            QueryStatsChanged = !from || *from != to;
        }
    };

    struct TSaveExternalEffectsState {
        bool WaitSave = false;
        std::queue<std::pair<TActorId, TEvSaveScriptExternalEffectRequest::TDescription>> Requests;
    };

    struct TSavePhysicalGraphState {
        bool WaitSave = false;
        TActorId Sender;
        std::queue<std::pair<bool, NKikimrKqp::TQueryPhysicalGraph>> GraphsToSave; // (send response, graph)
    };

    class TSaveResultsState {
        class TResultSetMeta {
        public:
            Ydb::Query::Internal::ResultSetMeta& MutableMeta() {
                JsonMeta = std::nullopt;
                return Meta;
            }

            const NJson::TJsonValue& SaveJsonMeta() {
                if (!JsonMeta) {
                    JsonMeta = NJson::TJsonValue();
                    NProtobufJson::Proto2Json(Meta, *JsonMeta, NProtobufJson::TProto2JsonConfig());
                }

                return *JsonMeta;
            }

            bool IsSaved() const {
                return JsonMeta.has_value();
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
            ui64 FirstRowId = 0;
            ui64 AccumulatedSize = 0;
            TResultSetMeta Meta;
            Ydb::ResultSet PendingResult;

            ui64 GetBytesToSave() const {
                return ByteCount - AccumulatedSize;
            }

            bool ShouldSaveResult() const {
                const auto rowsCount = PendingResult.rows_size();
                return rowsCount && (Truncated || rowsCount >= MIN_SAVE_RESULT_BATCH_ROWS || ByteCount - AccumulatedSize >= MIN_SAVE_RESULT_BATCH_SIZE);
            }
        };

    public:
        std::vector<TResultSetInfo> ResultSetInfos;
        ui64 PendingResultSetsSize = 0;
        ui64 SaveResultInflightBytes = 0;
        bool WaitSaveResult = false;
        bool WaitSaveMeta = false;

        i64 GetFreeSpaceBytes() const {
            return static_cast<i64>(RUN_SCRIPT_ACTOR_BUFFER_SIZE) - static_cast<i64>(PendingResultSetsSize) - static_cast<i64>(SaveResultInflightBytes);
        }

        bool HasMetaToSave() const {
            return std::any_of(ResultSetInfos.begin(), ResultSetInfos.end(), [](const TResultSetInfo& info) {
                return !info.Meta.IsSaved();
            });
        }

        bool HasResultsToSave() const {
            return std::any_of(ResultSetInfos.begin(), ResultSetInfos.end(), [](const TResultSetInfo& info) {
                return !info.PendingResult.rows().empty();
            });
        }

        std::optional<TInstant> GetExpireAt(const TScriptExecutionContext& ctx) {
            if (!ExpireAt && ctx.ResultsTtl) {
                ExpireAt = TInstant::Now() + ctx.ResultsTtl;
            }
            return ExpireAt;
        }

    private:
        std::optional<TInstant> ExpireAt;
    };

    struct TProducerState {
        std::optional<ui64> LastSeqNo;
        i64 AckedFreeSpaceBytes = 0;
        TActorId ActorId;
        ui64 ChannelId = 0;
        bool Enough = false;

        void SendAck(const TActorIdentity& actor) const {
            auto response = std::make_unique<NKqp::TEvKqpExecuter::TEvStreamDataAck>(*LastSeqNo, ChannelId);
            response->Record.SetFreeSpace(AckedFreeSpaceBytes);
            response->Record.SetEnough(Enough);
            actor.Send(ActorId, response.release());
        }

        bool ResumeIfStopped(const TActorIdentity& actor, const i64 freeSpaceBytes) {
            if (LastSeqNo && freeSpaceBytes > 0 && AckedFreeSpaceBytes < freeSpaceBytes) {
                AckedFreeSpaceBytes = freeSpaceBytes;
                SendAck(actor);
                return true;
            }

            return false;
        }
    };

public:
    static constexpr char ActorName[] = "KQP_SCRIPT_RESULT_HANDLER_ACTOR";

    TScriptResultHandlerActor(TScriptExecutionContext::TPtr ctx, std::optional<NKikimrKqp::TQueryPhysicalGraph> physicalGraph, NKikimrConfig::TQueryServiceConfig queryServiceConfig)
        : Ctx(std::move(ctx))
        , QueryServiceConfig(std::move(queryServiceConfig))
        , PhysicalGraph(std::move(physicalGraph))
    {
        Y_VALIDATE(Ctx && Ctx->UserRequestContext, "Missing script execution context");
        Y_VALIDATE(Ctx->UserRequestContext->SessionId, "Missing session id");
    }

    void Bootstrap() {
        LOG_I("Bootstrap");
        Become(&TThis::StateFunc);
        ContinueExecute();
    }

private:
    void Registered(TActorSystem* sys, const TActorId& owner) final {
        TBase::Registered(sys, owner);
        Owner = owner;
    }

    bool OnUnhandledException(const std::exception& e) final {
        Finish(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "Got unexpected exception: " << e.what());
        return true;
    }

    // Execution events order for several query transactions (transactions are executed consequently):
    // - For each transaction:
    //   1. TEvSaveScriptExternalEffectRequest -- once
    //   2. If this transaction is last one TEvSaveScriptPhysicalGraphRequest -- once
    //   3. If this transaction is last one and query was not restored TEvZeroCheckpointDone -- once
    //   4. TEvExecuterProgress + TEvStreamData -- periodically
    // - When all transaction are finished:
    //   1. TEvQueryResponse -- once

    STRICT_STFUNC(StateFunc,
        hFunc(TEvSaveScriptExternalEffectRequest, Handle);
        hFunc(TEvSaveScriptExternalEffectResponse, Handle);
        hFunc(TEvSaveScriptPhysicalGraphRequest, Handle);
        hFunc(TEvSaveScriptPhysicalGraphResponse, Handle);
        hFunc(NFq::TEvCheckpointCoordinator::TEvZeroCheckpointDone, Handle);
        hFunc(TEvKqpExecuter::TEvExecuterProgress, Handle);
        hFunc(TEvSaveScriptProgressResponse, Handle);
        hFunc(TEvKqpExecuter::TEvStreamData, Handle);
        hFunc(TEvSaveScriptResultMetaFinished, Handle);
        hFunc(TEvSaveScriptResultFinished, Handle);
        hFunc(TEvKqp::TEvQueryResponse, Handle);
        hFunc(TEvKqp::TEvCancelQueryResponse, Handle);
        sFunc(TEvents::TEvPoison, Finish);
    )

    void Handle(TEvSaveScriptExternalEffectRequest::TPtr& ev) {
        auto& description = ev->Get()->Description;
        auto& sinks = description.Sinks;
        sinks = FilterExternalSinksWithEffects(sinks);
        LOG_D("Got script external effect request from " << ev->Sender << ", sinks #" << sinks.size() << ", secrets #" << description.SecretNames.size());

        if (!sinks.empty()) {
            SaveExternalEffectsState.Requests.emplace(ev->Sender, std::move(description));
            ContinueExecute();
        } else {
            LOG_D("No external effects to save, reply immediately");
            Send(ev->Sender, new TEvSaveScriptExternalEffectResponse(Ydb::StatusIds::SUCCESS, {}));
        }
    }

    void Handle(TEvSaveScriptExternalEffectResponse::TPtr& ev) {
        SaveExternalEffectsState.WaitSave = false;
        Y_VALIDATE(!SaveExternalEffectsState.Requests.empty(), "Unexpected event");

        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            LOG_W("Failed to save external effects " << ev->Sender << ", fail: " << status << ", issues: " << ev->Get()->Issues.ToOneLineString());
        } else {
            LOG_D("External effects saved " << ev->Sender);
        }

        Forward(ev, SaveExternalEffectsState.Requests.front().first);
        SaveExternalEffectsState.Requests.pop();
        ContinueExecute();
    }

    void Handle(TEvSaveScriptPhysicalGraphRequest::TPtr& ev) {
        LOG_I("Got save script physical graph request from " << ev->Sender);

        if (SavePhysicalGraphState.Sender) {
            Send(ev->Sender, new TEvSaveScriptPhysicalGraphResponse(Ydb::StatusIds::INTERNAL_ERROR, {NYql::TIssue(TStringBuilder() << "Can not save graph twice, previous sender was: " << SavePhysicalGraphState.Sender << ", got graph from: " << ev->Sender)}));
            return;
        }

        auto& evPhysicalGraph = ev->Get()->PhysicalGraph;
        if (PhysicalGraph) {
            // If zero checkpoint was done on previous query execution, now checkpointing will continue without zero checkpoint
            evPhysicalGraph.SetZeroCheckpointSaved(PhysicalGraph->GetZeroCheckpointSaved());
        } else {
            PhysicalGraph = evPhysicalGraph;
        }

        SavePhysicalGraphState.Sender = ev->Sender;
        SavePhysicalGraphState.GraphsToSave.emplace(true, std::move(evPhysicalGraph));
        ContinueExecute();
    }

    void Handle(TEvSaveScriptPhysicalGraphResponse::TPtr& ev) {
        SavePhysicalGraphState.WaitSave = false;
        Y_VALIDATE(!SavePhysicalGraphState.GraphsToSave.empty(), "Unexpected event");

        const auto status = ev->Get()->Status;
        const bool saveFailed = status != Ydb::StatusIds::SUCCESS;
        const auto& issues = ev->Get()->Issues;
        if (saveFailed) {
            LOG_W("Failed to save physical graph " << ev->Sender << ", fail: " << status << ", issues: " << issues.ToOneLineString());
        } else {
            LOG_D("Physical graph saved " << ev->Sender);
        }

        const auto sendResponse = SavePhysicalGraphState.GraphsToSave.front().first;
        SavePhysicalGraphState.GraphsToSave.pop();

        if (sendResponse) {
            Y_VALIDATE(SavePhysicalGraphState.Sender, "Can not reply without sender");
            Forward(ev, SavePhysicalGraphState.Sender);
        } else if (saveFailed) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, AddRootIssue("Failed to update query physical graph", issues));
            return;
        }

        ContinueExecute();
    }

    void Handle(NFq::TEvCheckpointCoordinator::TEvZeroCheckpointDone::TPtr& ev) {
        LOG_I("Zero checkpoint saved by " << ev->Sender);

        if (!PhysicalGraph) {
            Finish(Ydb::StatusIds::INTERNAL_ERROR, "Zero checkpoint saved before physical graph saved");
            return;
        }

        if (!PhysicalGraph->GetZeroCheckpointSaved()) {
            PhysicalGraph->SetZeroCheckpointSaved(true);
            SavePhysicalGraphState.GraphsToSave.emplace(false, *PhysicalGraph);
            ContinueExecute();
        }
    }

    void Handle(TEvKqpExecuter::TEvExecuterProgress::TPtr& ev) {
        auto& record = ev->Get()->Record;
        const bool hasPlan = record.HasQueryPlan();
        const bool hasAst = record.HasQueryAst();
        LOG_T("Got script progress from " << ev->Sender << ", has plan: " << hasPlan << ", has ast: " << hasAst);

        if (hasPlan) {
            SaveProgressState.UpdateChanged(ExecutionInfo.QueryPlan, record.GetQueryPlan());
            ExecutionInfo.QueryPlan = std::move(*record.MutableQueryPlan());
        }

        if (!ExecutionInfo.QueryAst && hasAst) {
            SaveProgressState.UpdateChanged(ExecutionInfo.QueryAst, record.GetQueryAst());
            ExecutionInfo.QueryAst = std::move(*record.MutableQueryAst());
        }

        ContinueExecute();
    }

    void Handle(TEvSaveScriptProgressResponse::TPtr& ev) {
        SaveProgressState.WaitSave = false;

        const auto astSaved = ev->Get()->AstSaved;
        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            LOG_N("Script progress updated " << ev->Sender << ", fail: " << status << ", issues: " << ev->Get()->Issues.ToOneLineString());
            SaveProgressState.QueryStatsChanged = true;
        } else {
            LOG_T("Script progress updated " << ev->Sender << ", ast saved: " << astSaved);
            SaveProgressState.AstSaved = SaveProgressState.AstSaved || astSaved;
        }

        ContinueExecute();
    }

    void Handle(TEvKqpExecuter::TEvStreamData::TPtr& ev) {
        auto& record = ev->Get()->Record;
        const auto seqNo = record.GetSeqNo();
        const ui32 resultSetIndex = record.GetQueryResultIndex();
        const auto& resultSet = record.GetResultSet();
        const auto rowsCount = resultSet.rows_size();
        const auto finished = record.GetFinished();
        LOG_D("Compute stream data"
            << ", seq no: " << record.GetSeqNo()
            << ", query result index: " << resultSetIndex
            << ", rows count: " << rowsCount
            << ", finished: " << finished
            << ", from: " << ev->Sender);

        auto& resultSetInfos = SaveResultsState.ResultSetInfos;
        if (resultSetIndex >= resultSetInfos.size()) {
            // we don't know result set count, so just accept all of them
            // it's possible to have several result sets per script
            // they can arrive in any order and may be missed for some indices
            Y_VALIDATE(resultSetIndex < std::numeric_limits<ui32>::max(), "Unexpected result set index: " << resultSetIndex);
            resultSetInfos.resize(resultSetIndex + 1);
        }

        auto& resultSetInfo = resultSetInfos[resultSetIndex];
        if (!resultSetInfo.Truncated) {
            auto& rowCount = resultSetInfo.RowCount;
            auto& byteCount = resultSetInfo.ByteCount;
            const auto rowsLimit = QueryServiceConfig.GetScriptResultRowsLimit();
            const auto sizeLimit = QueryServiceConfig.GetScriptResultSizeLimit();

            for (auto& row : *record.MutableResultSet()->mutable_rows()) {
                if (rowsLimit && rowCount + 1 > rowsLimit) {
                    resultSetInfo.Truncated = true;
                    break;
                }

                const auto serializedSize = row.ByteSizeLong();
                if (sizeLimit && byteCount + serializedSize > sizeLimit) {
                    resultSetInfo.Truncated = true;
                    break;
                }

                rowCount++;
                byteCount += serializedSize;
                SaveResultsState.PendingResultSetsSize += serializedSize;
                *resultSetInfo.PendingResult.add_rows() = std::move(row);
            }

            resultSetInfo.Finished = finished;
            if (const auto newResultSet = std::exchange(resultSetInfo.NewResultSet, false); newResultSet || resultSetInfo.Truncated) {
                auto& meta = resultSetInfo.Meta.MutableMeta();
                if (newResultSet) {
                    meta.set_enabled_runtime_results(true);
                    *meta.mutable_columns() = resultSet.columns();

                    if (const auto& issues = NFq::ValidateResultSetColumns(meta.columns())) {
                        meta.clear_columns();
                        Finish(Ydb::StatusIds::INTERNAL_ERROR, AddRootIssue(TStringBuilder() << "Invalid result set " << resultSetIndex << " columns, please contact internal support", issues));
                        return;
                    }
                }

                if (resultSetInfo.Truncated) {
                    meta.set_truncated(true);
                }
            }
        } else {
            LOG_T("Skip truncated result part with #" << rowsCount << " rows");
        }

        const auto channelId = record.GetChannelId();
        auto& channel = StreamChannels[channelId];
        channel.ActorId = ev->Sender;
        channel.LastSeqNo = seqNo;
        channel.AckedFreeSpaceBytes = SaveResultsState.GetFreeSpaceBytes();
        channel.ChannelId = channelId;
        channel.Enough = resultSetInfo.Truncated;
        channel.SendAck(SelfId());

        ContinueExecute();
    }

    void Handle(TEvSaveScriptResultMetaFinished::TPtr& ev) {
        SaveResultsState.WaitSaveMeta = false;

        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            const auto& issues = ev->Get()->Issues;
            LOG_E("Save result meta " << ev->Sender << " failed " << status << ", issues: " << issues.ToOneLineString());
            Finish(status, AddRootIssue("Failed to save result set meta", issues));
            return;
        }

        LOG_D("Save result meta " << ev->Sender << " finished");
        ContinueExecute();
    }

    void Handle(TEvSaveScriptResultFinished::TPtr& ev) {
        SaveResultsState.WaitSaveResult = false;
        SaveResultsState.SaveResultInflightBytes = 0;

        const auto resultSetIndex = ev->Get()->ResultSetId;
        auto& infos = SaveResultsState.ResultSetInfos;
        Y_VALIDATE(resultSetIndex < infos.size(), "Unexpected result set index: " << resultSetIndex << " amount result sets #" << infos.size());

        if (const auto status = ev->Get()->Status; status != Ydb::StatusIds::SUCCESS) {
            const auto& issues = ev->Get()->Issues;
            LOG_E("Save result " << ev->Sender << " set #" << resultSetIndex << " failed " << status << ", issues: " << issues.ToOneLineString());
            Finish(status, AddRootIssue(TStringBuilder() << "Failed to save result set #" << resultSetIndex, issues));
            return;
        }

        LOG_D("Save result " << ev->Sender << " set #" << resultSetIndex << " finished");

        auto& resultSetInfo = infos[resultSetIndex];
        auto& meta = resultSetInfo.Meta.MutableMeta();
        meta.set_number_rows(resultSetInfo.RowCount);
        if (resultSetInfo.PendingResult.rows().empty() && (resultSetInfo.Truncated || resultSetInfo.Finished)) {
            meta.set_finished(true);
        }

        if (const auto freeSpaceBytes = SaveResultsState.GetFreeSpaceBytes(); freeSpaceBytes > 0) {
            for (auto& [channelId, channel] : StreamChannels) {
                if (channel.ResumeIfStopped(SelfId(), freeSpaceBytes)) {
                    LOG_D("Resume execution, " << ", channel: " << channelId << ", seqNo: " << channel.LastSeqNo << ", freeSpace: " << freeSpaceBytes);
                }
            }
        }

        ContinueExecute();
    }

    void Handle(TEvKqp::TEvQueryResponse::TPtr& ev) {
        QueryIsRunning = false;

        auto& record = ev->Get()->Record;
        auto& response = *record.MutableResponse();

        NYql::TIssues issues;
        NYql::IssuesFromMessage(response.GetQueryIssues(), issues);
        issues = NFq::TruncateIssues(issues);

        const auto status = record.GetYdbStatus();
        if (status == Ydb::StatusIds::SUCCESS) {
            LOG_I("Script query successfully finished from " << ev->Sender << ", issues: " << issues.ToOneLineString());
        } else {
            LOG_W("Script query failed from " << ev->Sender << " " << record.GetYdbStatus() << ", issues: " << issues.ToOneLineString());
        }

        if (status == Ydb::StatusIds::TIMEOUT) {
            NYql::TIssue timeoutIssue(TStringBuilder() << "Current request timeout is " << Ctx->Timeout.MilliSeconds() << "ms");
            timeoutIssue.SetCode(NYql::DEFAULT_ERROR, NYql::TSeverityIds::S_INFO);
            issues.AddIssue(timeoutIssue);
        }

        if (response.HasQueryPlan()) {
            ExecutionInfo.QueryPlan = std::move(*response.MutableQueryPlan());
        }

        if (response.HasQueryStats()) {
            ExecutionInfo.QueryStats = std::move(*response.MutableQueryStats());
        }

        if (response.HasQueryAst()) {
            ExecutionInfo.QueryAst = std::move(*response.MutableQueryAst());
        }

        Finish(status, std::move(issues));
    }

    void Handle(TEvKqp::TEvCancelQueryResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (const auto status = record.GetStatus(); status != Ydb::StatusIds::SUCCESS) {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(record.GetIssues(), issues);
            LOG_E("Failed to cancel query " << status << ", issues: " << issues.ToOneLineString() << ", response from: " << ev->Sender);

            // We can not finish query manually, consider it is already finished
            QueryIsRunning = false;
            Finish(status, AddRootIssue(TStringBuilder() << "Failed to cancel query (" << status << ")", issues));
            return;
        }

        // Wait for normal query finish
        LOG_I("Query cancelled, response from: " << ev->Sender);
    }

    bool HasMetadataOperationInflight() const {
        return SaveProgressState.WaitSave || SaveExternalEffectsState.WaitSave || SavePhysicalGraphState.WaitSave || SaveResultsState.WaitSaveMeta;
    }

    bool HasOperationInflight() const {
        return HasMetadataOperationInflight() || SaveResultsState.WaitSaveResult;
    }

    void ContinueExecute() {
        // Check exit condition after failure
        if (FinishInfo.IsFailed() && !QueryIsRunning && !HasOperationInflight()) {
            return Finish();
        }

        // Save info to script execution results table

        if (!SaveResultsState.WaitSaveResult) {
            TryToDrainResults();
        } else {
            LOG_T("Wait for operations on table `result_sets` to finish");
        }

        // Save info to script execution metadata table

        if (HasMetadataOperationInflight()) {
            LOG_T("Wait for operations on table `script_executions` to finish"
                << ", save progress: " << SaveProgressState.WaitSave
                << ", save external effects: " << SaveExternalEffectsState.WaitSave
                << ", save physical graph: " << SavePhysicalGraphState.WaitSave
                << ", save results meta: " << SaveResultsState.WaitSaveMeta);
            return;
        }

        if (!SaveExternalEffectsState.Requests.empty()) {
            return SaveExternalEffects();
        }

        if (!SavePhysicalGraphState.GraphsToSave.empty()) {
            return SavePhysicalGraph();
        }

        if (SaveResultsState.HasMetaToSave()) {
            return SaveResultsMeta();
        }

        if (SaveProgressState.QueryStatsChanged) {
            return UpdateScriptProgress();
        }

        // Check success exit condition
        if (!SaveResultsState.WaitSaveResult && FinishInfo.IsFinished()) {
            return Finish();
        }
    }

    void TryToDrainResults() {
        Y_VALIDATE(!SaveResultsState.WaitSaveResult, "Unexpected call");

        if (FinishInfo.IsFailed()) {
            // Skip results saving after failure
            return;
        }

        const auto freeSpaceBytes = SaveResultsState.GetFreeSpaceBytes();
        const auto forceSaveResults = FinishInfo.IsSuccess() || freeSpaceBytes <= 0; 
        LOG_T("Try to drain results, free space: " << freeSpaceBytes << ", force save: " << forceSaveResults);

        // We save results when:
        // - Where is large enough result batch
        // - No free space in buffer
        // - Query is finished

        std::optional<ui64> resultToSave;
        ui64 maxResultSetBytes = 0;
        for (ui64 i = 0; i < SaveResultsState.ResultSetInfos.size(); ++i) {
            const auto& info = SaveResultsState.ResultSetInfos[i];
            if (info.ShouldSaveResult()) {
                resultToSave = i;
                break;
            }

            const auto resultSize = info.GetBytesToSave();
            if (forceSaveResults && (resultSize > maxResultSetBytes || (!resultToSave && info.PendingResult.rows_size()))) {
                resultToSave = i;
                maxResultSetBytes = resultSize;
            }
        }

        if (resultToSave) {
            auto& info = SaveResultsState.ResultSetInfos[*resultToSave];
            const auto& saverId = Register(CreateSaveScriptExecutionResultActor(SelfId(), Ctx->UserRequestContext->Database, Ctx->UserRequestContext->CurrentExecutionId, *resultToSave, SaveResultsState.GetExpireAt(*Ctx), info.FirstRowId, info.AccumulatedSize, std::move(info.PendingResult)));
            LOG_D("Save part for result set #" << *resultToSave << ", saver id: " << saverId);
            SaveResultsState.WaitSaveResult = true;

            const auto bytes = info.GetBytesToSave();
            SaveResultsState.PendingResultSetsSize -= bytes;
            SaveResultsState.SaveResultInflightBytes = bytes;
            info.FirstRowId = info.RowCount;
            info.AccumulatedSize = info.ByteCount;
            info.PendingResult = Ydb::ResultSet();
        }
    }

    void SaveExternalEffects() {
        Y_VALIDATE(!SaveExternalEffectsState.Requests.empty() && !SaveExternalEffectsState.WaitSave, "Unexpected call");

        const auto& saverId = Register(CreateSaveScriptExternalEffectActor(SelfId(), Ctx->UserRequestContext->Database, Ctx->UserRequestContext->CurrentExecutionId, std::move(SaveExternalEffectsState.Requests.front().second), Ctx->LeaseGeneration));
        LOG_D("Save external effect, saver id: " << saverId);
        SaveExternalEffectsState.WaitSave = true;
    }

    void SavePhysicalGraph() {
        Y_VALIDATE(!SavePhysicalGraphState.GraphsToSave.empty() && !SavePhysicalGraphState.WaitSave, "Unexpected call");

        const auto& saverId = Register(CreateSaveScriptExecutionPhysicalGraphActor(SelfId(), Ctx->UserRequestContext->Database, Ctx->UserRequestContext->CurrentExecutionId, std::move(SavePhysicalGraphState.GraphsToSave.front().second), Ctx->LeaseGeneration, QueryServiceConfig));
        LOG_D("Save script physical graph, saver id: " << saverId);
        SavePhysicalGraphState.WaitSave = true;
    }

    void SaveResultsMeta() {
        Y_VALIDATE(!SaveResultsState.WaitSaveMeta, "Unexpected call");

        const auto resultsCount = SaveResultsState.ResultSetInfos.size();
        auto metas = SequenceToJsonString(resultsCount, [infos = &SaveResultsState.ResultSetInfos](const ui64 i, NJson::TJsonValue& value) {
            value = infos->at(i).Meta.SaveJsonMeta();
        });

        const auto& saverId = Register(CreateSaveScriptExecutionResultMetaActor(SelfId(), Ctx->UserRequestContext->Database, Ctx->UserRequestContext->CurrentExecutionId, std::move(metas), Ctx->LeaseGeneration));
        LOG_D("Save result meta for result sets #" << resultsCount << ", saver id: " << saverId);
        SaveResultsState.WaitSaveMeta = true;
    }

    void UpdateScriptProgress() {
        Y_VALIDATE(SaveProgressState.QueryStatsChanged && !SaveProgressState.WaitSave, "Unexpected call");
        SaveProgressState.QueryStatsChanged = false;

        const auto& updaterId = Register(CreateScriptProgressActor(
            Ctx->UserRequestContext->Database,
            Ctx->UserRequestContext->CurrentExecutionId,
            ExecutionInfo.QueryPlan,
            SaveProgressState.AstSaved ? std::nullopt : ExecutionInfo.QueryAst,
            Ctx->LeaseGeneration,
            QueryServiceConfig
        ));
        LOG_T("Start TScriptProgressActor " << updaterId);
        SaveProgressState.WaitSave = true;
    }

    void Finish() {
        Finish(Ydb::StatusIds::SUCCESS);
    }

    void Finish(const Ydb::StatusIds::StatusCode status, const TString& message) {
        Finish(status, {NYql::TIssue(message)});
    }

    void Finish(const Ydb::StatusIds::StatusCode status, NYql::TIssues issues = {}) {
        if (status != Ydb::StatusIds::SUCCESS) {
            LOG_E("Finish with error " << status << ", issues: " << issues.ToOneLineString());
        } else if (!FinishInfo.IsFailed()) {
            LOG_I("Finish successfully");
        }

        FinishInfo.Update(status, std::move(issues));

        if (QueryIsRunning) {
            // We should abort query before finish
            FinishInfo.Update(Ydb::StatusIds::CANCELLED, {NYql::TIssue("Query was cancelled")});
            LOG_I("Wait for query finish, started cancel: " << QueryIsCancelling);

            if (!QueryIsCancelling) {
                auto ev = MakeHolder<TEvKqp::TEvCancelQueryRequest>();
                ev->Record.MutableRequest()->SetSessionId(Ctx->UserRequestContext->SessionId);
                Send(MakeKqpProxyID(SelfId().NodeId()), ev.Release());
                QueryIsCancelling = true;
            }
            return;
        }

        if (FinishInfo.IsSuccess() && SaveResultsState.HasResultsToSave()) {
            LOG_D("Wait for results to save");
            ContinueExecute();
            return;
        }

        if (HasOperationInflight()) {
            LOG_D("Wait for inflight queries to complete");
            return;
        }

        LOG_I("Exit, send response to " << Owner);
        Send(Owner, new TEvRunScriptPrivate::TEvScriptResultHandlerFinished(*FinishInfo.Status, std::move(ExecutionInfo), std::move(FinishInfo.Issues)));
        PassAway();
    }

    TString LogPrefix() const {
        return TStringBuilder() << "[" << ActorName << "] " << SelfId() << ". Owner: " << Owner << ". Ctx: " << *Ctx->UserRequestContext << ". LeaseGeneration: " << Ctx->LeaseGeneration << ". ";
    }

    const TScriptExecutionContext::TPtr Ctx;
    const NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    std::optional<NKikimrKqp::TQueryPhysicalGraph> PhysicalGraph;
    TActorId Owner;
    TFinishInfo FinishInfo;
    TExecutionInfo ExecutionInfo;
    TSaveProgressState SaveProgressState;
    TSaveExternalEffectsState SaveExternalEffectsState;
    TSavePhysicalGraphState SavePhysicalGraphState;
    TSaveResultsState SaveResultsState;
    TMap<ui64, TProducerState> StreamChannels;
    bool QueryIsRunning = true;
    bool QueryIsCancelling = false;
};

} // anonymous namespace

IActor* CreateScriptResultHandlerActor(TScriptExecutionContext::TPtr ctx, std::optional<NKikimrKqp::TQueryPhysicalGraph> physicalGraph, NKikimrConfig::TQueryServiceConfig queryServiceConfig) {
    return new TScriptResultHandlerActor(std::move(ctx), std::move(physicalGraph), std::move(queryServiceConfig));
}

} // namespace NKikimr::NKqp::NPrivate
