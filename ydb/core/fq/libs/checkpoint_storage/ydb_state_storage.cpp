#include "ydb_state_storage.h"

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/ydb/ydb.h>

#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

#include <util/stream/str.h>
#include <util/string/join.h>

namespace NFq {

using namespace NThreading;
using namespace NYdb;
using namespace NYdb::NTable;

using NYql::TIssues;

namespace {

////////////////////////////////////////////////////////////////////////////////

const char* StatesTable = "states";

////////////////////////////////////////////////////////////////////////////////

struct TContext : public TThrRefBase {
    const TString TablePathPrefix;
    const std::vector<ui64> TaskIds;
    const TString GraphId;
    const TCheckpointId CheckpointId;
    std::vector<NYql::NDqProto::TComputeActorState> States;
    TMaybe<TSession> Session;

    TContext(const TString& tablePathPrefix,
             const std::vector<ui64>& taskIds,
             TString graphId,
             const TCheckpointId& checkpointId,
             std::vector<NYql::NDqProto::TComputeActorState> states = {},
             TMaybe<TSession> session = {})
        : TablePathPrefix(tablePathPrefix)
        , TaskIds(taskIds)
        , GraphId(std::move(graphId))
        , CheckpointId(checkpointId)
        , States(std::move(states))
        , Session(session)
    {
    }

    TContext(const TString& tablePathPrefix,
             ui64 taskId,
             TString graphId,
             const TCheckpointId& checkpointId,
             NYql::NDqProto::TComputeActorState state = {},
             TMaybe<TSession> session = {})
        : TContext(tablePathPrefix, std::vector{taskId}, std::move(graphId), checkpointId, std::vector{std::move(state)}, std::move(session))
    {
    }
};

using TContextPtr = TIntrusivePtr<TContext>;

////////////////////////////////////////////////////////////////////////////////

struct TCountStateContext : public TThrRefBase {
    size_t Count = 0;
};

using TCountStateContextPtr = TIntrusivePtr<TCountStateContext>;

////////////////////////////////////////////////////////////////////////////////

static void LoadState(NYql::NDqProto::TComputeActorState& state, const TString& serializedState) {
    if (!state.ParseFromString(serializedState)) { // backward compatibility with YQL serialization
        state.Clear();
        state.MutableMiniKqlProgram()->MutableData()->MutableStateData()->SetBlob(serializedState);
    }
}

TFuture<TStatus> ProcessState(
    const TDataQueryResult& selectResult,
    const TContextPtr& context)
{
    if (!selectResult.IsSuccess()) {
        return MakeFuture<TStatus>(selectResult);
    }

    TResultSetParser parser(selectResult.GetResultSet(0));
    TStringBuilder errorMessage;
    if (parser.RowsCount() == context->TaskIds.size()) {
        context->States.resize(context->TaskIds.size());
        std::vector<bool> processed;
        processed.resize(context->TaskIds.size());
        for (size_t i = 0; i < context->TaskIds.size(); ++i) {
            if (!parser.TryNextRow()) {
                errorMessage << "Can't get next row";
                break;
            }
            auto taskId = parser.ColumnParser("task_id").GetOptionalUint64();
            if (!taskId) {
                errorMessage << "No task id in result";
                break;
            }
            const auto taskIt = std::find(context->TaskIds.begin(), context->TaskIds.end(), *taskId);
            if (taskIt == context->TaskIds.end()) {
                errorMessage << "Got unexpected task id";
                break;
            }
            const size_t taskIndex = std::distance(context->TaskIds.begin(), taskIt);
            if (processed[taskIndex]) {
                errorMessage << "Got duplicated task id";
                break;
            } else {
                processed[taskIndex] = true;
            }
            LoadState(context->States[taskIndex], *parser.ColumnParser("blob").GetOptionalString());
        }
    } else {
        errorMessage << "Not all states exist in database";
    }

    if (errorMessage) {
        TIssues issues;
        TStringStream ss;
        ss << "Failed to select state of checkpoint '" << context->CheckpointId << "'"
           << ", taskIds={" << JoinSeq(", ", context->TaskIds) << "}. Selected rows: " << parser.RowsCount();

        const auto& stats = selectResult.GetStats();
        if (stats) {
            ss << ". Stats: " << stats->ToString();
        }
        ss << ". " << errorMessage;

        // TODO: print status, etc

        // we use GENERIC_ERROR, because not sure if NOT_FOUND non-retrieable
        // also severity is error, because user expects checkpoint to be existed

        return MakeFuture(MakeErrorStatus(EStatus::GENERIC_ERROR, ss.Str()));
    }

    return MakeFuture<TStatus>(selectResult);
}

////////////////////////////////////////////////////////////////////////////////

class TStateStorage : public IStateStorage {
    TYqSharedResources::TPtr YqSharedResources;
    TYdbConnectionPtr YdbConnection;
    const NConfig::TYdbStorageConfig Config;

public:
    explicit TStateStorage(
        const NConfig::TYdbStorageConfig& config,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const TYqSharedResources::TPtr& yqSharedResources);
    ~TStateStorage() = default;

    TFuture<TIssues> Init() override;

    TFuture<TIssues> SaveState(
        ui64 taskId,
        const TString& graphId,
        const TCheckpointId& checkpointId,
        const NYql::NDqProto::TComputeActorState& state) override;

    TFuture<TGetStateResult> GetState(
        const std::vector<ui64>& taskIds,
        const TString& graphId,
        const TCheckpointId& checkpointId) override;

    TFuture<TCountStatesResult> CountStates(
        const TString& graphId,
        const TCheckpointId& checkpointId) override;

    TFuture<TIssues> DeleteGraph(
        const TString& graphId) override;

    TFuture<TIssues> DeleteCheckpoints(
        const TString& graphId,
        const TCheckpointId& checkpointId) override;

    TFuture<TDataQueryResult> SelectState(const TContextPtr& context);
    TFuture<TStatus> UpsertState(const TContextPtr& context);
    TExecDataQuerySettings DefaultExecDataQuerySettings();
};

////////////////////////////////////////////////////////////////////////////////

TStateStorage::TStateStorage(
    const NConfig::TYdbStorageConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources)
    : YqSharedResources(yqSharedResources)
    , YdbConnection(NewYdbConnection(config, credentialsProviderFactory, YqSharedResources->UserSpaceYdbDriver))
    , Config(config)
{
}

TFuture<TIssues> TStateStorage::Init()
{
    TIssues issues;

    // TODO: list at first?
    if (YdbConnection->DB != YdbConnection->TablePathPrefix) {
        //LOG_STREAMS_STORAGE_SERVICE_INFO("Creating directory: " << YdbConnection->TablePathPrefix);
        auto status = YdbConnection->SchemeClient.MakeDirectory(YdbConnection->TablePathPrefix).GetValueSync();
        if (!status.IsSuccess() && status.GetStatus() != EStatus::ALREADY_EXISTS) {
            issues = status.GetIssues();

            TStringStream ss;
            ss << "Failed to create path '" << YdbConnection->TablePathPrefix << "': " << status.GetStatus();
            if (issues) {
                ss << ", issues: ";
                issues.PrintTo(ss);
            }

            //LOG_STREAMS_STORAGE_SERVICE_DEBUG(ss.Str());
            return MakeFuture(std::move(issues));
        }
    }

    auto stateDesc = TTableBuilder()
        .AddNullableColumn("graph_id", EPrimitiveType::String)
        .AddNullableColumn("task_id", EPrimitiveType::Uint64)
        .AddNullableColumn("coordinator_generation", EPrimitiveType::Uint64)
        .AddNullableColumn("seq_no", EPrimitiveType::Uint64)
        .AddNullableColumn("blob", EPrimitiveType::String)
        .SetPrimaryKeyColumns({"graph_id", "task_id", "coordinator_generation", "seq_no"})
        .Build();

    auto status = CreateTable(YdbConnection, StatesTable, std::move(stateDesc)).GetValueSync();
    if (!IsTableCreated(status)) {
        issues = status.GetIssues();

        TStringStream ss;
        ss << "Failed to create " << StatesTable << " table: " << status.GetStatus();
        if (issues) {
            ss << ", issues: ";
            issues.PrintTo(ss);
        }

        //LOG_STREAMS_STORAGE_SERVICE_DEBUG(ss.Str());
    }

    return MakeFuture(std::move(issues));
}

TFuture<TIssues> TStateStorage::SaveState(
    ui64 taskId,
    const TString& graphId,
    const TCheckpointId& checkpointId,
    const NYql::NDqProto::TComputeActorState& state)
{
    auto future = YdbConnection->TableClient.RetryOperation(
        [prefix = YdbConnection->TablePathPrefix, taskId, graphId, checkpointId, state, thisPtr = TIntrusivePtr(this)] (TSession session) {
            auto context = MakeIntrusive<TContext>(
                prefix,
                taskId,
                graphId,
                checkpointId,
                state,
                session);

            return thisPtr->UpsertState(context);
        });

    return StatusToIssues(future);
}

TFuture<IStateStorage::TGetStateResult> TStateStorage::GetState(
    const std::vector<ui64>& taskIds,
    const TString& graphId,
    const TCheckpointId& checkpointId)
{
    if (taskIds.empty()) {
        IStateStorage::TGetStateResult result;
        result.second.AddIssue("Internal error loading state: no task ids specified");
        return MakeFuture<IStateStorage::TGetStateResult>(result);
    }

    if (taskIds.size() > 1 && std::set<ui64>(taskIds.begin(), taskIds.end()).size() != taskIds.size()) {
        IStateStorage::TGetStateResult result;
        result.second.AddIssue("Internal error loading state: duplicated task ids specified");
        return MakeFuture<IStateStorage::TGetStateResult>(result);
    }

    auto context = MakeIntrusive<TContext>(
        YdbConnection->TablePathPrefix,
        taskIds,
        graphId,
        checkpointId);

    auto future = YdbConnection->TableClient.RetryOperation(
        [context, thisPtr = TIntrusivePtr(this)] (TSession session) {
            context->Session = session;
            auto future = thisPtr->SelectState(context);
            return future.Apply(
                [context] (const TFuture<TDataQueryResult>& future) {
                    return ProcessState(future.GetValue(), context);
                });
        });

    return StatusToIssues(future).Apply(
        [context] (const TFuture<TIssues>& future) {
            TGetStateResult result;
            std::swap(result.first, context->States);
            result.second = future.GetValue();
            return MakeFuture(std::move(result));
        });
}

TFuture<IStateStorage::TCountStatesResult> TStateStorage::CountStates(
    const TString& graphId,
    const TCheckpointId& checkpointId)
{
    auto context = MakeIntrusive<TCountStateContext>();

    auto future = YdbConnection->TableClient.RetryOperation(
        [prefix = YdbConnection->TablePathPrefix, graphId, checkpointId, context, thisPtr = TIntrusivePtr(this)] (TSession session) {

            // publish nodes
            NYdb::TParamsBuilder paramsBuilder;
            paramsBuilder.AddParam("$graph_id").String(graphId).Build();
            paramsBuilder.AddParam("$coordinator_generation").Uint64(checkpointId.CoordinatorGeneration).Build();
            paramsBuilder.AddParam("$seq_no").Uint64(checkpointId.SeqNo).Build();

            auto params = paramsBuilder.Build();
            auto query = Sprintf(R"(
                --!syntax_v1
                PRAGMA TablePathPrefix("%s");

                declare $graph_id as string;
                declare $coordinator_generation as Uint64;
                declare $seq_no as Uint64;

                SELECT COUNT(*) as cnt
                FROM %s
                WHERE graph_id = $graph_id AND coordinator_generation = $coordinator_generation and seq_no = $seq_no;
            )", prefix.c_str(), StatesTable);

            auto future = session.ExecuteDataQuery(
                query,
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
                params,
                thisPtr->DefaultExecDataQuerySettings());

            return future.Apply(
                [context] (const TFuture<TDataQueryResult>& future) {
                    const auto& selectResult = future.GetValue();
                    if (!selectResult.IsSuccess()) {
                        return MakeFuture<TStatus>(selectResult);
                    }

                    TResultSetParser parser(selectResult.GetResultSet(0));
                    if (parser.TryNextRow()) {
                        context->Count = parser.ColumnParser("cnt").GetUint64();
                    }

                    return MakeFuture<TStatus>(selectResult);
            });
        });

    return future.Apply(
        [context] (const TFuture<TStatus>& future) {
            TCountStatesResult countResult;
            countResult.first = context->Count;
            const auto& status = future.GetValue();
            if (!status.IsSuccess()) {
                countResult.second = status.GetIssues();
            }
            return countResult;
        });
}
TExecDataQuerySettings TStateStorage::DefaultExecDataQuerySettings() {
    return TExecDataQuerySettings()
        .KeepInQueryCache(true)
        .ClientTimeout(TDuration::Seconds(Config.GetClientTimeoutSec()))
        .OperationTimeout(TDuration::Seconds(Config.GetOperationTimeoutSec()))
        .CancelAfter(TDuration::Seconds(Config.GetCancelAfterSec()));
}

TFuture<TIssues> TStateStorage::DeleteGraph(const TString& graphId) {
    auto future = YdbConnection->TableClient.RetryOperation(
        [prefix = YdbConnection->TablePathPrefix, graphId, thisPtr = TIntrusivePtr(this)] (TSession session) {

            // publish nodes
            NYdb::TParamsBuilder paramsBuilder;
            paramsBuilder.AddParam("$graph_id").String(graphId).Build();

            auto params = paramsBuilder.Build();

            auto query = Sprintf(R"(
                --!syntax_v1
                PRAGMA TablePathPrefix("%s");

                declare $graph_id as string;

                DELETE
                FROM %s
                WHERE graph_id = "%s";
            )", prefix.c_str(), StatesTable, graphId.c_str());

            auto future = session.ExecuteDataQuery(
                query,
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
                params,
                thisPtr->DefaultExecDataQuerySettings());

            return future.Apply(
                [] (const TFuture<TDataQueryResult>& future) {
                    TStatus status = future.GetValue();
                    return status;
            });
        });

    return StatusToIssues(future);
}

TFuture<TIssues> TStateStorage::DeleteCheckpoints(
    const TString& graphId,
    const TCheckpointId& checkpointUpperBound)
{
    auto future = YdbConnection->TableClient.RetryOperation(
        [prefix = YdbConnection->TablePathPrefix, graphId, checkpointUpperBound, thisPtr = TIntrusivePtr(this)] (TSession session) {

            // publish nodes
            NYdb::TParamsBuilder paramsBuilder;
            paramsBuilder.AddParam("$graph_id").String(graphId).Build();
            paramsBuilder.AddParam("$coordinator_generation").Uint64(checkpointUpperBound.CoordinatorGeneration).Build();
            paramsBuilder.AddParam("$seq_no").Uint64(checkpointUpperBound.SeqNo).Build();

            auto params = paramsBuilder.Build();

            auto query = Sprintf(R"(
                --!syntax_v1
                PRAGMA TablePathPrefix("%s");

                declare $graph_id as string;
                declare $coordinator_generation as Uint64;
                declare $seq_no as Uint64;

                DELETE
                FROM %s
                WHERE graph_id = $graph_id AND
                    (coordinator_generation < $coordinator_generation OR
                        (coordinator_generation = $coordinator_generation AND seq_no < $seq_no));
            )", prefix.c_str(), StatesTable);

            auto future = session.ExecuteDataQuery(
                query,
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
                params,
                thisPtr->DefaultExecDataQuerySettings());

            return future.Apply(
                [] (const TFuture<TDataQueryResult>& future) {
                    TStatus status = future.GetValue();
                    return status;
            });
        });

    return StatusToIssues(future);
}
TFuture<TDataQueryResult> TStateStorage::SelectState(const TContextPtr& context)
{
    NYdb::TParamsBuilder paramsBuilder;
    Y_VERIFY(!context->TaskIds.empty());
    if (context->TaskIds.size() == 1) {
        paramsBuilder.AddParam("$task_id").Uint64(context->TaskIds[0]).Build();
    } else {
        auto& taskIdsParam = paramsBuilder.AddParam("$task_ids").BeginList();
        for (const ui64 taskId : context->TaskIds) {
            taskIdsParam.AddListItem().Uint64(taskId);
        }
        taskIdsParam.EndList().Build();
    }
    paramsBuilder.AddParam("$graph_id").String(context->GraphId).Build();
    paramsBuilder.AddParam("$coordinator_generation").Uint64(context->CheckpointId.CoordinatorGeneration).Build();
    paramsBuilder.AddParam("$seq_no").Uint64(context->CheckpointId.SeqNo).Build();

    auto params = paramsBuilder.Build();

    auto query = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");

        %s;
        DECLARE $graph_id AS string;
        DECLARE $coordinator_generation AS Uint64;
        DECLARE $seq_no AS Uint64;

        SELECT task_id, blob
        FROM %s
        WHERE %s AND graph_id = $graph_id AND coordinator_generation = $coordinator_generation AND seq_no = $seq_no;
    )",
        context->TablePathPrefix.c_str(),
        context->TaskIds.size() == 1 ? "DECLARE $task_id AS Uint64" : "DECLARE $task_ids AS List<Uint64>",
        StatesTable,
        context->TaskIds.size() == 1 ? "task_id = $task_id" : "task_id IN $task_ids");

    Y_VERIFY(context->Session);
    return context->Session->ExecuteDataQuery(
        query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        params,
        DefaultExecDataQuerySettings());
}

TFuture<TStatus> TStateStorage::UpsertState(const TContextPtr& context) {
    Y_VERIFY(context->States.size() == 1);
    TString serializedState;
    if (!context->States[0].SerializeToString(&serializedState)) {
        return MakeFuture(MakeErrorStatus(EStatus::BAD_REQUEST, "Failed to serialize compute actor state", NYql::TSeverityIds::S_ERROR));
    }

    // publish nodes
    NYdb::TParamsBuilder paramsBuilder;
    Y_VERIFY(context->TaskIds.size() == 1);
    paramsBuilder.AddParam("$task_id").Uint64(context->TaskIds[0]).Build();
    paramsBuilder.AddParam("$graph_id").String(context->GraphId).Build();
    paramsBuilder.AddParam("$coordinator_generation").Uint64(context->CheckpointId.CoordinatorGeneration).Build();
    paramsBuilder.AddParam("$seq_no").Uint64(context->CheckpointId.SeqNo).Build();
    paramsBuilder.AddParam("$blob").String(serializedState).Build();

    auto params = paramsBuilder.Build();

    auto query = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");

        declare $task_id as Uint64;
        declare $graph_id as string;
        declare $coordinator_generation as Uint64;
        declare $seq_no as Uint64;
        declare $blob as string;

        UPSERT INTO %s (task_id, graph_id, coordinator_generation, seq_no, blob) VALUES
            ($task_id, $graph_id, $coordinator_generation, $seq_no, $blob);
    )", context->TablePathPrefix.c_str(), StatesTable);

    Y_VERIFY(context->Session);
    auto future = context->Session->ExecuteDataQuery(
        query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        params,
        DefaultExecDataQuerySettings());
    return future.Apply(
        [] (const TFuture<TDataQueryResult>& future) {
          TStatus status = future.GetValue();
          return status;
        });
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TStateStoragePtr NewYdbStateStorage(
    const NConfig::TYdbStorageConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources)
{
    return new TStateStorage(config, credentialsProviderFactory, yqSharedResources);
}

} // namespace NFq
