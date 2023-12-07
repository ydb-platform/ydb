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
    struct TaskInfo {
        TaskInfo (ui64 taskId) : TaskId(taskId) {}

        ui64 TaskId = 0;
        ui64 RowsCount = 0;
        size_t CurrentRow = 0;
        std::list<TString> NewStates;
    };

    const TString TablePathPrefix;
    const TString GraphId;
    const TCheckpointId CheckpointId;
    TMaybe<TSession> Session;
    size_t CurrentTaskIndex = 0;
    std::vector<TaskInfo> Tasks;
    std::function<void(TFuture<TStatus>)> Callback;

    TContext(const TString& tablePathPrefix,
             const std::vector<ui64>& taskIds,
             TString graphId,
             const TCheckpointId& checkpointId,
             TMaybe<TSession> session = {})
        : TablePathPrefix(tablePathPrefix)
        , GraphId(std::move(graphId))
        , CheckpointId(checkpointId)
        , Session(session)
    {
        for (auto taskId : taskIds) {
            Tasks.emplace_back(taskId);
        }
    }

    TContext(const TString& tablePathPrefix,
             ui64 taskId,
             TString graphId,
             const TCheckpointId& checkpointId,
             TMaybe<TSession> session = {},
             const std::list<TString>& newStates = {})
        : TContext(tablePathPrefix, std::vector{taskId}, std::move(graphId), checkpointId, std::move(session))
    {
        Tasks[0].NewStates = newStates;
    }
};

using TContextPtr = TIntrusivePtr<TContext>;

////////////////////////////////////////////////////////////////////////////////

struct TCountStateContext : public TThrRefBase {
    size_t Count = 0;
};

using TCountStateContextPtr = TIntrusivePtr<TCountStateContext>;

////////////////////////////////////////////////////////////////////////////////

static std::vector<NYql::NDqProto::TComputeActorState> DeserealizeState(const TContextPtr& context)
{
    std::vector<NYql::NDqProto::TComputeActorState> states(context->Tasks.size());
    size_t i = 0;
    for (auto& tastInfo : context->Tasks) {
        TString blob;
        for (auto it = tastInfo.NewStates.begin(); it != tastInfo.NewStates.end();) {
            blob += *it;
            it = tastInfo.NewStates.erase(it);
        }
        if (!states[i].ParseFromString(blob)) { // backward compatibility with YQL serialization
            states[i].Clear();
            states[i].MutableMiniKqlProgram()->MutableData()->MutableStateData()->SetBlob(blob);
        }
        ++i;
    }
    return states;
}

TStatus ProcessRowState(
    const TDataQueryResult& selectResult,
    const TContextPtr& context)
{
    if (!selectResult.IsSuccess()) {
        return selectResult;
    }

    TResultSetParser parser(selectResult.GetResultSet(0));
    TStringBuilder errorMessage;
    auto& taskInfo = context->Tasks[context->CurrentTaskIndex];

    do {
        if (!parser.TryNextRow()) {
            errorMessage << "Can't get next row";
            break;
        }
        auto taskId = parser.ColumnParser("task_id").GetOptionalUint64();
        if (!taskId) {
            errorMessage << "No task id in result";
            break;
        }
        const auto taskIt = std::find_if(context->Tasks.begin(), context->Tasks.end(), [&](const auto& item) { return item.TaskId == *taskId; });
        if (taskIt == context->Tasks.end()) {
            errorMessage << "Got unexpected task id";
            break;
        }
        taskInfo.NewStates.push_back(*parser.ColumnParser("blob").GetOptionalString());
    } while(0);

    if (errorMessage) {
        TIssues issues;
        TStringStream ss;
        ss << "Failed to select state of checkpoint '" << context->CheckpointId << "'"
           << ", taskIds={";
        for (const auto& item : context->Tasks) {
            ss << item.TaskId << ", ";
        } 
        ss << "}. Selected rows: " << parser.RowsCount();

        const auto& stats = selectResult.GetStats();
        if (stats) {
            ss << ". Stats: " << stats->ToString();
        }
        ss << ". " << errorMessage;

        // TODO: print status, etc

        // we use GENERIC_ERROR, because not sure if NOT_FOUND non-retrieable
        // also severity is error, because user expects checkpoint to be existed

        return MakeErrorStatus(EStatus::GENERIC_ERROR, ss.Str());
    }

    return selectResult;
}

////////////////////////////////////////////////////////////////////////////////

class TStateStorage : public IStateStorage {
    TYqSharedResources::TPtr YqSharedResources;
    TYdbConnectionPtr YdbConnection;
    const NConfig::TYdbStorageConfig StorageConfig;
    const NConfig::TCheckpointCoordinatorConfig Config;

public:
    explicit TStateStorage(
        const NConfig::TCheckpointCoordinatorConfig& config,
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

private:
    TFuture<TDataQueryResult> SelectState(
        const TContextPtr& context);

    TFuture<TStatus> UpsertRow(
        const TContextPtr& context);

    TExecDataQuerySettings DefaultExecDataQuerySettings();

    TFuture<TStatus> SelectRowState(
        const TContextPtr& context);

    TFuture<TStatus> ReadRowsCountPerTask(
        const TContextPtr& context);

    std::list<TString> SerializeState(
        const NYql::NDqProto::TComputeActorState& state);
};

////////////////////////////////////////////////////////////////////////////////

TStateStorage::TStateStorage(
    const NConfig::TCheckpointCoordinatorConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources)
    : YqSharedResources(yqSharedResources)
    , YdbConnection(NewYdbConnection(config.GetStorage(), credentialsProviderFactory, YqSharedResources->UserSpaceYdbDriver))
    , StorageConfig(config.GetStorage())
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
        .AddNullableColumn("blob_seq_num", EPrimitiveType::Uint64)
        .SetPrimaryKeyColumns({"graph_id", "task_id", "coordinator_generation", "seq_no", "blob_seq_num"})
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
    }

    return MakeFuture(std::move(issues));
}

std::list<TString> TStateStorage::SerializeState(const NYql::NDqProto::TComputeActorState& state)
{
    std::list<TString> result;
    TString serializedState;
    if (!state.SerializeToString(&serializedState)) {
        return result;
    }

    auto size = serializedState.size();
    size_t rowLimit = Config.GetStateStorageLimits().GetMaxRowSizeBytes();
    size_t offset = 0;
    while (size) {
        size_t chunkSize = size <= rowLimit ? size : rowLimit;
        result.push_back(serializedState.substr(offset, chunkSize));
        offset += chunkSize;
        size -= chunkSize;
    }
    return result;
}

TFuture<TIssues> TStateStorage::SaveState(
    ui64 taskId,
    const TString& graphId,
    const TCheckpointId& checkpointId,
    const NYql::NDqProto::TComputeActorState& state)
{
    auto serializedState = SerializeState(state);
    if (serializedState.empty()) {
        return MakeFuture(NYql::TIssues{NYql::TIssue{"Failed to serialize compute actor state"}});
    }

    auto context = MakeIntrusive<TContext>(
        YdbConnection->TablePathPrefix,
        taskId,
        graphId,
        checkpointId,
        TMaybe<TSession>(), 
        serializedState);

    auto promise = NewPromise<TIssues>();
    auto future = UpsertRow(context);

    context->Callback = [promise, context, thisPtr = TIntrusivePtr(this)] (TFuture<TStatus> upsertRowStatus) mutable {
        TStatus status = upsertRowStatus.GetValue();
        if (!status.IsSuccess()) {
            promise.SetValue(StatusToIssues(status));
            return;
        }
        auto& taskInfo = context->Tasks[context->CurrentTaskIndex];
        taskInfo.NewStates.pop_front();
        ++taskInfo.CurrentRow;

        if (!taskInfo.NewStates.empty()) {
            auto nextFuture = thisPtr->UpsertRow(context);
            nextFuture.Subscribe(context->Callback);
            return;
        }
        promise.SetValue(StatusToIssues(status));
    };
    future.Subscribe(context->Callback);
    return promise.GetFuture();
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

    auto promise = NewPromise<IStateStorage::TGetStateResult>();

    auto context = MakeIntrusive<TContext>(
        YdbConnection->TablePathPrefix,
        taskIds,
        graphId,
        checkpointId);

    context->CurrentTaskIndex = 0;

    auto future = ReadRowsCountPerTask(context);

    future.Subscribe([promise, context, thisPtr = TIntrusivePtr(this)](TFuture<TStatus> result) mutable
        {
            TStatus status = result.GetValue();
            if (!result.GetValue().IsSuccess()) {
                promise.SetValue(IStateStorage::TGetStateResult{{}, status.GetIssues()});
                return;
            }

            context->Callback = 
                [context, promise, thisPtr] (const TFuture<TStatus>& future) mutable {
                    TStatus status = future.GetValue();
                    if (!status.IsSuccess()) {
                        promise.SetValue(IStateStorage::TGetStateResult{{}, status.GetIssues()});
                        return;
                    }
                    auto& taskInfo = context->Tasks[context->CurrentTaskIndex];
                    ++taskInfo.CurrentRow;

                    if (taskInfo.CurrentRow != taskInfo.RowsCount
                        || context->CurrentTaskIndex != context->Tasks.size() - 1)
                    {
                        if (taskInfo.CurrentRow == taskInfo.RowsCount) {
                            ++context->CurrentTaskIndex;
                            taskInfo.CurrentRow = 0;
                        }
                        auto nextFuture = thisPtr->SelectRowState(context);
                        nextFuture.Subscribe(context->Callback);
                        return;
                    }
                    auto states = DeserealizeState(context);
                    IStateStorage::TGetStateResult result{std::move(states), status.GetIssues()};
                    promise.SetValue(std::move(result));
                };

            auto future = thisPtr->SelectRowState(context);
            future.Subscribe(context->Callback);
        });
    return promise.GetFuture();
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

                SELECT COUNT(*) as cnt FROM ( 
                    SELECT task_id
                    FROM %s
                    WHERE graph_id = $graph_id AND coordinator_generation = $coordinator_generation and seq_no = $seq_no
                    GROUP BY task_id);
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

TFuture<TStatus> TStateStorage::ReadRowsCountPerTask(const TContextPtr& context)
{
    return YdbConnection->TableClient.RetryOperation(
        [prefix = YdbConnection->TablePathPrefix, context, thisPtr = TIntrusivePtr(this)] (TSession session) {

            // publish nodes
            NYdb::TParamsBuilder paramsBuilder;
            paramsBuilder.AddParam("$graph_id").String(context->GraphId).Build();
            paramsBuilder.AddParam("$coordinator_generation").Uint64(context->CheckpointId.CoordinatorGeneration).Build();
            paramsBuilder.AddParam("$seq_no").Uint64(context->CheckpointId.SeqNo).Build();

            if (context->Tasks.size() == 1) {
                paramsBuilder.AddParam("$task_id").Uint64(context->Tasks[0].TaskId).Build();
            } else {
                auto& taskIdsParam = paramsBuilder.AddParam("$task_ids").BeginList();
                for (const auto& taskInfo : context->Tasks) {
                    taskIdsParam.AddListItem().Uint64(taskInfo.TaskId);
                }
                taskIdsParam.EndList().Build();
            }

            auto params = paramsBuilder.Build();
            auto query = Sprintf(R"(
                --!syntax_v1
                PRAGMA AnsiInForEmptyOrNullableItemsCollections;
                PRAGMA TablePathPrefix("%s");

                declare $graph_id as string;
                declare $coordinator_generation as Uint64;
                declare $seq_no as Uint64;
                %s;

                SELECT task_id, CAST(COUNT(*) as UINT64) as cnt
                FROM %s
                WHERE graph_id = $graph_id AND coordinator_generation = $coordinator_generation and seq_no = $seq_no AND %s
                GROUP BY task_id;
            )", prefix.c_str(),
                context->Tasks.size() == 1 ? "DECLARE $task_id AS Uint64" : "DECLARE $task_ids AS List<Uint64>",
                StatesTable,
                context->Tasks.size() == 1 ? "task_id = $task_id" : "task_id IN $task_ids");

            auto future = session.ExecuteDataQuery(
                query,
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
                params,
                thisPtr->DefaultExecDataQuerySettings());

            return future.Apply(
                [context] (const TFuture<TDataQueryResult>& future) {
                    TStatus status = future.GetValue();

                    if (!status.IsSuccess()) {
                        return status;
                    }

                    try {
                        const auto& selectResult = future.GetValue();
                        TResultSetParser parser(selectResult.GetResultSet(0));
                        while (parser.TryNextRow()) {
                            auto taskId = parser.ColumnParser("task_id").GetOptionalUint64();
                            auto cnt = parser.ColumnParser("cnt").GetUint64();
                            if (!taskId) {
                                return TStatus(EStatus::BAD_REQUEST, NYql::TIssues{NYql::TIssue{"Got unexpected task id"}});
                            }
                            const auto taskIt = std::find_if(context->Tasks.begin(), context->Tasks.end(), [&] (const auto& item) { return item.TaskId == *taskId; } );
                            if (taskIt == context->Tasks.end()) {
                                return TStatus(EStatus::BAD_REQUEST, NYql::TIssues{NYql::TIssue{"Got unexpected task id"}});
                            }
                            taskIt->RowsCount = cnt;
                        }
                    }
                    catch (const std::exception& e) {
                        return TStatus(EStatus::BAD_REQUEST, NYql::TIssues{NYql::TIssue{e.what()}});
                    }

                    return status;
            });
        });
}

TExecDataQuerySettings TStateStorage::DefaultExecDataQuerySettings() {
    return TExecDataQuerySettings()
        .KeepInQueryCache(true)
        .ClientTimeout(TDuration::Seconds(StorageConfig.GetClientTimeoutSec()))
        .OperationTimeout(TDuration::Seconds(StorageConfig.GetOperationTimeoutSec()))
        .CancelAfter(TDuration::Seconds(StorageConfig.GetCancelAfterSec()));
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

TFuture<TStatus> TStateStorage::SelectRowState(const TContextPtr& context)
{
    return YdbConnection->TableClient.RetryOperation(
        [context, this] (TSession session) {
            context->Session = session;
            auto future = SelectState(context);
            return future.Apply(
                [context] (const TFuture<TDataQueryResult>& future) {  
                return ProcessRowState(future.GetValue(), context);
            });
        });
}

TFuture<TDataQueryResult> TStateStorage::SelectState(const TContextPtr& context)
{
    NYdb::TParamsBuilder paramsBuilder;
    Y_ABORT_UNLESS(!context->Tasks.empty());
    auto& taskInfo = context->Tasks[context->CurrentTaskIndex];

    paramsBuilder.AddParam("$task_id").Uint64(taskInfo.TaskId).Build();
    paramsBuilder.AddParam("$graph_id").String(context->GraphId).Build();
    paramsBuilder.AddParam("$coordinator_generation").Uint64(context->CheckpointId.CoordinatorGeneration).Build();
    paramsBuilder.AddParam("$seq_no").Uint64(context->CheckpointId.SeqNo).Build();
    paramsBuilder.AddParam("$blob_seq_num").Uint64(taskInfo.CurrentRow).Build();

    auto params = paramsBuilder.Build();

    auto query = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");
        PRAGMA AnsiInForEmptyOrNullableItemsCollections;

        DECLARE $task_id AS Uint64;
        DECLARE $graph_id AS string;
        DECLARE $coordinator_generation AS Uint64;
        DECLARE $seq_no AS Uint64;
        DECLARE $blob_seq_num AS Uint64;

        SELECT task_id, blob
        FROM %s
        WHERE task_id = $task_id AND graph_id = $graph_id AND coordinator_generation = $coordinator_generation AND seq_no = $seq_no AND blob_seq_num = $blob_seq_num;
    )",
        context->TablePathPrefix.c_str(),
        StatesTable);

    Y_ABORT_UNLESS(context->Session);
    return context->Session->ExecuteDataQuery(
        query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        params,
        DefaultExecDataQuerySettings());
}

TFuture<TStatus> TStateStorage::UpsertRow(const TContextPtr& context) {

    return YdbConnection->TableClient.RetryOperation(
        [context, thisPtr = TIntrusivePtr(this)] (TSession session) {
            context->Session = session;
            // publish nodes
            NYdb::TParamsBuilder paramsBuilder;
            Y_ABORT_UNLESS(context->Tasks.size() == 1);
            auto& taskInfo = context->Tasks[context->CurrentTaskIndex];

            paramsBuilder.AddParam("$task_id").Uint64(taskInfo.TaskId).Build();
            paramsBuilder.AddParam("$graph_id").String(context->GraphId).Build();
            paramsBuilder.AddParam("$coordinator_generation").Uint64(context->CheckpointId.CoordinatorGeneration).Build();
            paramsBuilder.AddParam("$seq_no").Uint64(context->CheckpointId.SeqNo).Build();
            paramsBuilder.AddParam("$blob").String(taskInfo.NewStates.front()).Build();
            paramsBuilder.AddParam("$blob_seq_num").Uint64(taskInfo.CurrentRow).Build();

            auto params = paramsBuilder.Build();

            auto query = Sprintf(R"(
                --!syntax_v1
                PRAGMA TablePathPrefix("%s");

                declare $task_id as Uint64;
                declare $graph_id as string;
                declare $coordinator_generation as Uint64;
                declare $seq_no as Uint64;
                declare $blob as string;
                declare $blob_seq_num as Uint64;

                UPSERT INTO %s (task_id, graph_id, coordinator_generation, seq_no, blob, blob_seq_num) VALUES
                    ($task_id, $graph_id, $coordinator_generation, $seq_no, $blob, $blob_seq_num);
            )", context->TablePathPrefix.c_str(), StatesTable);

            Y_ABORT_UNLESS(context->Session);

            auto future = context->Session->ExecuteDataQuery(
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
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TStateStoragePtr NewYdbStateStorage(
    const NConfig::TCheckpointCoordinatorConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources)
{
    return new TStateStorage(config, credentialsProviderFactory, yqSharedResources);
}

} // namespace NFq
