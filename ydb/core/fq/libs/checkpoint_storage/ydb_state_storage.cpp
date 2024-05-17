#include "ydb_state_storage.h"

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/ydb/ydb.h>

#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

#include <ydb/library/yql/minikql/comp_nodes/mkql_saveload.h>

#include <util/stream/str.h>
#include <util/string/join.h>

namespace NFq {

using namespace NThreading;
using namespace NYdb;
using namespace NYdb::NTable;

using NYql::TIssues;

namespace {

#define LOG_STORAGE_DEBUG(context, stream) LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(*context->ActorSystem, "[" << context->GraphId << "] [" << context->CheckpointId << "] " << stream);

////////////////////////////////////////////////////////////////////////////////

const char* StatesTable = "states";
const ui64 DeleteStateTimeoutMultiplier = 10;

////////////////////////////////////////////////////////////////////////////////

enum class EStateType {
    Snapshot = 1,
    Increment = 2
};

////////////////////////////////////////////////////////////////////////////////

class TIncrementLogic {
public:

    void Apply(NYql::NDq::TComputeActorState& update) {
        Sources = update.Sources;      // Always snapshot - copy it;
        Sinks = update.Sinks;
        ui64 nodeNum = 0;

        if (update.MiniKqlProgram) {
            const TString& blob = update.MiniKqlProgram->Data.Blob;
            ui64 version = update.MiniKqlProgram->Data.Version;
            if (LastVersion) {
                Y_ENSURE(*LastVersion == version, "Version is different: " << *LastVersion << ", " << version);
            }
            LastVersion = version;
            TStringBuf buf(blob);        

            while (!buf.empty()) {
                auto nodeStateSize = NKikimr::NMiniKQL::ReadUi64(buf);
                Y_ENSURE(buf.Size() >= nodeStateSize, "State/buf is corrupted");
                TStringBuf nodeStateBuf(buf.Data(), nodeStateSize);
                buf.Skip(nodeStateSize);

                NKikimr::NMiniKQL::TInputSerializer reader(nodeStateBuf);
                auto type = reader.GetType();
                auto& nodeState = NodeStates[nodeNum];
                nodeState.Type = type;

                switch (type) {
                    case NKikimr::NMiniKQL::EMkqlStateType::SIMPLE_BLOB:
                    {
                        nodeState.SimpleBlobNodeState = TString(nodeStateBuf.Data(), nodeStateBuf.Size());
                    }
                    break;
                    case NKikimr::NMiniKQL::EMkqlStateType::SNAPSHOT:
                    case NKikimr::NMiniKQL::EMkqlStateType::INCREMENT:
                    {
                        reader.ReadItems(
                            [&](std::string_view key, std::string_view value) {
                                nodeState.Items[TString(key)] = TString(value);
                            },
                            [&](std::string_view key) {
                                // Not used for SNAPSHOT.
                                nodeState.Items.erase(TString(key));
                            });
                    }
                    break;
                }
                ++nodeNum;
            }
            Y_ENSURE(buf.empty(), "State/buf is corrupted");
        }
    }

    NYql::NDq::TComputeActorState Build() {
        NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        NYql::NDq::TComputeActorState state;
        state.Sources = Sources;
        state.Sinks = Sinks;
        TString result;                
        for (const auto& [nodeNum, nodeState] : NodeStates) {
            
            if (nodeState.Type == NKikimr::NMiniKQL::EMkqlStateType::SIMPLE_BLOB) {
                NKikimr::NMiniKQL::TNodeStateHelper::AddNodeState(result, nodeState.SimpleBlobNodeState);
            } else {
                NYql::NUdf::TUnboxedValue saved =
                     NKikimr::NMiniKQL::TOutputSerializer::MakeSnapshotState(nodeState.Items, 0);
                const TStringBuf savedBuf = saved.AsStringRef();
                NKikimr::NMiniKQL::WriteUi64(result, savedBuf.Size());
                result.AppendNoAlias(savedBuf.Data(), savedBuf.Size());
            }
        }
        auto& stateData = state.MiniKqlProgram.ConstructInPlace().Data;
        stateData.Blob = result;
        Y_ENSURE(LastVersion, "LastVersion is empty");
        stateData.Version = *LastVersion;
        return state;
    }

    static EStateType GetStateType(const NYql::NDq::TComputeActorState& state) {
        if (!state.MiniKqlProgram) {
            return EStateType::Snapshot;
        }
        const TString& blob = state.MiniKqlProgram->Data.Blob;
        TStringBuf buf(blob);
        while (!buf.empty()) {
            auto nodeStateSize = NKikimr::NMiniKQL::ReadUi64(buf);
            Y_ENSURE(buf.Size() >= nodeStateSize, "State/buf is corrupted");
            TStringBuf nodeStateBuf(buf.Data(), nodeStateSize);
            if (NKikimr::NMiniKQL::TInputSerializer(nodeStateBuf).GetType() == NKikimr::NMiniKQL::EMkqlStateType::INCREMENT) {
                return EStateType::Increment;
            }
            buf.Skip(nodeStateSize);
        }
        Y_ENSURE(buf.empty(), "State/buf is corrupted");
        return EStateType::Snapshot;
    }

private:
    struct NodeState {
        NKikimr::NMiniKQL::EMkqlStateType Type;
        std::map<TString, TString> Items;
        TString SimpleBlobNodeState;
    };
    std::map<ui64, NodeState> NodeStates;
    std::list<NYql::NDq::TSourceState> Sources;
    std::list<NYql::NDq::TSinkState> Sinks;
    TMaybe<ui64> LastVersion;
};

////////////////////////////////////////////////////////////////////////////////

struct TContext : public TThrRefBase {

    struct TStateInfo {
        TCheckpointId CheckpointId;
        ui64 StateRowsCount = 0;
    };

    struct TaskInfo {
        TaskInfo (ui64 taskId) : TaskId(taskId) {}

        ui64 TaskId = 0;
        size_t CurrentProcessingRow = 0;
        std::list<TString> Rows;
        EStateType Type = EStateType::Snapshot;
        std::list<TStateInfo> ListOfStatesForReading;     // ordered by desc
        std::list<NYql::NDq::TComputeActorState> States;
    };

    const NActors::TActorSystem* ActorSystem;
    const TString TablePathPrefix;
    const TString GraphId;
    const TCheckpointId CheckpointId;
    TMaybe<TSession> Session;
    size_t CurrentProcessingTaskIndex = 0;
    std::vector<TaskInfo> Tasks;
    std::function<void(TFuture<TStatus>)> Callback;    

    TContext(
        const NActors::TActorSystem* actorSystem,
        const TString& tablePathPrefix,
        const std::vector<ui64>& taskIds,
        TString graphId,
        const TCheckpointId& checkpointId,
        TMaybe<TSession> session = {})
        : ActorSystem(actorSystem)
        , TablePathPrefix(tablePathPrefix)
        , GraphId(std::move(graphId))
        , CheckpointId(checkpointId)
        , Session(session)
    {
        for (auto taskId : taskIds) {
            Tasks.emplace_back(taskId);
        }
    }

    TContext(
        const NActors::TActorSystem* actorSystem,
        const TString& tablePathPrefix,
        ui64 taskId,
        TString graphId,
        const TCheckpointId& checkpointId,
        TMaybe<TSession> session = {},
        const std::list<TString>& rows = {},
        EStateType type = EStateType::Snapshot)
        : TContext(actorSystem, tablePathPrefix, std::vector{taskId}, std::move(graphId), checkpointId, std::move(session))
    {
        Tasks[0].Rows = rows;
        Tasks[0].Type = type;
    }
};

using TContextPtr = TIntrusivePtr<TContext>;

////////////////////////////////////////////////////////////////////////////////

struct TCountStateContext : public TThrRefBase {
    size_t Count = 0;
};

using TCountStateContextPtr = TIntrusivePtr<TCountStateContext>;

////////////////////////////////////////////////////////////////////////////////

TStatus ProcessRowState(
    const TDataQueryResult& selectResult,
    const TContextPtr& context) {
    if (!selectResult.IsSuccess()) {
        return selectResult;
    }

    TResultSetParser parser(selectResult.GetResultSet(0));
    TStringBuilder errorMessage;
    auto& taskInfo = context->Tasks[context->CurrentProcessingTaskIndex];

    auto parse = [&]() {
        if (!parser.TryNextRow()) {
            errorMessage << "Can't get next row";
            return;
        }
        auto taskId = parser.ColumnParser("task_id").GetOptionalUint64();
        if (!taskId) {
            errorMessage << "No task id in result";
            return;
        }
        const auto taskIt = std::find_if(context->Tasks.begin(), context->Tasks.end(), [&](const auto& item) { return item.TaskId == *taskId; });
        if (taskIt == context->Tasks.end()) {
            errorMessage << "Got unexpected task id";
            return;
        }
        taskInfo.Rows.push_back(*parser.ColumnParser("blob").GetOptionalString());
    };
    parse();

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

    TFuture<TSaveStateResult> SaveState(
        ui64 taskId,
        const TString& graphId,
        const TCheckpointId& checkpointId,
        const NYql::NDq::TComputeActorState& state) override;

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

    TExecDataQuerySettings GetExecDataQuerySettings(ui64 multiplier = 1);

    TFuture<TStatus> SelectRowState(
        const TContextPtr& context);

    TFuture<TStatus> ListStates(
        const TContextPtr& context);

    size_t SerializeState(
        const NYql::NDq::TComputeActorState& state,
        std::list<TString>& outSerializedState);
    
    EStateType DeserializeState(
        const TContextPtr& context,
        TContext::TaskInfo& taskInfo);

    TFuture<TStatus> SkipStatesInFuture(
        const TContextPtr& context);

    TFuture<TStatus> ReadRows(
        const TContextPtr& context);

    std::vector<NYql::NDq::TComputeActorState> ApplyIncrements(
        const TContextPtr& context,
        NYql::TIssues& issues);

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

TFuture<TIssues> TStateStorage::Init() {
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
        .AddNullableColumn("type", EPrimitiveType::Uint8)
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

EStateType TStateStorage::DeserializeState(const TContextPtr& context, TContext::TaskInfo& taskInfo) {
    TString blob;
    for (auto it = taskInfo.Rows.begin(); it != taskInfo.Rows.end();) {
        blob += *it;
        it = taskInfo.Rows.erase(it);
    }
    taskInfo.States.push_front({});
    NYql::NDq::TComputeActorState& state = taskInfo.States.front();

    LOG_STORAGE_DEBUG(context, "DeserializeState, task id " << taskInfo.TaskId <<  ", blob size " << blob.size());

    auto res = state.ParseFromString(blob);
    Y_ENSURE(res, "Parsing error");
    return TIncrementLogic::GetStateType(state);
}

size_t TStateStorage::SerializeState(
    const NYql::NDq::TComputeActorState& state,
    std::list<TString>& outSerializedState) {
    outSerializedState.clear();

    TString serializedState;
    if (!state.SerializeToString(&serializedState)) {
        return 0;
    }

    auto size = serializedState.size();
    size_t result = size;
    size_t rowLimit = Config.GetStateStorageLimits().GetMaxRowSizeBytes();
    size_t offset = 0;
    while (size) {
        size_t chunkSize = (rowLimit && (size > rowLimit)) ? rowLimit : size;
        outSerializedState.push_back(serializedState.substr(offset, chunkSize));
        offset += chunkSize;
        size -= chunkSize;
    }
    return result;
}

TFuture<IStateStorage::TSaveStateResult> TStateStorage::SaveState(
    ui64 taskId,
    const TString& graphId,
    const TCheckpointId& checkpointId,
    const NYql::NDq::TComputeActorState& state) {

    std::list<TString> serializedState;
    EStateType type = EStateType::Snapshot;
    size_t size = 0;

    try {
        type = TIncrementLogic::GetStateType(state);
        size = SerializeState(state, serializedState);
        if (!size || serializedState.empty()) {
            return MakeFuture(TSaveStateResult(0, NYql::TIssues{NYql::TIssue{"Failed to serialize compute actor state"}}));
        }
    } catch (...) {
        return MakeFuture(TSaveStateResult(0, NYql::TIssues{NYql::TIssue{CurrentExceptionMessage()}}));
    }

    auto context = MakeIntrusive<TContext>(
        NActors::TActivationContext::ActorSystem(),
        YdbConnection->TablePathPrefix,
        taskId,
        graphId,
        checkpointId,
        TMaybe<TSession>(), 
        serializedState,
        type);

    auto promise = NewPromise<TSaveStateResult>();
    auto future = UpsertRow(context);

    context->Callback = [promise, context, size, thisPtr = TIntrusivePtr(this)] (TFuture<TStatus> upsertRowStatus) mutable {
        TStatus status = upsertRowStatus.GetValue();
        if (!status.IsSuccess()) {
            context->Callback = nullptr;
            promise.SetValue(TSaveStateResult(0, StatusToIssues(status)));
            return;
        }
        auto& taskInfo = context->Tasks[context->CurrentProcessingTaskIndex];
        taskInfo.Rows.pop_front();
        ++taskInfo.CurrentProcessingRow;

        if (!taskInfo.Rows.empty()) {
            auto nextFuture = thisPtr->UpsertRow(context);
            nextFuture.Subscribe(context->Callback);
            return;
        }
        context->Callback = nullptr;
        promise.SetValue(TSaveStateResult(size, StatusToIssues(status)));
    };
    future.Subscribe(context->Callback);
    return promise.GetFuture();
}

TFuture<IStateStorage::TGetStateResult> TStateStorage::GetState(
    const std::vector<ui64>& taskIds,
    const TString& graphId,
    const TCheckpointId& checkpointId) {
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
        NActors::TActivationContext::ActorSystem(),
        YdbConnection->TablePathPrefix,
        taskIds,
        graphId,
        checkpointId);

    LOG_STORAGE_DEBUG(context, "GetState, tasks: " << JoinSeq(", ", taskIds));

    return ListStates(context)
        .Apply([context, thisPtr = TIntrusivePtr(this)] (const TFuture<TStatus>& result) mutable {
            if (!result.GetValue().IsSuccess()) {
                return result;
            }
            return thisPtr->SkipStatesInFuture(context);
        })
        .Apply([context, thisPtr = TIntrusivePtr(this)](const TFuture<TStatus>& result) mutable {
            if (!result.GetValue().IsSuccess()) {
                return result;
            }
            context->CurrentProcessingTaskIndex = 0;
            return thisPtr->ReadRows(context);
        })
        .Apply([context, thisPtr = TIntrusivePtr(this)](const TFuture<TStatus>& result) {
            if (!result.GetValue().IsSuccess()) {
                return IStateStorage::TGetStateResult{{}, result.GetValue().GetIssues()};
            }
            NYql::TIssues issues = result.GetValue().GetIssues();

            auto states = thisPtr->ApplyIncrements(context, issues);
            return IStateStorage::TGetStateResult{std::move(states), issues};
        });
}

TFuture<IStateStorage::TCountStatesResult> TStateStorage::CountStates(
    const TString& graphId,
    const TCheckpointId& checkpointId) {
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

                $tasks = SELECT task_id
                    FROM %s
                    WHERE graph_id = $graph_id AND coordinator_generation = $coordinator_generation and seq_no = $seq_no
                    GROUP BY task_id;
                SELECT COUNT(*) as cnt FROM $tasks;
            )", prefix.c_str(), StatesTable);

            auto future = session.ExecuteDataQuery(
                query,
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
                params,
                thisPtr->GetExecDataQuerySettings());

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

    return StatusToIssues(future).Apply(
        [context] (const TFuture<TIssues>& future) {
            return TCountStatesResult{context->Count, future.GetValue()};
        });
}

TFuture<TStatus> TStateStorage::ListStates(const TContextPtr& context) {
    return YdbConnection->TableClient.RetryOperation(
        [prefix = YdbConnection->TablePathPrefix, context, thisPtr = TIntrusivePtr(this)] (TSession session) {
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
                %s;

                SELECT task_id, coordinator_generation, seq_no, CAST(COUNT(*) as UINT64) as cnt
                FROM %s
                WHERE graph_id = $graph_id AND %s
                GROUP by task_id, coordinator_generation, seq_no
                ORDER BY task_id, coordinator_generation DESC, seq_no DESC;
            )", prefix.c_str(),
                context->Tasks.size() == 1 ? "DECLARE $task_id AS Uint64" : "DECLARE $task_ids AS List<Uint64>",
                StatesTable,
                context->Tasks.size() == 1 ? "task_id = $task_id" : "task_id IN $task_ids");

            auto future = session.ExecuteDataQuery(
                query,
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
                params,
                thisPtr->GetExecDataQuerySettings());

            return future.Apply(
                [context] (const TFuture<TDataQueryResult>& future) {
                    TStatus status = future.GetValue();
                    if (!status.IsSuccess()) {
                        return status;
                    }

                    LOG_STORAGE_DEBUG(context, "ListOfStates results:");
                    try {
                        const auto& selectResult = future.GetValue();
                        TResultSetParser parser(selectResult.GetResultSet(0));
                        while (parser.TryNextRow()) {
                            auto taskId = parser.ColumnParser("task_id").GetOptionalUint64();
                            auto coordinatorGeneration = parser.ColumnParser("coordinator_generation").GetOptionalUint64();
                            auto seqNo = parser.ColumnParser("seq_no").GetOptionalUint64();
                            auto cnt = parser.ColumnParser("cnt").GetUint64();

                            if (!taskId || !coordinatorGeneration || !seqNo) {
                                return TStatus(EStatus::BAD_REQUEST, NYql::TIssues{NYql::TIssue{"Unexpected empty field"}});
                            }
                            const auto taskIt = std::find_if(context->Tasks.begin(), context->Tasks.end(), [&] (const auto& item) { return item.TaskId == *taskId; } );
                            if (taskIt == context->Tasks.end()) {
                                return TStatus(EStatus::BAD_REQUEST, NYql::TIssues{NYql::TIssue{"Got unexpected task id"}});
                            }

                            auto& taskInfo = *taskIt;
                            TCheckpointId checkpointId(*coordinatorGeneration, *seqNo);
                            taskInfo.ListOfStatesForReading.push_back(TContext::TStateInfo{checkpointId, cnt});
                            LOG_STORAGE_DEBUG(context, "taskId " << taskId <<  " checkpoint id: " << checkpointId << ", rows count: " << cnt);
                        }
                    }
                    catch (const std::exception& e) {
                        return TStatus(EStatus::BAD_REQUEST, NYql::TIssues{NYql::TIssue{e.what()}});
                    }
                    return status;
            });
        });
}

TExecDataQuerySettings TStateStorage::GetExecDataQuerySettings(ui64 multiplier) {
    return TExecDataQuerySettings()
        .KeepInQueryCache(true)
        .ClientTimeout(TDuration::Seconds(StorageConfig.GetClientTimeoutSec() * multiplier))
        .OperationTimeout(TDuration::Seconds(StorageConfig.GetOperationTimeoutSec() * multiplier))
        .CancelAfter(TDuration::Seconds(StorageConfig.GetCancelAfterSec() * multiplier));
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
                thisPtr->GetExecDataQuerySettings(DeleteStateTimeoutMultiplier));

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
    const TCheckpointId& checkpointUpperBound) {
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
                thisPtr->GetExecDataQuerySettings(DeleteStateTimeoutMultiplier));

            return future.Apply(
                [] (const TFuture<TDataQueryResult>& future) {
                    TStatus status = future.GetValue();
                    return status;
            });
        });

    return StatusToIssues(future);
}

TFuture<TStatus> TStateStorage::SelectRowState(const TContextPtr& context) {
    return YdbConnection->TableClient.RetryOperation(
        [context, this] (TSession session) {
            context->Session = session;
            auto future = SelectState(context);
            return future.Apply(
                [context] (const TFuture<TDataQueryResult>& future) {  
                try {
                    return ProcessRowState(future.GetValue(), context);
                }
                catch (const std::exception& e) {
                    return TStatus(EStatus::INTERNAL_ERROR, NYql::TIssues{NYql::TIssue{e.what()}});
                }
            });
        });
}

TFuture<TDataQueryResult> TStateStorage::SelectState(const TContextPtr& context) {
    NYdb::TParamsBuilder paramsBuilder;
    Y_ENSURE(!context->Tasks.empty(), "Tasks is empty");
    auto& taskInfo = context->Tasks[context->CurrentProcessingTaskIndex];

    LOG_STORAGE_DEBUG(context, "SelectState: task_id " << taskInfo.TaskId << ", seq_no " 
        << taskInfo.ListOfStatesForReading.front().CheckpointId.SeqNo << ", blob_seq_num " << taskInfo.CurrentProcessingRow);
    paramsBuilder.AddParam("$task_id").Uint64(taskInfo.TaskId).Build();
    paramsBuilder.AddParam("$graph_id").String(context->GraphId).Build();
    paramsBuilder.AddParam("$coordinator_generation").Uint64(taskInfo.ListOfStatesForReading.front().CheckpointId.CoordinatorGeneration).Build();
    paramsBuilder.AddParam("$seq_no").Uint64(taskInfo.ListOfStatesForReading.front().CheckpointId.SeqNo).Build();
    paramsBuilder.AddParam("$blob_seq_num").Uint64(taskInfo.CurrentProcessingRow).Build();

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

        SELECT task_id, blob, type
        FROM %s
        WHERE task_id = $task_id AND graph_id = $graph_id AND coordinator_generation = $coordinator_generation AND seq_no = $seq_no AND (blob_seq_num = $blob_seq_num %s);
    )",
        context->TablePathPrefix.c_str(),
        StatesTable,
        (taskInfo.CurrentProcessingRow == 0) ? " OR blob_seq_num is NULL" : "");

    Y_ENSURE(context->Session, "Session is empty");
    return context->Session->ExecuteDataQuery(
        query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        params,
        GetExecDataQuerySettings());
}

TFuture<TStatus> TStateStorage::UpsertRow(const TContextPtr& context) {

    return YdbConnection->TableClient.RetryOperation(
        [context, thisPtr = TIntrusivePtr(this)] (TSession session) {
            context->Session = session;
            // publish nodes
            NYdb::TParamsBuilder paramsBuilder;
            Y_ENSURE(context->Tasks.size() == 1, "Tasks size != 1");
            auto& taskInfo = context->Tasks[context->CurrentProcessingTaskIndex];

            paramsBuilder.AddParam("$task_id").Uint64(taskInfo.TaskId).Build();
            paramsBuilder.AddParam("$graph_id").String(context->GraphId).Build();
            paramsBuilder.AddParam("$coordinator_generation").Uint64(context->CheckpointId.CoordinatorGeneration).Build();
            paramsBuilder.AddParam("$seq_no").Uint64(context->CheckpointId.SeqNo).Build();
            paramsBuilder.AddParam("$blob").String(taskInfo.Rows.front()).Build();
            paramsBuilder.AddParam("$blob_seq_num").Uint64(taskInfo.CurrentProcessingRow).Build();
            paramsBuilder.AddParam("$type").Uint8(static_cast<ui8>(taskInfo.Type)).Build();

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
                declare $type as Uint8;

                UPSERT INTO %s (task_id, graph_id, coordinator_generation, seq_no, blob, blob_seq_num, type) VALUES
                    ($task_id, $graph_id, $coordinator_generation, $seq_no, $blob, $blob_seq_num, $type);
            )", context->TablePathPrefix.c_str(), StatesTable);

            Y_ENSURE(context->Session, "Session is empty");

            auto future = context->Session->ExecuteDataQuery(
                query,
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
                params,
                thisPtr->GetExecDataQuerySettings());

            return future.Apply(
                [] (const TFuture<TDataQueryResult>& future) {
                    TStatus status = future.GetValue();
                    return status;
                });
        });
}

TFuture<TStatus> TStateStorage::SkipStatesInFuture(const TContextPtr& context) {
    size_t eraseCount = 0;
    for (auto& taskInfo : context->Tasks) {
        auto it = taskInfo.ListOfStatesForReading.begin();
        while (it != taskInfo.ListOfStatesForReading.end())
        {
            if (it->CheckpointId == context->CheckpointId) {
                break;
            }
            if (it->CheckpointId < context->CheckpointId) {
                // ListOfStatesForReading sorted by "time"
                return MakeFuture(TStatus{EStatus::INTERNAL_ERROR, NYql::TIssues{NYql::TIssue{"Checkpoint is not found"}}});
            }
            it = taskInfo.ListOfStatesForReading.erase(it);
            ++eraseCount;
        }
        if (taskInfo.ListOfStatesForReading.empty()) {
            return MakeFuture(TStatus{EStatus::INTERNAL_ERROR, NYql::TIssues{NYql::TIssue{"Checkpoint is not found"}}});
        }
    }
    LOG_STORAGE_DEBUG(context, "SkipStatesInFuture, skip " << eraseCount << " checkpoints");
    return MakeFuture(TStatus{EStatus::SUCCESS,  NYql::TIssues{}});
}

TFuture<TStatus> TStateStorage::ReadRows(const TContextPtr& context) {
    auto promise = NewPromise<TStatus>();

    context->Callback = 
        [context, promise, thisPtr = TIntrusivePtr(this)] (const TFuture<TStatus>& future) mutable {
            try {
                TStatus status = future.GetValue();
                if (!status.IsSuccess()) {
                    promise.SetValue(status);
                    context->Callback = nullptr;
                    return;
                }
                auto& taskInfo = context->Tasks[context->CurrentProcessingTaskIndex];
                ++taskInfo.CurrentProcessingRow;

                if (taskInfo.CurrentProcessingRow == taskInfo.ListOfStatesForReading.front().StateRowsCount) {
                    auto type = thisPtr->DeserializeState(context, taskInfo);
                    if (type != EStateType::Snapshot) {
                        taskInfo.ListOfStatesForReading.pop_front();
                        taskInfo.CurrentProcessingRow = 0;
                        if (taskInfo.ListOfStatesForReading.empty()) {
                            promise.SetValue(TStatus{EStatus::INTERNAL_ERROR, NYql::TIssues{NYql::TIssue{"Checkpoint is not found"}}});
                            context->Callback = nullptr;
                            return;
                        }
                    }
                    else if (context->CurrentProcessingTaskIndex != context->Tasks.size() - 1) {
                        ++context->CurrentProcessingTaskIndex;
                        taskInfo.CurrentProcessingRow = 0;
                    }
                    else {
                        context->Callback = nullptr;
                        promise.SetValue(TStatus{EStatus::SUCCESS,  NYql::TIssues{}});
                        return;
                    }
                }
                auto nextFuture = thisPtr->SelectRowState(context);
                nextFuture.Subscribe(context->Callback);
            } catch (...) {
                promise.SetValue(TStatus{EStatus::INTERNAL_ERROR, NYql::TIssues{NYql::TIssue{CurrentExceptionMessage()}}});
                context->Callback = nullptr;
                return;
            }

        };

    auto future = SelectRowState(context);
    future.Subscribe(context->Callback);

    return promise.GetFuture();
}

std::vector<NYql::NDq::TComputeActorState> TStateStorage::ApplyIncrements(
    const TContextPtr& context,
    NYql::TIssues& issues) {
    LOG_STORAGE_DEBUG(context, "ApplyIncrements");

    std::vector<NYql::NDq::TComputeActorState> states;
    try {
        for (auto& task : context->Tasks)
        {
            TIncrementLogic logic;
            for (auto& state : task.States)
            {
                logic.Apply(state);
            }
            states.push_back(std::move(logic.Build()));
        }
    } catch (...) {
        issues.AddIssue(CurrentExceptionMessage());
    } 
    return states;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TStateStoragePtr NewYdbStateStorage(
    const NConfig::TCheckpointCoordinatorConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const TYqSharedResources::TPtr& yqSharedResources) {
    return new TStateStorage(config, credentialsProviderFactory, yqSharedResources);
}

} // namespace NFq
