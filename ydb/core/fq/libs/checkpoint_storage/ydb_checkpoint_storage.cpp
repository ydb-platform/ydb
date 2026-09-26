#include "ydb_checkpoint_storage.h"

#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/sdk/cpp/adapters/issue/issue.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

#include <library/cpp/threading/future/wait/wait.h>

#include <fmt/format.h>

#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

#include <exception>
#include <utility>

#define YDB_LOG_THIS_FILE_COMPONENT ::NKikimrServices::STREAMS_STORAGE_SERVICE

namespace NFq {

using namespace NThreading;
using namespace NYdb;
using namespace NYdb::NTable;

using NYql::TIssues;
using TTxControl = NFq::ISession::TTxControl;

namespace {

////////////////////////////////////////////////////////////////////////////////

const char* const CoordinatorsSyncTable = "coordinators_sync";
const char* const CheckpointsMetadataTable = "checkpoints_metadata";
const char* const CheckpointsGraphsDescriptionTable = "checkpoints_graphs_description";

////////////////////////////////////////////////////////////////////////////////

struct TCheckpointGraphDescriptionContext : public TThrRefBase {
    static constexpr ui64 MAX_GRAPH_DESC_ID_GENERATION_ATTEMPTS = 100;

    TString GraphDescId;
    const TMaybe<NProto::TCheckpointGraphDescription> NewGraphDescription;
    ui64 GraphDescIdGenerationAttempts = 0;

    explicit TCheckpointGraphDescriptionContext(const TString& graphDescId)
        : GraphDescId(graphDescId)
    {
    }

    explicit TCheckpointGraphDescriptionContext(const NProto::TCheckpointGraphDescription& desc)
        : NewGraphDescription(desc)
    {
    }
};

using TCheckpointGraphDescriptionContextPtr = TIntrusivePtr<TCheckpointGraphDescriptionContext>;

////////////////////////////////////////////////////////////////////////////////

struct TCheckpointContext : public TThrRefBase {
    const TCheckpointId CheckpointId;
    const ECheckpointStatus Status; // optional new status
    const ECheckpointStatus ExpectedStatus; // optional expected current status, used only in some operations
    const ui64 StateSizeBytes;

    TGenerationContextPtr GenerationContext;
    TCheckpointGraphDescriptionContextPtr CheckpointGraphDescriptionContext;
    IEntityIdGenerator::TPtr EntityIdGenerator;

    TCheckpointContext(const TCheckpointId& id,
                       ECheckpointStatus status,
                       ECheckpointStatus expected,
                       ui64 stateSizeBytes)
        : CheckpointId(id)
        , Status(status)
        , ExpectedStatus(expected)
        , StateSizeBytes(stateSizeBytes)
    {
    }
};

using TCheckpointContextPtr = TIntrusivePtr<TCheckpointContext>;

////////////////////////////////////////////////////////////////////////////////

struct TGetCheckpointsContext : public TThrRefBase {
    TCheckpoints Checkpoints;
};

using TGetCheckpointsContextPtr = TIntrusivePtr<TGetCheckpointsContext>;

////////////////////////////////////////////////////////////////////////////////

struct TGetCoordinatorsContext : public TThrRefBase {
    TCoordinators Coordinators;
};

using TGetCoordinatorsContextPtr = TIntrusivePtr<TGetCoordinatorsContext>;

////////////////////////////////////////////////////////////////////////////////

struct TGetTotalCheckpointsStateSizeContext : public TThrRefBase {
    ui64 Size = 0;
};

////////////////////////////////////////////////////////////////////////////////

TFuture<TDataQueryResult> SelectGraphCoordinators(const TGenerationContextPtr& context)
{
    // TODO: use prepared queries

    auto query = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");

        SELECT *
        FROM %s;
    )", context->TablePathPrefix.c_str(), CoordinatorsSyncTable);

    auto params = std::make_shared<NYdb::TParamsBuilder>();
    return context->Session->ExecuteDataQuery(
        query,
        TTxControl::BeginAndCommitTx(),
        params,
        context->ExecDataQuerySettings);
}

TFuture<TStatus> ProcessCoordinators(
    const TDataQueryResult& selectResult,
    const TGenerationContextPtr&,
    const TGetCoordinatorsContextPtr& getContext)
{
    if (!selectResult.IsSuccess()) {
        return MakeFuture<TStatus>(selectResult);
    }

    TResultSetParser parser(selectResult.GetResultSet(0));

    while (parser.TryNextRow()) {
        getContext->Coordinators.emplace_back(
            *parser.ColumnParser("graph_id").GetOptionalString(),
            *parser.ColumnParser("generation").GetOptionalUint64());
    }

    return MakeFuture<TStatus>(selectResult);
}

TFuture<TStatus> CreateCheckpoint(const TCheckpointContextPtr& context) {
    // TODO: use prepared query

    const auto& generationContext = context->GenerationContext;
    const auto& graphDescContext = context->CheckpointGraphDescriptionContext;

    TStringBuilder query;
    using namespace fmt::literals;
    const TString firstPart = fmt::format(R"sql(
        --!syntax_v1
        PRAGMA TablePathPrefix("{table_path_prefix}");
        DECLARE $ts AS Timestamp;
        DECLARE $graph_id AS String;
        DECLARE $graph_desc_id AS String;
        DECLARE $coordinator_generation AS Uint64;
        DECLARE $seq_no AS Uint64;
        DECLARE $status AS Uint8;
        {optional_graph_description_declaration}
        INSERT INTO {checkpoints_metadata_table_name}
          (graph_id, coordinator_generation, seq_no, status, created_by, modified_by, state_size, graph_description_id)
          VALUES ($graph_id, $coordinator_generation, $seq_no, $status, $ts, $ts, 0, $graph_desc_id);
    )sql",
    "table_path_prefix"_a = generationContext->TablePathPrefix,
    "checkpoints_metadata_table_name"_a = CheckpointsMetadataTable,
    "optional_graph_description_declaration"_a = graphDescContext->NewGraphDescription ? "DECLARE $graph_description AS String;" : ""
    );

    query << firstPart;

    auto params = std::make_shared<NYdb::TParamsBuilder>();
    params->
         AddParam("$graph_id")
            .String(generationContext->PrimaryKey)
            .Build()
        .AddParam("$graph_desc_id")
            .String(graphDescContext->GraphDescId)
            .Build()
        .AddParam("$coordinator_generation")
            .Uint64(context->CheckpointId.CoordinatorGeneration)
            .Build()
        .AddParam("$seq_no")
            .Uint64(context->CheckpointId.SeqNo)
            .Build()
        .AddParam("$status")
            .Uint8((ui8)context->Status)
            .Build()
        .AddParam("$ts")
            .Timestamp(TInstant::Now())
            .Build();

    if (graphDescContext->NewGraphDescription) {
        const TString graphDescriptionPart = fmt::format(R"sql(
            INSERT INTO {checkpoints_graphs_description_table_name}
                (id, ref_count, graph_description)
                VALUES ($graph_desc_id, 1, $graph_description);
        )sql",
        "checkpoints_graphs_description_table_name"_a = CheckpointsGraphsDescriptionTable
        );

        query << graphDescriptionPart;

        TString serializedGraphDescription;
        if (!graphDescContext->NewGraphDescription->SerializeToString(&serializedGraphDescription)) {
            NYdb::NIssue::TIssues issues;
            issues.AddIssue("Failed to serialize graph description proto");
            return MakeFuture(TStatus(EStatus::BAD_REQUEST, std::move(issues)));
        }

        params->
            AddParam("$graph_description")
                .String(serializedGraphDescription)
                .Build();
    } else {
        const TString graphDescriptionPart = fmt::format(R"sql(
            UPDATE {checkpoints_graphs_description_table_name}
                SET ref_count = ref_count + 1
                WHERE id = $graph_desc_id;
        )sql",
        "checkpoints_graphs_description_table_name"_a = CheckpointsGraphsDescriptionTable
        );

        query << graphDescriptionPart;
    }

    auto ttxControl = TTxControl::ContinueAndCommitTx();
    return generationContext->Session->ExecuteDataQuery(query, ttxControl, std::move(params), generationContext->ExecDataQuerySettings).Apply(
        [] (const TFuture<TDataQueryResult>& future) {
            TStatus status = future.GetValue();
            return status;
        });
}

TFuture<TStatus> UpdateCheckpoint(const TCheckpointContextPtr& context) {
    const auto& generationContext = context->GenerationContext;

    // TODO: use prepared query

    // TODO: UPSERT VS UPDATE (especially with WHERE status = X)
    auto query = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");
        DECLARE $graph_id AS String;
        DECLARE $coordinator_generation AS Uint64;
        DECLARE $seq_no AS Uint64;
        DECLARE $status AS Uint8;
        DECLARE $state_size AS Uint64;
        DECLARE $ts AS Timestamp;

        UPSERT INTO %s (graph_id, coordinator_generation, seq_no, status, state_size, modified_by) VALUES
            ($graph_id, $coordinator_generation, $seq_no, $status, $state_size, $ts);
    )", generationContext->TablePathPrefix.c_str(),
        CheckpointsMetadataTable);

    auto params = std::make_shared<NYdb::TParamsBuilder>();
    params->
         AddParam("$graph_id")
            .String(generationContext->PrimaryKey)
            .Build()
        .AddParam("$coordinator_generation")
            .Uint64(context->CheckpointId.CoordinatorGeneration)
            .Build()
        .AddParam("$seq_no")
            .Uint64(context->CheckpointId.SeqNo)
            .Build()
        .AddParam("$status")
            .Uint8((ui8)context->Status)
            .Build()
        .AddParam("$state_size")
            .Uint64(context->StateSizeBytes)
            .Build()
        .AddParam("$ts")
            .Timestamp(TInstant::Now())
            .Build();

    auto ttxControl = TTxControl::ContinueAndCommitTx();
    return generationContext->Session->ExecuteDataQuery(query, ttxControl, std::move(params), generationContext->ExecDataQuerySettings).Apply(
        [] (const TFuture<TDataQueryResult>& future) {
            TStatus status = future.GetValue();
            return status;
        });
}

TFuture<TDataQueryResult> SelectGraphDescId(const TCheckpointContextPtr& context) {
    const auto& generationContext = context->GenerationContext;
    const auto& graphDescContext = context->CheckpointGraphDescriptionContext;

    auto query = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");
        DECLARE $graph_desc_id AS String;

        SELECT ref_count
        FROM %s
        WHERE id = $graph_desc_id;
    )", generationContext->TablePathPrefix.c_str(),
        CheckpointsGraphsDescriptionTable);
    auto params = std::make_shared<NYdb::TParamsBuilder>();
    params->
         AddParam("$graph_desc_id")
            .String(graphDescContext->GraphDescId)
            .Build();

    return generationContext->Session->ExecuteDataQuery(
        query,
        TTxControl::ContinueTx(),
        std::move(params),
        generationContext->ExecDataQuerySettings);
}

bool GraphDescIdExists(const TFuture<TDataQueryResult>& result) {
    return result.GetValue().GetResultSet(0).RowsCount() != 0;
}

TFuture<TStatus> GenerateGraphDescId(const TCheckpointContextPtr& context) {
    if (context->CheckpointGraphDescriptionContext->GraphDescId) { // already given
        return MakeFuture(TStatus(EStatus::SUCCESS, NYdb::NIssue::TIssues()));
    }

    if (++context->CheckpointGraphDescriptionContext->GraphDescIdGenerationAttempts > TCheckpointGraphDescriptionContext::MAX_GRAPH_DESC_ID_GENERATION_ATTEMPTS) {
        return MakeFuture(TStatus(EStatus::INTERNAL_ERROR, {NYdb::NIssue::TIssue("Too many attempts to generate graph desc id")}));
    }

    Y_ABORT_UNLESS(context->EntityIdGenerator);
    context->CheckpointGraphDescriptionContext->GraphDescId = context->EntityIdGenerator->Generate(EEntityType::CHECKPOINT_GRAPH_DESCRIPTION);
    return SelectGraphDescId(context)
        .Apply(
            [context](const TFuture<TDataQueryResult>& result) {
                if (!result.GetValue().IsSuccess()) {
                    return MakeFuture<TStatus>(result.GetValue());
                }

                if (!GraphDescIdExists(result)) {
                    return MakeFuture(TStatus(EStatus::SUCCESS, NYdb::NIssue::TIssues()));
                } else {
                    context->CheckpointGraphDescriptionContext->GraphDescId = {}; // Regenerate
                    return GenerateGraphDescId(context);
                }
            });
}

TFuture<TStatus> CreateCheckpointWrapper(
    const TFuture<TStatus>& generationFuture,
    const TCheckpointContextPtr& context)
{
    return generationFuture.Apply(
        [context] (const TFuture<TStatus>& generationFuture) {
            auto generationSelect = generationFuture.GetValue();
            if (!generationSelect.IsSuccess()) {
                return MakeFuture(generationSelect);
            }

            return GenerateGraphDescId(context)
                .Apply(
                    [context](const TFuture<TStatus>& result) {
                        if (!result.GetValue().IsSuccess()) {
                            return MakeFuture(result.GetValue());
                        }
                        return CreateCheckpoint(context);
                    });
        });
}

TFuture<TDataQueryResult> SelectGraphCheckpoints(const TGenerationContextPtr& context, const TVector<ECheckpointStatus>& statuses, ui64 limit, bool loadGraphDescription)
{
    auto paramsBuilder = std::make_shared<NYdb::TParamsBuilder>();
    if (statuses) {
        auto& statusesParam = paramsBuilder->AddParam("$statuses").BeginList();
        for (const auto& status : statuses) {
            statusesParam.AddListItem().Uint8(static_cast<ui8>(status));
        }
        statusesParam.EndList().Build();
    }

    paramsBuilder->AddParam("$graph_id").String(context->PrimaryKey).Build();
    if (limit < std::numeric_limits<ui64>::max()) {
        paramsBuilder->AddParam("$limit").Uint64(limit).Build();
    }

    using namespace fmt::literals;
    TString join;
    if (loadGraphDescription) {
        join = fmt::format(R"sql(
            INNER JOIN {checkpoints_graphs_description_table_name} AS desc
                ON metadata.graph_description_id = desc.id
        )sql",
        "checkpoints_graphs_description_table_name"_a = CheckpointsGraphsDescriptionTable
        );
    }

    const TString query = fmt::format(R"sql(
        --!syntax_v1
        PRAGMA TablePathPrefix("{table_path_prefix}");
        PRAGMA AnsiInForEmptyOrNullableItemsCollections;

        DECLARE $graph_id AS String;
        {optional_statuses_declaration}
        {optional_limit_declaration}

        SELECT
            {graph_description_field}
            metadata.coordinator_generation AS coordinator_generation,
            metadata.seq_no AS seq_no,
            metadata.status AS status,
            metadata.created_by AS created_by,
            metadata.modified_by AS modified_by
        FROM {checkpoints_metadata_table_name} AS metadata
            {join}
        WHERE metadata.graph_id = $graph_id
            {statuses_condition}
        ORDER BY coordinator_generation DESC, seq_no DESC
        {limit_condition};
    )sql",
    "table_path_prefix"_a = context->TablePathPrefix,
    "optional_statuses_declaration"_a = statuses ? "DECLARE $statuses AS List<Uint8>;" : "",
    "statuses_condition"_a = statuses ? "AND metadata.status IN $statuses" : "",
    "optional_limit_declaration"_a = limit < std::numeric_limits<ui64>::max() ? "DECLARE $limit AS Uint64;" : "",
    "limit_condition"_a = limit < std::numeric_limits<ui64>::max() ? "LIMIT $limit" : "",
    "checkpoints_metadata_table_name"_a = CheckpointsMetadataTable,
    "graph_description_field"_a = loadGraphDescription ? "desc.graph_description AS graph_description," : "",
    "join"_a = join
    );

    return context->Session->ExecuteDataQuery(
        query,
        TTxControl::BeginAndCommitTx(),
        std::move(paramsBuilder),
        context->ExecDataQuerySettings);
}

TFuture<TStatus> ProcessCheckpoints(
    const TDataQueryResult& selectResult,
    const TGenerationContextPtr& context,
    const TGetCheckpointsContextPtr& getContext,
    bool loadGraphDescription)
{
    if (!selectResult.IsSuccess()) {
        return MakeFuture<TStatus>(selectResult);
    }

    TResultSetParser parser(selectResult.GetResultSet(0));

    while (parser.TryNextRow()) {
        TCheckpointId checkpointId(
            *parser.ColumnParser("coordinator_generation").GetOptionalUint64(),
            *parser.ColumnParser("seq_no").GetOptionalUint64());

        getContext->Checkpoints.emplace_back(
            context->PrimaryKey,
            checkpointId,
            ECheckpointStatus(*parser.ColumnParser("status").GetOptionalUint8()),
            *parser.ColumnParser("created_by").GetOptionalTimestamp(),
            *parser.ColumnParser("modified_by").GetOptionalTimestamp());

        if (loadGraphDescription) {
            if (const std::optional<std::string> graphDescription = parser.ColumnParser("graph_description").GetOptionalString(); graphDescription && !graphDescription.value().empty()) {
                NProto::TCheckpointGraphDescription graphDesc;
                if (!graphDesc.ParseFromString(*graphDescription)) {
                    NYdb::NIssue::TIssues issues;
                    issues.AddIssue("Failed to deserialize graph description proto");
                    return MakeFuture(TStatus(EStatus::INTERNAL_ERROR, std::move(issues)));
                }

                NProto::TGraphParams& graphParams = getContext->Checkpoints.back().Graph.ConstructInPlace();
                graphParams.Swap(graphDesc.MutableGraph());
            }
        }
    }

    return MakeFuture<TStatus>(selectResult);
}

TFuture<TDataQueryResult> SelectCheckpoint(const TCheckpointContextPtr& context)
{
    // TODO: use prepared queries

    const auto& generationContext = context->GenerationContext;

    auto query = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");
        DECLARE $graph_id AS String;
        DECLARE $coordinator_generation AS Uint64;
        DECLARE $seq_no AS Uint64;

        SELECT status
        FROM %s
        WHERE graph_id = $graph_id AND  coordinator_generation = $coordinator_generation AND seq_no = $seq_no;
    )", generationContext->TablePathPrefix.c_str(),
        CheckpointsMetadataTable);

    auto params = std::make_shared<NYdb::TParamsBuilder>();
    params->
         AddParam("$graph_id")
            .String(generationContext->PrimaryKey)
            .Build()
        .AddParam("$coordinator_generation")
            .Uint64(context->CheckpointId.CoordinatorGeneration)
            .Build()
        .AddParam("$seq_no")
            .Uint64(context->CheckpointId.SeqNo)
            .Build();

    return generationContext->Session->ExecuteDataQuery(
        query,
        TTxControl::ContinueTx(),
        std::move(params),
        generationContext->ExecDataQuerySettings);
}

TFuture<TStatus> CheckCheckpoint(
    const TDataQueryResult& selectResult,
    const TCheckpointContextPtr& context)
{
    if (!selectResult.IsSuccess()) {
        return MakeFuture<TStatus>(selectResult);
    }

    TResultSetParser parser(selectResult.GetResultSet(0));

    ECheckpointStatus statusRead;
    if (parser.TryNextRow()) {
        statusRead = static_cast<ECheckpointStatus>(*parser.ColumnParser("status").GetOptionalUint8());
    } else {
        TIssues issues;
        TStringStream ss;
        ss << "Failed to select checkpoint '" << context->CheckpointId << "'";

        const auto& stats = selectResult.GetStats();
        if (stats) {
            ss << ", stats: " << stats->ToString();
        }

        // TODO: print status, etc

        // we use GENERIC_ERROR, because not sure if NOT_FOUND non-retrieable
        // also severity is error, because user expects checkpoint to be existed

        return MakeFuture(MakeErrorStatus(EStatus::GENERIC_ERROR, ss.Str()));
    }

    if (statusRead == ECheckpointStatus::GC) {
        TIssues issues;
        TStringStream ss;
        ss << "Selected checkpoint '" << context->CheckpointId
           << "' is owned by GC";

        return MakeFuture(MakeErrorStatus(EStatus::GENERIC_ERROR, ss.Str()));
    }

    bool isAbort = context->Status == ECheckpointStatus::Aborted;
    if (!isAbort && statusRead != context->ExpectedStatus) {
        TIssues issues;
        TStringStream ss;
        ss << "Selected checkpoint '" << context->CheckpointId
           << "' with status " << statusRead
           << ", while expected " << context->ExpectedStatus;

        return MakeFuture(MakeErrorStatus(EStatus::GENERIC_ERROR, ss.Str()));
    }

    return MakeFuture<TStatus>(selectResult);
}

TFuture<TStatus> SelectCheckpointWithCheck(const TCheckpointContextPtr& context)
{
    auto future = SelectCheckpoint(context);
    return future.Apply(
        [context] (const TFuture<TDataQueryResult>& future) {
            return CheckCheckpoint(future.GetValue(), context);
        });
}

TFuture<TStatus> UpdateCheckpointWithCheckWrapper(
    const TFuture<TStatus>& generationFuture,
    const TCheckpointContextPtr& context)
{
    return generationFuture.Apply(
        [context] (const TFuture<TStatus>& generationFuture) {
            auto generationSelect = generationFuture.GetValue();
            if (!generationSelect.IsSuccess()) {
                return MakeFuture(generationSelect);
            }

            auto future = SelectCheckpointWithCheck(context);
            return future.Apply(
                [context] (const TFuture<TStatus>& selectFuture) {
                    auto selectResult = selectFuture.GetValue();
                    if (!selectResult.IsSuccess()) {
                        return MakeFuture(selectResult);
                    }

                    return UpdateCheckpoint(context);
                });
        });
}

TFuture<TIssues> CleanupGraph(const TCheckpointProviderIntegrations& checkpointProviderIntegrations, const TDataQueryResult& graphs, std::optional<ui64> generationUpperBound) {
    if (checkpointProviderIntegrations.empty()) {
        return MakeFuture(TIssues{});
    }

    THashMap<TString, ICheckpointProviderIntegration::TPtr> sinksCleanup;
    sinksCleanup.reserve(checkpointProviderIntegrations.size());
    for (const auto& [_, integration] : checkpointProviderIntegrations) {
        Y_VALIDATE(sinksCleanup.emplace(integration->GetSinkName(), integration).second, "Duplicated sink name: " << integration->GetSinkName());
    }

    TVector<TFuture<TIssues>> cleanupFutures;
    for (auto parser = graphs.GetResultSetParser(0); parser.TryNextRow();) {
        NProto::TCheckpointGraphDescription graphDesc;
        const auto description = parser.ColumnParser("graph_description").GetOptionalString();
        if (!description || !graphDesc.ParseFromString(*description)) {
            cleanupFutures.push_back(MakeFuture(TIssues{NYql::TIssue("Failed to parse checkpoint graph description for cleanup")}));
            continue;
        }

        THashMap<std::pair<ui32, ui64>, ICheckpointProviderIntegration::TCleanupGraphSink> sinksCleanupRequests; // (stageId, outputIndex) -> cleanup request
        for (const auto& task : graphDesc.GetGraph().GetTasks()) {
            for (size_t outputIndex = 0; outputIndex < task.OutputsSize(); ++outputIndex) {
                const auto& output = task.GetOutputs(outputIndex);
                if (!output.HasSink()) {
                    continue;
                }

                const auto& sink = output.GetSink();
                const auto& sinkType = sink.GetType();
                if (!sinksCleanup.contains(sinkType)) {
                    continue;
                }

                const auto [it, inserted] = sinksCleanupRequests.try_emplace(std::make_pair(task.GetStageId(), outputIndex));
                auto& args = it->second.Args;
                if (inserted) {
                    it->second.Sink = sink;
                    args.OutputIndex = outputIndex;
                    args.SecureParams = {task.GetSecureParams().begin(), task.GetSecureParams().end()};
                    args.RequestContext = {task.GetRequestContext().begin(), task.GetRequestContext().end()};
                } else {
                    Y_VALIDATE(it->second.Sink.GetType() == sinkType, "Sink type must be equal for stage tasks");
                    Y_VALIDATE(it->second.Sink.GetSettings().type_url() == sink.GetSettings().type_url()
                        && it->second.Sink.GetSettings().value() == sink.GetSettings().value(), "Sink settings must be equal for stage tasks");
                    Y_VALIDATE((args.SecureParams == THashMap<TString, TString>(task.GetSecureParams().begin(), task.GetSecureParams().end())), "Secure params must be equal for stage tasks");
                    Y_VALIDATE((args.RequestContext == THashMap<TString, TString>(task.GetRequestContext().begin(), task.GetRequestContext().end())), "Request context must be equal for stage tasks");
                }

                args.TaskIds.emplace_back(task.GetId());
            }
        }

        THashMap<TString, TVector<ICheckpointProviderIntegration::TCleanupGraphSink>> providerRequests;
        for (auto& [_, request] : sinksCleanupRequests) {
            const auto sinkType = request.Sink.GetType();
            providerRequests[sinkType].emplace_back(std::move(request));
        }

        for (auto& [provider, requests] : providerRequests) {
            cleanupFutures.push_back(sinksCleanup.at(provider)->CleanupGraphSinks(std::move(requests), generationUpperBound));
        }
    }

    return WaitAll(cleanupFutures).Apply([cleanupFutures = std::move(cleanupFutures)](const TFuture<void>&) {
        TIssues issues;
        for (const auto& future : cleanupFutures) {
            issues.AddIssues(future.GetValue());
        }
        return issues;
    });
}

////////////////////////////////////////////////////////////////////////////////

class TCheckpointStorage : public ICheckpointStorage {
    IYdbConnection::TPtr YdbConnection;
    const TExternalStorageSettings Config;

public:
    explicit TCheckpointStorage(
        const TExternalStorageSettings& config,
        const IEntityIdGenerator::TPtr& entityIdGenerator,
        const IYdbConnection::TPtr& ydbConnection,
        TCheckpointProviderIntegrations checkpointProviderIntegrations);

    ~TCheckpointStorage() = default;

    TFuture<TIssues> Init(const NACLib::TDiffACL& acl) override;

    TFuture<TIssues> RegisterGraphCoordinator(const TCoordinatorId& coordinator) override;

    TFuture<TGetCoordinatorsResult> GetCoordinators() override;

    TFuture<TCreateCheckpointResult> CreateCheckpoint(
        const TCoordinatorId& coordinator,
        const TCheckpointId& checkpointId,
        const TString& graphDescId,
        ECheckpointStatus status) override;

    TFuture<TCreateCheckpointResult> CreateCheckpoint(
        const TCoordinatorId& coordinator,
        const TCheckpointId& checkpointId,
        const NProto::TCheckpointGraphDescription& graphDesc,
        ECheckpointStatus status) override;

    TFuture<TIssues> UpdateCheckpointStatus(
        const TCoordinatorId& coordinator,
        const TCheckpointId& checkpointId,
        ECheckpointStatus newStatus,
        ECheckpointStatus prevStatus,
        ui64 stateSizeBytes) override;

    TFuture<TIssues> AbortCheckpoint(
        const TCoordinatorId& coordinator,
        const TCheckpointId& checkpointId) override;

    TFuture<TGetCheckpointsResult> GetCheckpoints(
        const TString& graph) override;

    TFuture<TGetCheckpointsResult> GetCheckpoints(
        const TString& graph, const TVector<ECheckpointStatus>& statuses, ui64 limit, bool loadGraphDescription) override;

    TFuture<TIssues> DeleteGraph(
        const TString& graphId) override;

    TFuture<TIssues> MarkCheckpointsGC(
        const TString& graphId,
        const TCheckpointId& checkpointUpperBound) override;

    TFuture<TIssues> DeleteMarkedCheckpoints(
        const TString& graphId,
        const TCheckpointId& checkpointUpperBound) override;

    TFuture<ICheckpointStorage::TGetTotalCheckpointsStateSizeResult> GetTotalCheckpointsStateSize(const TString& graphId) override;
    TExecDataQuerySettings DefaultExecDataQuerySettings();

    NYdb::NRetry::TRetryOperationSettings GetRetryOperationSettings();

private:
    TFuture<TCreateCheckpointResult> CreateCheckpointImpl(const TCoordinatorId& coordinator, const TCheckpointContextPtr& context);

private:
    TFuture<TIssues> DeleteCheckpoints(const TString& graphId, const std::optional<TCheckpointId>& checkpointUpperBound);

    IEntityIdGenerator::TPtr EntityIdGenerator;
    const TCheckpointProviderIntegrations CheckpointProviderIntegrations;
};

////////////////////////////////////////////////////////////////////////////////

TCheckpointStorage::TCheckpointStorage(
    const TExternalStorageSettings& config,
    const IEntityIdGenerator::TPtr& entityIdGenerator,
    const IYdbConnection::TPtr& ydbConnection,
    TCheckpointProviderIntegrations checkpointProviderIntegrations)
    : YdbConnection(ydbConnection)
    , Config(config)
    , EntityIdGenerator(entityIdGenerator)
    , CheckpointProviderIntegrations(std::move(checkpointProviderIntegrations))
{
}

TFuture<TIssues> TCheckpointStorage::Init(const NACLib::TDiffACL& acl)
{
    auto graphDesc = TTableBuilder()
        .AddNullableColumn("graph_id", EPrimitiveType::String)
        .AddNullableColumn("generation", EPrimitiveType::Uint64)
        .SetPrimaryKeyColumn("graph_id")
        .BeginPartitioningSettings()
            .SetPartitioningBySize(true)
            .SetMinPartitionsCount(1)
        .EndPartitioningSettings()
        .Build();
    auto f1 = CreateTable(YdbConnection, CoordinatorsSyncTable, std::move(graphDesc), acl);

    // TODO: graph_id could be just secondary index, but API forbids it,
    // so we set it primary key column to have index
    auto checkpointDesc = TTableBuilder()
        .AddNullableColumn("graph_id", EPrimitiveType::String)
        .AddNullableColumn("coordinator_generation", EPrimitiveType::Uint64)
        .AddNullableColumn("seq_no", EPrimitiveType::Uint64)
        .AddNullableColumn("status", EPrimitiveType::Uint8)
        .AddNullableColumn("created_by", EPrimitiveType::Timestamp)
        .AddNullableColumn("modified_by", EPrimitiveType::Timestamp)
        .AddNullableColumn("state_size", EPrimitiveType::Uint64)
        .AddNullableColumn("graph_description_id", EPrimitiveType::String)
        .SetPrimaryKeyColumns({"graph_id", "coordinator_generation", "seq_no"})
        .BeginPartitioningSettings()
            .SetPartitioningBySize(true)
            .SetMinPartitionsCount(1)
        .EndPartitioningSettings()
        .Build();
    auto f2 = CreateTable(YdbConnection, CheckpointsMetadataTable, std::move(checkpointDesc), acl);

    auto checkpointGraphsDescDesc = TTableBuilder()
        .AddNullableColumn("id", EPrimitiveType::String)
        .AddNullableColumn("ref_count", EPrimitiveType::Uint64)
        .AddNullableColumn("graph_description", EPrimitiveType::String)
        .SetPrimaryKeyColumn("id")
        .BeginPartitioningSettings()
            .SetPartitioningBySize(true)
            .SetMinPartitionsCount(1)
        .EndPartitioningSettings()
        .Build();
    auto f3 = CreateTable(YdbConnection, CheckpointsGraphsDescriptionTable, std::move(checkpointGraphsDescDesc), acl);

    std::vector<NThreading::TFuture<NYdb::TStatus>> futures{f1, f2, f3};

    auto promise = NThreading::NewPromise<TIssues>();
    auto voidFuture = NThreading::WaitAll(futures);

    return voidFuture.Apply([futures = std::move(futures), promise](const auto& ) mutable {
        TIssues issues;
        auto check = [&issues] (const NYdb::TStatus& status) {
            if (IsTableCreated(status)) {
                return;
            }
            issues = NYdb::NAdapters::ToYqlIssues(status.GetIssues());
            TStringStream ss;
            ss << "Failed to create table: " << status.GetStatus();
            if (issues) {
                ss << ", issues: ";
                issues.PrintTo(ss);
            }
        };
        check(futures[0].GetValue());
        check(futures[1].GetValue());
        check(futures[2].GetValue());
        return NThreading::MakeFuture(issues);
    });
}

TFuture<TIssues> TCheckpointStorage::RegisterGraphCoordinator(const TCoordinatorId& coordinator)
{
    auto future = YdbConnection->GetTableClient()->RetryOperation(
        [prefix = YdbConnection->GetTablePathPrefix(), coordinator,
         execDataQuerySettings = DefaultExecDataQuerySettings()] (ISession::TPtr session) {

            auto context = MakeIntrusive<TGenerationContext>(
                session,
                true,
                prefix,
                CoordinatorsSyncTable,
                "graph_id",
                "generation",
                coordinator.GraphId,
                coordinator.Generation,
                execDataQuerySettings);

            return RegisterCheckGeneration(context);
        }, GetRetryOperationSettings());

    return StatusToIssues(future);
}

TFuture<ICheckpointStorage::TGetCoordinatorsResult> TCheckpointStorage::GetCoordinators() {
    auto getContext = MakeIntrusive<TGetCoordinatorsContext>();

    auto future = YdbConnection->GetTableClient()->RetryOperation(
        [prefix = YdbConnection->GetTablePathPrefix(), getContext, execDataQuerySettings = DefaultExecDataQuerySettings()] (ISession::TPtr session) {
            auto generationContext = MakeIntrusive<TGenerationContext>(
                session,
                false,
                prefix,
                CoordinatorsSyncTable,
                "graph_id",
                "generation",
                "",
                0UL,
                execDataQuerySettings);

            auto future = SelectGraphCoordinators(generationContext);
            return future.Apply(
                [generationContext, getContext] (const TFuture<TDataQueryResult>& future) {
                    return ProcessCoordinators(future.GetValue(), generationContext, getContext);
                });
        }, GetRetryOperationSettings());

    return StatusToIssues(future).Apply(
        [getContext] (const TFuture<TIssues>& future) {
            auto result = TGetCoordinatorsResult(
                std::move(getContext->Coordinators),
                future.GetValue());
            return MakeFuture(result);
        });
}

TFuture<ICheckpointStorage::TCreateCheckpointResult> TCheckpointStorage::CreateCheckpoint(
    const TCoordinatorId& coordinator,
    const TCheckpointId& checkpointId,
    const TString& graphDescId,
    ECheckpointStatus status)
{
    Y_ABORT_UNLESS(graphDescId);
    auto checkpointContext = MakeIntrusive<TCheckpointContext>(checkpointId, status, ECheckpointStatus::Pending, 0ul);
    checkpointContext->CheckpointGraphDescriptionContext = MakeIntrusive<TCheckpointGraphDescriptionContext>(graphDescId);
    return CreateCheckpointImpl(coordinator, checkpointContext);
}

TFuture<ICheckpointStorage::TCreateCheckpointResult> TCheckpointStorage::CreateCheckpoint(
    const TCoordinatorId& coordinator,
    const TCheckpointId& checkpointId,
    const NProto::TCheckpointGraphDescription& graphDesc,
    ECheckpointStatus status)
{
    auto checkpointContext = MakeIntrusive<TCheckpointContext>(checkpointId, status, ECheckpointStatus::Pending, 0ul);
    checkpointContext->CheckpointGraphDescriptionContext = MakeIntrusive<TCheckpointGraphDescriptionContext>(graphDesc);
    checkpointContext->EntityIdGenerator = EntityIdGenerator;
    return CreateCheckpointImpl(coordinator, checkpointContext);
}

TFuture<ICheckpointStorage::TCreateCheckpointResult> TCheckpointStorage::CreateCheckpointImpl(const TCoordinatorId& coordinator, const TCheckpointContextPtr& checkpointContext) {
    Y_ABORT_UNLESS(checkpointContext->CheckpointGraphDescriptionContext->GraphDescId || checkpointContext->EntityIdGenerator);
    auto future = YdbConnection->GetTableClient()->RetryOperation(
        [prefix = YdbConnection->GetTablePathPrefix(), coordinator, checkpointContext, execDataQuerySettings = DefaultExecDataQuerySettings()] (ISession::TPtr session) {
            auto generationContext = MakeIntrusive<TGenerationContext>(
                session,
                false,
                prefix,
                CoordinatorsSyncTable,
                "graph_id",
                "generation",
                coordinator.GraphId,
                coordinator.Generation,
                execDataQuerySettings);

            checkpointContext->GenerationContext = generationContext;

            auto future = CheckGeneration(generationContext);
            return CreateCheckpointWrapper(future, checkpointContext);
        }, GetRetryOperationSettings());

    return StatusToIssues(future).Apply(
        [checkpointContext] (const TFuture<TIssues>& future) {
            NYql::TIssues issues = future.GetValue();
            TString descId  = !issues ? checkpointContext->CheckpointGraphDescriptionContext->GraphDescId : TString();
            return TCreateCheckpointResult(descId, issues);
        });
}

TFuture<TIssues> TCheckpointStorage::UpdateCheckpointStatus(
    const TCoordinatorId& coordinator,
    const TCheckpointId& checkpointId,
    ECheckpointStatus newStatus,
    ECheckpointStatus prevStatus,
    ui64 stateSizeBytes)
{
    auto checkpointContext = MakeIntrusive<TCheckpointContext>(checkpointId, newStatus, prevStatus, stateSizeBytes);
    auto future = YdbConnection->GetTableClient()->RetryOperation(
        [prefix = YdbConnection->GetTablePathPrefix(), coordinator, checkpointContext, execDataQuerySettings = DefaultExecDataQuerySettings()] (ISession::TPtr session) {
            auto generationContext = MakeIntrusive<TGenerationContext>(
                session,
                false,
                prefix,
                CoordinatorsSyncTable,
                "graph_id",
                "generation",
                coordinator.GraphId,
                coordinator.Generation,
                execDataQuerySettings);

            checkpointContext->GenerationContext = generationContext;

            auto future = CheckGeneration(generationContext);
            return UpdateCheckpointWithCheckWrapper(future, checkpointContext);
        }, GetRetryOperationSettings());

    return StatusToIssues(future);
}

TFuture<TIssues> TCheckpointStorage::AbortCheckpoint(
    const TCoordinatorId& coordinator,
    const TCheckpointId& checkpointId)
{
    auto checkpointContext = MakeIntrusive<TCheckpointContext>(checkpointId, ECheckpointStatus::Aborted, ECheckpointStatus::Pending, 0ul);
    auto future = YdbConnection->GetTableClient()->RetryOperation(
        [prefix = YdbConnection->GetTablePathPrefix(), coordinator, checkpointContext, execDataQuerySettings = DefaultExecDataQuerySettings()] (ISession::TPtr session) {
            auto generationContext = MakeIntrusive<TGenerationContext>(
                session,
                false,
                prefix,
                CoordinatorsSyncTable,
                "graph_id",
                "generation",
                coordinator.GraphId,
                coordinator.Generation,
                execDataQuerySettings);

            checkpointContext->GenerationContext = generationContext;

            auto future = CheckGeneration(generationContext);
            return UpdateCheckpointWithCheckWrapper(future, checkpointContext);
        }, GetRetryOperationSettings());

    return StatusToIssues(future);
}

TFuture<ICheckpointStorage::TGetCheckpointsResult> TCheckpointStorage::GetCheckpoints(const TString& graph) {
    return GetCheckpoints(graph, TVector<ECheckpointStatus>(), std::numeric_limits<ui64>::max(), true);
}

TFuture<ICheckpointStorage::TGetCheckpointsResult> TCheckpointStorage::GetCheckpoints(
    const TString& graph, const TVector<ECheckpointStatus>& statuses, ui64 limit, bool loadGraphDescription)
{
    auto getContext = MakeIntrusive<TGetCheckpointsContext>();

    auto future = YdbConnection->GetTableClient()->RetryOperation(
        [prefix = YdbConnection->GetTablePathPrefix(), graph, getContext, statuses, limit, loadGraphDescription, execDataQuerySettings = DefaultExecDataQuerySettings()] (ISession::TPtr session) {
            auto generationContext = MakeIntrusive<TGenerationContext>(
                session,
                false,
                prefix,
                CoordinatorsSyncTable,
                "graph_id",
                "generation",
                graph,
                0UL,
                execDataQuerySettings);

            auto future = SelectGraphCheckpoints(generationContext, statuses, limit, loadGraphDescription);
            return future.Apply(
                [generationContext, getContext, loadGraphDescription] (const TFuture<TDataQueryResult>& future) {
                    return ProcessCheckpoints(future.GetValue(), generationContext, getContext, loadGraphDescription);
                });
        }, GetRetryOperationSettings());

    return StatusToIssues(future).Apply(
        [getContext] (const TFuture<TIssues>& future) {
            auto result = TGetCheckpointsResult(std::move(getContext->Checkpoints), future.GetValue());
            return MakeFuture(result);
        });
}

TFuture<TIssues> TCheckpointStorage::DeleteGraph(const TString& graphId) {
    return DeleteCheckpoints(graphId, std::nullopt);
}

TFuture<TIssues> TCheckpointStorage::MarkCheckpointsGC(
    const TString& graphId,
    const TCheckpointId& checkpointUpperBound)
{
    auto future = YdbConnection->GetTableClient()->RetryOperation(
        [prefix = YdbConnection->GetTablePathPrefix(), graphId, checkpointUpperBound, thisPtr = TIntrusivePtr(this)] (ISession::TPtr session) {
            // TODO: use prepared queries
            auto query = Sprintf(R"(
                --!syntax_v1
                PRAGMA TablePathPrefix("%s");
                DECLARE $ts AS Timestamp;
                DECLARE $status AS Uint8;
                DECLARE $graph_id AS String;
                DECLARE $coordinator_generation AS Uint64;
                DECLARE $seq_no AS Uint64;

                UPDATE %s
                SET status = $status, modified_by = $ts
                WHERE graph_id = $graph_id AND
                    (coordinator_generation < $coordinator_generation OR
                        (coordinator_generation = $coordinator_generation AND seq_no < $seq_no));
            )", prefix.c_str(),
                CheckpointsMetadataTable);

    auto params = std::make_shared<NYdb::TParamsBuilder>();
    params->
         AddParam("$graph_id")
            .String(graphId)
            .Build()
        .AddParam("$coordinator_generation")
            .Uint64(checkpointUpperBound.CoordinatorGeneration)
            .Build()
        .AddParam("$seq_no")
            .Uint64(checkpointUpperBound.SeqNo)
            .Build()
        .AddParam("$status")
            .Uint8((ui8)ECheckpointStatus::GC)
            .Build()
        .AddParam("$ts")
            .Timestamp(TInstant::Now())
            .Build();

            auto future = session->ExecuteDataQuery(
                query,
                TTxControl::BeginAndCommitTx(),
                std::move(params),
                thisPtr->DefaultExecDataQuerySettings());

            return future.Apply(
                [] (const TFuture<TDataQueryResult>& future) {
                    TStatus status = future.GetValue();
                    return status;
            });
        }, GetRetryOperationSettings());

    return StatusToIssues(future);
}

TFuture<TIssues> TCheckpointStorage::DeleteMarkedCheckpoints(
    const TString& graphId,
    const TCheckpointId& checkpointUpperBound)
{
    return DeleteCheckpoints(graphId, checkpointUpperBound);
}

TFuture<TIssues> TCheckpointStorage::DeleteCheckpoints(
    const TString& graphId,
    const std::optional<TCheckpointId>& checkpointUpperBound)
{
    auto* actorSystem = NActors::TlsActivationContext ? NActors::TActivationContext::ActorSystem() : nullptr;
    auto future = YdbConnection->GetTableClient()->RetryOperation([prefix = YdbConnection->GetTablePathPrefix(), graphId, checkpointUpperBound, checkpointProviderIntegrations = CheckpointProviderIntegrations, settings = DefaultExecDataQuerySettings(), actorSystem](ISession::TPtr session) {
        using namespace fmt::literals;

        auto declarations = TStringBuilder() << "DECLARE $graph_id AS String;";
        auto filter = TStringBuilder() << "graph_id = $graph_id";

        if (checkpointUpperBound) {
            declarations << R"sql(
                DECLARE $coordinator_generation AS Uint64;
                DECLARE $seq_no AS Uint64;
            )sql";
            filter << fmt::format(R"sql(
                AND status = {}
                AND (coordinator_generation < $coordinator_generation OR
                    (coordinator_generation = $coordinator_generation AND seq_no < $seq_no))
                )sql",
                static_cast<ui32>(ECheckpointStatus::GC)
            );
        }

        const auto makeParams = [graphId, checkpointUpperBound] {
            auto params = std::make_shared<NYdb::TParamsBuilder>();
            params
                ->AddParam("$graph_id")
                    .String(graphId)
                    .Build();

            if (checkpointUpperBound) {
                params
                    ->AddParam("$coordinator_generation")
                        .Uint64(checkpointUpperBound->CoordinatorGeneration)
                        .Build()
                    .AddParam("$seq_no")
                        .Uint64(checkpointUpperBound->SeqNo)
                        .Build();
            }

            return params;
        };

        const TString queryPrefix = fmt::format(R"sql(
            --!syntax_v1
            PRAGMA TablePathPrefix("{table_path_prefix}");
            {declarations}

            $refs = SELECT
                COUNT(*) AS refs,
                graph_description_id
            FROM {metadata}
            WHERE {filter}
                AND graph_description_id != ""  -- legacy condition (excludes old records without graph description)
            GROUP BY graph_description_id;

            $update = SELECT
                graphs.id AS id,
                graphs.ref_count - refs.refs AS ref_count,
                graphs.graph_description AS graph_description
            FROM $refs AS refs
            INNER JOIN {descriptions} AS graphs ON refs.graph_description_id = graphs.id;
            )sql",
            "table_path_prefix"_a = prefix,
            "declarations"_a = declarations,
            "metadata"_a = CheckpointsMetadataTable,
            "descriptions"_a = CheckpointsGraphsDescriptionTable,
            "filter"_a = filter
        );

        auto deleteQuery = TStringBuilder() << queryPrefix << fmt::format(R"sql(
            UPDATE {descriptions} ON SELECT id, ref_count FROM $update WHERE ref_count > 0;

            DELETE FROM {descriptions} ON SELECT id FROM $update WHERE ref_count = 0;

            DELETE FROM {metadata} WHERE {filter};
            )sql",
            "descriptions"_a = CheckpointsGraphsDescriptionTable,
            "metadata"_a = CheckpointsMetadataTable,
            "filter"_a = filter);

        if (!checkpointUpperBound) {
            deleteQuery << "DELETE FROM " << CoordinatorsSyncTable << " WHERE graph_id = $graph_id;";
        }

        // Sink cleanup is best effort for both explicit deletion and ordinary GC.
        return session->ExecuteDataQuery(
            TStringBuilder() << queryPrefix << "SELECT graph_description FROM $update WHERE ref_count = 0;",
            TTxControl::BeginTx(),
            makeParams(),
            settings
        ).Apply([session, makeParams, settings, deleteQuery = TString(deleteQuery), checkpointProviderIntegrations, graphId, actorSystem, generationUpperBound = checkpointUpperBound ? std::make_optional(checkpointUpperBound->CoordinatorGeneration) : std::nullopt](const TFuture<TDataQueryResult>& future) -> TFuture<TStatus> {
            const auto& result = future.GetValue();
            if (!result.IsSuccess()) {
                return MakeFuture<TStatus>(result);
            }
            session->UpdateTransaction(result.GetTransaction());

            auto cleanup = future.Apply([checkpointProviderIntegrations, generationUpperBound](const TFuture<TDataQueryResult>& future) {
                return CleanupGraph(checkpointProviderIntegrations, future.GetValue(), generationUpperBound);
            });
            return cleanup.Apply([session, makeParams, settings, deleteQuery, graphId, actorSystem](const TFuture<TIssues>& future) -> TFuture<TStatus> {
                TIssues issues;
                try {
                    issues = future.GetValue();
                } catch (const std::exception& e) {
                    issues.AddIssue(NYql::TIssue(TStringBuilder() << "Checkpoint graph cleanup failed: " << e.what()));
                }

                if (issues && actorSystem) {
                    YDB_LOG_WARN_CTX(*actorSystem, "Deleting checkpoints despite failed sink cleanup",
                        {"graphId", graphId},
                        {"issues", issues.ToOneLineString()});
                }

                return session->ExecuteDataQuery(deleteQuery, TTxControl::ContinueAndCommitTx(), makeParams(), settings)
                    .Apply([](const TFuture<TDataQueryResult>& future) { return TStatus(future.GetValue()); });
            });
        });
    }, GetRetryOperationSettings());

    return StatusToIssues(future);
}

TFuture<ICheckpointStorage::TGetTotalCheckpointsStateSizeResult> TCheckpointStorage::GetTotalCheckpointsStateSize(const TString& graphId) {
    auto result = MakeIntrusive<TGetTotalCheckpointsStateSizeContext>();
    auto future = YdbConnection->GetTableClient()->RetryOperation(
        [prefix = YdbConnection->GetTablePathPrefix(), graphId, thisPtr = TIntrusivePtr(this), result,
         actorSystem = NActors::TActivationContext::ActorSystem()](ISession::TPtr session) {

            auto paramsBuilder = std::make_shared<NYdb::TParamsBuilder>();
            paramsBuilder->AddParam("$graph_id").String(graphId).Build();

            auto query = Sprintf(R"(
                --!syntax_v1
                PRAGMA TablePathPrefix("%s");

                declare $graph_id as string;

                SELECT SUM(state_size)
                FROM %s
                WHERE graph_id = $graph_id
            )", prefix.c_str(), CheckpointsMetadataTable);

            return session->ExecuteDataQuery(
                query,
                TTxControl::BeginAndCommitTx(true),
                std::move(paramsBuilder),
                thisPtr->DefaultExecDataQuerySettings())
              .Apply(
                  [graphId, result, actorSystem](const TFuture<TDataQueryResult>& future) {
                        const auto& queryResult = future.GetValue();
                        auto status = TStatus(queryResult);

                        if (!queryResult.IsSuccess()) {
                            YDB_LOG_ERROR_CTX(*actorSystem, "GetTotalCheckpointsStateSize: can't get total graph's checkpoints size",
                                {"graphId", graphId},
                                {"issues", queryResult.GetIssues()});
                            return status;
                        }

                        TResultSetParser parser = queryResult.GetResultSetParser(0);
                        if (parser.TryNextRow()) {
                            result->Size = parser.ColumnParser(0).GetOptionalUint64().value_or(0);
                        } else {
                            result->Size = 0;
                        }
                        return status;
                    });
        }, GetRetryOperationSettings());

    return StatusToIssues(future).Apply(
        [result] (const TFuture<TIssues>& future) {
            return std::make_pair(result->Size, future.GetValue());
        });
}

NYdb::NRetry::TRetryOperationSettings TCheckpointStorage::GetRetryOperationSettings() {
    return NYdb::NRetry::TRetryOperationSettings()
        .MaxRetries(Config.GetMaxRetries())
        .MaxTimeout(Config.GetMaxRetryTimeout());
}

TExecDataQuerySettings TCheckpointStorage::DefaultExecDataQuerySettings() {
    return TExecDataQuerySettings()
        .KeepInQueryCache(true)
        .ClientTimeout(Config.GetClientTimeout())
        .OperationTimeout(Config.GetOperationTimeout())
        .CancelAfter(Config.GetCancelAfter());
}

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

TCheckpointStoragePtr NewYdbCheckpointStorage(
    const TExternalStorageSettings& config,
    const IEntityIdGenerator::TPtr& entityIdGenerator,
    const IYdbConnection::TPtr& ydbConnection,
    TCheckpointProviderIntegrations checkpointProviderIntegrations)
{
    Y_ABORT_UNLESS(entityIdGenerator);
    return new TCheckpointStorage(config, entityIdGenerator, ydbConnection, std::move(checkpointProviderIntegrations));
}

} // namespace NFq
