#include "ydb_checkpoint_storage.h"

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/ydb/ydb.h>

#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

#include <fmt/format.h>

namespace NFq {

using namespace NThreading;
using namespace NYdb;
using namespace NYdb::NTable;

using NYql::TIssues;

namespace {

////////////////////////////////////////////////////////////////////////////////

const char* const CoordinatorsSyncTable = "coordinators_sync";
const char* const CheckpointsMetadataTable = "checkpoints_metadata";
const char* const CheckpointsGraphsDescriptionTable = "checkpoints_graphs_description";

////////////////////////////////////////////////////////////////////////////////

struct TCheckpointGraphDescriptionContext : public TThrRefBase {
    TString GraphDescId;
    const TMaybe<NProto::TCheckpointGraphDescription> NewGraphDescription;

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
    const ECheckpointStatus ExpectedStatus; // optional expecrted current status, used only in some operations
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

    return context->Session.ExecuteDataQuery(
        query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx());
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

    NYdb::TParamsBuilder params;
    params
        .AddParam("$graph_id")
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
            NYql::TIssues issues;
            issues.AddIssue("Failed to serialize graph description proto");
            return MakeFuture(TStatus(EStatus::BAD_REQUEST, std::move(issues)));
        }

        params
            .AddParam("$graph_description")
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

    auto ttxControl = TTxControl::Tx(*generationContext->Transaction).CommitTx();
    return generationContext->Session.ExecuteDataQuery(query, ttxControl, params.Build()).Apply(
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
        $ts = cast(%lu as Timestamp);

        UPSERT INTO %s (graph_id, coordinator_generation, seq_no, status, state_size, modified_by) VALUES
            ("%s", %lu, %lu, %u, %lu, $ts);
    )", generationContext->TablePathPrefix.c_str(),
        TInstant::Now().MicroSeconds(),
        CheckpointsMetadataTable,
        generationContext->PrimaryKey.c_str(),
        context->CheckpointId.CoordinatorGeneration,
        context->CheckpointId.SeqNo,
        (ui32)context->Status,
        context->StateSizeBytes);

    auto ttxControl = TTxControl::Tx(*generationContext->Transaction).CommitTx();
    return generationContext->Session.ExecuteDataQuery(query, ttxControl).Apply(
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

        SELECT ref_count
        FROM %s
        WHERE id = "%s";
    )", generationContext->TablePathPrefix.c_str(),
        CheckpointsGraphsDescriptionTable,
        graphDescContext->GraphDescId.c_str());

    return generationContext->Session.ExecuteDataQuery(query, TTxControl::Tx(*generationContext->Transaction));
}

bool GraphDescIdExists(const TFuture<TDataQueryResult>& result) {
    return result.GetValue().GetResultSet(0).RowsCount() != 0;
}

TFuture<TStatus> GenerateGraphDescId(const TCheckpointContextPtr& context) {
    if (context->CheckpointGraphDescriptionContext->GraphDescId) { // already given
        return MakeFuture(TStatus(EStatus::SUCCESS, NYql::TIssues()));
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
                    return MakeFuture(TStatus(EStatus::SUCCESS, NYql::TIssues()));
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

TFuture<TDataQueryResult> SelectGraphCheckpoints(const TGenerationContextPtr& context, const TVector<ECheckpointStatus>& statuses, ui64 limit, TExecDataQuerySettings settings, bool loadGraphDescription)
{
    NYdb::TParamsBuilder paramsBuilder;
    if (statuses) {
        auto& statusesParam = paramsBuilder.AddParam("$statuses").BeginList();
        for (const auto& status : statuses) {
            statusesParam.AddListItem().Uint8(static_cast<ui8>(status));
        }
        statusesParam.EndList().Build();
    }

    paramsBuilder.AddParam("$graph_id").String(context->PrimaryKey).Build();
    if (limit < std::numeric_limits<ui64>::max()) {
        paramsBuilder.AddParam("$limit").Uint64(limit).Build();
    }

    auto params = paramsBuilder.Build();

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

    return context->Session.ExecuteDataQuery(
        query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        params,
        settings);
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
            if (const TMaybe<TString> graphDescription = parser.ColumnParser("graph_description").GetOptionalString(); graphDescription && *graphDescription) {
                NProto::TCheckpointGraphDescription graphDesc;
                if (!graphDesc.ParseFromString(*graphDescription)) {
                    NYql::TIssues issues;
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

        SELECT status
        FROM %s
        WHERE graph_id = "%s" AND  coordinator_generation = %lu AND seq_no = %lu;
    )", generationContext->TablePathPrefix.c_str(),
        CheckpointsMetadataTable,
        generationContext->PrimaryKey.c_str(),
        context->CheckpointId.CoordinatorGeneration,
        context->CheckpointId.SeqNo);

    return generationContext->Session.ExecuteDataQuery(
        query,
        TTxControl::Tx(*generationContext->Transaction));
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

////////////////////////////////////////////////////////////////////////////////

class TCheckpointStorage : public ICheckpointStorage {
    TYqSharedResources::TPtr YqSharedResources;
    TYdbConnectionPtr YdbConnection;
    const NConfig::TYdbStorageConfig Config;

public:
    explicit TCheckpointStorage(
        const NConfig::TYdbStorageConfig& config,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        const IEntityIdGenerator::TPtr& entityIdGenerator,
        const TYqSharedResources::TPtr& yqSharedResources);

    ~TCheckpointStorage() = default;

    TFuture<TIssues> Init() override;

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

private:
    TFuture<TCreateCheckpointResult> CreateCheckpointImpl(const TCoordinatorId& coordinator, const TCheckpointContextPtr& context);

private:
    IEntityIdGenerator::TPtr EntityIdGenerator;
};

////////////////////////////////////////////////////////////////////////////////

TCheckpointStorage::TCheckpointStorage(
    const NConfig::TYdbStorageConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const IEntityIdGenerator::TPtr& entityIdGenerator,
    const TYqSharedResources::TPtr& yqSharedResources)
    : YqSharedResources(yqSharedResources)
    , YdbConnection(NewYdbConnection(config, credentialsProviderFactory, YqSharedResources->CoreYdbDriver))
    , Config(config)
    , EntityIdGenerator(entityIdGenerator)
{
}

TFuture<TIssues> TCheckpointStorage::Init()
{
    TIssues issues;

    // TODO: list at first?
    if (YdbConnection->DB != YdbConnection->TablePathPrefix) {
        auto status = YdbConnection->SchemeClient.MakeDirectory(YdbConnection->TablePathPrefix).GetValueSync();
        if (!status.IsSuccess() && status.GetStatus() != EStatus::ALREADY_EXISTS) {
            issues = status.GetIssues();

            TStringStream ss;
            ss << "Failed to create path '" << YdbConnection->TablePathPrefix << "': " << status.GetStatus();
            if (issues) {
                ss << ", issues: ";
                issues.PrintTo(ss);
            }

            return MakeFuture(std::move(issues));
        }
    }

#define RUN_CREATE_TABLE(tableName, desc)                           \
    {                                                               \
        auto status = CreateTable(YdbConnection,                    \
                                  tableName,                        \
                                  std::move(desc)).GetValueSync();  \
        if (!IsTableCreated(status)) {                              \
            issues = status.GetIssues();                            \
                                                                    \
            TStringStream ss;                                       \
            ss << "Failed to create " << tableName                  \
               << " table: " << status.GetStatus();                 \
            if (issues) {                                           \
                ss << ", issues: ";                                 \
                issues.PrintTo(ss);                                 \
            }                                                       \
                                                                    \
            return MakeFuture(std::move(issues));                   \
        }                                                           \
    }

    auto graphDesc = TTableBuilder()
        .AddNullableColumn("graph_id", EPrimitiveType::String)
        .AddNullableColumn("generation", EPrimitiveType::Uint64)
        .SetPrimaryKeyColumn("graph_id")
        .Build();

    RUN_CREATE_TABLE(CoordinatorsSyncTable, graphDesc);

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
        .Build();

    RUN_CREATE_TABLE(CheckpointsMetadataTable, checkpointDesc);

    auto checkpointGraphsDescDesc = TTableBuilder()
        .AddNullableColumn("id", EPrimitiveType::String)
        .AddNullableColumn("ref_count", EPrimitiveType::Uint64)
        .AddNullableColumn("graph_description", EPrimitiveType::String)
        .SetPrimaryKeyColumn("id")
        .Build();

    RUN_CREATE_TABLE(CheckpointsGraphsDescriptionTable, checkpointGraphsDescDesc);

#undef RUN_CREATE_TABLE

    return MakeFuture(std::move(issues));
}

TFuture<TIssues> TCheckpointStorage::RegisterGraphCoordinator(const TCoordinatorId& coordinator)
{
    auto future = YdbConnection->TableClient.RetryOperation(
        [prefix = YdbConnection->TablePathPrefix, coordinator] (TSession session) {
            auto context = MakeIntrusive<TGenerationContext>(
                session,
                true,
                prefix,
                CoordinatorsSyncTable,
                "graph_id",
                "generation",
                coordinator.GraphId,
                coordinator.Generation);

            return RegisterCheckGeneration(context);
        });

    return StatusToIssues(future);
}

TFuture<ICheckpointStorage::TGetCoordinatorsResult> TCheckpointStorage::GetCoordinators() {
    auto getContext = MakeIntrusive<TGetCoordinatorsContext>();

    auto future = YdbConnection->TableClient.RetryOperation(
        [prefix = YdbConnection->TablePathPrefix, getContext] (TSession session) {
            auto generationContext = MakeIntrusive<TGenerationContext>(
                session,
                false,
                prefix,
                CoordinatorsSyncTable,
                "graph_id",
                "generation",
                "",
                0UL);

            auto future = SelectGraphCoordinators(generationContext);
            return future.Apply(
                [generationContext, getContext] (const TFuture<TDataQueryResult>& future) {
                    return ProcessCoordinators(future.GetValue(), generationContext, getContext);
                });
        });

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
    auto future = YdbConnection->TableClient.RetryOperation(
        [prefix = YdbConnection->TablePathPrefix, coordinator, checkpointContext] (TSession session) {
            auto generationContext = MakeIntrusive<TGenerationContext>(
                session,
                false,
                prefix,
                CoordinatorsSyncTable,
                "graph_id",
                "generation",
                coordinator.GraphId,
                coordinator.Generation);

            checkpointContext->GenerationContext = generationContext;

            auto future = CheckGeneration(generationContext);
            return CreateCheckpointWrapper(future, checkpointContext);
        });

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
    auto future = YdbConnection->TableClient.RetryOperation(
        [prefix = YdbConnection->TablePathPrefix, coordinator, checkpointContext] (TSession session) {
            auto generationContext = MakeIntrusive<TGenerationContext>(
                session,
                false,
                prefix,
                CoordinatorsSyncTable,
                "graph_id",
                "generation",
                coordinator.GraphId,
                coordinator.Generation);

            checkpointContext->GenerationContext = generationContext;

            auto future = CheckGeneration(generationContext);
            return UpdateCheckpointWithCheckWrapper(future, checkpointContext);
        });

    return StatusToIssues(future);
}

TFuture<TIssues> TCheckpointStorage::AbortCheckpoint(
    const TCoordinatorId& coordinator,
    const TCheckpointId& checkpointId)
{
    auto checkpointContext = MakeIntrusive<TCheckpointContext>(checkpointId, ECheckpointStatus::Aborted, ECheckpointStatus::Pending, 0ul);
    auto future = YdbConnection->TableClient.RetryOperation(
        [prefix = YdbConnection->TablePathPrefix, coordinator, checkpointContext] (TSession session) {
            auto generationContext = MakeIntrusive<TGenerationContext>(
                session,
                false,
                prefix,
                CoordinatorsSyncTable,
                "graph_id",
                "generation",
                coordinator.GraphId,
                coordinator.Generation);

            checkpointContext->GenerationContext = generationContext;

            auto future = CheckGeneration(generationContext);
            return UpdateCheckpointWithCheckWrapper(future, checkpointContext);
        });

    return StatusToIssues(future);
}

TFuture<ICheckpointStorage::TGetCheckpointsResult> TCheckpointStorage::GetCheckpoints(const TString& graph) {
    return GetCheckpoints(graph, TVector<ECheckpointStatus>(), std::numeric_limits<ui64>::max(), true);
}

TFuture<ICheckpointStorage::TGetCheckpointsResult> TCheckpointStorage::GetCheckpoints(
    const TString& graph, const TVector<ECheckpointStatus>& statuses, ui64 limit, bool loadGraphDescription)
{
    auto getContext = MakeIntrusive<TGetCheckpointsContext>();

    auto future = YdbConnection->TableClient.RetryOperation(
        [prefix = YdbConnection->TablePathPrefix, graph, getContext, statuses, limit, loadGraphDescription, settings = DefaultExecDataQuerySettings()] (TSession session) {
            auto generationContext = MakeIntrusive<TGenerationContext>(
                session,
                false,
                prefix,
                CoordinatorsSyncTable,
                "graph_id",
                "generation",
                graph,
                0UL);

            auto future = SelectGraphCheckpoints(generationContext, statuses, limit, settings, loadGraphDescription);
            return future.Apply(
                [generationContext, getContext, loadGraphDescription] (const TFuture<TDataQueryResult>& future) {
                    return ProcessCheckpoints(future.GetValue(), generationContext, getContext, loadGraphDescription);
                });
        });

    return StatusToIssues(future).Apply(
        [getContext] (const TFuture<TIssues>& future) {
            auto result = TGetCheckpointsResult(std::move(getContext->Checkpoints), future.GetValue());
            return MakeFuture(result);
        });
}

TFuture<TIssues> TCheckpointStorage::DeleteGraph(const TString& graphId) {
    auto future = YdbConnection->TableClient.RetryOperation(
        [prefix = YdbConnection->TablePathPrefix, graphId] (TSession session) {
            // TODO: use prepared queries
            auto query = Sprintf(R"(
                --!syntax_v1
                PRAGMA TablePathPrefix("%s");

                DELETE
                FROM %s
                WHERE graph_id = "%s";

                DELETE
                FROM %s
                WHERE graph_id = "%s";
            )", prefix.c_str(),
                CoordinatorsSyncTable,
                graphId.c_str(),
                CheckpointsMetadataTable,
                graphId.c_str());

            auto future = session.ExecuteDataQuery(
                query,
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx());

            return future.Apply(
                [] (const TFuture<TDataQueryResult>& future) {
                    TStatus status = future.GetValue();
                    return status;
            });
        });

    return StatusToIssues(future);
}

TFuture<TIssues> TCheckpointStorage::MarkCheckpointsGC(
    const TString& graphId,
    const TCheckpointId& checkpointUpperBound)
{
    auto future = YdbConnection->TableClient.RetryOperation(
        [prefix = YdbConnection->TablePathPrefix, graphId, checkpointUpperBound] (TSession session) {
            // TODO: use prepared queries
            auto query = Sprintf(R"(
                --!syntax_v1
                PRAGMA TablePathPrefix("%s");
                $ts = cast(%lu as Timestamp);

                UPDATE %s
                SET status = %u, modified_by = $ts
                WHERE graph_id = "%s" AND
                    (coordinator_generation < %lu OR
                        (coordinator_generation = %lu AND seq_no < %lu));
            )", prefix.c_str(),
                TInstant::Now().MicroSeconds(),
                CheckpointsMetadataTable,
                (ui32)ECheckpointStatus::GC,
                graphId.c_str(),
                checkpointUpperBound.CoordinatorGeneration,
                checkpointUpperBound.CoordinatorGeneration,
                checkpointUpperBound.SeqNo);

            auto future = session.ExecuteDataQuery(
                query,
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx());

            return future.Apply(
                [] (const TFuture<TDataQueryResult>& future) {
                    TStatus status = future.GetValue();
                    return status;
            });
        });

    return StatusToIssues(future);
}

TFuture<TIssues> TCheckpointStorage::DeleteMarkedCheckpoints(
    const TString& graphId,
    const TCheckpointId& checkpointUpperBound)
{
    auto future = YdbConnection->TableClient.RetryOperation(
        [prefix = YdbConnection->TablePathPrefix, graphId, checkpointUpperBound] (TSession session) {
            // TODO: use prepared queries
            using namespace fmt::literals;
            const TString query = fmt::format(R"sql(
                --!syntax_v1
                PRAGMA TablePathPrefix("{table_path_prefix}");
                DECLARE $graph_id AS String;
                DECLARE $coordinator_generation AS Uint64;
                DECLARE $seq_no AS Uint64;

                $refs =
                  SELECT
                    COUNT(*) AS refs, graph_description_id
                  FROM {checkpoints_metadata_table_name}
                  WHERE graph_id = $graph_id AND status = {gc_status}
                    AND (coordinator_generation < $coordinator_generation OR
                      (coordinator_generation = $coordinator_generation AND seq_no < $seq_no))
                    AND graph_description_id != "" -- legacy condition (excludes old records without graph description)
                  GROUP BY graph_description_id;

                $update =
                  SELECT
                    checkpoints_graphs_description.id AS id,
                    checkpoints_graphs_description.ref_count - refs.refs AS ref_count
                  FROM $refs AS refs
                    INNER JOIN {checkpoints_graphs_description_table_name}
                      ON refs.graph_description_id = checkpoints_graphs_description.id;

                UPDATE {checkpoints_graphs_description_table_name}
                  ON SELECT * FROM $update WHERE ref_count > 0;

                DELETE FROM {checkpoints_graphs_description_table_name}
                  ON SELECT * FROM $update WHERE ref_count = 0;

                DELETE FROM {checkpoints_metadata_table_name}
                WHERE graph_id = $graph_id AND status = {gc_status}
                  AND (coordinator_generation < $coordinator_generation OR
                    (coordinator_generation = $coordinator_generation AND seq_no < $seq_no));
            )sql",
            "table_path_prefix"_a = prefix,
            "checkpoints_metadata_table_name"_a = CheckpointsMetadataTable,
            "checkpoints_graphs_description_table_name"_a = CheckpointsGraphsDescriptionTable,
            "gc_status"_a = static_cast<ui32>(ECheckpointStatus::GC)
            );

            NYdb::TParamsBuilder params;
            params
                .AddParam("$graph_id")
                    .String(graphId)
                    .Build()
                .AddParam("$coordinator_generation")
                    .Uint64(checkpointUpperBound.CoordinatorGeneration)
                    .Build()
                .AddParam("$seq_no")
                    .Uint64(checkpointUpperBound.SeqNo)
                    .Build();

            auto future = session.ExecuteDataQuery(
                query,
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params.Build());

            return future.Apply(
                [] (const TFuture<TDataQueryResult>& future) {
                    TStatus status = future.GetValue();
                    return status;
            });
        });

    return StatusToIssues(future);
}

TFuture<ICheckpointStorage::TGetTotalCheckpointsStateSizeResult> TCheckpointStorage::GetTotalCheckpointsStateSize(const TString& graphId) {
    auto result = MakeIntrusive<TGetTotalCheckpointsStateSizeContext>();
    auto future = YdbConnection->TableClient.RetryOperation(
        [prefix = YdbConnection->TablePathPrefix, graphId, thisPtr = TIntrusivePtr(this), result,
         actorSystem = NActors::TActivationContext::ActorSystem()](TSession session) {
          NYdb::TParamsBuilder paramsBuilder;
          paramsBuilder.AddParam("$graph_id").String(graphId).Build();
          auto params = paramsBuilder.Build();

          auto query = Sprintf(R"(
                --!syntax_v1
                PRAGMA TablePathPrefix("%s");

                declare $graph_id as string;

                SELECT SUM(state_size)
                FROM %s
                WHERE graph_id = $graph_id
            )", prefix.c_str(), CheckpointsMetadataTable);

          return session.ExecuteDataQuery(
                  query,
                  TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx(),
                  params,
                  thisPtr->DefaultExecDataQuerySettings())
              .Apply(
                  [graphId, result, actorSystem](const TFuture<TDataQueryResult>& future) {
                        const auto& queryResult = future.GetValue();
                        auto status = TStatus(queryResult);

                        if (!queryResult.IsSuccess()) {
                            LOG_STREAMS_STORAGE_SERVICE_AS_ERROR(*actorSystem, TStringBuilder() << "GetTotalCheckpointsStateSize: can't get total graph's checkpoints size [" << graphId << "] " << queryResult.GetIssues().ToString());                            return status;
                        }

                        TResultSetParser parser = queryResult.GetResultSetParser(0);
                        if (parser.TryNextRow()) {
                            result->Size = parser.ColumnParser(0).GetOptionalUint64().GetOrElse(0);
                        } else {
                            result->Size = 0;
                        }
                        return status;
                    });
        });

    return StatusToIssues(future).Apply(
        [result] (const TFuture<TIssues>& future) {
            return std::make_pair(result->Size, future.GetValue());
        });
}

TExecDataQuerySettings TCheckpointStorage::DefaultExecDataQuerySettings() {
    return TExecDataQuerySettings()
        .KeepInQueryCache(true)
        .ClientTimeout(TDuration::Seconds(Config.GetClientTimeoutSec()))
        .OperationTimeout(TDuration::Seconds(Config.GetOperationTimeoutSec()))
        .CancelAfter(TDuration::Seconds(Config.GetCancelAfterSec()));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TCheckpointStoragePtr NewYdbCheckpointStorage(
    const NConfig::TYdbStorageConfig& config,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const IEntityIdGenerator::TPtr& entityIdGenerator,
    const TYqSharedResources::TPtr& yqSharedResources)
{
    Y_ABORT_UNLESS(entityIdGenerator);
    return new TCheckpointStorage(config, credentialsProviderFactory, entityIdGenerator, yqSharedResources);
}

} // namespace NFq
