#include "ydb_service_table.h"

#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <ydb/public/lib/ydb_cli/common/print_operation.h>
#include <ydb/public/lib/ydb_cli/common/query_stats.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>
#include <ydb/public/lib/stat_visualization/flame_graph_builder.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <library/cpp/json/json_prettifier.h>
#include <google/protobuf/util/json_util.h>

#include <util/string/split.h>
#include <util/folder/path.h>
#include <util/folder/dirut.h>
#include <util/generic/guid.h>

#include <math.h>

namespace NYdb {
namespace NConsoleClient {

TCommandTable::TCommandTable()
    : TClientCommandTree("table", {}, "Table service operations")
{
    //AddCommand(std::make_unique<TCommandCreateTable>());
    AddCommand(std::make_unique<TCommandDropTable>());
    AddCommand(std::make_unique<TCommandTableQuery>());
    AddCommand(std::make_unique<TCommandReadTable>());
    AddCommand(std::make_unique<TCommandIndex>());
    AddCommand(std::make_unique<TCommandAttribute>());
    AddCommand(std::make_unique<TCommandTtl>());
}

TCommandTableQuery::TCommandTableQuery()
    : TClientCommandTree("query", {}, "Query operations")
{
    AddCommand(std::make_unique<TCommandExecuteQuery>());
    AddCommand(std::make_unique<TCommandExplain>());
}

TCommandIndex::TCommandIndex()
    : TClientCommandTree("index", {}, "Index operations")
{
    AddCommand(std::make_unique<TCommandIndexAdd>());
    AddCommand(std::make_unique<TCommandIndexDrop>());
    AddCommand(std::make_unique<TCommandIndexRename>());
}

TCommandAttribute::TCommandAttribute()
    : TClientCommandTree("attribute", {"attr"}, "Attribute operations")
{
    AddCommand(std::make_unique<TCommandAttributeAdd>());
    AddCommand(std::make_unique<TCommandAttributeDrop>());
}

TCommandTtl::TCommandTtl()
    : TClientCommandTree("ttl", {}, "Ttl operations")
{
    AddCommand(std::make_unique<TCommandTtlSet>());
    AddCommand(std::make_unique<TCommandTtlReset>());
}

TCommandIndexAdd::TCommandIndexAdd()
    : TClientCommandTree("add", {}, "Add index in to the specified table")
{
    AddCommand(std::make_unique<TCommandIndexAddGlobalSync>());
    AddCommand(std::make_unique<TCommandIndexAddGlobalAsync>());
}

TTableCommand::TTableCommand(const TString& name, const std::initializer_list<TString>& aliases, const TString& description)
    : TYdbOperationCommand(name, aliases, description)
{}

void TTableCommand::Config(TConfig& config) {
    TYdbOperationCommand::Config(config);
    // TODO: Session options?
}

NTable::TSession TTableCommand::GetSession(TConfig& config) {
    NTable::TTableClient client(CreateDriver(config));
    NTable::TCreateSessionResult result = client.GetSession(NTable::TCreateSessionSettings()).GetValueSync();
    ThrowOnError(result);
    return result.GetSession();
}

namespace {
    TList<std::pair<TString, EPrimitiveType>> YdbPrimitives = {
        {"Bool", EPrimitiveType::Bool},
        {"Int8", EPrimitiveType::Int8},
        {"Uint8", EPrimitiveType::Uint8},
        {"Int16", EPrimitiveType::Int16},
        {"Uint16", EPrimitiveType::Uint16},
        {"Int32", EPrimitiveType::Int32},
        {"Uint32", EPrimitiveType::Uint32},
        {"Int64", EPrimitiveType::Int64},
        {"Uint64", EPrimitiveType::Uint64},
        {"Float", EPrimitiveType::Float},
        {"Double", EPrimitiveType::Double},
        {"Date", EPrimitiveType::Date},
        {"Datetime", EPrimitiveType::Datetime},
        {"Timestamp", EPrimitiveType::Timestamp},
        {"Interval", EPrimitiveType::Interval},
        {"Date32", EPrimitiveType::Date32},
        {"Datetime64", EPrimitiveType::Datetime64},
        {"Timestamp64", EPrimitiveType::Timestamp64},
        {"Interval64", EPrimitiveType::Interval64},
        {"TzDate", EPrimitiveType::TzDate},
        {"TzDatetime", EPrimitiveType::TzDatetime},
        {"TzTimestamp", EPrimitiveType::TzTimestamp},
        {"String", EPrimitiveType::String},
        {"Utf8", EPrimitiveType::Utf8},
        {"Yson", EPrimitiveType::Yson},
        {"Json", EPrimitiveType::Json},
        {"Uuid", EPrimitiveType::Uuid},
        {"JsonDocument", EPrimitiveType::JsonDocument},
        {"DyNumber", EPrimitiveType::DyNumber},
    };

    TString GetAllTypesString() {
        TStringBuilder result;
        for (auto& type : YdbPrimitives) {
            result << type.first;
            result << ", ";
        }
        result << "Decimal:<precision>:<scale>";
        return result;
    }

    EPrimitiveType ConvertStringToYdbPrimitive(const TString& type) {
        auto result = find_if(
            YdbPrimitives.begin(),
            YdbPrimitives.end(),
            [&type](const std::pair<TString, EPrimitiveType>& it) { return it.first == type; }
        );
        if (result == YdbPrimitives.end()) {
            throw TMisuseException() << "Unknown type: " << type << Endl << "Allowed types: " << GetAllTypesString();
        }
        return result->second;
    }
}

TCommandCreateTable::TCommandCreateTable()
    : TTableCommand("create", {}, "Create new table")
{}

void TCommandCreateTable::Config(TConfig& config) {
    TTableCommand::Config(config);

    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<table path>", "New table path");

    config.Opts->AddLongOption('c', "Column",
        TStringBuilder() << "[At least one] Column(s)." << Endl << "Allowed types : " << GetAllTypesString())
        .RequiredArgument("<name>:<type>[:<family>]").AppendTo(&Columns);
    config.Opts->AddLongOption('p', "primary-key", "[At least one] Primary key(s)")
        .RequiredArgument("NAME").AppendTo(&PrimaryKeys);
    config.Opts->AddLongOption('i', "index", "Index(es).")
        .RequiredArgument("<name>:<column1>[,<column2>...]").AppendTo(&Indexes);
    config.Opts->AddLongOption("preset-name", "Create table preset name")
        .RequiredArgument("NAME").StoreResult(&PresetName);
    config.Opts->AddLongOption("execution-policy", "Execution policy preset name")
        .RequiredArgument("NAME").StoreResult(&ExecutionPolicy);
    config.Opts->AddLongOption("compaction-policy", "Compaction policy preset name")
        .RequiredArgument("NAME").StoreResult(&CompactionPolicy);
    config.Opts->AddLongOption("partitioning-policy", "Partitioning policy preset name")
        .RequiredArgument("NAME").StoreResult(&PartitioningPolicy);
    config.Opts->AddLongOption("auto-partitioning", "Auto-partitioning policy. [Disabled, AutoSplit, AutoSplitMerge]")
        .RequiredArgument("[String]").StoreResult(&AutoPartitioning);
    config.Opts->AddLongOption("uniform-partitions", "Enable uniform sharding using given shards number."
        "The first components of primary key must have Uint32/Uint64 type.")
        .RequiredArgument("[Uint64]").StoreResult(&UniformPartitions);
    config.Opts->AddLongOption("replication-policy", "Replication policy preset name")
        .RequiredArgument("NAME").StoreResult(&ReplicationPolicy);
    config.Opts->AddLongOption("replicas-count", "If value is non-zero then it specifies a number of read-only "
        "replicas to create for a table. Zero value means preset setting usage.")
        .RequiredArgument("Ui32").StoreResult(&ReplicasCount);
    config.Opts->AddLongOption("per-availability-zone", "If this feature in enabled then requested number of replicas "
        "will be created in each availability zone.")
        .StoreTrue(&CreatePerAvailabilityZone);
    config.Opts->AddLongOption("allow-promotion", "If this feature in enabled then read-only replicas can be promoted "
        "to leader.")
        .StoreTrue(&AllowPromotion);
}

void TCommandCreateTable::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParsePath(config, 0);
    if (!Columns.size()) {
        throw TMisuseException() << "At least one column should be provided";
    }
    if (!PrimaryKeys.size()) {
        throw TMisuseException() << "At least one primary key should be provided";
    }
}

int TCommandCreateTable::Run(TConfig& config) {
    NTable::TTableBuilder builder;
    for (const TString& column : Columns) {
        TVector<TString> parts = StringSplitter(column).Split(':');
        if (parts[1] == "Decimal") {
            if (parts.size() != 4 && parts.size() != 5) {
                throw TMisuseException() << "Can't parse column \"" << column
                    << "\". Expected decimal format: \"<name>:Decimal:<precision>:<scale>[:<family>]\"";
            }
            TString family;
            if (parts.size() == 5) {
                family = parts[4];
            }
            builder.AddNullableColumn(
                parts[0],
                TDecimalType(FromString<ui8>(parts[2]), FromString<ui8>(parts[3])),
                family
            );
        } else {
            if (parts.size() != 2 && parts.size() != 3) {
                throw TMisuseException()
                    << "Can't parse column \"" << column << "\". Expected format: \"<name>:<type>[:<family>]\"";
            }
            TString family;
            if (parts.size() == 3) {
                family = parts[2];
            }
            builder.AddNullableColumn(parts[0], ConvertStringToYdbPrimitive(parts[1]), family);
        }
    }
    builder.SetPrimaryKeyColumns(PrimaryKeys);
    for (const TString& index : Indexes) {
        TVector<TString> parts = StringSplitter(index).Split(':');
        if (parts.size() != 2 || !parts[0] || !parts[1]) {
            throw TMisuseException() << "Can't parse index \"" << index
                << "\". Need exactly one colon. Expected format: \"<name>:<column1>[,<column2>,...]\"";
        }
        TVector<TString> columns = StringSplitter(parts[1]).Split(',');
        for (TString& column : columns) {
            if (!column) {
                throw TMisuseException() << "Can't parse index \"" << index
                    << "\". Empty column names found. Expected format: \"<name>:<column1>[,<column2>,...]\"";
            }
        }
        builder.AddSecondaryIndex(parts[0], columns);
    }

    NTable::TCreateTableSettings tableSettings = FillSettings(NTable::TCreateTableSettings());
    if (PresetName) {
        tableSettings.PresetName(PresetName);
    }
    if (ExecutionPolicy) {
        tableSettings.ExecutionPolicy(ExecutionPolicy);
    }
    if (CompactionPolicy) {
        tableSettings.CompactionPolicy(CompactionPolicy);
    }

    NTable::TPartitioningPolicy partitioningPolicy;
    if (PartitioningPolicy) {
        partitioningPolicy.PresetName(PartitioningPolicy);
    }
    if (AutoPartitioning) {
        if (AutoPartitioning == "Disabled") {
            partitioningPolicy.AutoPartitioning(NTable::EAutoPartitioningPolicy::Disabled);
        } else {
            if (AutoPartitioning == "AutoSplit") {
                partitioningPolicy.AutoPartitioning(NTable::EAutoPartitioningPolicy::AutoSplit);
            } else {
                if (AutoPartitioning == "AutoSplitMerge") {
                    partitioningPolicy.AutoPartitioning(NTable::EAutoPartitioningPolicy::AutoSplitMerge);
                } else {
                    throw TMisuseException() << "Unknown auto-partitioning policy.";
                }
            }
        }
    }
    if (UniformPartitions) {
        partitioningPolicy.UniformPartitions(FromString<ui64>(UniformPartitions));
    }
    tableSettings.PartitioningPolicy(partitioningPolicy);

    NTable::TReplicationPolicy replicationPolicy;
    if (ReplicationPolicy) {
        replicationPolicy.PresetName(ReplicationPolicy);
    }
    if (ReplicasCount) {
        replicationPolicy.ReplicasCount(FromString<ui32>(ReplicasCount));
    }
    if (CreatePerAvailabilityZone) {
        replicationPolicy.CreatePerAvailabilityZone(CreatePerAvailabilityZone);
    }
    if (AllowPromotion) {
        replicationPolicy.AllowPromotion(AllowPromotion);
    }

    ThrowOnError(
        GetSession(config).CreateTable(
            Path,
            builder.Build(),
            std::move(tableSettings)
        ).GetValueSync()
    );
    return EXIT_SUCCESS;
}

TCommandDropTable::TCommandDropTable()
    : TTableCommand("drop", {}, "Drop a table")
{}

void TCommandDropTable::Config(TConfig& config) {
    TTableCommand::Config(config);

    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<table path>", "table to drop path");
}

void TCommandDropTable::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParsePath(config, 0);
}

int TCommandDropTable::Run(TConfig& config) {
    ThrowOnError(
        GetSession(config).DropTable(
            Path,
            FillSettings(NTable::TDropTableSettings())
        ).GetValueSync()
    );
    return EXIT_SUCCESS;
}

void TCommandQueryBase::CheckQueryOptions() const {
    if (!Query && !QueryFile) {
        throw TMisuseException() << "Neither \"Text of query\" (\"--query\", \"-q\") "
            << "nor \"Path to file with query text\" (\"--file\", \"-f\") were provided.";
    }
    if (Query && QueryFile) {
        throw TMisuseException() << "Both mutually exclusive options \"Text of query\" (\"--query\", \"-q\") "
            << "and \"Path to file with query text\" (\"--file\", \"-f\") were provided.";
    }
}

void TCommandQueryBase::CheckQueryFile() {
    if (QueryFile) {
        Query = ReadFromFile(QueryFile, "query");
    }
}

TCommandExecuteQuery::TCommandExecuteQuery()
    : TTableCommand("execute", {"exec"}, "Execute query")
{}

void TCommandExecuteQuery::Config(TConfig& config) {
    TTableCommand::Config(config);
    AddExamplesOption(config);

    config.Opts->AddLongOption('t', "type", "Query type [data, scheme, scan, generic]")
        .RequiredArgument("[String]").DefaultValue("data").StoreResult(&QueryType);
    config.Opts->AddLongOption("stats", "Collect statistics mode (for data & scan & generic queries) [none, basic, full]")
        .RequiredArgument("[String]").StoreResult(&CollectStatsMode);
    config.Opts->AddLongOption("flame-graph", "Builds resource usage flame graph, based on statistics info")
            .RequiredArgument("PATH").StoreResult(&FlameGraphPath);
    config.Opts->AddCharOption('s', "Collect statistics in basic mode").StoreTrue(&BasicStats);
    config.Opts->AddLongOption("tx-mode", "Transaction mode (for generic & data queries) [serializable-rw, online-ro, stale-ro, notx (generic queries only)]")
        .RequiredArgument("[String]").DefaultValue("serializable-rw").StoreResult(&TxMode);
    config.Opts->AddLongOption('q', "query", "Text of query to execute").RequiredArgument("[String]").StoreResult(&Query);
    config.Opts->AddLongOption('f', "file", "Path to file with query text to execute")
        .RequiredArgument("PATH").StoreResult(&QueryFile);

    AddFormats(config, {
        EOutputFormat::Pretty,
        EOutputFormat::JsonUnicode,
        EOutputFormat::JsonUnicodeArray,
        EOutputFormat::JsonBase64,
        EOutputFormat::JsonBase64Array,
        EOutputFormat::Csv,
        EOutputFormat::Tsv,
        EOutputFormat::Parquet,
    });

    AddParametersOption(config, "(for data & scan queries)");

    AddInputFormats(config, {
        EOutputFormat::JsonUnicode,
        EOutputFormat::JsonBase64
    });

    AddStdinFormats(config, {
        EOutputFormat::JsonUnicode,
        EOutputFormat::JsonBase64,
        EOutputFormat::Raw,
        EOutputFormat::Csv,
        EOutputFormat::Tsv,
    }, {
        EOutputFormat::NoFraming,
        EOutputFormat::NewlineDelimited
    });

    AddParametersStdinOption(config, "query");

    CheckExamples(config);

    config.SetFreeArgsNum(0);
}

void TCommandExecuteQuery::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParseFormats();
    if (BasicStats && CollectStatsMode) {
        throw TMisuseException() << "Both mutually exclusive options \"--stats\" and \"-s\" are provided.";
    }
    if ((!ParameterOptions.empty() || !ParameterFiles.empty() || !StdinParameters.empty() || IsStdinFormatSet || IsFramingFormatSet ||
            config.ParseResult->Has("batch")) && QueryType == "scheme") {
        throw TMisuseException() << "Scheme queries does not support parameters.";
    }
    CheckQueryOptions();
    CheckQueryFile();
    ParseParameters(config);
}

int TCommandExecuteQuery::Run(TConfig& config) {
    if (QueryType) {
        if (QueryType == "data") {
            return ExecuteDataQuery(config);
        }
        if (QueryType == "generic") {
            return ExecuteGenericQuery(config);
        }
        if (QueryType == "scheme") {
            return ExecuteSchemeQuery(config);
        }
        if (QueryType == "scan") {
            return ExecuteScanQuery(config);
        }
    }
    throw TMisuseException() << "Unknown query type.";
}

int TCommandExecuteQuery::ExecuteDataQuery(TConfig& config) {
    auto defaultStatsMode = BasicStats ? NTable::ECollectQueryStatsMode::Basic : NTable::ECollectQueryStatsMode::None;
    NTable::TExecDataQuerySettings settings;
    settings.KeepInQueryCache(true);
    settings.CollectQueryStats(ParseQueryStatsModeOrThrow(CollectStatsMode, defaultStatsMode));

    NTable::TTxSettings txSettings;
    if (TxMode) {
        if (TxMode == "serializable-rw") {
            txSettings = NTable::TTxSettings::SerializableRW();
        } else if (TxMode == "online-ro")  {
            txSettings = NTable::TTxSettings::OnlineRO();
        } else if (TxMode == "stale-ro") {
            txSettings = NTable::TTxSettings::StaleRO();
        } else {
            throw TMisuseException() << "Unknown transaction mode.";
        }
    }

    NTable::TTableClient client(CreateDriver(config));
    NTable::TAsyncDataQueryResult asyncResult;

    if (!Parameters.empty() || !IsStdinInteractive()) {
        ValidateResult = MakeHolder<NScripting::TExplainYqlResult>(
            ExplainQuery(config, Query, NScripting::ExplainYqlRequestMode::Validate));
        THolder<TParamsBuilder> paramBuilder;
        while (GetNextParams(paramBuilder)) {
            TParams params = paramBuilder->Build();
            auto operation = [this, &txSettings, &params, &settings, &asyncResult](NTable::TSession session) {
                auto promise = NThreading::NewPromise<NTable::TDataQueryResult>();
                asyncResult = promise.GetFuture();
                auto result = session.ExecuteDataQuery(
                    Query,
                    NTable::TTxControl::BeginTx(txSettings).CommitTx(),
                    params,
                    FillSettings(settings)
                );
                return result.Apply([promise](const NTable::TAsyncDataQueryResult& result) mutable {
                    promise.SetValue(result.GetValue());
                    return static_cast<TStatus>(result.GetValue());
                });
            };
            auto status = client.RetryOperation(std::move(operation)).GetValueSync();
            ThrowOnError(status);
            auto result = asyncResult.GetValueSync();
            PrintDataQueryResponse(result);
        }
    } else {
        auto operation = [this, &txSettings, &settings, &asyncResult](NTable::TSession session) {
            auto promise = NThreading::NewPromise<NTable::TDataQueryResult>();
            asyncResult = promise.GetFuture();
            auto result = session.ExecuteDataQuery(
                Query,
                NTable::TTxControl::BeginTx(txSettings).CommitTx(),
                FillSettings(settings)
            );
            return result.Apply([promise](const NTable::TAsyncDataQueryResult& result) mutable {
                promise.SetValue(result.GetValue());
                return static_cast<TStatus>(result.GetValue());
            });
        };
        auto status = client.RetryOperation(std::move(operation)).GetValueSync();
        ThrowOnError(status);
        auto result = asyncResult.GetValueSync();
        PrintDataQueryResponse(result);
    }
    return EXIT_SUCCESS;
}

void TCommandExecuteQuery::PrintDataQueryResponse(NTable::TDataQueryResult& result) {
    {
        TResultSetPrinter printer(OutputFormat);
        const TVector<TResultSet>& resultSets = result.GetResultSets();
        for (auto resultSetIt = resultSets.begin(); resultSetIt != resultSets.end(); ++resultSetIt) {
            if (resultSetIt != resultSets.begin()) {
                printer.Reset();
            }
            printer.Print(*resultSetIt);
        }
    } // TResultSetPrinter destructor should be called before printing stats

    const TMaybe<NTable::TQueryStats>& stats = result.GetStats();
    if (stats.Defined()) {
        Cout << Endl << "Statistics:" << Endl << stats->ToString();
        PrintFlameGraph(stats->GetPlan());
    }
    if (FlameGraphPath && !stats.Defined())
    {
        Cout << Endl << "Flame graph is available for full or profile stats only" << Endl;
    }
}

int TCommandExecuteQuery::ExecuteSchemeQuery(TConfig& config) {
    ThrowOnError(
        GetSession(config).ExecuteSchemeQuery(
            Query,
            FillSettings(NTable::TExecSchemeQuerySettings())
        ).GetValueSync()
    );
    return EXIT_SUCCESS;
}

namespace {
    template <typename TClient>
    using TPartIterator = std::conditional_t<
        std::is_same_v<TClient, NTable::TTableClient>,
        NTable::TScanQueryPartIterator,
        NQuery::TExecuteQueryIterator>;

    template <typename TClient>
    using TAsyncPartIterator = std::conditional_t<
        std::is_same_v<TClient, NTable::TTableClient>,
        NTable::TAsyncScanQueryPartIterator,
        NQuery::TAsyncExecuteQueryIterator>;

    template <typename TClient>
    using TSettings = std::conditional_t<
        std::is_same_v<TClient, NTable::TTableClient>,
        NTable::TStreamExecScanQuerySettings,
        NQuery::TExecuteQuerySettings>;

    template <typename TClient>
    auto GetSettings(const TString& collectStatsMode, const bool basicStats, std::optional<TDuration> timeout) {
        if constexpr (std::is_same_v<TClient, NTable::TTableClient>) {
            const auto defaultStatsMode = basicStats
                ? NTable::ECollectQueryStatsMode::Basic
                : NTable::ECollectQueryStatsMode::None;
            NTable::TStreamExecScanQuerySettings settings;
            settings.CollectQueryStats(ParseQueryStatsModeOrThrow(collectStatsMode, defaultStatsMode));
            if (timeout.has_value()) {
                settings.ClientTimeout(*timeout);
            }
            return settings;
        } else if constexpr (std::is_same_v<TClient, NQuery::TQueryClient>) {
            const auto defaultStatsMode = basicStats
                ? NQuery::EStatsMode::Basic
                : NQuery::EStatsMode::None;
            NQuery::TExecuteQuerySettings settings;
            settings.StatsMode(ParseQueryStatsModeOrThrow(collectStatsMode, defaultStatsMode));
            if (timeout.has_value()) {
                settings.ClientTimeout(*timeout);
            }
            return settings;
        }
        Y_UNREACHABLE();
    }

    template <typename TClient>
    auto StreamExecuteQuery(
        TClient client,
        const TString& query,
        const TSettings<TClient>& settings,
        const TString& TxMode = "",
        const std::optional<TParams>& params = std::nullopt
    ) {
        NQuery::TTxSettings txSettings;
        if (TxMode) {
            if (TxMode == "serializable-rw") {
                txSettings = NQuery::TTxSettings::SerializableRW();
            } else if (TxMode == "online-ro")  {
                txSettings = NQuery::TTxSettings::OnlineRO();
            } else if (TxMode == "stale-ro") {
                txSettings = NQuery::TTxSettings::StaleRO();
            } else if (TxMode != "notx") {
                throw TMisuseException() << "Unknown transaction mode.";
            }
        }

        if constexpr (std::is_same_v<TClient, NTable::TTableClient>) {
            if (params) {
                return client.StreamExecuteScanQuery(
                    query,
                    *params,
                    settings
                );
            } else {
                return client.StreamExecuteScanQuery(
                    query,
                    settings
                );
            }
        } else if constexpr (std::is_same_v<TClient, NQuery::TQueryClient>) {
            if (params) {
                return client.StreamExecuteQuery(
                    query,
                    (TxMode == "notx" ? NQuery::TTxControl::NoTx() : NQuery::TTxControl::BeginTx(txSettings).CommitTx()),
                    *params,
                    settings
                );
            } else {
                return client.StreamExecuteQuery(
                    query,
                    (TxMode == "notx" ? NQuery::TTxControl::NoTx() : NQuery::TTxControl::BeginTx(txSettings).CommitTx()),
                    settings
                );
            }
        }
        Y_UNREACHABLE();
    }

    template <typename TQueryPart>
    bool HasStats(const TQueryPart& part) {
        if constexpr (std::is_same_v<TQueryPart, NTable::TScanQueryPart>) {
            return part.HasQueryStats();
        } else if constexpr (std::is_same_v<TQueryPart, NQuery::TExecuteQueryPart>) {
            return !part.GetStats().Empty();
        }
        Y_UNREACHABLE();
    }
    
    template <typename TQueryPart>
    const NQuery::TExecStats& GetStats(const TQueryPart& part) {
        if constexpr (std::is_same_v<TQueryPart, NTable::TScanQueryPart>) {
            return part.GetQueryStats();
        } else if constexpr (std::is_same_v<TQueryPart, NQuery::TExecuteQueryPart>) {
            return *part.GetStats();
        }
        Y_UNREACHABLE();
    }

    template <typename TClient, typename TOperationFunc>
    TAsyncStatus RunOperation(TClient client, TOperationFunc&& operation) {
        if constexpr (std::is_same_v<TClient, NTable::TTableClient>) {
            return client.RetryOperation(std::move(operation));
        } else if constexpr (std::is_same_v<TClient, NQuery::TQueryClient>) {
            // I guess streaming generic queries aren't supposed to be retried?
            return operation(client);
        }
        Y_UNREACHABLE();
    }
}

int TCommandExecuteQuery::ExecuteGenericQuery(TConfig& config) {
    return ExecuteQueryImpl<NQuery::TQueryClient>(config);
}

int TCommandExecuteQuery::ExecuteScanQuery(TConfig& config) {
    return ExecuteQueryImpl<NTable::TTableClient>(config);
}

template <typename TClient>
int TCommandExecuteQuery::ExecuteQueryImpl(TConfig& config) {
    TClient client(CreateDriver(config));
    std::optional<TDuration> optTimeout;
    if (OperationTimeout) {
        optTimeout = TDuration::MilliSeconds(FromString<ui64>(OperationTimeout));
    }
    const auto settings = GetSettings<TClient>(CollectStatsMode, BasicStats, optTimeout);

    TAsyncPartIterator<TClient> asyncResult;
    SetInterruptHandlers();
    if (!Parameters.empty() || !IsStdinInteractive()) {
        ValidateResult = MakeHolder<NScripting::TExplainYqlResult>(
            ExplainQuery(config, Query, NScripting::ExplainYqlRequestMode::Validate));
        THolder<TParamsBuilder> paramBuilder;
        while (GetNextParams(paramBuilder)) {
            auto operation = [this, &paramBuilder, &settings, &asyncResult](TClient client) {
                auto promise = NThreading::NewPromise<TPartIterator<TClient>>();
                asyncResult = promise.GetFuture();
                auto result = StreamExecuteQuery(
                    client,
                    Query,
                    settings,
                    TxMode,
                    paramBuilder->Build()
                );
                return result.Apply([promise](const auto& result) mutable {
                    promise.SetValue(result.GetValue());
                    return static_cast<TStatus>(result.GetValue());
                });
            };
            auto status = RunOperation(client, operation).GetValueSync();
            ThrowOnError(status);
            auto result = asyncResult.GetValueSync();
            if (!PrintQueryResponse(result)) {
                return EXIT_FAILURE;
            }
        }
    } else {
        auto operation = [this, &settings, &asyncResult](TClient client) {
            auto promise = NThreading::NewPromise<TPartIterator<TClient>>();
            asyncResult = promise.GetFuture();
            auto result = StreamExecuteQuery(
                client,
                Query,
                settings,
                TxMode
            );
            return result.Apply([promise](const auto& result) mutable {
                promise.SetValue(result.GetValue());
                return static_cast<TStatus>(result.GetValue());
            });
        };
        auto status = RunOperation(client, operation).GetValueSync();
        ThrowOnError(status);
        auto result = asyncResult.GetValueSync();
        if (!PrintQueryResponse(result)) {
            return EXIT_FAILURE;
        }
    }
    return EXIT_SUCCESS;
}

template <typename TIterator>
bool TCommandExecuteQuery::PrintQueryResponse(TIterator& result) {
    TMaybe<TString> stats;
    TMaybe<TString> fullStats;
    {
        TResultSetPrinter printer(OutputFormat, &IsInterrupted);

        while (!IsInterrupted()) {
            auto streamPart = result.ReadNext().GetValueSync();
            if (!streamPart.IsSuccess()) {
                if (streamPart.EOS()) {
                    break;
                }
                ThrowOnError(streamPart);
            }

            if (streamPart.HasResultSet()) {
                printer.Print(streamPart.GetResultSet());
            }

            if (HasStats(streamPart)) {
                const auto& queryStats = GetStats(streamPart);
                stats = queryStats.ToString();

                if (queryStats.GetPlan()) {
                    fullStats = queryStats.GetPlan();
                }
            }
        }
    } // TResultSetPrinter destructor should be called before printing stats

    if (stats) {
        Cout << Endl << "Statistics:" << Endl << *stats;
    }

    if (fullStats) {
        Cout << Endl << "Full statistics:" << Endl;

        TQueryPlanPrinter queryPlanPrinter(OutputFormat, /* analyzeMode */ true);
        queryPlanPrinter.Print(*fullStats);
    }

    PrintFlameGraph(fullStats);

    if (IsInterrupted()) {
        Cerr << "<INTERRUPTED>" << Endl;
        return false;
    }
    return true;
}

void TCommandExecuteQuery::PrintFlameGraph(const TMaybe<TString>& plan)
{
    if (!FlameGraphPath) {
        return;
    }
    if (FlameGraphPath->Empty()) {
        Cout << Endl << "FlameGraph path can not be empty." << Endl;
        return;
    }
    if (!plan) {
        Cout << Endl << "Flame graph is available for full or profile stats only" << Endl;
        return;
    }
    try {
        NKikimr::NVisual::GenerateFlameGraphSvg(FlameGraphPath.GetRef(), *plan);
        Cout << Endl << "Resource usage flame graph is successfully saved to " << FlameGraphPath << Endl;
    }
    catch (const yexception &ex) {
        Cout << Endl << "Can't save resource usage flame graph, error: " << ex.what() << Endl;
    }
}


TCommandExplain::TCommandExplain()
    : TTableCommand("explain", {}, "Explain query")
{}

TCommandExplain::TCommandExplain(TString query, TString queryType, bool printAst)
    : TTableCommand("explain", {}, "Explain query")
{
    Query = std::move(query);
    QueryType = std::move(queryType);
    PrintAst = printAst;
}

void TCommandExplain::Config(TConfig& config) {
    TTableCommand::Config(config);

    config.Opts->AddLongOption('q', "query", "Text of query to explain").RequiredArgument("[String]").StoreResult(&Query);
    config.Opts->AddLongOption('f', "file", "Path to file with query text to explain")
        .RequiredArgument("PATH").StoreResult(&QueryFile);
    config.Opts->AddLongOption("ast", "Print query AST")
        .StoreTrue(&PrintAst);

    config.Opts->AddLongOption('t', "type", "Query type [data, scan, generic]")
        .RequiredArgument("[String]").DefaultValue("data").StoreResult(&QueryType);
    config.Opts->AddLongOption("analyze", "Run query and collect execution statistics")
        .NoArgument().SetFlag(&Analyze);
    config.Opts->AddLongOption("flame-graph", "Builds resource usage flame graph, based on analyze info")
            .RequiredArgument("PATH").StoreResult(&FlameGraphPath);
    config.Opts->AddLongOption("collect-diagnostics", "Collects diagnostics and saves it to file")
        .StoreTrue(&CollectFullDiagnostics);

    AddFormats(config, {
            EOutputFormat::Pretty,
            EOutputFormat::PrettyTable,
            EOutputFormat::JsonUnicode,
            EOutputFormat::JsonBase64,
            EOutputFormat::JsonBase64Simplify
    });

    config.SetFreeArgsNum(0);
}

void TCommandExplain::SaveDiagnosticsToFile(const TString& diagnostics) {
    TFileOutput file(TStringBuilder() << "diagnostics_" << TGUID::Create().AsGuidString() << ".txt");
    file << diagnostics;
}

void TCommandExplain::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParseFormats();
    CheckQueryOptions();
}

int TCommandExplain::Run(TConfig& config) {
    CheckQueryFile();

    TString planJson;
    TString ast;
    std::optional<TDuration> timeout;
    if (OperationTimeout) {
        timeout = TDuration::MilliSeconds(FromString<ui64>(OperationTimeout));
    }

    if (QueryType == "scan") {
        NTable::TTableClient client(CreateDriver(config));
        NTable::TStreamExecScanQuerySettings settings;
        settings.ClientTimeout(timeout.value_or(TDuration()));

        if (Analyze) {
            settings.CollectQueryStats(NTable::ECollectQueryStatsMode::Full);
        } else {
            settings.Explain(true);
        }

        if (CollectFullDiagnostics) {
            settings.CollectFullDiagnostics(true);
        }

        auto result = client.StreamExecuteScanQuery(Query, settings).GetValueSync();
        ThrowOnError(result);

        TString diagnostics;

        SetInterruptHandlers();
        while (!IsInterrupted()) {
            auto tablePart = result.ReadNext().GetValueSync();
            if (!tablePart.IsSuccess()) {
                if (tablePart.EOS()) {
                    break;
                }
                ThrowOnError(tablePart);
            }
            if (tablePart.HasQueryStats()) {
                auto proto = NYdb::TProtoAccessor::GetProto(tablePart.GetQueryStats());
                planJson = proto.query_plan();
                ast = proto.query_ast();
            }
            if (tablePart.HasDiagnostics()) {
                diagnostics = tablePart.ExtractDiagnostics();
            }
        }

        if (CollectFullDiagnostics) {
            SaveDiagnosticsToFile(diagnostics);
        }

        if (IsInterrupted()) {
            Cerr << "<INTERRUPTED>" << Endl;
        }
    } else if (QueryType == "generic") {
        NQuery::TQueryClient client(CreateDriver(config));
        NQuery::TExecuteQuerySettings settings;
        settings.ClientTimeout(timeout.value_or(TDuration()));

        if (Analyze) {
            settings.StatsMode(NQuery::EStatsMode::Full);
        } else {
            settings.ExecMode(NQuery::EExecMode::Explain);
        }

        auto result = client.StreamExecuteQuery(
            Query,
            NQuery::TTxControl::BeginTx().CommitTx(),
            settings).GetValueSync();
        ThrowOnError(result);

        SetInterruptHandlers();
        while (!IsInterrupted()) {
            auto tablePart = result.ReadNext().GetValueSync();
            if (!tablePart.IsSuccess()) {
                if (tablePart.EOS()) {
                    break;
                }
                ThrowOnError(tablePart);
            }
            if (tablePart.GetStats()) {
                auto proto = NYdb::TProtoAccessor::GetProto(*tablePart.GetStats());
                planJson = proto.query_plan();
                ast = proto.query_ast();
            }
        }

        if (IsInterrupted()) {
            Cerr << "<INTERRUPTED>" << Endl;
        }
    } else if (QueryType == "data" && (Analyze || FlameGraphPath)) {
        NTable::TExecDataQuerySettings settings;
        settings.CollectQueryStats(NTable::ECollectQueryStatsMode::Full);

        auto result = GetSession(config).ExecuteDataQuery(
            Query,
            NTable::TTxControl::BeginTx(NTable::TTxSettings::SerializableRW()).CommitTx(),
            FillSettings(settings)
        ).ExtractValueSync();
        ThrowOnError(result);
        planJson = result.GetQueryPlan();
        if (auto stats = result.GetStats()) {
            auto proto = NYdb::TProtoAccessor::GetProto(*stats);
            ast = proto.query_ast();
        }
    } else if (QueryType == "data" && !Analyze) {
        NTable::TExplainDataQuerySettings settings(FillSettings(NTable::TExplainDataQuerySettings()));
        if (CollectFullDiagnostics) {
            settings.WithCollectFullDiagnostics(true);
        }

        NTable::TExplainQueryResult result = GetSession(config).ExplainDataQuery(
            Query,
            settings
        ).GetValueSync();
        ThrowOnError(result);
        planJson = result.GetPlan();
        ast = result.GetAst();

        if (CollectFullDiagnostics) {
            SaveDiagnosticsToFile(result.GetDiagnostics());
        }

    } else {
        throw TMisuseException() << "Unknown query type for explain.";
    }

    if (PrintAst) {
        Cout << "Query AST:" << Endl << ast << Endl;
    } else {
        Cout << "Query Plan:" << Endl;
        TQueryPlanPrinter queryPlanPrinter(OutputFormat, Analyze);
        queryPlanPrinter.Print(planJson);

        if( FlameGraphPath && !FlameGraphPath->Empty() ) {
            try {
                NKikimr::NVisual::GenerateFlameGraphSvg(FlameGraphPath.GetRef(), planJson);
                Cout << Endl << "Resource usage flame graph is successfully saved to " << FlameGraphPath.GetRef() << Endl;
            }
            catch (const yexception& ex) {
                Cout << Endl << "Can't save resource usage flame graph, error: " << ex.what() << Endl;
            }
        }
        else if( FlameGraphPath && FlameGraphPath->Empty() ) {
            Cout << Endl << "FlameGraph path can not be empty." << Endl;
        }
    }

    return EXIT_SUCCESS;
}

TCommandReadTable::TCommandReadTable()
    : TYdbCommand("read", {"readtable"}, "Stream read table")
{}

void TCommandReadTable::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.Opts->AddLongOption("ordered", "Result should be ordered by primary key")
        .NoArgument().SetFlag(&Ordered);
    config.Opts->AddLongOption("limit", "Limit result rows count")
        .RequiredArgument("NUM").StoreResult(&RowLimit);
    config.Opts->AddLongOption("columns", "Comma separated list of columns to read")
        .RequiredArgument("CSV").StoreResult(&Columns);
    config.Opts->AddLongOption("count-only", "Print only rows count")
        .NoArgument().SetFlag(&CountOnly);
    config.Opts->AddLongOption("from", "Key prefix value to start read from.\n"
            "  Format should be a json-string containing array of elements representing a tuple - key prefix.\n"
            "  Option \"--input-format\" defines how to parse binary strings.\n"
            "  Examples:\n"
            "    1) using one column from PK:\n"
            "      --from [10] --to [100]\n"
            "    2) using two columns from PK (forwarding strings in command line is OS-specific. Example for linux):\n"
            "      --from [10,\\\"OneWord\\\"] --to '[100,\"Two Words\"]'")
        .RequiredArgument("JSON").StoreResult(&From);
    config.Opts->AddLongOption("to", "Key prefix value to read until.\n"
            "  Same format as for \"--from\" option.")
        .RequiredArgument("JSON").StoreResult(&To);
    config.Opts->AddLongOption("from-exclusive", "Don't include the left border element into response")
        .NoArgument().SetFlag(&FromExclusive);
    config.Opts->AddLongOption("to-exclusive", "Don't include the right border element into response")
        .NoArgument().SetFlag(&ToExclusive);

    AddInputFormats(config, {
        EOutputFormat::JsonUnicode,
        EOutputFormat::JsonBase64
    });

    AddFormats(config, {
        EOutputFormat::Pretty,
        EOutputFormat::JsonUnicode,
        EOutputFormat::JsonUnicodeArray,
        EOutputFormat::JsonBase64,
        EOutputFormat::JsonBase64Array,
        EOutputFormat::Csv,
        EOutputFormat::Tsv,
        EOutputFormat::Parquet,
    });

    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<table path>", "Path to a table");
}

void TCommandReadTable::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParseFormats();
    ParsePath(config, 0);
}

namespace {
    TType GetKeyPrefixTypeFromJson(const TString& jsonString, const TString& optionName
            , NTable::TTableDescription& tableDescription) {
        NJson::TJsonValue jsonValue;
        if (!ReadJsonTree(jsonString, &jsonValue)) {
            throw TMisuseException() << "Can't parse string \"" << jsonString << "\" (--" << optionName << " option) as json";
        }
        if (!jsonValue.IsArray()) {
            throw TMisuseException() << "json string in \"--" << optionName
                << "\" should contain array of elements representing tuple with key prefix, but it doesn't";
        }
        TTypeBuilder typebuilder;
        typebuilder.BeginTuple();
        const auto& pkColumnNames = tableDescription.GetPrimaryKeyColumns();
        auto pkColumnNamesIterator = pkColumnNames.begin();
        for (const auto& element : jsonValue.GetArray()) {
            Y_UNUSED(element);
            if (pkColumnNamesIterator == pkColumnNames.end()) {
                throw TMisuseException() << "json string in \"--" << optionName << "\" option contains more elements ("
                    << jsonValue.GetArray().size() << ") then columns in table primary key (" << pkColumnNames.size() << ")";
            }
            for (const auto& column : tableDescription.GetTableColumns()) {
                if (*pkColumnNamesIterator == column.Name) {
                    typebuilder.AddElement(column.Type);
                    break;
                }
            }
            ++pkColumnNamesIterator;
        }
        typebuilder.EndTuple();
        return typebuilder.Build();
    }
}

int TCommandReadTable::Run(TConfig& config) {
    NTable::TTableClient client(CreateDriver(config));

    NTable::TReadTableSettings readTableSettings;
    if (RowLimit) {
        readTableSettings.RowLimit(RowLimit);
    }
    if (Ordered) {
        readTableSettings.Ordered(Ordered);
    }
    if (Columns) {
        readTableSettings.Columns_ = StringSplitter(Columns).Split(',').ToList<TString>();
    }

    if (From || To) {
        NTable::TCreateSessionResult sessionResult = client.GetSession(NTable::TCreateSessionSettings()).GetValueSync();
        ThrowOnError(sessionResult);
        NTable::TDescribeTableResult tableResult = sessionResult.GetSession().DescribeTable(Path).GetValueSync();
        NTable::TTableDescription tableDescription = tableResult.GetTableDescription();

        EBinaryStringEncoding encoding;
        switch (InputFormat) {
        case EOutputFormat::Default:
        case EOutputFormat::JsonUnicode:
            encoding = EBinaryStringEncoding::Unicode;
            break;
        case EOutputFormat::JsonBase64:
            encoding = EBinaryStringEncoding::Base64;
            break;
        default:
            throw TMisuseException() << "Unknown input format: " << InputFormat;
        }

        if (From) {
            TValue fromValue = JsonToYdbValue(From, GetKeyPrefixTypeFromJson(From, "from", tableDescription), encoding);
            readTableSettings.From(FromExclusive
                ? NTable::TKeyBound::Exclusive(fromValue)
                : NTable::TKeyBound::Inclusive(fromValue));
        }

        if (To) {
            TValue toValue = JsonToYdbValue(To, GetKeyPrefixTypeFromJson(To, "to", tableDescription), encoding);
            readTableSettings.To(ToExclusive
                ? NTable::TKeyBound::Exclusive(toValue)
                : NTable::TKeyBound::Inclusive(toValue));
        }
    }

    TMaybe<NTable::TTablePartIterator> tableIterator;

    ThrowOnError(client.RetryOperationSync([this, &readTableSettings, &tableIterator](NTable::TSession session) {
        NTable::TTablePartIterator result = session.ReadTable(Path, readTableSettings).GetValueSync();

        if (result.IsSuccess()) {
            tableIterator = result;
        }

        return result;
    }));

    PrintResponse(tableIterator.GetRef());
    return EXIT_SUCCESS;
}

void TCommandReadTable::PrintResponse(NTable::TTablePartIterator& result) {
    size_t totalRows = 0;
    SetInterruptHandlers();
    TResultSetPrinter printer(OutputFormat, &IsInterrupted);

    while (!IsInterrupted()) {
        auto tablePart = result.ReadNext().GetValueSync();
        if (!tablePart.IsSuccess()) {
            if (tablePart.EOS()) {
                break;
            }
            ThrowOnError(tablePart);
        }
        if (CountOnly) {
            TResultSetParser parser(tablePart.ExtractPart());
            while (parser.TryNextRow()) {
                ++totalRows;
            }
            continue;
        }
        printer.Print(tablePart.GetPart());
    }
    if (CountOnly) {
        Cout << totalRows << Endl;
    }

    if (IsInterrupted()) {
        Cerr << "<INTERRUPTED>" << Endl;
    }
}

TCommandIndexAddGlobal::TCommandIndexAddGlobal(
        NTable::EIndexType type,
        const TString& name,
        const std::initializer_list<TString>& aliases,
        const TString& description)
    : TYdbCommand(name, aliases, description)
    , IndexType(type)
{}

void TCommandIndexAddGlobal::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.Opts->AddLongOption("index-name", "Name of index to add.")
        .RequiredArgument("NAME").StoreResult(&IndexName);
    config.Opts->AddLongOption("columns", "Ordered comma separated list of columns to build index for")
        .RequiredArgument("CSV").StoreResult(&Columns);
    config.Opts->AddLongOption("cover", "Ordered comma separated list of cover columns. (Data for those columns will be duplicated to index)")
        .RequiredArgument("CSV").StoreResult(&DataColumns);

    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<table path>", "Path to a table");
}

void TCommandIndexAddGlobal::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParseFormats();
    ParsePath(config, 0);
}

int TCommandIndexAddGlobal::Run(TConfig& config) {
    NTable::TTableClient client(CreateDriver(config));
    auto columns = StringSplitter(Columns).Split(',').ToList<TString>();
    TVector<TString> dataColumns;
    if (DataColumns) {
        dataColumns = StringSplitter(DataColumns).Split(',').ToList<TString>();
    }

    auto settings = NTable::TAlterTableSettings()
        .AppendAddIndexes({NTable::TIndexDescription(IndexName, IndexType, columns, dataColumns)});
    auto session = client.GetSession().GetValueSync();
    ThrowOnError(session);
    auto opResult = session.GetSession().AlterTableLong(Path, settings).GetValueSync();
    ThrowOnError(opResult);
    PrintOperation(opResult, OutputFormat);

    return EXIT_SUCCESS;
}

TCommandIndexAddGlobalSync::TCommandIndexAddGlobalSync()
    : TCommandIndexAddGlobal(NTable::EIndexType::GlobalSync, "global-sync", {"global"}, "Add global sync index. The command returns operation")
{}

TCommandIndexAddGlobalAsync::TCommandIndexAddGlobalAsync()
    : TCommandIndexAddGlobal(NTable::EIndexType::GlobalAsync, "global-async", {}, "Add global async index. The command returns operation")
{}

TCommandIndexDrop::TCommandIndexDrop()
    : TYdbCommand("drop", {}, "Drop index from the specified table")
{}

void TCommandIndexDrop::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.Opts->AddLongOption("index-name", "Name of index to drop.")
        .RequiredArgument("NAME").StoreResult(&IndexName);

    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<table path>", "Path to a table");
}

void TCommandIndexDrop::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParsePath(config, 0);
}

int TCommandIndexDrop::Run(TConfig& config) {
    NTable::TTableClient client(CreateDriver(config));

    auto settings = NTable::TAlterTableSettings()
        .AppendDropIndexes({IndexName});
    auto session = client.GetSession().GetValueSync();
    ThrowOnError(session);
    auto result = session.GetSession().AlterTable(Path, settings).GetValueSync();
    ThrowOnError(result);

    return EXIT_SUCCESS;
}

TCommandIndexRename::TCommandIndexRename()
    : TYdbCommand("rename", {}, "Rename index for specified table")
{}

void TCommandIndexRename::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.Opts->AddLongOption("index-name", "Name of index to rename.").Required()
        .RequiredArgument("NAME").StoreResult(&IndexName);

    config.Opts->AddLongOption("to", "New index name").Required()
        .RequiredArgument("NAME").StoreResult(&NewIndexName);

    config.Opts->AddLongOption("replace", "Allow to replace existing index. In case if there already exists an index with the same name that current index is renamed to, the existing one will be deleted.")
        .StoreTrue(&Replace);

    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<table path>", "Path to a table");
}

void TCommandIndexRename::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParsePath(config, 0);
}

int TCommandIndexRename::Run(TConfig& config) {
    NTable::TTableClient client(CreateDriver(config));

    auto settings = NTable::TAlterTableSettings()
        .AppendRenameIndexes({IndexName, NewIndexName, Replace});
    auto session = client.GetSession().GetValueSync();
    ThrowOnError(session);
    auto result = session.GetSession().AlterTable(Path, settings).GetValueSync();
    ThrowOnError(result);

    return EXIT_SUCCESS;
}


TCommandAttributeAdd::TCommandAttributeAdd()
    : TYdbCommand("add", {}, "Add attributes to the specified table")
{}

void TCommandAttributeAdd::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.Opts->AddLongOption("attribute", "[At least one] key=value pair(s) to add.")
        .RequiredArgument("KEY=VALUE").KVHandler([&](TString key, TString value) {
            Attributes[key] = value;
        });

    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<table path>", "Path to a table");
}

void TCommandAttributeAdd::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParsePath(config, 0);
}

int TCommandAttributeAdd::Run(TConfig& config) {
    NTable::TTableClient client(CreateDriver(config));

    auto settings = NTable::TAlterTableSettings()
        .AlterAttributes(Attributes);

    auto session = client.GetSession().GetValueSync();
    ThrowOnError(session);
    auto result = session.GetSession().AlterTable(Path, settings).GetValueSync();
    ThrowOnError(result);

    return EXIT_SUCCESS;
}

TCommandAttributeDrop::TCommandAttributeDrop()
    : TYdbCommand("drop", {}, "Drop attributes from the specified table")
{}

void TCommandAttributeDrop::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.Opts->AddLongOption("attributes", "Attribute keys to drop.")
        .RequiredArgument("KEY,[KEY...]").SplitHandler(&AttributeKeys, ',');

    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<table path>", "Path to a table");
}

void TCommandAttributeDrop::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParsePath(config, 0);
}

int TCommandAttributeDrop::Run(TConfig& config) {
    NTable::TTableClient client(CreateDriver(config));

    auto settings = NTable::TAlterTableSettings();
    auto alterAttrs = settings.BeginAlterAttributes();
    for (const auto& key : AttributeKeys) {
        alterAttrs.Drop(key);
    }
    alterAttrs.EndAlterAttributes();

    auto session = client.GetSession().GetValueSync();
    ThrowOnError(session);
    auto result = session.GetSession().AlterTable(Path, settings).GetValueSync();
    ThrowOnError(result);

    return EXIT_SUCCESS;
}

TCommandTtlSet::TCommandTtlSet()
    : TYdbCommand("set", {}, "Set ttl settings for the specified table")
{}

void TCommandTtlSet::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.Opts->AddLongOption("column", "Name of date- or integral-type column to be used to calculate expiration threshold.")
        .RequiredArgument("NAME").StoreResult(&ColumnName);
    config.Opts->AddLongOption("expire-after", "Additional time that must pass since expiration threshold.")
        .RequiredArgument("SECONDS").DefaultValue(0).Handler1T<TDuration::TValue>(0, [this](const TDuration::TValue& arg) {
            ExpireAfter = TDuration::Seconds(arg);
        });

    const TString allowedUnits = "seconds (s, sec), milliseconds (ms, msec), microseconds (us, usec), nanoseconds (ns, nsec)";
    auto unitHelp = TStringBuilder()
        << "Interpretation of the value stored in integral-type column." << Endl
        << "Allowed units: " << allowedUnits;
    config.Opts->AddLongOption("unit", unitHelp)
        .RequiredArgument("STRING").Handler1T<TString>("", [this, allowedUnits](const TString& arg) {
            if (!arg) {
                return;
            }

            const auto value = NTable::TValueSinceUnixEpochModeSettings::UnitFromString(arg);
            if (value == NTable::TTtlSettings::EUnit::Unknown) {
                throw TMisuseException() << "Unknown unit: " << arg << Endl << "Allowed units: " << allowedUnits;
            }

            ColumnUnit = value;
        });

    config.Opts->AddLongOption("run-interval", "[Advanced] How often to run cleanup operation on the same partition.")
        .RequiredArgument("SECONDS").Handler1T<TDuration::TValue>([this](const TDuration::TValue& arg) {
            RunInterval = TDuration::Seconds(arg);
        });

    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<table path>", "Path to a table");
}

void TCommandTtlSet::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParsePath(config, 0);
}

int TCommandTtlSet::Run(TConfig& config) {
    NTable::TTableClient client(CreateDriver(config));

    TMaybe<NTable::TTtlSettings> ttl;
    if (ColumnUnit) {
        ttl = NTable::TTtlSettings(ColumnName, *ColumnUnit, ExpireAfter);
    } else {
        ttl = NTable::TTtlSettings(ColumnName, ExpireAfter);
    }
    if (RunInterval) {
        ttl->SetRunInterval(RunInterval);
    }

    auto settings = NTable::TAlterTableSettings()
        .BeginAlterTtlSettings()
            .Set(std::move(ttl.GetRef()))
        .EndAlterTtlSettings();

    auto session = client.GetSession().GetValueSync();
    ThrowOnError(session);
    auto result = session.GetSession().AlterTable(Path, settings).GetValueSync();
    ThrowOnError(result);

    return EXIT_SUCCESS;
}

TCommandTtlReset::TCommandTtlReset()
    : TYdbCommand("reset", {"drop"}, "Reset ttl settings of the specified table")
{}

void TCommandTtlReset::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<table path>", "Path to a table");
}

void TCommandTtlReset::Parse(TConfig& config) {
    TClientCommand::Parse(config);
    ParsePath(config, 0);
}

int TCommandTtlReset::Run(TConfig& config) {
    NTable::TTableClient client(CreateDriver(config));

    auto settings = NTable::TAlterTableSettings()
        .BeginAlterTtlSettings()
            .Drop()
        .EndAlterTtlSettings();

    auto session = client.GetSession().GetValueSync();
    ThrowOnError(session);
    auto result = session.GetSession().AlterTable(Path, settings).GetValueSync();
    ThrowOnError(result);

    return EXIT_SUCCESS;
}

}
}
