#include "table_commands.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/skynet.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/formats/config.h>
#include <yt/yt/client/formats/parser.h>

#include <yt/yt/client/table_client/adapters.h>
#include <yt/yt/client/table_client/blob_reader.h>
#include <yt/yt/client/table_client/columnar_statistics.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/table_consumer.h>
#include <yt/yt/client/table_client/table_output.h>
#include <yt/yt/client/table_client/unversioned_writer.h>
#include <yt/yt/client/table_client/versioned_writer.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NDriver {

using namespace NApi;
using namespace NChaosClient;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NFormats;
using namespace NHiveClient;
using namespace NQueryClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

static NLogging::TLogger WithCommandTag(
    const NLogging::TLogger& logger,
    const ICommandContextPtr& context)
{
    return logger.WithTag("Command: %v",
        context->Request().CommandName);
}

////////////////////////////////////////////////////////////////////////////////

void TReadTableCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
    registrar.Parameter("table_reader", &TThis::TableReader)
        .Default();
    registrar.Parameter("control_attributes", &TThis::ControlAttributes)
        .DefaultNew();
    registrar.Parameter("unordered", &TThis::Unordered)
        .Default(false);
    registrar.Parameter("start_row_index_only", &TThis::StartRowIndexOnly)
        .Default(false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "omit_inaccessible_columns",
        [] (TThis* command) -> auto& {
            return command->Options.OmitInaccessibleColumns;
        })
        .Default(false);
}

void TReadTableCommand::DoExecute(ICommandContextPtr context)
{
    YT_LOG_DEBUG("Executing \"read_table\" command (Path: %v, Unordered: %v, StartRowIndexOnly: %v, "
        "OmitInaccessibleColumns: %v)",
        Path,
        Unordered,
        StartRowIndexOnly,
        Options.OmitInaccessibleColumns);
    Options.Ping = true;
    Options.EnableTableIndex = ControlAttributes->EnableTableIndex;
    Options.EnableRowIndex = ControlAttributes->EnableRowIndex;
    Options.EnableRangeIndex = ControlAttributes->EnableRangeIndex;
    Options.EnableTabletIndex = ControlAttributes->EnableTabletIndex;
    Options.Config = UpdateYsonStruct(
        context->GetConfig()->TableReader,
        TableReader);

    if (StartRowIndexOnly) {
        Options.Config->WindowSize = 1;
        Options.Config->GroupSize = 1;
    }

    PutMethodInfoInTraceContext("read_table");

    auto reader = WaitFor(context->GetClient()->CreateTableReader(
        Path,
        Options))
        .ValueOrThrow();

    ProduceResponseParameters(context, [&] (IYsonConsumer* consumer) {
        BuildYsonMapFragmentFluently(consumer)
            .Item("approximate_row_count").Value(reader->GetTotalRowCount())
            .Item("omitted_inaccessible_columns").Value(reader->GetOmittedInaccessibleColumns())
            .DoIf(reader->GetTotalRowCount() > 0, [&] (auto fluent) {
                fluent
                    .Item("start_row_index").Value(reader->GetStartRowIndex());
            });
    });

    if (StartRowIndexOnly) {
        return;
    }

    auto format = context->GetOutputFormat();
    auto writer = CreateStaticTableWriterForFormat(
        format,
        reader->GetNameTable(),
        {reader->GetTableSchema()},
        context->Request().OutputStream,
        false,
        ControlAttributes,
        0);

    auto finally = Finally([&] {
        auto dataStatistics = reader->GetDataStatistics();
        YT_LOG_DEBUG("Command statistics (RowCount: %v, WrittenSize: %v, "
            "ReadUncompressedDataSize: %v, ReadCompressedDataSize: %v, "
            "OmittedInaccessibleColumns: %v)",
            dataStatistics.row_count(),
            writer->GetWrittenSize(),
            dataStatistics.uncompressed_data_size(),
            dataStatistics.compressed_data_size(),
            reader->GetOmittedInaccessibleColumns());
    });

    TRowBatchReadOptions options{
        .MaxRowsPerRead = context->GetConfig()->ReadBufferRowCount,
        .Columnar = (format.GetType() == EFormatType::Arrow)
    };

    PipeReaderToWriterByBatches(
        reader,
        writer,
        options);
}

bool TReadTableCommand::HasResponseParameters() const
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TReadBlobTableCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
    registrar.Parameter("part_size", &TThis::PartSize);
    registrar.Parameter("table_reader", &TThis::TableReader)
        .Default();
    registrar.Parameter("part_index_column_name", &TThis::PartIndexColumnName)
        .Default();
    registrar.Parameter("data_column_name", &TThis::DataColumnName)
        .Default();
    registrar.Parameter("start_part_index", &TThis::StartPartIndex)
        .Default(0);
    registrar.Parameter("offset", &TThis::Offset)
        .Default(0);
}

void TReadBlobTableCommand::DoExecute(ICommandContextPtr context)
{
    if (Offset < 0) {
        THROW_ERROR_EXCEPTION("Offset must be nonnegative");
    }

    if (PartSize <= 0) {
        THROW_ERROR_EXCEPTION("Part size must be positive");
    }
    Options.Ping = true;

    auto config = UpdateYsonStruct(
        context->GetConfig()->TableReader,
        TableReader);

    Options.Config = config;

    auto reader = WaitFor(context->GetClient()->CreateTableReader(
        Path,
        Options))
        .ValueOrThrow();

    auto input = CreateBlobTableReader(
        std::move(reader),
        PartIndexColumnName,
        DataColumnName,
        StartPartIndex,
        Offset,
        PartSize);

    auto output = context->Request().OutputStream;

    // TODO(ignat): implement proper Pipe* function.
    while (true) {
        auto block = WaitFor(input->Read())
            .ValueOrThrow();

        if (!block)
            break;

        WaitFor(output->Write(block))
            .ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TLocateSkynetShareCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
}

void TLocateSkynetShareCommand::DoExecute(ICommandContextPtr context)
{
    Options.Config = context->GetConfig()->TableReader;

    auto asyncSkynetPartsLocations = context->GetClient()->LocateSkynetShare(
        Path,
        Options);

    auto skynetPartsLocations = WaitFor(asyncSkynetPartsLocations);

    auto format = context->GetOutputFormat();
    auto syncOutputStream = CreateBufferedSyncAdapter(context->Request().OutputStream);

    auto consumer = CreateConsumerForFormat(
        format,
        EDataType::Structured,
        syncOutputStream.get());

    Serialize(*skynetPartsLocations.ValueOrThrow(), consumer.get());
    consumer->Flush();
}

////////////////////////////////////////////////////////////////////////////////

void TWriteTableCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
    registrar.Parameter("table_writer", &TThis::TableWriter)
        .Default();
    registrar.Parameter("max_row_buffer_size", &TThis::MaxRowBufferSize)
        .Default(1_MB);
}

void TWriteTableCommand::DoExecute(ICommandContextPtr context)
{
    auto transaction = AttachTransaction(context, false);

    auto config = UpdateYsonStruct(
        context->GetConfig()->TableWriter,
        TableWriter);

    // XXX(babenko): temporary workaround; this is how it actually works but not how it is intended to be.
    Options.PingAncestors = true;
    Options.Config = config;

    PutMethodInfoInTraceContext("write_table");

    auto apiWriter = WaitFor(context->GetClient()->CreateTableWriter(
        Path,
        Options))
        .ValueOrThrow();

    auto schemalessWriter = CreateSchemalessFromApiWriterAdapter(std::move(apiWriter));

    TWritingValueConsumer valueConsumer(
        schemalessWriter,
        ConvertTo<TTypeConversionConfigPtr>(context->GetInputFormat().Attributes()),
        MaxRowBufferSize);

    TTableOutput output(CreateParserForFormat(
        context->GetInputFormat(),
        &valueConsumer));

    PipeInputToOutput(context->Request().InputStream, &output);

    WaitFor(valueConsumer.Flush())
        .ThrowOnError();

    WaitFor(schemalessWriter->Close())
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TGetTableColumnarStatisticsCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("paths", &TThis::Paths);
    registrar.Parameter("fetcher_mode", &TThis::FetcherMode)
        .Default(EColumnarStatisticsFetcherMode::FromNodes);
    registrar.Parameter("max_chunks_per_node_fetch", &TThis::MaxChunksPerNodeFetch)
        .Default();
    registrar.Parameter("enable_early_finish", &TThis::EnableEarlyFinish)
        .Default(true);
}

void TGetTableColumnarStatisticsCommand::DoExecute(ICommandContextPtr context)
{
    Options.FetchChunkSpecConfig = context->GetConfig()->TableReader;
    Options.FetcherConfig = context->GetConfig()->Fetcher;
    Options.EnableEarlyFinish = EnableEarlyFinish;

    if (MaxChunksPerNodeFetch) {
        Options.FetcherConfig = CloneYsonStruct(Options.FetcherConfig);
        Options.FetcherConfig->MaxChunksPerNodeFetch = *MaxChunksPerNodeFetch;
    }

    Options.FetcherMode = FetcherMode;

    auto transaction = AttachTransaction(context, false);

    std::vector<TFuture<NYson::TYsonString>> asyncSchemaYsons;
    for (int index = 0; index < std::ssize(Paths); ++index) {
        if (Paths[index].GetColumns()) {
            continue;
        }
        TGetNodeOptions options;
        static_cast<TTransactionalOptions&>(options) = Options;
        static_cast<TTimeoutOptions&>(options) = Options;
        asyncSchemaYsons.push_back(context->GetClient()->GetNode(Paths[index].GetPath() + "/@schema", options));
    }

    if (!asyncSchemaYsons.empty()) {
        YT_LOG_DEBUG("Fetching schemas for tables without column selectors (TableCount: %v)", asyncSchemaYsons.size());
        auto allSchemas = WaitFor(AllSucceeded(asyncSchemaYsons))
            .ValueOrThrow();
        for (int pathIndex = 0, missingColumnIndex = 0; pathIndex < std::ssize(Paths); ++pathIndex) {
            if (!Paths[pathIndex].GetColumns()) {
                auto columnNames = ConvertTo<TTableSchema>(allSchemas[missingColumnIndex]).GetColumnNames();
                if (columnNames.empty()) {
                    THROW_ERROR_EXCEPTION("Table %Qv does not have schema and column selector is not specified", Paths[pathIndex]);
                }
                Paths[pathIndex].SetColumns(columnNames);
                ++missingColumnIndex;
            }
        }
    }

    YT_LOG_DEBUG("Starting fetching columnar statistics");

    auto allStatisticsOrError = WaitFor(context->GetClient()->GetColumnarStatistics(Paths, Options));

    YT_LOG_DEBUG("Finished fetching columnar statistics");

    auto allStatistics = allStatisticsOrError.ValueOrThrow();

    YT_VERIFY(allStatistics.size() == Paths.size());
    for (int index = 0; index < std::ssize(allStatistics); ++index) {
        YT_VERIFY(std::ssize(*Paths[index].GetColumns()) == allStatistics[index].GetColumnCount());
    }

    ProduceOutput(context, [&] (IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .DoList([&] (TFluentList fluent) {
                for (int index = 0; index < std::ssize(Paths); ++index) {
                    auto columns = *Paths[index].GetColumns();
                    const auto& statistics = allStatistics[index];
                    fluent
                        .Item()
                        .BeginMap()
                            .Item("column_data_weights").DoMap([&] (TFluentMap fluent) {
                                for (int index = 0; index < statistics.GetColumnCount(); ++index) {
                                    fluent.Item(columns[index]).Value(statistics.ColumnDataWeights[index]);
                                }
                            })
                            .OptionalItem("timestamp_total_weight", statistics.TimestampTotalWeight)
                            .Item("legacy_chunks_data_weight").Value(statistics.LegacyChunkDataWeight)
                            .DoIf(statistics.HasValueStatistics(), [&] (TFluentMap fluent) {
                                fluent
                                    .Item("column_min_values").DoMap([&] (TFluentMap fluent) {
                                        for (int index = 0; index < statistics.GetColumnCount(); ++index) {
                                            fluent.Item(columns[index]).Value(statistics.ColumnMinValues[index]);
                                        }
                                    })
                                    .Item("column_max_values").DoMap([&] (TFluentMap fluent) {
                                        for (int index = 0; index < statistics.GetColumnCount(); ++index) {
                                            fluent.Item(columns[index]).Value(statistics.ColumnMaxValues[index]);
                                        }
                                    })
                                    .Item("column_non_null_value_counts").DoMap([&] (TFluentMap fluent) {
                                        for (int index = 0; index < statistics.GetColumnCount(); ++index) {
                                            fluent.Item(columns[index]).Value(statistics.ColumnNonNullValueCounts[index]);
                                        }
                                    });
                            })
                            .OptionalItem("chunk_row_count", statistics.ChunkRowCount)
                            .OptionalItem("legacy_chunk_row_count", statistics.LegacyChunkRowCount)
                        .EndMap();
                }
            });
    });
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionTablesCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("paths", &TThis::Paths);
    registrar.Parameter("partition_mode", &TThis::PartitionMode)
        .Default(ETablePartitionMode::Unordered);
    registrar.Parameter("data_weight_per_partition", &TThis::DataWeightPerPartition)
        .GreaterThan(0);
    registrar.Parameter("max_partition_count", &TThis::MaxPartitionCount)
        .GreaterThan(0)
        .Default();
    registrar.Parameter("enable_key_guarantee", &TThis::EnableKeyGuarantee)
        .Default(false);
    registrar.Parameter("adjust_data_weight_per_partition", &TThis::AdjustDataWeightPerPartition)
        .Default(true);
}

void TPartitionTablesCommand::DoExecute(ICommandContextPtr context)
{
    Options.FetchChunkSpecConfig = context->GetConfig()->TableReader;
    Options.FetcherConfig = context->GetConfig()->Fetcher;
    Options.ChunkSliceFetcherConfig = New<TChunkSliceFetcherConfig>();

    Options.PartitionMode = PartitionMode;
    Options.DataWeightPerPartition = DataWeightPerPartition;
    Options.MaxPartitionCount = MaxPartitionCount;
    Options.EnableKeyGuarantee = EnableKeyGuarantee;
    Options.AdjustDataWeightPerPartition = AdjustDataWeightPerPartition;

    auto partitions = WaitFor(context->GetClient()->PartitionTables(Paths, Options))
        .ValueOrThrow();

    context->ProduceOutputValue(ConvertToYsonString(partitions));
}

////////////////////////////////////////////////////////////////////////////////

void TMountTableCommand::Register(TRegistrar registrar)
{
    registrar.ParameterWithUniversalAccessor<TTabletCellId>(
        "cell_id",
        [] (TThis* command) -> auto& {
            return command->Options.CellId;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "freeze",
        [] (TThis* command) -> auto& {
            return command->Options.Freeze;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::vector<TTabletCellId>>(
        "target_cell_ids",
        [] (TThis* command) -> auto& {
            return command->Options.TargetCellIds;
        })
        .Optional(/*init*/ false);
}

void TMountTableCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->MountTable(
        Path.GetPath(),
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TUnmountTableCommand::Register(TRegistrar registrar)
{
    registrar.ParameterWithUniversalAccessor<bool>(
        "force",
        [] (TThis* command) -> auto& {
            return command->Options.Force;
        })
        .Optional(/*init*/ false);
}

void TUnmountTableCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->UnmountTable(
        Path.GetPath(),
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TRemountTableCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->RemountTable(
        Path.GetPath(),
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TFreezeTableCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->FreezeTable(
        Path.GetPath(),
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TUnfreezeTableCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->UnfreezeTable(
        Path.GetPath(),
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TReshardTableCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("pivot_keys", &TThis::PivotKeys)
        .Default();

    registrar.Parameter("tablet_count", &TThis::TabletCount)
        .Default()
        .GreaterThan(0);

    registrar.ParameterWithUniversalAccessor<std::optional<bool>>(
        "uniform",
        [] (TThis* command) -> auto& {
            return command->Options.Uniform;
        })
        .Default();

    registrar.ParameterWithUniversalAccessor<std::optional<bool>>(
        "enable_slicing",
        [] (TThis* command) -> auto& {
            return command->Options.EnableSlicing;
        })
        .Default();

    registrar.ParameterWithUniversalAccessor<std::optional<double>>(
        "slicing_accuracy",
        [] (TThis* command) -> auto& {
            return command->Options.SlicingAccuracy;
        })
        .Default()
        .GreaterThan(0);

    registrar.ParameterWithUniversalAccessor<std::vector<i64>>(
        "trimmed_row_counts",
        [] (TThis* command) -> auto& {
            return command->Options.TrimmedRowCounts;
        })
        .Default();

    registrar.Postprocessor([] (TThis* command) {
        if (command->PivotKeys && command->TabletCount) {
            THROW_ERROR_EXCEPTION("Cannot specify both \"pivot_keys\" and \"tablet_count\"");
        }
        if (!command->PivotKeys && !command->TabletCount) {
            THROW_ERROR_EXCEPTION("Must specify either \"pivot_keys\" or \"tablet_count\"");
        }
        if (command->Options.Uniform && command->PivotKeys) {
            THROW_ERROR_EXCEPTION("\"uniform\" can be specified only with \"tablet_count\"");
        }
        if (command->Options.EnableSlicing && command->PivotKeys) {
            THROW_ERROR_EXCEPTION("\"enable_slicing\" can be specified only with \"tablet_count\"");
        }
        if (command->Options.EnableSlicing && command->Options.Uniform) {
            THROW_ERROR_EXCEPTION("Cannot specify both \"enable_slicing\" and \"uniform\"");
        }
        if (command->Options.SlicingAccuracy && !command->Options.EnableSlicing) {
            THROW_ERROR_EXCEPTION("\"slicing_accuracy\" can be specified only with \"enable_slicing\"");
        }
    });
}

void TReshardTableCommand::DoExecute(ICommandContextPtr context)
{
    TFuture<void> asyncResult;
    if (PivotKeys) {
        asyncResult = context->GetClient()->ReshardTable(
            Path.GetPath(),
            *PivotKeys,
            Options);
    } else {
        asyncResult = context->GetClient()->ReshardTable(
            Path.GetPath(),
            *TabletCount,
            Options);
    }
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TReshardTableAutomaticCommand::Register(TRegistrar registrar)
{
    registrar.ParameterWithUniversalAccessor<bool>(
        "keep_actions",
        [] (TThis* command) -> auto& {
            return command->Options.KeepActions;
        })
        .Default(false);
}

void TReshardTableAutomaticCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->ReshardTableAutomatic(
        Path.GetPath(),
        Options);
    auto tabletActions = WaitFor(asyncResult)
        .ValueOrThrow();
    context->ProduceOutputValue(BuildYsonStringFluently().List(tabletActions));
}

////////////////////////////////////////////////////////////////////////////////

void TAlterTableCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);

    registrar.ParameterWithUniversalAccessor<std::optional<TTableSchema>>(
        "schema",
        [] (TThis* command) -> auto& {
            return command->Options.Schema;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<TMasterTableSchemaId>>(
        "schema_id",
        [] (TThis* command) -> auto& {
            return command->Options.SchemaId;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<bool>>(
        "dynamic",
        [] (TThis* command) -> auto& {
            return command->Options.Dynamic;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<TTableReplicaId>>(
        "upstream_replica_id",
        [] (TThis* command) -> auto& {
            return command->Options.UpstreamReplicaId;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<ETableSchemaModification>>(
        "schema_modification",
        [] (TThis* command) -> auto& {
            return command->Options.SchemaModification;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<TReplicationProgress>>(
        "replication_progress",
        [] (TThis* command) -> auto& {
            return command->Options.ReplicationProgress;
        })
        .Optional(/*init*/ false);
}

void TAlterTableCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->AlterTable(
        Path.GetPath(),
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TSelectRowsCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("query", &TThis::Query);

    registrar.ParameterWithUniversalAccessor<std::optional<i64>>(
        "input_row_limit",
        [] (TThis* command) -> auto& {
            return command->Options.InputRowLimit;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<i64>>(
        "output_row_limit",
        [] (TThis* command) -> auto& {
            return command->Options.OutputRowLimit;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "allow_full_scan",
        [] (TThis* command) -> auto& {
            return command->Options.AllowFullScan;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "allow_join_without_index",
        [] (TThis* command) -> auto& {
            return command->Options.AllowJoinWithoutIndex;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<TString>>(
        "execution_pool",
        [] (TThis* command) -> auto& {
            return command->Options.ExecutionPool;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "fail_on_incomplete_result",
        [] (TThis* command) -> auto& {
            return command->Options.FailOnIncompleteResult;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "enable_code_cache",
        [] (TThis* command) -> auto& {
            return command->Options.EnableCodeCache;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<TUserWorkloadDescriptor>(
        "workload_descriptor",
        [] (TThis* command) -> auto& {
            return command->Options.WorkloadDescriptor;
        })
        .Optional(/*init*/ false);

    registrar.Parameter("enable_statistics", &TThis::EnableStatistics)
        .Optional();

    registrar.ParameterWithUniversalAccessor<EReplicaConsistency>(
        "replica_consistency",
        [] (TThis* command) -> auto& {
            return command->Options.ReplicaConsistency;
        })
        .Optional(/*init*/ false);

    registrar.Parameter("placeholder_values", &TThis::PlaceholderValues)
        .Optional();

    registrar.ParameterWithUniversalAccessor<bool>(
        "use_canonical_null_relations",
        [] (TThis* command) -> auto& {
            return command->Options.UseCanonicalNullRelations;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "merge_versioned_rows",
        [] (TThis* command) -> auto& {
            return command->Options.MergeVersionedRows;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<NApi::EExecutionBackend>>(
        "execution_backend",
        [] (TThis* command) -> auto& {
            return command->Options.ExecutionBackend;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<TVersionedReadOptions>(
        "versioned_read_options",
        [] (TThis* command) -> auto& {
            return command->Options.VersionedReadOptions;
        })
        .Optional(/*init*/ false);
}

bool TSelectRowsCommand::HasResponseParameters() const
{
    return true;
}

void TSelectRowsCommand::DoExecute(ICommandContextPtr context)
{
    auto clientBase = GetClientBase(context);

    if (PlaceholderValues) {
        Options.PlaceholderValues = ConvertToYsonString(PlaceholderValues);

        YT_LOG_DEBUG("Query: %v, Timestamp: %v, PlaceholderValues: %v",
            Query,
            Options.Timestamp,
            Options.PlaceholderValues);
    } else {
        YT_LOG_DEBUG("Query: %v, Timestamp: %v",
            Query,
            Options.Timestamp);
    }

    auto result = WaitFor(clientBase->SelectRows(Query, Options))
        .ValueOrThrow();

    const auto& rowset = result.Rowset;
    const auto& statistics = result.Statistics;

    YT_LOG_INFO("Query result statistics (%v)", statistics);

    if (EnableStatistics) {
        ProduceResponseParameters(context, [&] (NYson::IYsonConsumer* consumer) {
            Serialize(statistics, consumer);
        });
    }

    auto format = context->GetOutputFormat();
    auto output = context->Request().OutputStream;
    auto writer = CreateSchemafulWriterForFormat(format, rowset->GetSchema(), output);

    Y_UNUSED(writer->Write(rowset->GetRows()));

    WaitFor(writer->Close())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TExplainQueryCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("query", &TThis::Query);

    registrar.ParameterWithUniversalAccessor<bool>(
        "verbose_output",
        [] (TThis* command) -> auto& {
            return command->Options.VerboseOutput;
        })
        .Optional(/*init*/ false);
}

void TExplainQueryCommand::DoExecute(ICommandContextPtr context)
{
    auto clientBase = GetClientBase(context);
    auto result = WaitFor(clientBase->ExplainQuery(Query, Options))
        .ValueOrThrow();
    context->ProduceOutputValue(result);
}

////////////////////////////////////////////////////////////////////////////////

static std::vector<TUnversionedRow> ParseRows(
    ICommandContextPtr context,
    TBuildingValueConsumer* valueConsumer)
{
    TTableOutput output(CreateParserForFormat(
        context->GetInputFormat(),
        valueConsumer));

    PipeInputToOutput(context->Request().InputStream, &output);
    return valueConsumer->GetRows();
}

void TInsertRowsCommand::Register(TRegistrar registrar)
{
    registrar.ParameterWithUniversalAccessor<bool>(
        "require_sync_replica",
        [] (TThis* command) -> auto& {
            return command->Options.RequireSyncReplica;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<i64>>(
        "sequence_number",
        [] (TThis* command) -> auto& {
            return command->Options.SequenceNumber;
        })
        .Optional(/*init*/ false);

    registrar.Parameter("table_writer", &TThis::TableWriter)
        .Default();

    registrar.Parameter("path", &TThis::Path);

    registrar.Parameter("update", &TThis::Update)
        .Default(false);

    registrar.Parameter("aggregate", &TThis::Aggregate)
        .Default(false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "allow_missing_key_columns",
        [] (TThis* command) -> auto& {
            return command->Options.AllowMissingKeyColumns;
        })
        .Default(false);

    registrar.Parameter("lock_type", &TThis::LockType)
        .Default(ELockType::Exclusive);
}

void TInsertRowsCommand::DoExecute(ICommandContextPtr context)
{
    auto config = UpdateYsonStruct(
        context->GetConfig()->TableWriter,
        TableWriter);

    auto tableMountCache = context->GetClient()->GetTableMountCache();
    auto tableInfo = WaitFor(tableMountCache->GetTableInfo(Path.GetPath()))
        .ValueOrThrow();

    tableInfo->ValidateDynamic();

    if (!tableInfo->IsSorted() && Update) {
        THROW_ERROR_EXCEPTION("Cannot use \"update\" mode for ordered tables");
    }

    struct TInsertRowsBufferTag
    { };

    auto insertRowsFormatConfig = ConvertTo<TInsertRowsFormatConfigPtr>(context->GetInputFormat().Attributes());
    auto typeConversionConfig = ConvertTo<TTypeConversionConfigPtr>(context->GetInputFormat().Attributes());
    // Parse input data.
    TBuildingValueConsumer valueConsumer(
        tableInfo->Schemas[ETableSchemaKind::Write],
        WithCommandTag(Logger, context),
        insertRowsFormatConfig->EnableNullToYsonEntityConversion,
        typeConversionConfig);
    valueConsumer.SetAggregate(Aggregate);
    valueConsumer.SetTreatMissingAsNull(!Update);
    valueConsumer.SetAllowMissingKeyColumns(Options.AllowMissingKeyColumns);

    auto rows = ParseRows(context, &valueConsumer);
    auto rowBuffer = New<TRowBuffer>(TInsertRowsBufferTag());
    auto capturedRows = rowBuffer->CaptureRows(rows);
    auto rowRange = MakeSharedRange(
        std::vector<TUnversionedRow>(capturedRows.begin(), capturedRows.end()),
        std::move(rowBuffer));

    // Run writes.
    auto transaction = GetTransaction(context);

    transaction->WriteRows(
        Path.GetPath(),
        valueConsumer.GetNameTable(),
        std::move(rowRange),
        Options,
        LockType);

    if (ShouldCommitTransaction()) {
        WaitFor(transaction->Commit())
            .ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TLookupRowsCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("table_writer", &TThis::TableWriter)
        .Default();
    registrar.Parameter("path", &TThis::Path);
    registrar.Parameter("column_names", &TThis::ColumnNames)
        .Default();
    registrar.Parameter("versioned", &TThis::Versioned)
        .Default(false);
    registrar.Parameter("retention_config", &TThis::RetentionConfig)
        .Optional();

    registrar.ParameterWithUniversalAccessor<bool>(
        "keep_missing_rows",
        [] (TThis* command) -> auto& {
            return command->Options.KeepMissingRows;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "enable_partial_result",
        [] (TThis* command) -> auto& {
            return command->Options.EnablePartialResult;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<bool>>(
        "use_lookup_cache",
        [] (TThis* command) -> auto& {
            return command->Options.UseLookupCache;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<TDuration>>(
        "cached_sync_replicas_timeout",
        [] (TThis* command) -> auto& {
            return command->Options.CachedSyncReplicasTimeout;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<EReplicaConsistency>(
        "replica_consistency",
        [] (TThis* command) -> auto& {
            return command->Options.ReplicaConsistency;
        })
        .Optional(/*init*/ false);
}

void TLookupRowsCommand::DoExecute(ICommandContextPtr context)
{
    auto tableMountCache = context->GetClient()->GetTableMountCache();
    auto asyncTableInfo = tableMountCache->GetTableInfo(Path.GetPath());
    auto tableInfo = WaitFor(asyncTableInfo)
        .ValueOrThrow();

    tableInfo->ValidateDynamic();

    auto config = UpdateYsonStruct(
        context->GetConfig()->TableWriter,
        TableWriter);

    if (Path.GetColumns()) {
        THROW_ERROR_EXCEPTION("Columns cannot be specified with table path, use \"column_names\" instead")
            << TErrorAttribute("rich_ypath", Path);
    }
    if (Path.HasNontrivialRanges()) {
        THROW_ERROR_EXCEPTION("Ranges cannot be specified")
            << TErrorAttribute("rich_ypath", Path);
    }

    struct TLookupRowsBufferTag
    { };

    // Parse input data.
    TBuildingValueConsumer valueConsumer(
        tableInfo->Schemas[ETableSchemaKind::Lookup],
        WithCommandTag(Logger, context),
        /*convertNullToEntity*/ false,
        ConvertTo<TTypeConversionConfigPtr>(context->GetInputFormat().Attributes()));
    auto keys = ParseRows(context, &valueConsumer);
    auto rowBuffer = New<TRowBuffer>(TLookupRowsBufferTag());
    auto capturedKeys = rowBuffer->CaptureRows(keys);
    auto mutableKeyRange = MakeSharedRange(std::move(capturedKeys), std::move(rowBuffer));
    auto keyRange = TSharedRange<TUnversionedRow>(
        static_cast<const TUnversionedRow*>(mutableKeyRange.Begin()),
        static_cast<const TUnversionedRow*>(mutableKeyRange.End()),
        mutableKeyRange.GetHolder());
    auto nameTable = valueConsumer.GetNameTable();

    if (ColumnNames) {
        TColumnFilter::TIndexes columnFilterIndexes;
        columnFilterIndexes.reserve(ColumnNames->size());
        for (const auto& name : *ColumnNames) {
            auto optionalIndex = nameTable->FindId(name);
            if (!optionalIndex) {
                if (!tableInfo->Schemas[ETableSchemaKind::Primary]->FindColumn(name)) {
                    THROW_ERROR_EXCEPTION("No such column %Qv",
                        name);
                }
                optionalIndex = nameTable->GetIdOrRegisterName(name);
            }
            columnFilterIndexes.push_back(*optionalIndex);
        }
        Options.ColumnFilter = TColumnFilter(std::move(columnFilterIndexes));
    }

    // Run lookup.
    auto format = context->GetOutputFormat();
    auto output = context->Request().OutputStream;

    auto clientBase = GetClientBase(context);

    if (Versioned) {
        TVersionedLookupRowsOptions versionedOptions;
        versionedOptions.ColumnFilter = Options.ColumnFilter;
        versionedOptions.KeepMissingRows = Options.KeepMissingRows;
        versionedOptions.EnablePartialResult = Options.EnablePartialResult;
        versionedOptions.UseLookupCache = Options.UseLookupCache;
        versionedOptions.Timestamp = Options.Timestamp;
        versionedOptions.CachedSyncReplicasTimeout = Options.CachedSyncReplicasTimeout;
        versionedOptions.RetentionConfig = RetentionConfig;
        versionedOptions.ReplicaConsistency = Options.ReplicaConsistency;
        auto asyncRowset = clientBase->VersionedLookupRows(
            Path.GetPath(),
            std::move(nameTable),
            std::move(keyRange),
            versionedOptions);
        auto rowset = WaitFor(asyncRowset)
            .ValueOrThrow()
            .Rowset;
        auto writer = CreateVersionedWriterForFormat(format, rowset->GetSchema(), output);
        Y_UNUSED(writer->Write(rowset->GetRows()));
        WaitFor(writer->Close())
            .ThrowOnError();
    } else {
        auto asyncRowset = clientBase->LookupRows(
            Path.GetPath(),
            std::move(nameTable),
            std::move(keyRange),
            Options);
        auto rowset = WaitFor(asyncRowset)
            .ValueOrThrow()
            .Rowset;
        auto writer = CreateSchemafulWriterForFormat(format, rowset->GetSchema(), output);
        Y_UNUSED(writer->Write(rowset->GetRows()));
        WaitFor(writer->Close())
            .ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TPullRowsCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);

    registrar.ParameterWithUniversalAccessor<TReplicaId>(
        "upstream_replica_id",
        [] (TThis* command) -> auto& {
            return command->Options.UpstreamReplicaId;
        });

    registrar.ParameterWithUniversalAccessor<TReplicationProgress>(
        "replication_progress",
        [] (TThis* command) -> auto& {
            return command->Options.ReplicationProgress;
        });

    registrar.ParameterWithUniversalAccessor<TTimestamp>(
        "upper_timestamp",
        [] (TThis* command) -> auto& {
            return command->Options.UpperTimestamp;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "order_rows_by_timestamp",
        [] (TThis* command) -> auto& {
            return command->Options.OrderRowsByTimestamp;
        })
        .Default(false);
}

void TPullRowsCommand::DoExecute(ICommandContextPtr context)
{
    if (Path.HasNontrivialRanges()) {
        THROW_ERROR_EXCEPTION("Ranges cannot be specified")
            << TErrorAttribute("rich_ypath", Path);
    }

    auto format = context->GetOutputFormat();
    auto output = context->Request().OutputStream;

    auto client = context->GetClient();
    auto pullRowsFuture = client->PullRows(Path.GetPath(), Options);
    auto pullResult = WaitFor(pullRowsFuture)
        .ValueOrThrow();

    ProduceResponseParameters(context, [&] (IYsonConsumer* consumer) {
        BuildYsonMapFragmentFluently(consumer)
            .Item("replication_progress").Value(pullResult.ReplicationProgress)
            .Item("end_replication_row_indexes")
            .DoMapFor(pullResult.EndReplicationRowIndexes, [] (auto fluent, const auto& pair) {
                fluent
                    .Item(ToString(pair.first)).Value(pair.second);
            });
    });

    if (pullResult.Versioned) {
        auto writer = CreateVersionedWriterForFormat(format, pullResult.Rowset->GetSchema(), output);
        Y_UNUSED(writer->Write(ReinterpretCastRange<TVersionedRow>(pullResult.Rowset->GetRows())));
        WaitFor(writer->Close())
            .ThrowOnError();
    } else {
        auto writer = CreateSchemafulWriterForFormat(format, pullResult.Rowset->GetSchema(), output);
        Y_UNUSED(writer->Write(ReinterpretCastRange<TUnversionedRow>(pullResult.Rowset->GetRows())));
        WaitFor(writer->Close())
            .ThrowOnError();
    }
}

bool TPullRowsCommand::HasResponseParameters() const
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TGetInSyncReplicasCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);

    registrar.ParameterWithUniversalAccessor<TTimestamp>(
        "timestamp",
        [] (TThis* command) -> auto& {
            return command->Options.Timestamp;
        });

    registrar.Parameter("all_keys", &TThis::AllKeys)
        .Default(false);

    registrar.ParameterWithUniversalAccessor<std::optional<TDuration>>(
        "cached_sync_replicas_timeout",
        [] (TThis* command) -> auto& {
            return command->Options.CachedSyncReplicasTimeout;
        })
        .Optional(/*init*/ false);
}

void TGetInSyncReplicasCommand::DoExecute(ICommandContextPtr context)
{
    auto tableMountCache = context->GetClient()->GetTableMountCache();
    auto asyncTableInfo = tableMountCache->GetTableInfo(Path.GetPath());
    auto tableInfo = WaitFor(asyncTableInfo)
        .ValueOrThrow();

    tableInfo->ValidateDynamic();
    if (!tableInfo->IsChaosReplica() && !tableInfo->IsChaosReplicated()) {
        tableInfo->ValidateReplicated();
    }

    TFuture<std::vector<NTabletClient::TTableReplicaId>> asyncReplicas;
    if (AllKeys) {
        asyncReplicas = context->GetClient()->GetInSyncReplicas(
            Path.GetPath(),
            Options);
    } else {
        struct TInSyncBufferTag
        { };

        // Parse input data.
        TBuildingValueConsumer valueConsumer(
            tableInfo->Schemas[ETableSchemaKind::Lookup],
            WithCommandTag(Logger, context),
            /*convertNullToEntity*/ false,
            ConvertTo<TTypeConversionConfigPtr>(context->GetInputFormat().Attributes()));
        auto keys = ParseRows(context, &valueConsumer);
        auto rowBuffer = New<TRowBuffer>(TInSyncBufferTag());
        auto capturedKeys = rowBuffer->CaptureRows(keys);
        auto mutableKeyRange = MakeSharedRange(std::move(capturedKeys), std::move(rowBuffer));
        auto keyRange = TSharedRange<TUnversionedRow>(
            static_cast<const TUnversionedRow*>(mutableKeyRange.Begin()),
            static_cast<const TUnversionedRow*>(mutableKeyRange.End()),
            mutableKeyRange.GetHolder());
        auto nameTable = valueConsumer.GetNameTable();
        asyncReplicas = context->GetClient()->GetInSyncReplicas(
            Path.GetPath(),
            std::move(nameTable),
            std::move(keyRange),
            Options);
    }

    auto replicas = WaitFor(asyncReplicas)
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .List(replicas));
}

////////////////////////////////////////////////////////////////////////////////

void TDeleteRowsCommand::Register(TRegistrar registrar)
{
    registrar.ParameterWithUniversalAccessor<std::optional<i64>>(
        "sequence_number",
        [] (TThis* command) -> auto& {
            return command->Options.SequenceNumber;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "require_sync_replica",
        [] (TThis* command) -> auto& {
            return command->Options.RequireSyncReplica;
        })
        .Optional(/*init*/ false);

    registrar.Parameter("table_writer", &TThis::TableWriter)
        .Default();

    registrar.Parameter("path", &TThis::Path);
}

void TDeleteRowsCommand::DoExecute(ICommandContextPtr context)
{
    auto config = UpdateYsonStruct(
        context->GetConfig()->TableWriter,
        TableWriter);

    auto tableMountCache = context->GetClient()->GetTableMountCache();
    auto asyncTableInfo = tableMountCache->GetTableInfo(Path.GetPath());
    auto tableInfo = WaitFor(asyncTableInfo)
        .ValueOrThrow();

    tableInfo->ValidateDynamic();
    tableInfo->ValidateSorted();

    struct TDeleteRowsBufferTag
    { };

    // Parse input data.
    TBuildingValueConsumer valueConsumer(
        tableInfo->Schemas[ETableSchemaKind::Delete],
        WithCommandTag(Logger, context),
        /*convertNullToEntity*/ false,
        ConvertTo<TTypeConversionConfigPtr>(context->GetInputFormat().Attributes()));
    auto keys = ParseRows(context, &valueConsumer);
    auto rowBuffer = New<TRowBuffer>(TDeleteRowsBufferTag());
    auto capturedKeys = rowBuffer->CaptureRows(keys);
    auto keyRange = MakeSharedRange(
        std::vector<TLegacyKey>(capturedKeys.begin(), capturedKeys.end()),
        std::move(rowBuffer));

    // Run deletes.
    auto transaction = GetTransaction(context);

    transaction->DeleteRows(
        Path.GetPath(),
        valueConsumer.GetNameTable(),
        std::move(keyRange),
        Options);

    if (ShouldCommitTransaction()) {
        WaitFor(transaction->Commit())
            .ThrowOnError();
    }

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TLockRowsCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("table_writer", &TThis::TableWriter)
        .Default();
    registrar.Parameter("path", &TThis::Path);
    registrar.Parameter("locks", &TThis::Locks);
    registrar.Parameter("lock_type", &TThis::LockType)
        .Default(NTableClient::ELockType::SharedStrong);
}

void TLockRowsCommand::DoExecute(ICommandContextPtr context)
{
    auto config = UpdateYsonStruct(
        context->GetConfig()->TableWriter,
        TableWriter);

    auto tableMountCache = context->GetClient()->GetTableMountCache();
    auto asyncTableInfo = tableMountCache->GetTableInfo(Path.GetPath());
    auto tableInfo = WaitFor(asyncTableInfo)
        .ValueOrThrow();

    tableInfo->ValidateDynamic();

    auto transactionPool = context->GetDriver()->GetStickyTransactionPool();
    auto transaction = transactionPool->GetTransactionAndRenewLeaseOrThrow(Options.TransactionId);

    struct TLockRowsBufferTag
    { };

    // Parse input data.
    TBuildingValueConsumer valueConsumer(
        tableInfo->Schemas[ETableSchemaKind::Write],
        WithCommandTag(Logger, context),
        /*convertNullToEntity*/ false,
        ConvertTo<TTypeConversionConfigPtr>(context->GetInputFormat().Attributes()));
    auto keys = ParseRows(context, &valueConsumer);
    auto rowBuffer = New<TRowBuffer>(TLockRowsBufferTag());
    auto capturedKeys = rowBuffer->CaptureRows(keys);
    auto keyRange = MakeSharedRange(
        std::vector<TLegacyKey>(capturedKeys.begin(), capturedKeys.end()),
        std::move(rowBuffer));

    // Run locks.
    transaction->LockRows(
        Path.GetPath(),
        valueConsumer.GetNameTable(),
        std::move(keyRange),
        Locks,
        LockType);

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TTrimRowsCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
    registrar.Parameter("tablet_index", &TThis::TabletIndex);
    registrar.Parameter("trimmed_row_count", &TThis::TrimmedRowCount);
}

void TTrimRowsCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    auto asyncResult = client->TrimTable(
        Path.GetPath(),
        TabletIndex,
        TrimmedRowCount,
        Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TEnableTableReplicaCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("replica_id", &TThis::ReplicaId);

    registrar.Preprocessor([] (TThis* command) {
        command->Options.Enabled = true;
    });
}

void TEnableTableReplicaCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    auto asyncResult = client->AlterTableReplica(ReplicaId, Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TDisableTableReplicaCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("replica_id", &TThis::ReplicaId);

    registrar.Preprocessor([] (TThis* command) {
        command->Options.Enabled = false;
    });
}

void TDisableTableReplicaCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    auto asyncResult = client->AlterTableReplica(ReplicaId, Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TAlterTableReplicaCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("replica_id", &TThis::ReplicaId);

    registrar.ParameterWithUniversalAccessor<std::optional<bool>>(
        "enabled",
        [] (TThis* command) -> auto& {
            return command->Options.Enabled;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<ETableReplicaMode>>(
        "mode",
        [] (TThis* command) -> auto& {
            return command->Options.Mode;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<bool>>(
        "preserve_timestamps",
        [] (TThis* command) -> auto& {
            return command->Options.PreserveTimestamps;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<EAtomicity>>(
        "atomicity",
        [] (TThis* command) -> auto& {
            return command->Options.Atomicity;
        })
        .Optional(/*init*/ false);

    registrar.ParameterWithUniversalAccessor<std::optional<bool>>(
        "enable_replicated_table_tracker",
        [] (TThis* command) -> auto& {
            return command->Options.EnableReplicatedTableTracker;
        })
        .Optional(/*init*/ false);
}

void TAlterTableReplicaCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    auto asyncResult = client->AlterTableReplica(ReplicaId, Options);
    WaitFor(asyncResult)
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TGetTablePivotKeysCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);

    registrar.ParameterWithUniversalAccessor<bool>(
        "represent_key_as_list",
        [] (TThis* command) -> auto& {
            return command->Options.RepresentKeyAsList;
        })
        .Default(false);
}

void TGetTablePivotKeysCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->GetTablePivotKeys(Path, Options);
    auto result = WaitFor(asyncResult)
        .ValueOrThrow();
    context->ProduceOutputValue(result);
}

////////////////////////////////////////////////////////////////////////////////

void TCreateTableBackupCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("manifest", &TThis::Manifest);

    registrar.ParameterWithUniversalAccessor<TDuration>(
        "checkpoint_timestamp_delay",
        [] (TThis* command) -> auto& {
            return command->Options.CheckpointTimestampDelay;
        })
        .Default(TDuration::Seconds(5));

    registrar.ParameterWithUniversalAccessor<TDuration>(
        "checkpoint_check_period",
        [] (TThis* command) -> auto& {
            return command->Options.CheckpointCheckPeriod;
        })
        .Default(TDuration::Seconds(1));

    registrar.ParameterWithUniversalAccessor<TDuration>(
        "checkpoint_check_timeout",
        [] (TThis* command) -> auto& {
            return command->Options.CheckpointCheckTimeout;
        })
        .Default(TDuration::Seconds(10));

    registrar.ParameterWithUniversalAccessor<bool>(
        "force",
        [] (TThis* command) -> auto& {
            return command->Options.Force;
        })
        .Default(false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "preserve_account",
        [] (TThis* command) -> auto& {
            return command->Options.PreserveAccount;
        })
        .Default(false);
}

void TCreateTableBackupCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->CreateTableBackup(Manifest, Options);
    WaitFor(asyncResult)
        .ThrowOnError();
    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TRestoreTableBackupCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("manifest", &TThis::Manifest);

    registrar.ParameterWithUniversalAccessor<bool>(
        "force",
        [] (TThis* command) -> auto& {
            return command->Options.Force;
        })
        .Default(false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "mount",
        [] (TThis* command) -> auto& {
            return command->Options.Mount;
        })
        .Default(false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "enable_replicas",
        [] (TThis* command) -> auto& {
            return command->Options.EnableReplicas;
        })
        .Default(false);

    registrar.ParameterWithUniversalAccessor<bool>(
        "preserve_account",
        [] (TThis* command) -> auto& {
            return command->Options.PreserveAccount;
        })
        .Default(false);
}

void TRestoreTableBackupCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->RestoreTableBackup(Manifest, Options);
    WaitFor(asyncResult)
        .ThrowOnError();
    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TGetTabletInfosCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
    registrar.Parameter("tablet_indexes", &TThis::TabletIndexes);

    registrar.ParameterWithUniversalAccessor<bool>(
        "request_errors",
        [] (TThis* command) -> auto& {
            return command->Options.RequestErrors;
        })
        .Default(false);
}

void TGetTabletInfosCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    auto asyncTablets = client->GetTabletInfos(Path, TabletIndexes, Options);
    auto tablets = WaitFor(asyncTablets)
        .ValueOrThrow();

    auto addErrors = [] (const std::vector<TError>& errors, TFluentMap fluent) {
        fluent
            .Item("tablet_errors").DoListFor(errors, [] (TFluentList fluent, const auto& error) {
                fluent
                    .Item().Value(error);
                });
    };

    ProduceOutput(context, [&] (IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("tablets").DoListFor(tablets, [&] (auto fluent, const auto& tablet) {
                    fluent
                        .Item().BeginMap()
                            .Item("total_row_count").Value(tablet.TotalRowCount)
                            .Item("trimmed_row_count").Value(tablet.TrimmedRowCount)
                            .Item("delayed_lockless_row_count").Value(tablet.DelayedLocklessRowCount)
                            .Item("barrier_timestamp").Value(tablet.BarrierTimestamp)
                            .Item("last_write_timestamp").Value(tablet.LastWriteTimestamp)
                            .DoIf(Options.RequestErrors, BIND(addErrors, tablet.TabletErrors))
                            .DoIf(tablet.TableReplicaInfos.has_value(), [&] (TFluentMap fluent) {
                                fluent
                                    .Item("replica_infos").DoListFor(
                                        *tablet.TableReplicaInfos,
                                        [&] (TFluentList fluent, const auto& replicaInfo) {
                                            fluent
                                                .Item()
                                                .BeginMap()
                                                    .Item("replica_id").Value(replicaInfo.ReplicaId)
                                                    .Item("last_replication_timestamp").Value(replicaInfo.LastReplicationTimestamp)
                                                    .Item("mode").Value(replicaInfo.Mode)
                                                    .Item("current_replication_row_index").Value(replicaInfo.CurrentReplicationRowIndex)
                                                    .Item("committed_replication_row_index").Value(replicaInfo.CommittedReplicationRowIndex)
                                                    .DoIf(Options.RequestErrors, [&] (TFluentMap fluent) {
                                                        fluent
                                                            .Item("replication_error").Value(replicaInfo.ReplicationError);
                                                    })
                                                .EndMap();
                                        });
                            })
                        .EndMap();
                })
            .EndMap();
    });
}

////////////////////////////////////////////////////////////////////////////////

void TGetTabletErrorsCommand::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);

    registrar.ParameterWithUniversalAccessor<std::optional<i64>>(
        "limit",
        [] (TThis* command) -> auto& {
            return command->Options.Limit;
        })
        .Default()
        .GreaterThan(0);
}

void TGetTabletErrorsCommand::DoExecute(ICommandContextPtr context)
{
    auto client = context->GetClient();
    auto asyncErrors = client->GetTabletErrors(Path, Options);
    auto errors = WaitFor(asyncErrors)
        .ValueOrThrow();

    ProduceOutput(context, [&] (IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("tablet_errors").DoMapFor(errors.TabletErrors, [] (auto fluent, const auto& pair) {
                    fluent
                        .Item(ToString(pair.first)).DoListFor(pair.second, [] (auto fluent, const auto& error) {
                            fluent.Item().Value(error);
                        });
                })
                .Item("replication_errors").DoMapFor(errors.ReplicationErrors, [] (auto fluent, const auto& pair) {
                    fluent
                        .Item(ToString(pair.first)).DoListFor(pair.second, [] (auto fluent, const auto& error) {
                            fluent.Item().Value(error);
                        });
                })
                .DoIf(errors.Incomplete, [&] (TFluentMap fluent) {
                    fluent
                        .Item("incomplete").Value(errors.Incomplete);
                })
            .EndMap();
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
