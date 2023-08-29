#include "table_commands.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/skynet.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/table_client/adapters.h>
#include <yt/yt/client/table_client/blob_reader.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/table_consumer.h>
#include <yt/yt/client/table_client/table_output.h>
#include <yt/yt/client/table_client/unversioned_writer.h>
#include <yt/yt/client/table_client/versioned_writer.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/formats/config.h>
#include <yt/yt/client/formats/parser.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NDriver {

using namespace NApi;
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

TReadTableCommand::TReadTableCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("table_reader", TableReader)
        .Default();
    RegisterParameter("control_attributes", ControlAttributes)
        .DefaultNew();
    RegisterParameter("unordered", Unordered)
        .Default(false);
    RegisterParameter("start_row_index_only", StartRowIndexOnly)
        .Default(false);
    RegisterParameter("omit_inaccessible_columns", Options.OmitInaccessibleColumns)
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
            .DoIf(reader->GetTotalRowCount() > 0, [&](auto fluent) {
                fluent
                    .Item("start_row_index").Value(reader->GetStartRowIndex());
            });
    });

    if (StartRowIndexOnly) {
        return;
    }

    auto writer = CreateStaticTableWriterForFormat(
        context->GetOutputFormat(),
        reader->GetNameTable(),
        {reader->GetTableSchema()},
        context->Request().OutputStream,
        false,
        ControlAttributes,
        0);

    auto finally = Finally([&] () {
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
        .MaxRowsPerRead = context->GetConfig()->ReadBufferRowCount
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

TReadBlobTableCommand::TReadBlobTableCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("part_size", PartSize);
    RegisterParameter("table_reader", TableReader)
        .Default();
    RegisterParameter("part_index_column_name", PartIndexColumnName)
        .Default();
    RegisterParameter("data_column_name", DataColumnName)
        .Default();
    RegisterParameter("start_part_index", StartPartIndex)
        .Default(0);
    RegisterParameter("offset", Offset)
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

TLocateSkynetShareCommand::TLocateSkynetShareCommand()
{
    RegisterParameter("path", Path);
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

TWriteTableCommand::TWriteTableCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("table_writer", TableWriter)
        .Default();
    RegisterParameter("max_row_buffer_size", MaxRowBufferSize)
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

    PipeInputToOutput(context->Request().InputStream, &output, MaxRowBufferSize);

    WaitFor(valueConsumer.Flush())
        .ThrowOnError();

    WaitFor(schemalessWriter->Close())
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TGetTableColumnarStatisticsCommand::TGetTableColumnarStatisticsCommand()
{
    RegisterParameter("paths", Paths);
    RegisterParameter("fetcher_mode", FetcherMode)
        .Default(EColumnarStatisticsFetcherMode::FromNodes);
    RegisterParameter("max_chunks_per_node_fetch", MaxChunksPerNodeFetch)
        .Default();
    RegisterParameter("enable_early_finish", EnableEarlyFinish)
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

TPartitionTablesCommand::TPartitionTablesCommand()
{
    RegisterParameter("paths", Paths);
    RegisterParameter("partition_mode", PartitionMode)
        .Default(ETablePartitionMode::Unordered);
    RegisterParameter("data_weight_per_partition", DataWeightPerPartition);
    RegisterParameter("max_partition_count", MaxPartitionCount)
        .Default();
    RegisterParameter("enable_key_guarantee", EnableKeyGuarantee)
        .Default(false);
    RegisterParameter("adjust_data_weight_per_partition", AdjustDataWeightPerPartition)
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

TMountTableCommand::TMountTableCommand()
{
    RegisterParameter("cell_id", Options.CellId)
        .Optional();
    RegisterParameter("freeze", Options.Freeze)
        .Optional();
    RegisterParameter("target_cell_ids", Options.TargetCellIds)
        .Optional();
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

TUnmountTableCommand::TUnmountTableCommand()
{
    RegisterParameter("force", Options.Force)
        .Optional();
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

TReshardTableCommand::TReshardTableCommand()
{
    RegisterParameter("pivot_keys", PivotKeys)
        .Default();
    RegisterParameter("tablet_count", TabletCount)
        .Default()
        .GreaterThan(0);
    RegisterParameter("uniform", Options.Uniform)
        .Default();
    RegisterParameter("enable_slicing", Options.EnableSlicing)
        .Default();
    RegisterParameter("slicing_accuracy", Options.SlicingAccuracy)
        .Default()
        .GreaterThan(0);

    RegisterPostprocessor([&] () {
        if (PivotKeys && TabletCount) {
            THROW_ERROR_EXCEPTION("Cannot specify both \"pivot_keys\" and \"tablet_count\"");
        }
        if (!PivotKeys && !TabletCount) {
            THROW_ERROR_EXCEPTION("Must specify either \"pivot_keys\" or \"tablet_count\"");
        }
        if (Options.Uniform && PivotKeys) {
            THROW_ERROR_EXCEPTION("\"uniform\" can be specified only with \"tablet_count\"");
        }
        if (Options.EnableSlicing && PivotKeys) {
            THROW_ERROR_EXCEPTION("\"enable_slicing\" can be specified only with \"tablet_count\"");
        }
        if (Options.EnableSlicing && Options.Uniform) {
            THROW_ERROR_EXCEPTION("Cannot specify both \"enable_slicing\" and \"uniform\"");
        }
        if (Options.SlicingAccuracy && !Options.EnableSlicing) {
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

TReshardTableAutomaticCommand::TReshardTableAutomaticCommand()
{
    RegisterParameter("keep_actions", Options.KeepActions)
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

TAlterTableCommand::TAlterTableCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("schema", Options.Schema)
        .Optional();
    RegisterParameter("schema_id", Options.SchemaId)
        .Optional();
    RegisterParameter("dynamic", Options.Dynamic)
        .Optional();
    RegisterParameter("upstream_replica_id", Options.UpstreamReplicaId)
        .Optional();
    RegisterParameter("schema_modification", Options.SchemaModification)
        .Optional();
    RegisterParameter("replication_progress", Options.ReplicationProgress)
        .Optional();
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

TSelectRowsCommand::TSelectRowsCommand()
{
    RegisterParameter("query", Query);
    RegisterParameter("input_row_limit", Options.InputRowLimit)
        .Optional();
    RegisterParameter("output_row_limit", Options.OutputRowLimit)
        .Optional();
    RegisterParameter("allow_full_scan", Options.AllowFullScan)
        .Optional();
    RegisterParameter("allow_join_without_index", Options.AllowJoinWithoutIndex)
        .Optional();
    RegisterParameter("execution_pool", Options.ExecutionPool)
        .Optional();
    RegisterParameter("fail_on_incomplete_result", Options.FailOnIncompleteResult)
        .Optional();
    RegisterParameter("enable_code_cache", Options.EnableCodeCache)
        .Optional();
    RegisterParameter("workload_descriptor", Options.WorkloadDescriptor)
        .Optional();
    RegisterParameter("enable_statistics", EnableStatistics)
        .Optional();
    RegisterParameter("replica_consistency", Options.ReplicaConsistency)
        .Optional();
    RegisterParameter("placeholder_values", PlaceholderValues)
        .Optional();
    RegisterParameter("use_canonical_null_relations", Options.UseCanonicalNullRelations)
        .Optional();
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

    writer->Write(rowset->GetRows());

    WaitFor(writer->Close())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TExplainQueryCommand::TExplainQueryCommand()
{
    RegisterParameter("query", Query);
    RegisterParameter("verbose_output", Options.VerboseOutput)
        .Optional();
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

    PipeInputToOutput(context->Request().InputStream, &output, 64_KB);
    return valueConsumer->GetRows();
}

TInsertRowsCommand::TInsertRowsCommand()
{
    RegisterParameter("require_sync_replica", Options.RequireSyncReplica)
        .Optional();
    RegisterParameter("sequence_number", Options.SequenceNumber)
        .Optional();
    RegisterParameter("table_writer", TableWriter)
        .Default();
    RegisterParameter("path", Path);
    RegisterParameter("update", Update)
        .Default(false);
    RegisterParameter("aggregate", Aggregate)
        .Default(false);
    RegisterParameter("allow_missing_key_columns", Options.AllowMissingKeyColumns)
        .Default(false);
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
        Options);

    if (ShouldCommitTransaction()) {
        WaitFor(transaction->Commit())
            .ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

TLookupRowsCommand::TLookupRowsCommand()
{
    RegisterParameter("table_writer", TableWriter)
        .Default();
    RegisterParameter("path", Path);
    RegisterParameter("column_names", ColumnNames)
        .Default();
    RegisterParameter("versioned", Versioned)
        .Default(false);
    RegisterParameter("retention_config", RetentionConfig)
        .Optional();
    RegisterParameter("keep_missing_rows", Options.KeepMissingRows)
        .Optional();
    RegisterParameter("enable_partial_result", Options.EnablePartialResult)
        .Optional();
    RegisterParameter("use_lookup_cache", Options.UseLookupCache)
        .Optional();
    RegisterParameter("cached_sync_replicas_timeout", Options.CachedSyncReplicasTimeout)
        .Optional();
    RegisterParameter("replica_consistency", Options.ReplicaConsistency)
        .Optional();
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
        auto asyncRowset = clientBase->VersionedLookupRows(Path.GetPath(), std::move(nameTable), std::move(keyRange), versionedOptions);
        auto rowset = WaitFor(asyncRowset)
            .ValueOrThrow();
        auto writer = CreateVersionedWriterForFormat(format, rowset->GetSchema(), output);
        writer->Write(rowset->GetRows());
        WaitFor(writer->Close())
            .ThrowOnError();
    } else {
        auto asyncRowset = clientBase->LookupRows(Path.GetPath(), std::move(nameTable), std::move(keyRange), Options);
        auto rowset = WaitFor(asyncRowset)
            .ValueOrThrow();

        auto writer = CreateSchemafulWriterForFormat(format, rowset->GetSchema(), output);
        writer->Write(rowset->GetRows());
        WaitFor(writer->Close())
            .ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

TPullRowsCommand::TPullRowsCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("upstream_replica_id", Options.UpstreamReplicaId);
    RegisterParameter("replication_progress", Options.ReplicationProgress);
    RegisterParameter("upper_timestamp", Options.UpperTimestamp)
        .Optional();
    RegisterParameter("order_rows_by_timestamp", Options.OrderRowsByTimestamp)
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
        writer->Write(ReinterpretCastRange<TVersionedRow>(pullResult.Rowset->GetRows()));
        WaitFor(writer->Close())
            .ThrowOnError();
    } else {
        auto writer = CreateSchemafulWriterForFormat(format, pullResult.Rowset->GetSchema(), output);
        writer->Write(ReinterpretCastRange<TUnversionedRow>(pullResult.Rowset->GetRows()));
        WaitFor(writer->Close())
            .ThrowOnError();
    }
}

bool TPullRowsCommand::HasResponseParameters() const
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TGetInSyncReplicasCommand::TGetInSyncReplicasCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("timestamp", Options.Timestamp);
    RegisterParameter("all_keys", AllKeys)
        .Default(false);
    RegisterParameter("cached_sync_replicas_timeout", Options.CachedSyncReplicasTimeout)
        .Optional();
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

TDeleteRowsCommand::TDeleteRowsCommand()
{
    RegisterParameter("sequence_number", Options.SequenceNumber)
        .Optional();
    RegisterParameter("require_sync_replica", Options.RequireSyncReplica)
        .Optional();
    RegisterParameter("table_writer", TableWriter)
        .Default();
    RegisterParameter("path", Path);
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

TLockRowsCommand::TLockRowsCommand()
{
    RegisterParameter("table_writer", TableWriter)
        .Default();
    RegisterParameter("path", Path);
    RegisterParameter("locks", Locks);
    RegisterParameter("lock_type", LockType)
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

TTrimRowsCommand::TTrimRowsCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("tablet_index", TabletIndex);
    RegisterParameter("trimmed_row_count", TrimmedRowCount);
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

TEnableTableReplicaCommand::TEnableTableReplicaCommand()
{
    RegisterParameter("replica_id", ReplicaId);
    Options.Enabled = true;
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

TDisableTableReplicaCommand::TDisableTableReplicaCommand()
{
    RegisterParameter("replica_id", ReplicaId);
    Options.Enabled = false;
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

TAlterTableReplicaCommand::TAlterTableReplicaCommand()
{
    RegisterParameter("replica_id", ReplicaId);
    RegisterParameter("enabled", Options.Enabled)
        .Optional();
    RegisterParameter("mode", Options.Mode)
        .Optional();
    RegisterParameter("preserve_timestamps", Options.PreserveTimestamps)
        .Optional();
    RegisterParameter("atomicity", Options.Atomicity)
        .Optional();
    RegisterParameter("enable_replicated_table_tracker", Options.EnableReplicatedTableTracker)
        .Optional();
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

TGetTablePivotKeysCommand::TGetTablePivotKeysCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("represent_key_as_list", Options.RepresentKeyAsList)
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

TCreateTableBackupCommand::TCreateTableBackupCommand()
{
    RegisterParameter("manifest", Manifest);
    RegisterParameter("checkpoint_timestamp_delay", Options.CheckpointTimestampDelay)
        .Default(TDuration::Seconds(5));
    RegisterParameter("checkpoint_check_period", Options.CheckpointCheckPeriod)
        .Default(TDuration::Seconds(1));
    RegisterParameter("checkpoint_check_timeout", Options.CheckpointCheckTimeout)
        .Default(TDuration::Seconds(10));
    RegisterParameter("force", Options.Force)
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

TRestoreTableBackupCommand::TRestoreTableBackupCommand()
{
    RegisterParameter("manifest", Manifest);
    RegisterParameter("force", Options.Force)
        .Default(false);
    RegisterParameter("mount", Options.Mount)
        .Default(false);
    RegisterParameter("enable_replicas", Options.EnableReplicas)
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

TGetTabletInfosCommand::TGetTabletInfosCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("tablet_indexes", TabletIndexes);
    RegisterParameter("request_errors", Options.RequestErrors)
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

TGetTabletErrorsCommand::TGetTabletErrorsCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("limit", Options.Limit)
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
