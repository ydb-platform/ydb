#include <library/cpp/threading/future/core/future.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/zigzag.h>

#include <util/folder/tempdir.h>
#include <util/generic/buffer.h>
#include <util/stream/file.h>
#include <util/stream/str.h>

#include <util/system/shellcommand.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_fmr_sorting_block_reader.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_job_impl.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_reader.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_sorted_writer.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_writer.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/tvm/impl/yql_yt_fmr_tvm_impl.h>
#include <yt/yql/providers/yt/fmr/utils/hasher/yql_yt_binary_yson_hasher.h>
#include <yt/yql/providers/yt/fmr/utils/yson_block_iterator/impl/yql_yt_yson_tds_block_iterator.h>
#include <yt/yql/providers/yt/fmr/utils/yson_block_iterator/impl/yql_yt_yson_yt_block_iterator.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_parse_records.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_table_input_streams.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/impl/yql_yt_job_service_impl.h>
#include <yt/yql/providers/yt/fmr/request_options/proto_helpers/yql_yt_request_proto_helpers.h>

#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

namespace {

// Encodes a ui64 as a base-128 varint and appends to buf.
void WriteVarint64(TBuffer& buf, ui64 value) {
    do {
        ui8 byte = value & 0x7F;
        value >>= 7;
        if (value != 0) {
            byte |= 0x80;
        }
        buf.Append(static_cast<char>(byte));
    } while (value != 0);
}

// Encodes a non-negative i32 length using zigzag+varint32 (as binary YSON uses for strings).
void WriteStringLengthVarint(TBuffer& buf, i32 length) {
    ui32 encoded = NYson::ZigZagEncode32(length);
    do {
        ui8 byte = encoded & 0x7F;
        encoded >>= 7;
        if (encoded != 0) {
            byte |= 0x80;
        }
        buf.Append(static_cast<char>(byte));
    } while (encoded != 0);
}

// Build binary YSON: { _yql_key_hash=<hash>; <original_map_contents> }
// originalRowBytes must start with '{' (BeginMapSymbol).
TBuffer BuildRowWithKeyHash(TStringBuf originalRowBytes, ui64 hash) {
    using namespace NYson::NDetail;
    Y_ENSURE(!originalRowBytes.empty() && originalRowBytes[0] == BeginMapSymbol,
             "Expected binary YSON map row starting with '{'");

    TBuffer row;
    row.Append(BeginMapSymbol);

    // Key: \x01 + zigzag_varint32(14) + "_yql_key_hash" + '='
    row.Append(StringMarker);
    WriteStringLengthVarint(row, static_cast<i32>(YqlKeyHashColumn.size()));
    row.Append(YqlKeyHashColumn.data(), YqlKeyHashColumn.size());
    row.Append(KeyValueSeparatorSymbol);
    // Value: \x06 + varint64(hash)
    row.Append(Uint64Marker);
    WriteVarint64(row, hash);

    // Append original map content after the opening '{'.
    TStringBuf rest = originalRowBytes.substr(1);
    if (!rest.empty() && rest[0] != EndMapSymbol) {
        row.Append(ListItemSeparatorSymbol);
    }
    row.Append(rest.data(), rest.size());

    return row;
}

// Wraps an IBlockIterator and inserts the computed _yql_key_hash column into each row.
// The inner iterator must track only the reduce key columns (no _yql_key_hash).
// This iterator presents rows with the full sort columns [_yql_key_hash, ...reduceBy].
class TKeyHashAddingBlockIterator final: public IBlockIterator {
public:
    TKeyHashAddingBlockIterator(
        IBlockIterator::TPtr inner,
        std::vector<TString> fullSortColumns,
        std::vector<ESortOrder> sortOrders
    )
        : Inner_(std::move(inner))
        , FullSortColumns_(std::move(fullSortColumns))
        , SortOrders_(std::move(sortOrders))
    {
        Y_ENSURE(!FullSortColumns_.empty() && FullSortColumns_[0] == TString(YqlKeyHashColumn),
                 "_yql_key_hash must be the first sort column");
        NumReduceKeyColumns_ = FullSortColumns_.size() - 1;
    }

    bool NextBlock(TIndexedBlock& out) final {
        TIndexedBlock raw;
        if (!Inner_->NextBlock(raw)) {
            return false;
        }
        if (raw.Rows.empty()) {
            out = std::move(raw);
            return true;
        }

        TBuffer newData;
        newData.Reserve(raw.Data.size() + raw.Rows.size() * 20);

        for (size_t rowIdx = 0; rowIdx < raw.Rows.size(); ++rowIdx) {
            const TRowIndexMarkup& markup = raw.Rows[rowIdx];
            const TColumnOffsetRange& rowRange = markup.back();
            TStringBuf rowBytes(raw.Data.data() + rowRange.StartOffset,
                                rowRange.EndOffset - rowRange.StartOffset);

            ui64 hash = HashKeyColumns(raw.Data, markup, NumReduceKeyColumns_);
            TBuffer newRow = BuildRowWithKeyHash(rowBytes, hash);

            newData.Append(newRow.Data(), newRow.Size());
            newData.Append(NYson::NDetail::ListItemSeparatorSymbol);
        }

        out.Data = TString(newData.Data(), newData.Size());
        TParserFragmentListIndex parser(out.Data, FullSortColumns_);
        parser.Parse();
        out.Rows = parser.GetRows();

        return true;
    }

    std::vector<ESortOrder> GetSortOrder() final {
        return SortOrders_;
    }

private:
    IBlockIterator::TPtr Inner_;
    std::vector<TString> FullSortColumns_;
    std::vector<ESortOrder> SortOrders_;
    size_t NumReduceKeyColumns_ = 0;
};

} // namespace

class TFmrJob: public IFmrJob {
public:
    TFmrJob(
        ITableDataServiceDiscovery::TPtr discovery,
        TMaybe<TVanillaInfo> vanillaInfo,
        IYtJobService::TPtr ytJobService,
        TFmrUserJobLauncher::TPtr jobLauncher,
        const TFmrJobSettings& settings,
        const TMaybe<TFmrTvmJobSettings>& tvmSettings = Nothing()
    )
        : Discovery_(std::move(discovery))
        , VanillaInfo_(std::move(vanillaInfo))
        , YtJobService_(ytJobService)
        , JobLauncher_(jobLauncher)
        , Settings_(settings)
        , TvmSettings_(tvmSettings)
    {
        InitTableDataService(tvmSettings);
    }

    // Intraprocess constructor: bypasses HTTP, uses the passed TDS directly.
    TFmrJob(
        ITableDataService::TPtr tableDataService,
        IYtJobService::TPtr ytJobService,
        TFmrUserJobLauncher::TPtr jobLauncher,
        const TFmrJobSettings& settings
    )
        : DirectTableDataService_(std::move(tableDataService))
        , YtJobService_(ytJobService)
        , JobLauncher_(jobLauncher)
        , Settings_(settings)
    {
        TableDataService_ = DirectTableDataService_;
    }

    virtual std::variant<TFmrError, TStatistics> Download(
        const TDownloadTaskParams& params,
        const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections,
        std::shared_ptr<std::atomic<bool>> cancelFlag
    ) override {
        auto downloadJobFunc = [&, cancelFlag] () -> TStatistics {
            const auto ytTableTaskRef = params.Input;
            const auto output = params.Output;
            const auto tableId = output.TableId;
            const auto partId = output.PartId;

            YQL_ENSURE(clusterConnections.size() == 1);

            std::vector<NYT::TRawTableReaderPtr> ytTableReaders = GetYtTableReaders(YtJobService_, ytTableTaskRef, clusterConnections);
            auto tableDataServiceWriter = MakeIntrusive<TFmrTableDataServiceWriter>(tableId, partId, TableDataService_, output.SerializedColumnGroups, Settings_.FmrWriterSettings);

            for (auto& ytTableReader: ytTableReaders) {
                ParseRecords(ytTableReader, tableDataServiceWriter, Settings_.ParseRecordSettings.DonwloadReadBlockCount, Settings_.ParseRecordSettings.DonwloadReadBlockSize, cancelFlag);
            }
            tableDataServiceWriter->Flush();

            TTableChunkStats stats = tableDataServiceWriter->GetStats();
            return TStatistics({{output, stats}});
        };
        return HandleFmrJob(downloadJobFunc, ETaskType::Download);
    }

    virtual std::variant<TFmrError, TStatistics> Upload(
        const TUploadTaskParams& params,
        const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections,
        std::shared_ptr<std::atomic<bool>> cancelFlag
    ) override {
        auto uploadJobFunc = [&, cancelFlag] () -> TStatistics {
            const auto ytTable = params.Output;
            const auto tableId = params.Input.TableId;
            const auto tableRanges = params.Input.TableRanges;
            const auto neededColumns = params.Input.Columns;
            const auto columnGroups = params.Input.SerializedColumnGroups;

            auto tableDataServiceReader = MakeIntrusive<TFmrTableDataServiceReader>(tableId, tableRanges, TableDataService_, neededColumns, columnGroups, Settings_.FmrReaderSettings);
            YQL_ENSURE(clusterConnections.size() == 1);
            auto& connection = clusterConnections.begin()->second;
            auto ytTableWriter = YtJobService_->MakeWriter(ytTable, connection, Settings_.YtWriterSettings);
            auto& parseRecordSettings = Settings_.ParseRecordSettings;
            if (parseRecordSettings.UploadNumThreads > 1) {
                ParseRecordsPipelined(tableDataServiceReader, ytTableWriter, parseRecordSettings.UploadReadBlockCount, parseRecordSettings.UploadReadBlockSize, Settings_.RawTableQueueSettings, cancelFlag);
            } else {
                ParseRecords(tableDataServiceReader, ytTableWriter, parseRecordSettings.UploadReadBlockCount, parseRecordSettings.UploadReadBlockSize, cancelFlag);
            }
            ytTableWriter->Flush();

            return TStatistics(); // TODO - get actual stats from yt table.
        };
        return HandleFmrJob(uploadJobFunc, ETaskType::Upload);
    }

    virtual std::variant<TFmrError, TStatistics> SortedUpload(
        const TSortedUploadTaskParams& params,
        const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections,
        std::shared_ptr<std::atomic<bool>> cancelFlag
    ) override {
        auto sortedUploadJobFunc = [&, cancelFlag] () -> TStatistics {
            const auto& input = params.Input;
            const auto tableId = input.TableId;
            const auto& tableRanges = input.TableRanges;
            const auto& neededColumns = input.Columns;
            const auto& columnGroups = input.SerializedColumnGroups;
            const auto order = params.Order;
            const auto& sortingColumns = params.SortingColumns;

            YQL_ENSURE(clusterConnections.size() == 1);
            const auto& clusterConnection = clusterConnections.begin()->second;

            NYT::TRawTableReaderPtr reader;
            bool hasSortingColumns = !sortingColumns.Columns.empty();

            if (hasSortingColumns) {
                std::vector<IBlockIterator::TPtr> blockIterators;
                for (const auto& range : tableRanges) {
                    std::vector<TTableRange> singleRange = {range};
                    blockIterators.push_back(MakeIntrusive<TTableDataServiceBlockIterator>(
                        tableId,
                        singleRange,
                        TableDataService_,
                        sortingColumns.Columns,
                        sortingColumns.SortOrders,
                        neededColumns,
                        columnGroups,
                        input.IsFirstRowInclusive,
                        input.IsLastRowInclusive,
                        input.FirstRowKeys,
                        input.LastRowKeys,
                        Settings_.FmrReaderSettings.ReadAheadChunks
                    ));
                }
                reader = MakeIntrusive<TSortedMergeReader>(blockIterators);
            } else {
                reader = MakeIntrusive<TFmrTableDataServiceReader>(
                    tableId, tableRanges, TableDataService_, neededColumns, columnGroups, Settings_.FmrReaderSettings);
            }

            auto writer = YtJobService_->GetDistributedWriter(
                params.CookieYson,
                clusterConnection
            );
            StreamBulkToYtDistributed(
                reader,
                *writer,
                Settings_.ParseRecordSettings.UploadReadBlockSize,
                cancelFlag);
            writer->Finish();

            auto fragmentResult = writer->GetResponse();
            TString fragmentResultYson = NYT::NodeToYsonString(fragmentResult);
            TStatistics stats;
            stats.TaskResult = TTaskSortedUploadResult{
                .FragmentResultYson = fragmentResultYson,
                .FragmentOrder = order
            };
            return stats;
        };
        return HandleFmrJob(sortedUploadJobFunc, ETaskType::SortedUpload);
    }

    virtual std::variant<TFmrError, TStatistics> Merge(
        const TMergeTaskParams& params,
        const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections,
        std::shared_ptr<std::atomic<bool>> cancelFlag
    ) override {
        auto mergeJobFunc = [&, cancelFlag] () -> TStatistics {
            const auto taskTableInputRef = params.Input;
            const auto output = params.Output;

            auto& parseRecordSettings = Settings_.ParseRecordSettings;

            auto tableDataServiceWriter = MakeIntrusive<TFmrTableDataServiceWriter>(output.TableId, output.PartId, TableDataService_, output.SerializedColumnGroups, Settings_.FmrWriterSettings);
            auto threadPool = CreateThreadPool(parseRecordSettings.MergeNumThreads, parseRecordSettings.MaxQueueSize, TThreadPool::TParams().SetBlocking(true).SetCatching(true));
            TMaybe<TMutex> mutex = TMutex();
            std::exception_ptr mergeException;
            for (const auto& inputTableRef : taskTableInputRef.Inputs) {
                threadPool->SafeAddFunc([&, tableDataServiceWriter] {
                    try {
                        auto inputTableReaders = GetTableInputStreams(YtJobService_, TableDataService_, inputTableRef, clusterConnections, Settings_.FmrReaderSettings);
                        for (auto& tableReader: inputTableReaders) {
                            ParseRecords(tableReader, tableDataServiceWriter, parseRecordSettings.MergeReadBlockCount, parseRecordSettings.MergeReadBlockSize, cancelFlag, mutex);
                        }
                    } catch (...) {
                        mergeException = std::current_exception();
                    }
                });
            }
            threadPool->Stop();

            if (mergeException) {
                std::rethrow_exception(mergeException);
            }

            tableDataServiceWriter->Flush();
            return TStatistics({{output, tableDataServiceWriter->GetStats()}});
        };
        return HandleFmrJob(mergeJobFunc, ETaskType::Merge);
    }

    virtual std::variant<TFmrError, TStatistics> SortedMerge(
        const TSortedMergeTaskParams& params,
        const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections,
        std::shared_ptr<std::atomic<bool>> cancelFlag
    ) override {
        auto sortedMergeJobFunc = [&, cancelFlag] () -> TStatistics {
            const auto taskTableInputRef = params.Input;
            const auto output = params.Output;

            auto& parseRecordSettings = Settings_.ParseRecordSettings;
            YQL_ENSURE(!output.SortingColumns.Columns.empty(), "SortedMerge output key columns must be set");

            auto writerSettings = Settings_.FmrWriterSettings;
            auto tableDataServiceWriter = MakeIntrusive<TFmrTableDataServiceSortedWriter>(
                output.TableId,
                output.PartId,
                TableDataService_,
                output.SerializedColumnGroups,
                writerSettings,
                output.SortingColumns
            );
            TMaybe<TMutex> mutex = TMutex();
            std::vector<IBlockIterator::TPtr> blockIterators;
            std::vector<NYT::TRawTableReaderPtr> ytReaders;

            for (const auto& inputTableRef : taskTableInputRef.Inputs) {
                if (const auto* fmrInput = std::get_if<TFmrTableInputRef>(&inputTableRef)) {
                    blockIterators.push_back(MakeIntrusive<TTableDataServiceBlockIterator>(
                        fmrInput->TableId,
                        fmrInput->TableRanges,
                        TableDataService_,
                        output.SortingColumns.Columns,
                        output.SortingColumns.SortOrders,
                        fmrInput->Columns,
                        fmrInput->SerializedColumnGroups,
                        fmrInput->IsFirstRowInclusive,
                        fmrInput->IsLastRowInclusive,
                        fmrInput->FirstRowKeys,
                        fmrInput->LastRowKeys,
                        Settings_.FmrReaderSettings.ReadAheadChunks
                    ));
                } else {
                    const auto& ytTableTaskRef = std::get<TYtTableTaskRef>(inputTableRef);
                    auto readers = GetYtTableReaders(YtJobService_, ytTableTaskRef, clusterConnections);
                    for (auto& reader : readers) {
                        ytReaders.push_back(std::move(reader));
                    }
                }
            }

            YQL_ENSURE(blockIterators.empty() || ytReaders.empty(),
                "SortedMerge task cannot mix FMR and YT table inputs");

            if (!ytReaders.empty()) {
                // Single YT table case: input is already sorted, read directly
                for (auto& ytReader : ytReaders) {
                    ParseRecords(ytReader, tableDataServiceWriter, parseRecordSettings.MergeReadBlockCount, parseRecordSettings.MergeReadBlockSize, cancelFlag, mutex);
                }
            } else {
                NYT::TRawTableReaderPtr mergeReader = MakeIntrusive<TSortedMergeReader>(blockIterators);
                ParseRecords(mergeReader, tableDataServiceWriter, parseRecordSettings.MergeReadBlockCount, parseRecordSettings.MergeReadBlockSize, cancelFlag, mutex);
            }

            tableDataServiceWriter->Flush();
            return TStatistics({{output, tableDataServiceWriter->GetStats()}});
        };
        return HandleFmrJob(sortedMergeJobFunc, ETaskType::SortedMerge);
    }

    virtual std::variant<TFmrError, TStatistics> Map(
        const TMapTaskParams& params,
        const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections,
        std::shared_ptr<std::atomic<bool>> /* cancelFlag */,
        const TMaybe<TString>& jobEnvironmentDir,
        const std::vector<TFileInfo>& jobFiles,
        const std::vector<TYtResourceInfo>& jobYtResources,
        const std::vector<TFmrResourceTaskInfo>& jobFmrResources
    ) override {
        auto mapJobFunc = [&, this] () {
            TFmrUserJobSettings userJobSettings = Settings_.FmrUserJobSettings;
            TFmrUserJob mapJob;
            // deserialize map job and fill params
            TStringStream serializedJobStateStream(params.SerializedMapJobState);
            mapJob.Load(serializedJobStateStream);
            FillMapFmrJob(mapJob, params, clusterConnections, Discovery_, VanillaInfo_, userJobSettings, YtJobService_);
            mapJob.SetTvmSettings(TvmSettings_);
            if (DirectTableDataService_) {
                mapJob.SetDirectTableDataService(DirectTableDataService_);
            }
            return JobLauncher_->LaunchJob(mapJob, jobEnvironmentDir, jobFiles, jobYtResources, jobFmrResources);
        };
        return HandleFmrJob(mapJobFunc, ETaskType::Map);
    }
    // TODO - figure out how to how to use cancel flag to kill map job.


    std::variant<TFmrError, TStatistics> LocalSort(
        const TLocalSortTaskParams& params,
        const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections = {},
        std::shared_ptr<std::atomic<bool>> cancelFlag = nullptr
    ) override {
        auto localSortJobFunc = [&, this, cancelFlag] () -> TStatistics {
            const auto taskTableInputRef = params.Input;
            const auto output = params.Output;

            auto& parseRecordSettings = Settings_.ParseRecordSettings;
            YQL_ENSURE(!output.SortingColumns.Columns.empty(), "Local sort output key columns must be set");

            auto writerSettings = Settings_.FmrWriterSettings;
            auto tableDataServiceWriter = MakeIntrusive<TFmrTableDataServiceSortedWriter>(
                output.TableId,
                output.PartId,
                TableDataService_,
                output.SerializedColumnGroups,
                writerSettings,
                output.SortingColumns
            );
            TMaybe<TMutex> mutex = TMutex();
            std::vector<IBlockIterator::TPtr> blockIterators;
            for (const auto& inputTableRef : taskTableInputRef.Inputs) {
                if (auto fmrInput = std::get_if<TFmrTableInputRef>(&inputTableRef)) {
                    blockIterators.emplace_back(MakeIntrusive<TTableDataServiceBlockIterator>(
                        fmrInput->TableId,
                        fmrInput->TableRanges,
                        TableDataService_,
                        output.SortingColumns.Columns,
                        output.SortingColumns.SortOrders,
                        fmrInput->Columns,
                        fmrInput->SerializedColumnGroups,
                        fmrInput->IsFirstRowInclusive,
                        fmrInput->IsLastRowInclusive,
                        fmrInput->FirstRowKeys,
                        fmrInput->LastRowKeys,
                        Settings_.FmrReaderSettings.ReadAheadChunks
                    ));
                } else {
                    auto ytTableTaskRef = std::get<TYtTableTaskRef>(inputTableRef);
                    auto ytReaders = GetYtTableReaders(YtJobService_, ytTableTaskRef, clusterConnections);
                    blockIterators.emplace_back(MakeIntrusive<TYtBlockIterator>(
                        ytReaders,
                        output.SortingColumns.Columns,
                        TYtBlockIteratorSettings(), // TODO - support parsing TYtBlockIteratorSettings from yson file.
                        output.SortingColumns.SortOrders
                    ));
                }
            }

            NYT::TRawTableReaderPtr sortingReader = MakeIntrusive<TFmrSortingBlockReader>(blockIterators);
            ParseRecords(sortingReader, tableDataServiceWriter, parseRecordSettings.LocalSortBlockCount, parseRecordSettings.LocalSortBlockSize, cancelFlag, mutex);

            tableDataServiceWriter->Flush();
            return TStatistics({{output, tableDataServiceWriter->GetStats()}});
        };
        return HandleFmrJob(localSortJobFunc, ETaskType::LocalSort);
    }

    std::variant<TFmrError, TStatistics> Reduce(
        const TReduceTaskParams& params,
        const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections,
        std::shared_ptr<std::atomic<bool>> /* cancelFlag */,
        const TMaybe<TString>& jobEnvironmentDir,
        const std::vector<TFileInfo>& jobFiles,
        const std::vector<TYtResourceInfo>& jobYtResources,
        const std::vector<TFmrResourceTaskInfo>& jobFmrResources
    ) override {
        auto reduceFunc = [&, this] () {
            TFmrUserJobSettings userJobSettings = Settings_.FmrUserJobSettings;
            TFmrUserJob reduceJob;
            // deserialize reduce job and fill params
            TStringStream serializedJobStateStream(params.SerializedReduceJobState);
            reduceJob.Load(serializedJobStateStream);
            FillReduceFmrJob(reduceJob, params, clusterConnections, Discovery_, VanillaInfo_, userJobSettings, YtJobService_);
            reduceJob.SetTvmSettings(TvmSettings_);
            if (DirectTableDataService_) {
                reduceJob.SetDirectTableDataService(DirectTableDataService_);
            }
            return JobLauncher_->LaunchJob(reduceJob, jobEnvironmentDir, jobFiles, jobYtResources, jobFmrResources);
        };
        return HandleFmrJob(reduceFunc, ETaskType::Reduce);
    }

    std::variant<TFmrError, TStatistics> Fill(
        const TFillTaskParams& params,
        std::shared_ptr<std::atomic<bool>> /* cancelFlag */,
        const TMaybe<TString>& jobEnvironmentDir,
        const std::vector<TFileInfo>& jobFiles,
        const std::vector<TYtResourceInfo>& jobYtResources,
        const std::vector<TFmrResourceTaskInfo>& jobFmrResources
    ) override {
        auto fillJobFunc = [&, this] () {
            TFmrUserJobSettings userJobSettings = Settings_.FmrUserJobSettings;
            TFmrUserJob fillJob;
            TStringStream serializedJobStateStream(params.SerializedFillJobState);
            fillJob.Load(serializedJobStateStream);
            FillFillFmrJob(fillJob, params, Discovery_, VanillaInfo_, userJobSettings, YtJobService_);
            fillJob.SetTvmSettings(TvmSettings_);
            if (DirectTableDataService_) {
                fillJob.SetDirectTableDataService(DirectTableDataService_);
            }
            return JobLauncher_->LaunchJob(fillJob, jobEnvironmentDir, jobFiles, jobYtResources, jobFmrResources);
        };
        return HandleFmrJob(fillJobFunc, ETaskType::Fill);
    }

    std::variant<TFmrError, TStatistics> MapReduceMap(
        const TMapReduceMapTaskParams& params,
        const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections,
        std::shared_ptr<std::atomic<bool>> cancelFlag,
        const TMaybe<TString>& jobEnvironmentDir,
        const std::vector<TFileInfo>& jobFiles,
        const std::vector<TYtResourceInfo>& jobYtResources,
        const std::vector<TFmrResourceTaskInfo>& jobFmrResources
    ) override {
        if (params.SerializedMapJobState.empty()) {
            // Identity (TCoVoid) mapper: sort input by reduce-by columns and write to intermediate.
            // _yql_key_hash is used only to determine physical ordering of rows but is never stored
            // in the row data — the codec must not see it.
            auto identityJobFunc = [&, this, cancelFlag] () -> TStatistics {
                const auto& sortColumns = params.Output.SortingColumns;
                YQL_ENSURE(!sortColumns.Columns.empty(), "MapReduceMap identity sort columns must be set");

                auto writerSettings = Settings_.FmrWriterSettings;
                auto tableDataServiceWriter = MakeIntrusive<TFmrTableDataServiceSortedWriter>(
                    params.Output.TableId, params.Output.PartId,
                    TableDataService_, params.Output.SerializedColumnGroups,
                    writerSettings, sortColumns
                );
                TMaybe<TMutex> mutex = TMutex();
                std::vector<IBlockIterator::TPtr> blockIterators;

                // The full sort columns are [_yql_key_hash, ...reduceBy].
                // Inner iterators track only the reduce key columns; TKeyHashAddingBlockIterator
                // computes _yql_key_hash from those columns and inserts it into the binary YSON.
                Y_ENSURE(sortColumns.Columns[0] == TString(YqlKeyHashColumn),
                         "First sort column must be _yql_key_hash in MapReduceMap identity path");
                std::vector<TString> reduceKeyColumns(sortColumns.Columns.begin() + 1, sortColumns.Columns.end());
                std::vector<ESortOrder> reduceKeySortOrders(sortColumns.SortOrders.begin() + 1, sortColumns.SortOrders.end());

                for (const auto& inputRef : params.Input.Inputs) {
                    IBlockIterator::TPtr inner;
                    if (auto fmrInput = std::get_if<TFmrTableInputRef>(&inputRef)) {
                        inner = MakeIntrusive<TTableDataServiceBlockIterator>(
                            fmrInput->TableId, fmrInput->TableRanges, TableDataService_,
                            reduceKeyColumns, reduceKeySortOrders,
                            fmrInput->Columns, fmrInput->SerializedColumnGroups,
                            fmrInput->IsFirstRowInclusive, fmrInput->IsLastRowInclusive,
                            fmrInput->FirstRowKeys, fmrInput->LastRowKeys,
                            Settings_.FmrReaderSettings.ReadAheadChunks
                        );
                    } else {
                        auto ytTableTaskRef = std::get<TYtTableTaskRef>(inputRef);
                        auto ytReaders = GetYtTableReaders(YtJobService_, ytTableTaskRef, clusterConnections);
                        inner = MakeIntrusive<TYtBlockIterator>(
                            ytReaders, reduceKeyColumns, TYtBlockIteratorSettings(), reduceKeySortOrders
                        );
                    }
                    blockIterators.emplace_back(MakeIntrusive<TKeyHashAddingBlockIterator>(
                        std::move(inner), sortColumns.Columns, sortColumns.SortOrders
                    ));
                }
                auto& parseRecordSettings = Settings_.ParseRecordSettings;
                NYT::TRawTableReaderPtr sortingReader = MakeIntrusive<TFmrSortingBlockReader>(blockIterators);
                ParseRecords(sortingReader, tableDataServiceWriter, parseRecordSettings.LocalSortBlockCount, parseRecordSettings.LocalSortBlockSize, cancelFlag, mutex);
                tableDataServiceWriter->Flush();
                return TStatistics({{params.Output, tableDataServiceWriter->GetStats()}});
            };
            return HandleFmrJob(identityJobFunc, ETaskType::MapReduceMap);
        }

        // Explicit mapper: two-phase approach.
        // TMkqlWriterImpl uses block-encoded YSON that TFmrTableDataServiceSortedWriter's
        // row-level parser cannot interpret. So we write the mapper output to an unsorted
        // temp partition first, then sort it via TFmrSortingBlockReader (same as LocalSort).
        auto mapReduceMapJobFunc = [&, this, cancelFlag] () -> TStatistics {
            const auto& sortColumns = params.Output.SortingColumns;
            YQL_ENSURE(!sortColumns.Columns.empty(), "MapReduceMap sort columns must be set");

            TFmrUserJobSettings userJobSettings = Settings_.FmrUserJobSettings;
            TFmrUserJob mapReduceMapJob;
            TStringStream serializedJobStateStream(params.SerializedMapJobState);
            mapReduceMapJob.Load(serializedJobStateStream);

            // Phase 1: run the mapper into a temp unsorted partition.
            const TString tempPartId = params.Output.PartId + "_tmp";
            TFmrTableOutputRef unsortedOutput;
            unsortedOutput.TableId = params.Output.TableId;
            unsortedOutput.PartId = tempPartId;
            unsortedOutput.SerializedColumnGroups = params.Output.SerializedColumnGroups;
            // SortingColumns intentionally empty → TFmrTableDataServiceWriter (not sorted)

            mapReduceMapJob.SetSettings(userJobSettings);
            if (VanillaInfo_.Defined()) {
                mapReduceMapJob.SetVanillaInfo(*VanillaInfo_);
            }
            if (Discovery_) {
                mapReduceMapJob.SetTableDataServiceDiscovery(Discovery_);
            }
            mapReduceMapJob.SetTaskInputTables(params.Input);
            mapReduceMapJob.SetTaskFmrOutputTables({unsortedOutput});
            mapReduceMapJob.SetClusterConnections(clusterConnections);
            mapReduceMapJob.SetYtJobService(YtJobService_);
            mapReduceMapJob.SetFmrJobType(EFmrJobType::Map);
            if (TvmSettings_) {
                mapReduceMapJob.SetTvmSettings(*TvmSettings_);
            }
            if (DirectTableDataService_) {
                mapReduceMapJob.SetDirectTableDataService(DirectTableDataService_);
            }
            YQL_CLOG(INFO, FastMapReduce) << "MapReduceMap explicit mapper: calling LaunchJob"
                << " numInputs=" << params.Input.Inputs.size()
                << " outputTableId=" << unsortedOutput.TableId
                << " outputPartId=" << unsortedOutput.PartId
                << " sortingColumnsSize=" << unsortedOutput.SortingColumns.Columns.size();
            auto mapResult = JobLauncher_->LaunchJob(mapReduceMapJob, jobEnvironmentDir, jobFiles, jobYtResources, jobFmrResources);
            if (auto* err = std::get_if<TFmrError>(&mapResult)) {
                ythrow yexception() << "MapReduceMap phase 1 (mapper) failed: " << err->ErrorMessage;
            }
            const auto& mapStats = std::get<TStatistics>(mapResult);
            YQL_CLOG(INFO, FastMapReduce) << "MapReduceMap explicit mapper: phase 1 done"
                << " tempPartId=" << tempPartId
                << " outputTables=" << mapStats.OutputTables.size();
            for (const auto& [ref, stats] : mapStats.OutputTables) {
                YQL_CLOG(INFO, FastMapReduce) << "  table=" << ref.TableId << " partId=" << ref.PartId
                    << " chunks=" << stats.PartIdChunkStats.size();
            }

            // Phase 2: sort the temp partition into the final sorted partition.
            auto writerSettings = Settings_.FmrWriterSettings;
            auto sortedWriter = MakeIntrusive<TFmrTableDataServiceSortedWriter>(
                params.Output.TableId, params.Output.PartId,
                TableDataService_, params.Output.SerializedColumnGroups,
                writerSettings, sortColumns
            );
            TMaybe<TMutex> mutex = TMutex();

            // Read all chunks of the temp partition using the exact count from phase 1 stats.
            auto tempStatsIt = mapStats.OutputTables.find(unsortedOutput);
            Y_ENSURE(tempStatsIt != mapStats.OutputTables.end(),
                "Phase 1 stats missing for temp partition " << tempPartId);
            ui64 tempChunkCount = tempStatsIt->second.PartIdChunkStats.size();
            TFmrTableInputRef tempInputRef;
            tempInputRef.TableId = unsortedOutput.TableId;
            tempInputRef.TableRanges = {{tempPartId, 0, tempChunkCount}};

            // sortColumns = [_yql_key_hash, ...reduceBy]. The mapper output does not contain
            // _yql_key_hash, so read using only the reduce-key columns and inject the hash
            // via TKeyHashAddingBlockIterator — same as the identity mapper path.
            Y_ENSURE(!sortColumns.Columns.empty() && sortColumns.Columns[0] == TString(YqlKeyHashColumn),
                "First sort column must be _yql_key_hash in MapReduceMap explicit mapper Phase 2");
            std::vector<TString> reduceKeyColumns(sortColumns.Columns.begin() + 1, sortColumns.Columns.end());
            std::vector<ESortOrder> reduceKeySortOrders(sortColumns.SortOrders.begin() + 1, sortColumns.SortOrders.end());

            std::vector<IBlockIterator::TPtr> blockIterators;
            blockIterators.emplace_back(MakeIntrusive<TKeyHashAddingBlockIterator>(
                MakeIntrusive<TTableDataServiceBlockIterator>(
                    tempInputRef.TableId, tempInputRef.TableRanges, TableDataService_,
                    reduceKeyColumns, reduceKeySortOrders,
                    tempInputRef.Columns, tempInputRef.SerializedColumnGroups,
                    Nothing(), Nothing(), Nothing(), Nothing(),
                    Settings_.FmrReaderSettings.ReadAheadChunks
                ),
                sortColumns.Columns, sortColumns.SortOrders
            ));

            auto& parseRecordSettings = Settings_.ParseRecordSettings;
            NYT::TRawTableReaderPtr sortingReader = MakeIntrusive<TFmrSortingBlockReader>(blockIterators);
            ParseRecords(sortingReader, sortedWriter, parseRecordSettings.LocalSortBlockCount, parseRecordSettings.LocalSortBlockSize, cancelFlag, mutex);
            sortedWriter->Flush();

            // Best-effort cleanup of the temp partition.
            TableDataService_->RegisterDeletion({tempPartId}).GetValueSync();

            return TStatistics({{params.Output, sortedWriter->GetStats()}});
        };
        return HandleFmrJob(mapReduceMapJobFunc, ETaskType::MapReduceMap);
    }

    std::variant<TFmrError, TString> Pull(
        const TPullTaskParams& params,
        std::shared_ptr<std::atomic<bool>> cancelFlag
    ) override {
        try {
            TString data;
            TStringOutput output(data);
            for (const auto& taskTableRef : params.Input.Inputs) {
                auto& fmrInputRef = std::get<TFmrTableInputRef>(taskTableRef);
                auto reader = MakeIntrusive<TFmrTableDataServiceReader>(
                    fmrInputRef.TableId,
                    fmrInputRef.TableRanges,
                    TableDataService_,
                    fmrInputRef.Columns,
                    fmrInputRef.SerializedColumnGroups,
                    Settings_.FmrReaderSettings
                );
                TBuffer buf(Settings_.ParseRecordSettings.DonwloadReadBlockSize);
                while (true) {
                    if (cancelFlag && cancelFlag->load()) {
                        throw yexception() << "Pull job cancelled";
                    }
                    size_t bytesRead = reader->Read(buf.Data(), buf.Capacity());
                    if (bytesRead == 0) {
                        break;
                    }
                    output.Write(buf.Data(), bytesRead);
                }
            }
            return std::move(data);
        } catch (...) {
            TString errorMessage = CurrentExceptionMessage();
            EFmrErrorReason reason = ParseFmrReasonFromErrorMessage(errorMessage);
            YQL_CLOG(ERROR, FastMapReduce) << "Exception inside fmr Pull job: " << errorMessage;
            return TFmrError{.Reason = reason, .ErrorMessage = errorMessage};
        }
    }

private:
    std::variant<TFmrError, TStatistics> HandleFmrJob(auto fmrJobFunc, ETaskType fmrJobType) {
        TString errorLogMessage;
        EFmrErrorReason errorReason;
        try {
            return fmrJobFunc();
        } catch (...) {
            errorLogMessage = CurrentExceptionMessage();
            errorReason = ParseFmrReasonFromErrorMessage(errorLogMessage);
        }
        YQL_CLOG(ERROR, FastMapReduce) << "Gotten exception inside fmr " << fmrJobType << " job with message " << errorLogMessage << " and reason " << errorReason;
        return TFmrError{.Reason = errorReason, .ErrorMessage = errorLogMessage};
    }

private:
    void InitTableDataService(const TMaybe<TFmrTvmJobSettings>& tvmSettings) {
        if (tvmSettings.Defined()) {
            TvmClient_ = MakeFmrTvmClient({
                .SourceTvmAlias = tvmSettings->WorkerTvmAlias,
                .TvmPort = tvmSettings->TvmPort,
                .TvmSecret = tvmSettings->TvmSecret
            });
            TableDataServiceTvmId_ = tvmSettings->TableDataServiceTvmId;
        }
        TableDataService_ = MakeTableDataServiceClient(Discovery_, TvmClient_, TableDataServiceTvmId_);
    }

    ITableDataService::TPtr TableDataService_; // Table data service (http client or direct intraprocess)
    ITableDataService::TPtr DirectTableDataService_; // Set for intraprocess mode, propagated to user jobs
    ITableDataServiceDiscovery::TPtr Discovery_;
    TMaybe<TVanillaInfo> VanillaInfo_;
    IYtJobService::TPtr YtJobService_;
    TFmrUserJobLauncher::TPtr JobLauncher_;
    TFmrJobSettings Settings_;
    IFmrTvmClient::TPtr TvmClient_ = nullptr;
    ui32 TableDataServiceTvmId_ = 0;
    TMaybe<TFmrTvmJobSettings> TvmSettings_;
};

IFmrJob::TPtr MakeFmrJob(
    ITableDataServiceDiscovery::TPtr discovery,
    TMaybe<TVanillaInfo> vanillaInfo,
    IYtJobService::TPtr ytJobService,
    TFmrUserJobLauncher::TPtr jobLauncher,
    const TFmrJobSettings& settings,
    const TMaybe<TFmrTvmJobSettings>& workerTvmSettings
) {
    return MakeIntrusive<TFmrJob>(std::move(discovery), std::move(vanillaInfo), ytJobService, jobLauncher, settings, workerTvmSettings);
}

IFmrJob::TPtr MakeFmrJob(
    ITableDataService::TPtr tableDataService,
    IYtJobService::TPtr ytJobService,
    TFmrUserJobLauncher::TPtr jobLauncher,
    const TFmrJobSettings& settings
) {
    return MakeIntrusive<TFmrJob>(std::move(tableDataService), ytJobService, jobLauncher, settings);
}

namespace {

// Encapsulates the two job-creation modes so RunJobImpl can be written once.
struct TDiscoveryJobSource {
    ITableDataServiceDiscovery::TPtr Discovery;
    TMaybe<TVanillaInfo> VanillaInfo;
    TMaybe<TFmrTvmJobSettings> TvmSettings;
};

struct TDirectTdsJobSource {
    ITableDataService::TPtr TableDataService;
};

using TJobSource = std::variant<TDiscoveryJobSource, TDirectTdsJobSource>;

TJobResult RunJobImpl(
    TTask::TPtr task,
    IYtJobService::TPtr ytJobService,
    TFmrUserJobLauncher::TPtr jobLauncher,
    std::shared_ptr<std::atomic<bool>> cancelFlag,
    TJobSource jobSource
) {
    TFmrJobSettings jobSettings = GetJobSettingsFromTask(task);

    IFmrJob::TPtr job = std::visit([&](auto&& source) -> IFmrJob::TPtr {
        using T = std::decay_t<decltype(source)>;
        if constexpr (std::is_same_v<T, TDiscoveryJobSource>) {
            return MakeFmrJob(std::move(source.Discovery), std::move(source.VanillaInfo), ytJobService, jobLauncher, jobSettings, source.TvmSettings);
        } else {
            return MakeFmrJob(std::move(source.TableDataService), ytJobService, jobLauncher, jobSettings);
        }
    }, std::move(jobSource));

    auto processTask = [job, task, cancelFlag] (auto&& taskParams) {
        using T = std::decay_t<decltype(taskParams)>;

        if constexpr (std::is_same_v<T, TUploadTaskParams>) {
            return job->Upload(taskParams, task->ClusterConnections, cancelFlag);
        } else if constexpr (std::is_same_v<T, TDownloadTaskParams>) {
            return job->Download(taskParams, task->ClusterConnections, cancelFlag);
        } else if constexpr (std::is_same_v<T, TMergeTaskParams>) {
            return job->Merge(taskParams, task->ClusterConnections, cancelFlag);
        } else if constexpr (std::is_same_v<T, TMapTaskParams>) {
            return job->Map(taskParams, task->ClusterConnections, cancelFlag, task->JobEnvironmentDir, task->Files, task->YtResources, task->FmrResources);
        } else if constexpr (std::is_same_v<T, TSortedUploadTaskParams>) {
            return job->SortedUpload(taskParams, task->ClusterConnections, cancelFlag);
        } else if constexpr (std::is_same_v<T, TSortedMergeTaskParams>) {
            return job->SortedMerge(taskParams, task->ClusterConnections, cancelFlag);
        } else if constexpr (std::is_same_v<T, TLocalSortTaskParams>) {
            return job->LocalSort(taskParams, task->ClusterConnections, cancelFlag);
        } else if constexpr (std::is_same_v<T, TReduceTaskParams>) {
            return job->Reduce(taskParams, task->ClusterConnections, cancelFlag, task->JobEnvironmentDir, task->Files, task->YtResources, task->FmrResources);
        } else if constexpr (std::is_same_v<T, TFillTaskParams>) {
            return job->Fill(taskParams, cancelFlag, task->JobEnvironmentDir, task->Files, task->YtResources, task->FmrResources);
        } else if constexpr (std::is_same_v<T, TMapReduceMapTaskParams>) {
            return job->MapReduceMap(taskParams, task->ClusterConnections, cancelFlag, task->JobEnvironmentDir, task->Files, task->YtResources, task->FmrResources);
        } else if constexpr (std::is_same_v<T, TPullTaskParams>) {
            auto pullResult = job->Pull(taskParams, cancelFlag);
            if (auto* err = std::get_if<TFmrError>(&pullResult)) {
                return std::variant<TFmrError, TStatistics>{*err};
            }
            TStatistics stats;
            stats.TaskResult = TTaskPullResult{.Data = std::get<TString>(pullResult)};
            return std::variant<TFmrError, TStatistics>{std::move(stats)};
        } else {
            ythrow yexception() << "Unsupported task type";
        }
    };

    std::variant<TFmrError, TStatistics> taskOutput = std::visit(processTask, task->TaskParams);
    auto err = std::get_if<TFmrError>(&taskOutput);
    if (err) {
        return TJobResult{.TaskStatus = ETaskStatus::Failed, .Error = *err};
    }
    auto statistics = std::get_if<TStatistics>(&taskOutput);
    return {ETaskStatus::Completed, *statistics};
}

} // namespace

TJobResult RunJob(
    TTask::TPtr task,
    ITableDataServiceDiscovery::TPtr discovery,
    TMaybe<TVanillaInfo> vanillaInfo,
    IYtJobService::TPtr ytJobService,
    TFmrUserJobLauncher::TPtr jobLauncher,
    std::shared_ptr<std::atomic<bool>> cancelFlag,
    const TMaybe<TFmrTvmJobSettings>& tvmSettings
) {
    return RunJobImpl(task, ytJobService, jobLauncher, cancelFlag,
        TDiscoveryJobSource{std::move(discovery), std::move(vanillaInfo), tvmSettings});
}

TJobResult RunJob(
    TTask::TPtr task,
    ITableDataService::TPtr tableDataService,
    IYtJobService::TPtr ytJobService,
    TFmrUserJobLauncher::TPtr jobLauncher,
    std::shared_ptr<std::atomic<bool>> cancelFlag
) {
    return RunJobImpl(task, ytJobService, jobLauncher, cancelFlag,
        TDirectTdsJobSource{std::move(tableDataService)});
}

void FillMapFmrJob(
    TFmrUserJob& mapJob,
    const TMapTaskParams& mapTaskParams,
    const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections,
    ITableDataServiceDiscovery::TPtr discovery,
    TMaybe<TVanillaInfo> vanillaInfo,
    const TFmrUserJobSettings& userJobSettings,
    IYtJobService::TPtr jobService
) {
    mapJob.SetSettings(userJobSettings);
    if (vanillaInfo.Defined()) {
        mapJob.SetVanillaInfo(*vanillaInfo);
    }
    mapJob.SetTableDataServiceDiscovery(std::move(discovery));
    mapJob.SetTaskInputTables(mapTaskParams.Input);
    mapJob.SetTaskFmrOutputTables(mapTaskParams.Output);
    mapJob.SetClusterConnections(clusterConnections);
    mapJob.SetYtJobService(jobService);
    mapJob.SetFmrJobType(mapTaskParams.MapJobType);
}

void FillReduceFmrJob(
    TFmrUserJob& reduceJob,
    const TReduceTaskParams& reduceTaskParams,
    const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections,
    ITableDataServiceDiscovery::TPtr discovery,
    TMaybe<TVanillaInfo> vanillaInfo,
    const TFmrUserJobSettings& userJobSettings,
    IYtJobService::TPtr jobService
) {
    reduceJob.SetSettings(userJobSettings);
    if (vanillaInfo.Defined()) {
        reduceJob.SetVanillaInfo(*vanillaInfo);
    }
    reduceJob.SetTableDataServiceDiscovery(std::move(discovery));
    reduceJob.SetTaskInputTables(reduceTaskParams.Input);
    reduceJob.SetTaskFmrOutputTables(reduceTaskParams.Output);
    reduceJob.SetClusterConnections(clusterConnections);
    reduceJob.SetYtJobService(jobService);
    reduceJob.SetFmrJobType(EFmrJobType::Reduce);
    reduceJob.SetReduceOperationSpec(reduceTaskParams.ReduceOperationSpec);
    reduceJob.SetIsMapReduceReducer(true);
}

void FillFillFmrJob(
    TFmrUserJob& fillJob,
    const TFillTaskParams& fillTaskParams,
    ITableDataServiceDiscovery::TPtr discovery,
    TMaybe<TVanillaInfo> vanillaInfo,
    const TFmrUserJobSettings& userJobSettings,
    IYtJobService::TPtr jobService
) {
    fillJob.SetSettings(userJobSettings);
    if (vanillaInfo.Defined()) {
        fillJob.SetVanillaInfo(*vanillaInfo);
    }
    fillJob.SetTableDataServiceDiscovery(std::move(discovery));
    // Fill has no input tables — leave InputTables_ empty so the queue is immediately finished.
    fillJob.SetTaskFmrOutputTables(fillTaskParams.Output);
    fillJob.SetYtJobService(jobService);
    fillJob.SetFmrJobType(EFmrJobType::Map);
}

void FillMapReduceMapFmrJob(
    TFmrUserJob& mapReduceMapJob,
    const TMapReduceMapTaskParams& params,
    const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections,
    ITableDataServiceDiscovery::TPtr discovery,
    TMaybe<TVanillaInfo> vanillaInfo,
    const TFmrUserJobSettings& userJobSettings,
    IYtJobService::TPtr jobService
) {
    mapReduceMapJob.SetSettings(userJobSettings);
    if (vanillaInfo.Defined()) {
        mapReduceMapJob.SetVanillaInfo(*vanillaInfo);
    }
    mapReduceMapJob.SetTableDataServiceDiscovery(std::move(discovery));
    mapReduceMapJob.SetTaskInputTables(params.Input);
    // SortingColumns were chosen by the coordinator (identity → [_yql_key_hash, ...reduceBy],
    // real mapper → [...reduceBy]). Use them as-is; TFmrUserJob picks the right writer.
    mapReduceMapJob.SetTaskFmrOutputTables({params.Output});
    mapReduceMapJob.SetClusterConnections(clusterConnections);
    mapReduceMapJob.SetYtJobService(jobService);
    mapReduceMapJob.SetFmrJobType(EFmrJobType::Map);
}

TFmrJobSettings GetJobSettingsFromTask(TTask::TPtr task) {
    if (!task->JobSettings) {
        return TFmrJobSettings();
    }
    auto jobSettings = *task->JobSettings;
    YQL_ENSURE(jobSettings.IsMap());
    TFmrJobSettings resultSettings{};

    auto& parseRecordSettings = resultSettings.ParseRecordSettings;
    parseRecordSettings.MergeReadBlockCount = jobSettings["merge"]["read_block_count"].AsInt64();
    parseRecordSettings.MergeReadBlockSize = jobSettings["merge"]["read_block_size"].AsInt64();
    parseRecordSettings.MergeNumThreads = jobSettings["merge"]["num_threads"].AsInt64();

    parseRecordSettings.UploadReadBlockCount = jobSettings["upload"]["read_block_count"].AsInt64();
    parseRecordSettings.UploadReadBlockSize = jobSettings["upload"]["read_block_size"].AsInt64();
    parseRecordSettings.UploadNumThreads = jobSettings["upload"]["num_threads"].AsInt64();

    auto& jobIoSettings = jobSettings["job_io"];
    resultSettings.FmrReaderSettings.ReadAheadChunks = jobIoSettings["fmr_table_reader"]["inflight_chunks"].AsInt64();

    auto& fmrWriterSettings = resultSettings.FmrWriterSettings;
    fmrWriterSettings.MaxInflightChunks = jobIoSettings["fmr_table_writer"]["inflight_chunks"].AsInt64();
    fmrWriterSettings.ChunkSize = jobIoSettings["fmr_table_writer"]["chunk_size"].AsInt64();
    fmrWriterSettings.MaxRowWeight = jobIoSettings["fmr_table_writer"]["max_row_weight"].AsInt64();
    if (jobIoSettings["fmr_table_writer"].HasKey("skip_sorted_check")) {
        fmrWriterSettings.SkipSortedCheck = jobIoSettings["fmr_table_writer"]["skip_sorted_check"].AsBool();
    }

    auto& jobProcessSettings = jobSettings["job_process"];
    auto& fmrUserJobSettings = resultSettings.FmrUserJobSettings;
    fmrUserJobSettings.QueueSizeLimit = jobProcessSettings["queue_size_limit"].AsInt64();
    fmrUserJobSettings.ThreadPoolSize = jobProcessSettings["num_threads"].AsInt64();

    resultSettings.YtWriterSettings.MaxRowWeight = jobIoSettings["yt_table_writer"]["max_row_weight"].AsInt64();

    if (jobIoSettings.HasKey("raw_table_queue") && jobIoSettings["raw_table_queue"].HasKey("max_inflight_bytes")) {
        resultSettings.RawTableQueueSettings.MaxInflightBytes = jobIoSettings["raw_table_queue"]["max_inflight_bytes"].AsInt64();
    }

    return resultSettings;
}

} // namespace NYql
