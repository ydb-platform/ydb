#include <library/cpp/threading/future/core/future.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>

#include <util/system/shellcommand.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_fmr_sorting_block_reader.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_job_impl.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_reader.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_sorted_writer.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_writer.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/tvm/impl/yql_yt_fmr_tvm_impl.h>
#include <yt/yql/providers/yt/fmr/utils/yson_block_iterator/impl/yql_yt_yson_tds_block_iterator.h>
#include <yt/yql/providers/yt/fmr/utils/yson_block_iterator/impl/yql_yt_yson_yt_block_iterator.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_parse_records.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_table_input_streams.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/impl/yql_yt_job_service_impl.h>
#include <yt/yql/providers/yt/fmr/request_options/proto_helpers/yql_yt_request_proto_helpers.h>

#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

class TFmrJob: public IFmrJob {
public:
    TFmrJob(
        const TString& tableDataServiceDiscoveryFilePath,
        IYtJobService::TPtr ytJobService,
        TFmrUserJobLauncher::TPtr jobLauncher,
        const TFmrJobSettings& settings,
        const TMaybe<TFmrTvmJobSettings>& tvmSettings = Nothing()
    )
        : TableDataServiceDiscoveryFilePath_(tableDataServiceDiscoveryFilePath)
        , YtJobService_(ytJobService)
        , JobLauncher_(jobLauncher)
        , Settings_(settings)
        , TvmSettings_(tvmSettings)
    {
        auto tableDataServiceDiscovery = MakeFileTableDataServiceDiscovery({.Path = tableDataServiceDiscoveryFilePath});
        if (tvmSettings.Defined()) {
            TvmClient_ = MakeFmrTvmClient({
                .SourceTvmAlias = tvmSettings->WorkerTvmAlias,
                .TvmPort = tvmSettings->TvmPort,
                .TvmSecret = tvmSettings->TvmSecret
            });
            TableDataServiceTvmId_ = tvmSettings->TableDataServiceTvmId;
        }
        TableDataService_ = MakeTableDataServiceClient(tableDataServiceDiscovery, TvmClient_, TableDataServiceTvmId_);
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
            const auto tableId = params.Input.TableId;
            const auto tableRanges = params.Input.TableRanges;
            const auto neededColumns = params.Input.Columns;
            const auto columnGroups = params.Input.SerializedColumnGroups;
            const auto order = params.Order;

            auto tableDataServiceReader = MakeIntrusive<TFmrTableDataServiceReader>(
                tableId, tableRanges, TableDataService_, neededColumns, columnGroups, Settings_.FmrReaderSettings);
            YQL_ENSURE(clusterConnections.size() == 1);
            const auto& clusterConnection = clusterConnections.begin()->second;

            auto writer = YtJobService_->GetDistributedWriter(
                params.CookieYson,
                clusterConnection
            );
            StreamBulkToYtDistributed(
                tableDataServiceReader,
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
        const std::unordered_map<TFmrTableId, TClusterConnection>& /*clusterConnections*/,
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
            for (const auto& inputTableRef : taskTableInputRef.Inputs) {
                if (auto fmrInput = std::get_if<TFmrTableInputRef>(&inputTableRef)) {
                    blockIterators.push_back(MakeIntrusive<TTableDataServiceBlockIterator>(
                        fmrInput->TableId,
                        fmrInput->TableRanges,
                        TableDataService_,
                        output.SortingColumns.Columns,
                        output.SortingColumns.SortOrders,
                        fmrInput->Columns,
                        fmrInput->SerializedColumnGroups,
                        fmrInput->IsFirstRowInclusive,
                        fmrInput->FirstRowKeys,
                        fmrInput->LastRowKeys,
                        Settings_.FmrReaderSettings.ReadAheadChunks
                    ));
                } else {
                    throw TFmrNonRetryableJobException() << "YtTables unsupported inside SortedMerge task";
                }
            }

            NYT::TRawTableReaderPtr mergeReader = MakeIntrusive<TSortedMergeReader>(blockIterators);
            ParseRecords(mergeReader, tableDataServiceWriter, parseRecordSettings.MergeReadBlockCount, parseRecordSettings.MergeReadBlockSize, cancelFlag, mutex);

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
            FillMapFmrJob(mapJob, params, clusterConnections, TableDataServiceDiscoveryFilePath_, userJobSettings, YtJobService_);
            mapJob.SetTvmSettings(TvmSettings_);
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
    ITableDataService::TPtr TableDataService_; // Table data service http client
    const TString TableDataServiceDiscoveryFilePath_;
    IYtJobService::TPtr YtJobService_;
    TFmrUserJobLauncher::TPtr JobLauncher_;
    TFmrJobSettings Settings_;
    IFmrTvmClient::TPtr TvmClient_ = nullptr;
    ui32 TableDataServiceTvmId_ = 0;
    TMaybe<TFmrTvmJobSettings> TvmSettings_;
};

IFmrJob::TPtr MakeFmrJob(
    const TString& tableDataServiceDiscoveryFilePath,
    IYtJobService::TPtr ytJobService,
    TFmrUserJobLauncher::TPtr jobLauncher,
    const TFmrJobSettings& settings,
    const TMaybe<TFmrTvmJobSettings>& workerTvmSettings
) {
    return MakeIntrusive<TFmrJob>(tableDataServiceDiscoveryFilePath, ytJobService, jobLauncher, settings, workerTvmSettings);
}

TJobResult RunJob(
    TTask::TPtr task,
    const TString& tableDataServiceDiscoveryFilePath,
    IYtJobService::TPtr ytJobService,
    TFmrUserJobLauncher::TPtr jobLauncher,
    std::shared_ptr<std::atomic<bool>> cancelFlag,
    const TMaybe<TFmrTvmJobSettings>& tvmSettings
) {
    TFmrJobSettings jobSettings = GetJobSettingsFromTask(task);
    IFmrJob::TPtr job = MakeFmrJob(tableDataServiceDiscoveryFilePath, ytJobService, jobLauncher, jobSettings, tvmSettings);

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
};

void FillMapFmrJob(
    TFmrUserJob& mapJob,
    const TMapTaskParams& mapTaskParams,
    const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections,
    const TString& tableDataServiceDiscoveryFilePath,
    const TFmrUserJobSettings& userJobSettings,
    IYtJobService::TPtr jobService
) {
    mapJob.SetSettings(userJobSettings);
    mapJob.SetTableDataService(tableDataServiceDiscoveryFilePath);
    mapJob.SetTaskInputTables(mapTaskParams.Input);
    mapJob.SetTaskFmrOutputTables(mapTaskParams.Output);
    mapJob.SetClusterConnections(clusterConnections);
    mapJob.SetYtJobService(jobService);
    mapJob.SetIsOrdered(mapTaskParams.IsOrdered);
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
