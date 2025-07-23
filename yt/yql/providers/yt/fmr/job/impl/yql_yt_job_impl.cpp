#include <library/cpp/threading/future/core/future.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/stream/file.h>

#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_job_impl.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_reader.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_writer.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_parse_records.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_table_input_streams.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/interface/yql_yt_job_service.h>

#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

class TFmrJob: public IFmrJob {
public:

    TFmrJob(ITableDataService::TPtr tableDataService, IYtJobService::TPtr ytJobService, const TFmrJobSettings& settings)
        : TableDataService_(tableDataService), YtJobService_(ytJobService), Settings_(settings)
    {
    }

    virtual std::variant<TError, TStatistics> Download(
        const TDownloadTaskParams& params,
        const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections,
        std::shared_ptr<std::atomic<bool>> cancelFlag
    ) override {
        try {
            const auto ytTableTaskRef = params.Input;
            const auto output = params.Output;
            const auto tableId = output.TableId;
            const auto partId = output.PartId;

            YQL_ENSURE(clusterConnections.size() == 1);

            std::vector<NYT::TRawTableReaderPtr> ytTableReaders = GetYtTableReaders(YtJobService_, ytTableTaskRef, clusterConnections);
            auto tableDataServiceWriter = MakeIntrusive<TFmrTableDataServiceWriter>(tableId, partId, TableDataService_, Settings_.FmrWriterSettings);

            for (auto& ytTableReader: ytTableReaders) {
                ParseRecords(ytTableReader, tableDataServiceWriter, Settings_.ParseRecordSettings.DonwloadReadBlockCount, Settings_.ParseRecordSettings.DonwloadReadBlockSize, cancelFlag);
            }
            tableDataServiceWriter->Flush();

            TTableChunkStats stats = tableDataServiceWriter->GetStats();
            auto statistics = TStatistics({{output, stats}});
            return statistics;
        } catch (...) {
            YQL_CLOG(ERROR, FastMapReduce) << "Gotten error inside download: " << CurrentExceptionMessage();
            return TError(CurrentExceptionMessage());
        }
    }

    virtual std::variant<TError, TStatistics> Upload(
        const TUploadTaskParams& params,
        const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections,
        std::shared_ptr<std::atomic<bool>> cancelFlag
    ) override {
        try {
            const auto ytTable = params.Output;
            const auto cluster = params.Output.Cluster;
            const auto path = params.Output.Path;
            const auto tableId = params.Input.TableId;
            const auto tableRanges = params.Input.TableRanges;

            auto tableDataServiceReader = MakeIntrusive<TFmrTableDataServiceReader>(tableId, tableRanges, TableDataService_, Settings_.FmrReaderSettings);
            YQL_ENSURE(clusterConnections.size() == 1);
            auto ytTableWriter = YtJobService_->MakeWriter(ytTable, clusterConnections.begin()->second, Settings_.YtWriterSettings);
            ParseRecords(tableDataServiceReader, ytTableWriter, Settings_.ParseRecordSettings.UploadReadBlockCount, Settings_.ParseRecordSettings.UploadReadBlockSize, cancelFlag);
            ytTableWriter->Flush();

            return TStatistics();
        } catch (...) {
            YQL_CLOG(ERROR, FastMapReduce) << "Gotten error inside upload: " << CurrentExceptionMessage();
            return TError(CurrentExceptionMessage());
        }
    }

    virtual std::variant<TError, TStatistics> Merge(
        const TMergeTaskParams& params,
        const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections,
        std::shared_ptr<std::atomic<bool>> cancelFlag
    ) override {
        try {
            const auto taskTableInputRef = params.Input;
            const auto output = params.Output;

            auto& parseRecordSettings = Settings_.ParseRecordSettings;

            auto tableDataServiceWriter = MakeIntrusive<TFmrTableDataServiceWriter>(output.TableId, output.PartId, TableDataService_, Settings_.FmrWriterSettings);
            auto threadPool = CreateThreadPool(parseRecordSettings.MergeNumThreads);
            TMaybe<TMutex> mutex = TMutex();
            for (const auto& inputTableRef : taskTableInputRef.Inputs) {
                threadPool->SafeAddFunc([&, tableDataServiceWriter] {
                    try {
                        auto inputTableReaders = GetTableInputStreams(YtJobService_, TableDataService_, inputTableRef, clusterConnections);
                        for (auto& tableReader: inputTableReaders) {
                            ParseRecords(tableReader, tableDataServiceWriter, parseRecordSettings.MergeReadBlockCount, parseRecordSettings.MergeReadBlockSize, cancelFlag, mutex);
                        }
                    } catch (...) {
                        YQL_CLOG(ERROR, FastMapReduce) << CurrentExceptionMessage();
                        throw yexception() << CurrentExceptionMessage();
                    }
                });
            }
            threadPool->Stop();

            tableDataServiceWriter->Flush();
            return TStatistics({{output, tableDataServiceWriter->GetStats()}});
        } catch (...) {
            YQL_CLOG(ERROR, FastMapReduce) << "Gotten error inside merge: " << CurrentExceptionMessage();
            return TError(CurrentExceptionMessage());
        }
    }

    virtual std::variant<TError, TStatistics> Map(
        const TMapTaskParams& /* params */,
        const std::unordered_map<TFmrTableId, TClusterConnection>& /* clusterConnections */,
        std::shared_ptr<std::atomic<bool>> /* cancelFlag */
    ) override {
        YQL_CLOG(ERROR, FastMapReduce) << "MAP NOT IMPLEMENTED";
        ythrow yexception() << "Not implemented";
    }

private:
    ITableDataService::TPtr TableDataService_;
    IYtJobService::TPtr YtJobService_;
    TFmrJobSettings Settings_;
};

IFmrJob::TPtr MakeFmrJob(
    ITableDataService::TPtr tableDataService,
    IYtJobService::TPtr ytJobService,
    const TFmrJobSettings& settings
) {
    return MakeIntrusive<TFmrJob>(tableDataService, ytJobService, settings);
}

TJobResult RunJob(
    TTask::TPtr task,
    ITableDataService::TPtr tableDataService,
    IYtJobService::TPtr ytJobService,
    std::shared_ptr<std::atomic<bool>> cancelFlag
) {
    TFmrJobSettings jobSettings = GetJobSettingsFromTask(task);
    IFmrJob::TPtr job = MakeFmrJob(tableDataService, ytJobService, jobSettings);

    auto processTask = [job, task, cancelFlag] (auto&& taskParams) {
        using T = std::decay_t<decltype(taskParams)>;

        if constexpr (std::is_same_v<T, TUploadTaskParams>) {
            return job->Upload(taskParams, task->ClusterConnections, cancelFlag);
        } else if constexpr (std::is_same_v<T, TDownloadTaskParams>) {
            return job->Download(taskParams, task->ClusterConnections, cancelFlag);
        } else if constexpr (std::is_same_v<T, TMergeTaskParams>) {
            return job->Merge(taskParams, task->ClusterConnections, cancelFlag);
        } else if constexpr (std::is_same_v<T, TMapTaskParams>) {
            return job->Map(taskParams, task->ClusterConnections, cancelFlag);;
        } else {
            ythrow yexception() << "Unsupported task type";
        }
    };

    std::variant<TError, TStatistics> taskResult = std::visit(processTask, task->TaskParams);
    auto err = std::get_if<TError>(&taskResult);
    if (err) {
        ythrow yexception() << "Job failed with error: " << err->ErrorMessage;
    }

    auto statistics = std::get_if<TStatistics>(&taskResult);
    return {ETaskStatus::Completed, *statistics};
};

void FillMapFmrJob(
    TFmrUserJob& mapJob,
    const TMapTaskParams& mapTaskParams,
    const std::unordered_map<TFmrTableId, TClusterConnection>& clusterConnections,
    const TString& tableDataServiceDiscoveryFilePath,
    bool useFileGateway
) {
    mapJob.SetTableDataService(tableDataServiceDiscoveryFilePath);
    mapJob.SetTaskInputTables(mapTaskParams.Input);
    mapJob.SetTaskFmrOutputTables(mapTaskParams.Output);
    mapJob.SetClusterConnections(clusterConnections);
    mapJob.SetYtJobService(useFileGateway);
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

    auto& jobIoSettings = jobSettings["job_io"];
    resultSettings.FmrReaderSettings.ReadAheadChunks = jobIoSettings["fmr_table_reader"]["inflight_chunks"].AsInt64();

    auto& fmrWriterSettings = resultSettings.FmrWriterSettings;
    fmrWriterSettings.MaxInflightChunks = jobIoSettings["fmr_table_writer"]["inflight_chunks"].AsInt64();
    fmrWriterSettings.ChunkSize = jobIoSettings["fmr_table_writer"]["chunk_size"].AsInt64();
    fmrWriterSettings.MaxRowWeight = jobIoSettings["fmr_table_writer"]["max_row_weight"].AsInt64();

    resultSettings.YtWriterSettings.MaxRowWeight = jobIoSettings["yt_table_writer"]["max_row_weight"].AsInt64();

    // TODO - maybe pass other optional settings here.
    return resultSettings;
}

} // namespace NYql
