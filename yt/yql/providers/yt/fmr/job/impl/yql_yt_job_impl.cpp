#include <library/cpp/threading/future/core/future.h>

#include <util/stream/file.h>

#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_job_impl.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_reader.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_table_data_service_writer.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/table_data_service/interface/table_data_service.h>
#include <yt/yql/providers/yt/fmr/utils/parse_records.h>
#include <yt/yql/providers/yt/fmr/yt_service/impl/yql_yt_yt_service_impl.h>

#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

class TFmrJob: public IFmrJob {
public:

    TFmrJob(ITableDataService::TPtr tableDataService, IYtService::TPtr ytService, std::shared_ptr<std::atomic<bool>> cancelFlag, const TFmrJobSettings& settings)
        : TableDataService_(tableDataService), YtService_(ytService), CancelFlag_(cancelFlag), Settings_(settings)
    {
    }

    virtual std::variant<TError, TStatistics> Download(
        const TDownloadTaskParams& params,
        const TClusterConnection& clusterConnection
    ) override {
        try {
            const auto ytTable = params.Input;
            const auto cluster = params.Input.Cluster;
            const auto path = params.Input.Path;
            const auto output = params.Output;
            const auto tableId = output.TableId;
            const auto partId = output.PartId;

            YQL_CLOG(DEBUG, FastMapReduce) << "Downloading " << cluster << '.' << path;

            auto ytTableReader = YtService_->MakeReader(ytTable, clusterConnection); // TODO - pass YtReader settings from Gateway
            auto tableDataServiceWriter = TFmrTableDataServiceWriter(tableId, partId, TableDataService_, Settings_.FmrTableDataServiceWriterSettings);

            ParseRecords(*ytTableReader, tableDataServiceWriter, Settings_.ParseRecordSettings.BlockCount, Settings_.ParseRecordSettings.BlockSize);
            tableDataServiceWriter.Flush();

            TTableStats stats = tableDataServiceWriter.GetStats();
            auto statistics = TStatistics({{output, stats}});
            return statistics;
        } catch (...) {
            return TError(CurrentExceptionMessage());
        }
    }

    virtual std::variant<TError, TStatistics> Upload(const TUploadTaskParams& params, const TClusterConnection& clusterConnection) override {
        try {
            const auto ytTable = params.Output;
            const auto cluster = params.Output.Cluster;
            const auto path = params.Output.Path;
            const auto tableId = params.Input.TableId;
            const auto tableRanges = params.Input.TableRanges;

            YQL_CLOG(DEBUG, FastMapReduce) << "Uploading " << cluster << '.' << path;

            auto tableDataServiceReader = TFmrTableDataServiceReader(tableId, tableRanges, TableDataService_, Settings_.FmrTableDataServiceReaderSettings);
            auto ytTableWriter = YtService_->MakeWriter(ytTable, clusterConnection); // TODO - pass YtReader settings from Gateway
            ParseRecords(tableDataServiceReader, *ytTableWriter, Settings_.ParseRecordSettings.BlockCount, Settings_.ParseRecordSettings.BlockSize);
            ytTableWriter->Flush();

            return TStatistics();
        } catch (...) {
            return TError(CurrentExceptionMessage());
        }
    }

    virtual std::variant<TError, TStatistics> Merge(const TMergeTaskParams& params, const TClusterConnection& clusterConnection) override {
        // расширить таск парамс. добавить туда мету
        try {
            const auto inputs = params.Input;
            const auto output = params.Output;

            YQL_CLOG(DEBUG, FastMapReduce) << "Merging " << inputs.size() << " inputs";

            auto tableDataServiceWriter = TFmrTableDataServiceWriter(output.TableId, output.PartId, TableDataService_, Settings_.FmrTableDataServiceWriterSettings);
            for (const auto& inputTableRef : inputs) {
                if (CancelFlag_->load()) {
                    return TError("Canceled");
                }
                auto inputTableReader = GetTableInputStream(inputTableRef, clusterConnection);
                ParseRecords(*inputTableReader, tableDataServiceWriter, Settings_.ParseRecordSettings.BlockCount, Settings_.ParseRecordSettings.BlockSize);
            }
            tableDataServiceWriter.Flush();
            return TStatistics({{output, tableDataServiceWriter.GetStats()}});
        } catch (...) {
            return TError(CurrentExceptionMessage());
        }
        return TError{"not implemented yet"};
    }

private:
    NYT::TRawTableReaderPtr GetTableInputStream(const TTaskTableRef& tableRef, const TClusterConnection& clusterConnection) {
        auto ytTable = std::get_if<TYtTableRef>(&tableRef);
        auto fmrTable = std::get_if<TFmrTableInputRef>(&tableRef);
        if (ytTable) {
            return YtService_->MakeReader(*ytTable, clusterConnection); // TODO - pass YtReader settings from Gateway
        } else if (fmrTable) {
            return MakeIntrusive<TFmrTableDataServiceReader>(fmrTable->TableId, fmrTable->TableRanges, TableDataService_, Settings_.FmrTableDataServiceReaderSettings);
        } else {
            ythrow yexception() << "Unsupported table type";
        }
    }

private:
    ITableDataService::TPtr TableDataService_;
    IYtService::TPtr YtService_;
    std::shared_ptr<std::atomic<bool>> CancelFlag_;
    const TFmrJobSettings Settings_;
};

IFmrJob::TPtr MakeFmrJob(
    ITableDataService::TPtr tableDataService,
    IYtService::TPtr ytService,
    std::shared_ptr<std::atomic<bool>> cancelFlag,
    const TFmrJobSettings& settings
) {
    return MakeIntrusive<TFmrJob>(tableDataService, ytService, cancelFlag, settings);
}

TJobResult RunJob(
    TTask::TPtr task,
    ITableDataService::TPtr tableDataService,
    IYtService::TPtr ytService,
    std::shared_ptr<std::atomic<bool>> cancelFlag,
    const TFmrJobSettings& settings
) {
    IFmrJob::TPtr job = MakeFmrJob(tableDataService, ytService, cancelFlag, settings);

    auto processTask = [job, task] (auto&& taskParams) {
        using T = std::decay_t<decltype(taskParams)>;

        if constexpr (std::is_same_v<T, TUploadTaskParams>) {
            return job->Upload(taskParams, task->ClusterConnection);
        } else if constexpr (std::is_same_v<T, TDownloadTaskParams>) {
            return job->Download(taskParams, task->ClusterConnection);
        } else if constexpr (std::is_same_v<T, TMergeTaskParams>) {
            return job->Merge(taskParams, task->ClusterConnection);
        } else {
            throw std::runtime_error{"Unsupported task type"};
        }
    };

    std::variant<TError, TStatistics> taskResult = std::visit(processTask, task->TaskParams);

    auto err = std::get_if<TError>(&taskResult);

    if (err) {
        YQL_CLOG(ERROR, FastMapReduce) << "Task failed: " << err->ErrorMessage;
        return {ETaskStatus::Failed, TStatistics()};
    }

    auto statistics = std::get_if<TStatistics>(&taskResult);

    return {ETaskStatus::Completed, *statistics};
};

} // namespace NYql
