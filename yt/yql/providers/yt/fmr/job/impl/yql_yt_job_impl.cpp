#include <library/cpp/threading/future/core/future.h>

#include <util/stream/file.h>

#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_job_impl.h>
#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_output_stream.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/table_data_service/interface/table_data_service.h>
#include <yt/yql/providers/yt/fmr/yt_service/interface/yql_yt_yt_service.h>

#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

class TFmrJob: public IFmrJob {
public:

    TFmrJob(ITableDataService::TPtr tableDataService, IYtService::TPtr ytService, std::shared_ptr<std::atomic<bool>> cancelFlag)
        : TableDataService_(tableDataService), YtService_(ytService), CancelFlag_(cancelFlag)
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

            ui64 rowsCount;
            auto get_yt_table_res = GetYtTableStream(ytTable, rowsCount, clusterConnection);
            auto err = std::get_if<TError>(&get_yt_table_res);
            if (err) {
                return *err;
            }
            auto inputStream = std::get_if<THolder<IInputStream>>(&get_yt_table_res);
            TFmrOutputStream outputStream = TFmrOutputStream(tableId, partId, TableDataService_);

            TransferData(inputStream->get(), &outputStream);
            outputStream.Flush();

            TTableStats stats = outputStream.GetStats();
            stats.Rows = rowsCount;

            auto statistics = TStatistics({{output, stats}});
            return statistics;
        } catch (...) {
            return TError(CurrentExceptionMessage());
        }
    }

    virtual std::variant<TError, TStatistics> Upload(const TUploadTaskParams& params, const TClusterConnection& clusterConnection) override {
        const auto ytTable = params.Output;
        const auto cluster = params.Output.Cluster;
        const auto path = params.Output.Path;
        const auto tableId = params.Input.TableId;

        YQL_CLOG(DEBUG, FastMapReduce) << "Uploading " << cluster << '.' << path;

        TMaybe<TString> getResult = TableDataService_->Get(tableId).GetValueSync();

        if (!getResult) {
            YQL_CLOG(ERROR, FastMapReduce) << "Table " << tableId << " not found";
            return TError("Table not found");
        }

        TString tableContent = getResult.GetRef();
        TStringInput inputStream(tableContent);

        YtService_->Upload(ytTable, inputStream, clusterConnection);

        return TStatistics();
    }

    virtual std::variant<TError, TStatistics> Merge(const TMergeTaskParams& params, const TClusterConnection& clusterConnection) override {
        // расширить таск парамс. добавить туда мету
        const auto inputs = params.Input;
        const auto output = params.Output;

        YQL_CLOG(DEBUG, FastMapReduce) << "Merging " << inputs.size() << " inputs";

        TFmrOutputStream outputStream(output.TableId, output.PartId, TableDataService_);

        ui32 totalRowsCount = 0;

        for (const auto& inputTableRef : inputs) {
            if (CancelFlag_->load()) {
                return TError("Canceled");
            }
            ui64 rowsCount = 0; // TMP Todo get rows count from input stats
            auto res = GetTableInputStream(inputTableRef, rowsCount, clusterConnection);
            totalRowsCount += rowsCount;

            auto err = std::get_if<TError>(&res);
            if (err) {
                return *err;
            }
            auto inputStream = std::get_if<THolder<IInputStream>>(&res);
            TransferData(inputStream->get(), &outputStream);
        }
        outputStream.Flush();

        TTableStats stats = outputStream.GetStats();
        stats.Rows = totalRowsCount;

        return TStatistics({{output, stats}});
    }

private:
    std::variant<THolder<IInputStream>, TError> GetTableInputStream(const TTaskTableRef& tableRef, ui64& rowsCount, const TClusterConnection& clusterConnection) {
        auto ytTable = std::get_if<TYtTableRef>(&tableRef);
        auto fmrTable = std::get_if<TFmrTableInputRef>(&tableRef);
        if (ytTable) {
            return GetYtTableStream(*ytTable, rowsCount, clusterConnection);
        } else if (fmrTable) {
            return GetFmrTableStream(*fmrTable);
        } else {
            ythrow yexception() << "Unsupported table type";
        }
    }

    std::variant<THolder<IInputStream>, TError> GetYtTableStream(const TYtTableRef& ytTable, ui64& rowsCount, const TClusterConnection& clusterConnection) {
        auto res = YtService_->Download(ytTable, rowsCount, clusterConnection);
        auto* err = std::get_if<TError>(&res);
        if (err) {
            return *err;
        }
        auto tableFile = std::get_if<THolder<TTempFileHandle>>(&res);
        // Временно. Тут надо будет менять на стрим, когда переделаем возвращаемое значение в YtService
        auto tableContent = TString(TFileInput(tableFile->Get()->Name()).ReadAll());
        TStringStream stream;
        stream << tableContent;
        return MakeHolder<TStringStream>(stream);;
    }

    std::variant<THolder<IInputStream>, TError> GetFmrTableStream(const TFmrTableInputRef& fmrTable) {
        auto res = TableDataService_->Get(fmrTable.TableId).GetValueSync();
        if (!res) {
            return TError("Table not found");
        }
        auto tableContent = *res;
        TStringStream stream;
        stream << tableContent;
        return MakeHolder<TStringStream>(stream);
    }

private:
    ITableDataService::TPtr TableDataService_;
    IYtService::TPtr YtService_;
    std::shared_ptr<std::atomic<bool>> CancelFlag_;
};

IFmrJob::TPtr MakeFmrJob(ITableDataService::TPtr tableDataService, IYtService::TPtr ytService, std::shared_ptr<std::atomic<bool>> cancelFlag) {
    return MakeIntrusive<TFmrJob>(tableDataService, ytService, cancelFlag);
}

TJobResult RunJob(
    TTask::TPtr task,
    ITableDataService::TPtr tableDataService,
    IYtService::TPtr ytService,
    std::shared_ptr<std::atomic<bool>> cancelFlag
) {
    IFmrJob::TPtr job = MakeFmrJob(tableDataService, ytService, cancelFlag);

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
