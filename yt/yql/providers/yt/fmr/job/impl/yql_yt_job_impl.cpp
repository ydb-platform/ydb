#include <library/cpp/threading/future/core/future.h>

#include <util/stream/file.h>

#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_job_impl.h>
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

    virtual TMaybe<TString> Download(const TDownloadTaskParams& params, const TClusterConnection& clusterConnection) override { // вынести в приватный метод для переиспользования
        try {
            const auto ytTable = params.Input;
            const auto cluster = params.Input.Cluster;
            const auto path = params.Input.Path;
            const auto tableId = params.Output.TableId;

            YQL_CLOG(DEBUG, FastMapReduce) << "Downloading " << cluster << '.' << path;

            auto res = GetYtTableContent(ytTable, clusterConnection);
            auto err = std::get_if<TError>(&res);
            if (err) {
                return err->ErrorMessage;
            }
            auto tableContent = std::get<TString>(res);
            TableDataService_->Put(tableId, tableContent).Wait();
        } catch (...) {
            return CurrentExceptionMessage();
        }

        return Nothing();
    }

    virtual TMaybe<TString> Upload(const TUploadTaskParams& params, const TClusterConnection& clusterConnection) override {
        const auto ytTable = params.Output;
        const auto cluster = params.Output.Cluster;
        const auto path = params.Output.Path;
        const auto tableId = params.Input.TableId;

        YQL_CLOG(DEBUG, FastMapReduce) << "Uploading " << cluster << '.' << path;

        TMaybe<TString> getResult = TableDataService_->Get(tableId).GetValueSync();

        if (!getResult) {
            YQL_CLOG(ERROR, FastMapReduce) << "Table " << tableId << " not found";
            return "Table not found";
        }

        TString tableContent = getResult.GetRef();
        TStringInput inputStream(tableContent);

        YtService_->Upload(ytTable, inputStream, clusterConnection);

        return Nothing();
    }

    virtual TMaybe<TString> Merge(const TMergeTaskParams& params, const TClusterConnection& clusterConnection) override {
        const auto inputs = params.Input;
        const auto output = params.Output;

        YQL_CLOG(DEBUG, FastMapReduce) << "Merging " << inputs.size() << " inputs";

        TString mergedTableContent = "";

        for (const auto& inputTableRef : inputs) {
            if (CancelFlag_->load()) {
                return "Canceled";
            }
            auto res = GetTableContent(inputTableRef, clusterConnection);

            auto err = std::get_if<TError>(&res);
            if (err) {
                return err->ErrorMessage;
            }
            TString tableContent = std::get<TString>(res);

            mergedTableContent += tableContent;
        }

        TableDataService_->Put(output.TableId, mergedTableContent).Wait();

        return Nothing();
    }
private:
    std::variant<TString, TError> GetTableContent(const TTaskTableRef& tableRef, const TClusterConnection& clusterConnection) {
        auto ytTable = std::get_if<TYtTableRef>(&tableRef);
        auto fmrTable = std::get_if<TFmrTableInputRef>(&tableRef);
        if (ytTable) {
            return GetYtTableContent(*ytTable, clusterConnection);
        } else if (fmrTable) {
            return GetFmrTableContent(*fmrTable);
        } else {
            ythrow yexception() << "Unsupported table type";
        }
    }

    std::variant<TString, TError> GetYtTableContent(const TYtTableRef& ytTable, const TClusterConnection& clusterConnection) {
        auto res = YtService_->Download(ytTable, clusterConnection);
        auto* err = std::get_if<TError>(&res);
        if (err) {
            return *err;
        }
        auto tableFile = std::get_if<THolder<TTempFileHandle>>(&res);
        TFileInput inputStream(tableFile->Get()->Name());
        TString tableContent = inputStream.ReadAll();
        return tableContent;
    }

    std::variant<TString, TError> GetFmrTableContent(const TFmrTableInputRef& fmrTable) {
        auto res = TableDataService_->Get(fmrTable.TableId);
        return res.GetValueSync().GetRef();
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

    TMaybe<TString> taskResult = std::visit(processTask, task->TaskParams);

    if (taskResult.Defined()) {
        YQL_CLOG(ERROR, FastMapReduce) << "Task failed: " << taskResult.GetRef();
        return {ETaskStatus::Failed, TStatistics()};
    }

    return {ETaskStatus::Completed, TStatistics()};
};

} // namespace NYql
