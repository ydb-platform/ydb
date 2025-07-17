#include "yql_yt_fmr_initializer.h"
#include <util/stream/file.h>

namespace NYql::NFmr {

std::pair<IYtGateway::TPtr, IFmrWorker::TPtr> InitializeFmrGateway(IYtGateway::TPtr slave, bool disableLocalFmrWorker, const TString& coordinatorServerUrl, bool isFileGateway, const TString& fmrOperationSpecFilePath) {
    TFmrCoordinatorSettings coordinatorSettings{};
    if (!fmrOperationSpecFilePath.empty()) {
        TFileInput input(fmrOperationSpecFilePath);
        auto fmrOperationSpec = NYT::NodeFromYsonStream(&input);
        coordinatorSettings.DefaultFmrOperationSpec = fmrOperationSpec;
    }

    ITableDataService::TPtr tableDataService = nullptr;
    if (!disableLocalFmrWorker) {
        tableDataService = MakeLocalTableDataService();
    }
    IFmrGcService::TPtr gcService = MakeGcService(tableDataService);

    auto coordinator = isFileGateway ?
        MakeFmrCoordinator(coordinatorSettings, MakeFileYtCoordinatorService(), gcService)
        : MakeFmrCoordinator(coordinatorSettings, MakeYtCoordinatorService(), gcService);
    if (!coordinatorServerUrl.empty()) {
        TFmrCoordinatorClientSettings coordinatorClientSettings;
        THttpURL parsedUrl;
        if (parsedUrl.Parse(coordinatorServerUrl) != THttpURL::ParsedOK) {
            ythrow yexception() << "Invalid fast map reduce coordinator server url passed in parameters";
        }
        coordinatorClientSettings.Port = parsedUrl.GetPort();
        coordinatorClientSettings.Host = parsedUrl.GetHost();
        coordinator = MakeFmrCoordinatorClient(coordinatorClientSettings);
    }

    IFmrWorker::TPtr worker = nullptr;
    if (!disableLocalFmrWorker) {
        auto fmrYtJobSerivce = isFileGateway ? MakeFileYtJobSerivce() : MakeYtJobSerivce();

        auto func = [tableDataService, fmrYtJobSerivce] (NFmr::TTask::TPtr task, std::shared_ptr<std::atomic<bool>> cancelFlag) mutable {
            return RunJob(task, tableDataService, fmrYtJobSerivce, cancelFlag);
        };

        NFmr::TFmrJobFactorySettings settings{.Function=func};
        auto jobFactory = MakeFmrJobFactory(settings);
        NFmr::TFmrWorkerSettings workerSettings{.WorkerId = 0, .RandomProvider = CreateDefaultRandomProvider(),
            .TimeToSleepBetweenRequests=TDuration::Seconds(1)};
        worker = MakeFmrWorker(coordinator, jobFactory, workerSettings);
        worker->Start();
    }
    return std::pair<IYtGateway::TPtr, IFmrWorker::TPtr>{CreateYtFmrGateway(slave, coordinator), std::move(worker)};
}

} // namespace NYql::NFmr
