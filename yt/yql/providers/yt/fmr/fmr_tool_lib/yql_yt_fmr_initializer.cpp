#include "yql_yt_fmr_initializer.h"
#include <util/stream/file.h>

namespace NYql::NFmr {

std::pair<IYtGateway::TPtr, IFmrWorker::TPtr> InitializeFmrGateway(IYtGateway::TPtr slave, const TFmrServices::TPtr fmrServices) {
    TFmrCoordinatorSettings coordinatorSettings{};
    TString fmrOperationSpecFilePath = fmrServices->FmrOperationSpecFilePath;
    if (!fmrOperationSpecFilePath.empty()) {
        TFileInput input(fmrOperationSpecFilePath);
        auto fmrOperationSpec = NYT::NodeFromYsonStream(&input);
        coordinatorSettings.DefaultFmrOperationSpec = fmrOperationSpec;
    }

    ITableDataService::TPtr tableDataService = nullptr;
    bool disableLocalFmrWorker = fmrServices->DisableLocalFmrWorker;
    TString tableDataServiceDiscoveryFilePath = fmrServices->TableDataServiceDiscoveryFilePath;
    TString coordinatorServerUrl = fmrServices->CoordinatorServerUrl;
    if (!disableLocalFmrWorker) {
        YQL_ENSURE(!tableDataServiceDiscoveryFilePath.empty());
        auto tableDataServiceDiscovery = MakeFileTableDataServiceDiscovery({.Path = tableDataServiceDiscoveryFilePath});
        tableDataService = MakeTableDataServiceClient(tableDataServiceDiscovery);
    }

    IFmrCoordinator::TPtr coordinator;

    if (!coordinatorServerUrl.empty()) {
        TFmrCoordinatorClientSettings coordinatorClientSettings;
        THttpURL parsedUrl;
        if (parsedUrl.Parse(coordinatorServerUrl) != THttpURL::ParsedOK) {
            ythrow yexception() << "Invalid fast map reduce coordinator server url passed in parameters";
        }
        coordinatorClientSettings.Port = parsedUrl.GetPort();
        coordinatorClientSettings.Host = parsedUrl.GetHost();
        coordinator = MakeFmrCoordinatorClient(coordinatorClientSettings);
        YQL_CLOG(INFO, FastMapReduce) << "Created client to connect to coordinator server with host " << parsedUrl.GetHost() << " and port " << parsedUrl.GetPort();
    } else {
        // creating local coordinator since url was not passed via services
        IFmrGcService::TPtr gcService = MakeGcService(tableDataService);
        coordinator = MakeFmrCoordinator(coordinatorSettings, fmrServices->YtCoordinatorService, gcService);
    }

    IFmrWorker::TPtr worker = nullptr;
    if (!disableLocalFmrWorker) {
        auto fmrYtJobSerivce = fmrServices->YtJobService;
        auto jobLauncher = fmrServices->JobLauncher;
        auto func = [tableDataServiceDiscoveryFilePath, fmrYtJobSerivce, jobLauncher] (NFmr::TTask::TPtr task, std::shared_ptr<std::atomic<bool>> cancelFlag) mutable {
            return RunJob(task, tableDataServiceDiscoveryFilePath, fmrYtJobSerivce, jobLauncher, cancelFlag);
        };

        NFmr::TFmrJobFactorySettings settings{.Function=func};
        auto jobFactory = MakeFmrJobFactory(settings);
        NFmr::TFmrWorkerSettings workerSettings{.WorkerId = 0, .RandomProvider = CreateDefaultRandomProvider(),
            .TimeToSleepBetweenRequests=TDuration::Seconds(1)};
        worker = MakeFmrWorker(coordinator, jobFactory, workerSettings);
        worker->Start();
    }
    return std::pair<IYtGateway::TPtr, IFmrWorker::TPtr>{CreateYtFmrGateway(slave, coordinator, fmrServices), std::move(worker)};
}

} // namespace NYql::NFmr
