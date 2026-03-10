#include "yql_yt_fmr_initializer.h"
#include <util/stream/file.h>
#include <util/string/strip.h>

namespace NYql::NFmr {

TFmrInitializationOptions GetFmrInitializationInfoFromConfig(
    const TFmrInstance& fmrConfiguration,
    const google::protobuf::RepeatedPtrField<TFmrFileRemoteCache>& fileCacheConfigurations
) {
    // initializing tvm

    TMaybe<TFmrTvmGatewaySettings> tvmSettings = Nothing();
    if (fmrConfiguration.HasTvmConfig()) {
        YQL_CLOG(DEBUG, FastMapReduce) << "Found tvm config " << fmrConfiguration.GetTvmConfig().DebugString() << " for fmr";
        tvmSettings = TFmrTvmGatewaySettings{
            .CoordinatorTvmId = static_cast<TTvmId>(fmrConfiguration.GetTvmConfig().GetCoordinatorTvmId()),
            .GatewayTvmId = static_cast<TTvmId>(fmrConfiguration.GetTvmConfig().GetGatewayTvmId()),
            .TvmDiskCacheDir = fmrConfiguration.GetTvmConfig().GetTvmDiskCacheDir()
        };

        TString gatewayTvmSecretFile = fmrConfiguration.GetTvmConfig().GetGatewayTvmSecretFile();
        YQL_ENSURE(NFs::Exists(gatewayTvmSecretFile), "Gateway tvm secret file should exist, if it is set in gateways.conf");
        tvmSettings->GatewayTvmSecret = StripStringRight(TFileInput(gatewayTvmSecretFile).ReadLine());
    }

    // initializing fmr file metadata and upload services
    TString coordinatorUrl = fmrConfiguration.GetCoordinatorUrl();
    if (!fmrConfiguration.HasFileRemoteCacheName()) {
        return TFmrInitializationOptions{coordinatorUrl, nullptr, nullptr, TFmrDistributedCacheSettings(), tvmSettings};
    }
    TString fmrRemoteCacheName = fmrConfiguration.GetFileRemoteCacheName();

    YQL_CLOG(INFO, FastMapReduce) << "Searching for distributed cache configuration with name " << fmrRemoteCacheName;
    TFmrFileRemoteCache fileCacheInfo;
    bool foundCacheConfiguration = false;
    for (auto& fileCache: fileCacheConfigurations) {
        if (fileCache.GetName() == fmrRemoteCacheName) {
            foundCacheConfiguration = true;
            fileCacheInfo = fileCache;
            break;
        }
    }
    YQL_ENSURE(foundCacheConfiguration, "Failed to find configuration with name " << fmrRemoteCacheName);

    YQL_ENSURE(!fileCacheInfo.GetCluster().empty() && !fileCacheInfo.GetPath().empty(), "Yt path and server name for fmr remote file cache should be set");

    TString distCacheYtCluster = fileCacheInfo.GetCluster(), distCacheYtPath = fileCacheInfo.GetPath();
    NFmr::TYtFileMetadataServiceOptions metadataOptions {
        .RemotePath = distCacheYtPath,
        .YtServerName = distCacheYtCluster
    };
    NFmr::TYtFileUploadServiceOptions uploadOptions{
        .RemotePath = distCacheYtPath,
        .YtServerName = distCacheYtCluster
    };
    if (fileCacheInfo.HasFileExpirationInterval()) {
        uploadOptions.ExpirationInterval = TDuration::Seconds(fileCacheInfo.GetFileExpirationInterval());
    }
    TString distCacheYtToken;
    if (fileCacheInfo.HasTokenFile()) {
        TString tokenFile = fileCacheInfo.GetTokenFile();
        YQL_ENSURE(NFs::Exists(tokenFile), "Token file should exist, if it is set in gateways.conf");
        distCacheYtToken = StripStringRight(TFileInput(tokenFile).ReadLine());
        metadataOptions.YtToken = distCacheYtToken;
        uploadOptions.YtToken = distCacheYtToken;
        YQL_CLOG(DEBUG, FastMapReduce) << "Found token for writing to fmr dist cache";
    }
    YQL_CLOG(DEBUG, FastMapReduce) << "Successfully initialized fmr remote file cache with cluster: " << distCacheYtCluster << " and path: " << distCacheYtPath;

    TFmrDistributedCacheSettings fmrDistCacheSettings{
        .Path = distCacheYtPath,
        .YtServerName = distCacheYtCluster,
        .YtToken = distCacheYtToken
    };

    return NFmr::TFmrInitializationOptions {
        .FmrCoordinatorUrl = coordinatorUrl,
        .FmrFileMetadataService =  NFmr::MakeYtFileMetadataService(metadataOptions),
        .FmrFileUploadService = NFmr::MakeYtFileUploadService(uploadOptions),
        .FmrDistributedCacheSettings = fmrDistCacheSettings,
        .FmrTvmSettings = tvmSettings
    };
}

std::pair<IYtGateway::TPtr, IFmrWorker::TPtr> InitializeFmrGateway(IYtGateway::TPtr slave, const TFmrServices::TPtr fmrServices) {
    TFmrCoordinatorSettings coordinatorSettings{};
    TString fmrOperationSpecFilePath = fmrServices->FmrOperationSpecFilePath;
    if (!fmrOperationSpecFilePath.empty()) {
        TFileInput input(fmrOperationSpecFilePath);
        auto fmrOperationSpec = NYT::NodeFromYsonStream(&input);
        coordinatorSettings.DefaultFmrOperationSpec = fmrOperationSpec;
    }

    auto tvmSettings = fmrServices->TvmSettings;

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
        IFmrTvmClient::TPtr coordinatorTvmClient = nullptr;

        if (tvmSettings) {
            coordinatorClientSettings.DestinationTvmId = tvmSettings->CoordinatorTvmId;
            TFmrTvmApiSettings gatewayTvmSettings{
                .SourceTvmId = tvmSettings->GatewayTvmId,
                .TvmSecret = tvmSettings->GatewayTvmSecret,
                .TvmDiskCacheDir = tvmSettings->TvmDiskCacheDir,
                .DestinationTvmIds = {tvmSettings->CoordinatorTvmId}
            };
            coordinatorTvmClient = MakeFmrTvmClient(gatewayTvmSettings);
        }
        coordinator = MakeFmrCoordinatorClient(coordinatorClientSettings, coordinatorTvmClient);
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

        worker = MakeFmrWorker(coordinator, jobFactory, fmrServices->JobPreparer, workerSettings);
        worker->Start();
    }
    return std::pair<IYtGateway::TPtr, IFmrWorker::TPtr>{CreateYtFmrGateway(slave, coordinator, fmrServices), std::move(worker)};
}

} // namespace NYql::NFmr
