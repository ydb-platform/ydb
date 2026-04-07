#pragma once

#include <yt/yql/providers/yt/fmr/coordinator/interface/yql_yt_coordinator.h>
#include <yt/yql/providers/yt/gateway/lib/exec_ctx.h>
#include <yt/yql/providers/yt/provider/yql_yt_forwarding_gateway.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/interface/yql_yt_job_service.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/interface/yql_yt_coordinator_service_interface.h>
#include <yt/yql/providers/yt/fmr/job_launcher/yql_yt_job_launcher.h>
#include <yt/yql/providers/yt/fmr/file/metadata/interface/yql_yt_file_metadata_interface.h>
#include <yt/yql/providers/yt/fmr/file/upload/interface/yql_yt_file_upload_interface.h>
#include <yt/yql/providers/yt/fmr/job_preparer/interface/yql_yt_job_preparer_interface.h>

namespace NYql::NFmr {

enum class ETablePresenceStatus {
    Undefined,
    OnlyInYt,
    OnlyInFmr,
    Both
};

struct TFmrServices: public TYtBaseServices {
    using TPtr = TIntrusivePtr<TFmrServices>;

    TString CoordinatorServerUrl;
    TString TableDataServiceDiscoveryFilePath;
    IYtJobService::TPtr YtJobService;
    IYtCoordinatorService::TPtr YtCoordinatorService;
    TFmrUserJobLauncher::TPtr JobLauncher;
    bool DisableLocalFmrWorker = false;
    TString FmrOperationSpecFilePath;
    IFileMetadataService::TPtr FileMetadataService;
    IFileUploadService::TPtr FileUploadService;
    IFmrJobPreparer::TPtr JobPreparer;
    TMaybe<TFmrTvmGatewaySettings> TvmSettings;
};

struct TFmrYtGatewaySettings {
    TIntrusivePtr<IRandomProvider> RandomProvider = CreateDefaultRandomProvider();
    TIntrusivePtr<ITimeProvider> TimeProvider = CreateDefaultTimeProvider();
    TDuration TimeToSleepBetweenGetOperationRequests = TDuration::Seconds(1);
    TDuration CoordinatorPingInterval = TDuration::Seconds(5);
};

IYtGateway::TPtr CreateYtFmrGateway(
    IYtGateway::TPtr slave,
    IFmrCoordinator::TPtr coordinator = nullptr,
    TFmrServices::TPtr fmrServices = nullptr,
    const TFmrYtGatewaySettings& settings = TFmrYtGatewaySettings{}
);

} // namespace NYql::NFmr
