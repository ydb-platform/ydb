#pragma once

#include <yt/yql/providers/yt/fmr/coordinator/interface/yql_yt_coordinator.h>
#include <yt/yql/providers/yt/gateway/lib/exec_ctx.h>
#include <yt/yql/providers/yt/provider/yql_yt_forwarding_gateway.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/interface/yql_yt_job_service.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/interface/yql_yt_coordinator_service_interface.h>
#include <yt/yql/providers/yt/fmr/job_launcher/yql_yt_job_launcher.h>

namespace NYql::NFmr {

struct TFmrServices: public TYtBaseServices {
    using TPtr = TIntrusivePtr<TFmrServices>;

    TString CoordinatorServerUrl;
    TString TableDataServiceDiscoveryFilePath;
    IYtJobService::TPtr YtJobService;
    IYtCoordinatorService::TPtr YtCoordinatorService;
    TFmrUserJobLauncher::TPtr JobLauncher;
    bool DisableLocalFmrWorker = false;
    TString FmrOperationSpecFilePath;
};

struct TFmrYtGatewaySettings {
    TIntrusivePtr<IRandomProvider> RandomProvider = CreateDefaultRandomProvider();
    TDuration TimeToSleepBetweenGetOperationRequests = TDuration::Seconds(1);
};

IYtGateway::TPtr CreateYtFmrGateway(
    IYtGateway::TPtr slave,
    IFmrCoordinator::TPtr coordinator = nullptr,
    TFmrServices::TPtr fmrServices = nullptr,
    const TFmrYtGatewaySettings& settings = TFmrYtGatewaySettings{}
);

} // namespace NYql::NFmr
