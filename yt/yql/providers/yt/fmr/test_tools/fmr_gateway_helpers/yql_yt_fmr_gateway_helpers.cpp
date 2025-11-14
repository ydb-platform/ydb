#include "yql_yt_fmr_gateway_helpers.h"

#include <yt/yql/providers/yt/fmr/worker/impl/yql_yt_worker_impl.h>
#include <yt/yql/providers/yt/fmr/test_tools/table_data_service/yql_yt_table_data_service_helpers.h>
#include <yt/yql/providers/yt/fmr/test_tools/mock_time_provider/yql_yt_mock_time_provider.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_services.h>
#include <yt/yql/providers/yt/fmr/job_launcher/yql_yt_job_launcher.h>
#include <yt/yql/providers/yt/fmr/yt_job_service/file/yql_yt_file_yt_job_service.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/file/yql_yt_file_coordinator_service.h>

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/core/file_storage/file_storage.h>
#include <yql/essentials/core/file_storage/proto/file_storage.pb.h>

namespace NYql::NFmr {

std::pair<IYtGateway::TPtr, IFmrWorker::TPtr> InitializeFmrGateway(IYtGateway::TPtr slave, const TFmrServices::TPtr fmrServices);

TTestFmrGatewayResult CreateTestFmrGateway(const TTestFmrGatewayOptions& options) {
    TTestFmrGatewayResult result;

    auto functionRegistry = options.FunctionRegistry;
    if (!functionRegistry) {
        functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(
            NKikimr::NMiniKQL::CreateBuiltinRegistry());
    }

    TFileStorageConfig fsConfig;
    auto fileStorage = CreateAsyncFileStorage(fsConfig);

    auto fileServices = NFile::TYtFileServices::Make(
        functionRegistry.Get(),
        options.Tables,
        fileStorage,
        fileStorage->GetTemp().GetPath(),
        false,
        {});
    auto slaveGateway = CreateYtFileGateway(fileServices);

    // Setup FMR services
    auto fmrServices = MakeIntrusive<TFmrServices>();
    fmrServices->FunctionRegistry = functionRegistry.Get();
    fmrServices->JobLauncher = MakeIntrusive<TFmrUserJobLauncher>(
        TFmrUserJobLauncherOptions{.RunInSeparateProcess = false});
    fmrServices->YtJobService = MakeFileYtJobSerivce();
    fmrServices->YtCoordinatorService = MakeFileYtCoordinatorService();

    // Setup TableDataService
    result.PortManager = MakeHolder<TPortManager>();
    const ui16 port = result.PortManager->GetPort();
    result.DiscoveryFile = TTempFileHandle();
    SetupTableDataServiceDiscovery(result.DiscoveryFile, port);
    result.TableDataServiceServer = MakeTableDataServiceServer(port);
    fmrServices->TableDataServiceDiscoveryFilePath = result.DiscoveryFile.Name();

    // Initialize FMR Gateway
    auto [fmrGateway, worker] = InitializeFmrGateway(slaveGateway, fmrServices);
    result.Gateway = fmrGateway;
    result.Worker = worker;

    // If coordinator is provided, wrap with coordinator-enabled gateway
    if (options.Coordinator) {
        TFmrYtGatewaySettings gatewaySettings;
        gatewaySettings.RandomProvider = CreateDeterministicRandomProvider(options.RandomSeed);
        gatewaySettings.TimeProvider = options.TimeProvider;
        gatewaySettings.TimeToSleepBetweenGetOperationRequests = options.TimeToSleepBetweenGetOperationRequests;
        gatewaySettings.CoordinatorPingInterval = options.CoordinatorPingInterval;

        result.Gateway = CreateYtFmrGateway(
            fmrGateway,
            options.Coordinator,
            fmrServices,
            gatewaySettings);
    }

    return result;
}

IYtGateway::TPtr CreateTestFmrGatewayWithCoordinator(
    IFmrCoordinator::TPtr coordinator,
    NKikimr::NMiniKQL::IFunctionRegistry::TPtr functionRegistry,
    TIntrusivePtr<ITimeProvider> timeProvider,
    TDuration pingInterval)
{
    if (!timeProvider) {
        timeProvider = MakeIntrusive<TMockTimeProvider>();
    }

    TFileStorageConfig fsConfig;
    auto fileStorage = CreateAsyncFileStorage(fsConfig);

    auto fileServices = NFile::TYtFileServices::Make(
        functionRegistry.Get(),
        {},
        fileStorage,
        fileStorage->GetTemp().GetPath(),
        false,
        {});
    auto slaveGateway = CreateYtFileGateway(fileServices);

    auto fmrServices = MakeIntrusive<TFmrServices>();
    fmrServices->FunctionRegistry = functionRegistry.Get();

    TFmrYtGatewaySettings gatewaySettings;
    gatewaySettings.RandomProvider = CreateDeterministicRandomProvider(1);
    gatewaySettings.TimeProvider = timeProvider;
    gatewaySettings.TimeToSleepBetweenGetOperationRequests = TDuration::MilliSeconds(100);
    gatewaySettings.CoordinatorPingInterval = pingInterval;

    return CreateYtFmrGateway(slaveGateway, coordinator, fmrServices, gatewaySettings);
}

} // namespace NYql::NFmr

