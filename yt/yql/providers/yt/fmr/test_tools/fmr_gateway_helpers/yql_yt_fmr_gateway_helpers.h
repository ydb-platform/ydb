#pragma once

#include <yt/yql/providers/yt/gateway/fmr/yql_yt_fmr.h>
#include <yt/yql/providers/yt/fmr/worker/impl/yql_yt_worker_impl.h>
#include <yt/yql/providers/yt/fmr/coordinator/interface/yql_yt_coordinator.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/utils/runnable.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/time_provider/time_provider.h>
#include <util/folder/dirut.h>
#include <util/system/tempfile.h>

namespace NYql::NFmr {

using IFmrServer = IRunnable;

struct TTestFmrGatewayOptions {
    NKikimr::NMiniKQL::IFunctionRegistry::TPtr FunctionRegistry;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    IFmrCoordinator::TPtr Coordinator;
    THashMap<TString, TString> Tables;

    TDuration CoordinatorPingInterval = TDuration::MilliSeconds(300);
    TDuration TimeToSleepBetweenGetOperationRequests = TDuration::MilliSeconds(100);
    ui32 RandomSeed = 1;
};

struct TTestFmrGatewayResult {
    IYtGateway::TPtr Gateway;
    IFmrWorker::TPtr Worker;
    IFmrServer::TPtr TableDataServiceServer;
    TTempFileHandle DiscoveryFile;
    THolder<TPortManager> PortManager;
};

/**
 * Creates a fully configured FMR gateway for testing purposes.
 * This includes:
 * - File-based YT gateway as a slave
 * - FMR services (JobLauncher, YtJobService, YtCoordinatorService)
 * - TableDataService with discovery
 * - FMR worker
 *
 * Usage:
 *   TTestFmrGatewayOptions options;
 *   options.Tables = myTables;
 *   auto result = CreateTestFmrGateway(options);
 *   // Use result.Gateway for testing
 */
TTestFmrGatewayResult CreateTestFmrGateway(const TTestFmrGatewayOptions& options = {});

/**
 * Creates a test FMR gateway with coordinator integration.
 * This is a simplified version for coordinator integration tests.
 */
IYtGateway::TPtr CreateTestFmrGatewayWithCoordinator(
    IFmrCoordinator::TPtr coordinator,
    NKikimr::NMiniKQL::IFunctionRegistry::TPtr functionRegistry,
    TIntrusivePtr<ITimeProvider> timeProvider = nullptr,
    TDuration pingInterval = TDuration::MilliSeconds(300));

} // namespace NYql::NFmr

