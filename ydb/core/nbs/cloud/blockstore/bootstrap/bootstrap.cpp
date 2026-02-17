#include "bootstrap.h"

#include "nbs_service.h"

#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/vhost_stats_simple.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/device_handler.h>
#include <ydb/core/nbs/cloud/blockstore/libs/vhost/server.h>
#include <ydb/core/nbs/cloud/blockstore/libs/vhost/vhost.h>

#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/logging.h>

namespace NYdb::NBS::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultLogLevel = 5;

TNbsServicePtr NbsService;

NVhost::TServerConfig CreateDefaultVhostServerConfig()
{
    NVhost::TServerConfig result;
    return result;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TNbsService::TNbsService(const NKikimrConfig::TNbsConfig& config)
    : Config(config)
{
    TLogSettings logSettings;
    logSettings.FiltrationLevel = static_cast<ELogPriority>(DefaultLogLevel);
    Logging = CreateLoggingService("console", logSettings);
    Log = Logging->CreateLog("NBS2_SERVICE");

    STORAGE_INFO("TNbsService create with config");

    NVhost::InitVhostLog(Logging);
    VhostQueueFactory = NVhost::CreateVhostQueueFactory();
    VHostStats = std::make_shared<TVHostStatsSimple>();

    VhostServer = NVhost::CreateServer(
        Logging, VHostStats, NVhost::CreateVhostQueueFactory(),
        CreateDefaultDeviceHandlerFactory(), CreateDefaultVhostServerConfig(),
        VhostCallbacks);
}

void TNbsService::Start()
{
    STORAGE_INFO("TNbsService start");
    VhostServer->Start();
}

void TNbsService::Stop()
{
    STORAGE_INFO("TNbsService stop");
    VhostServer->Stop();
}

const NKikimrConfig::TNbsConfig& TNbsService::GetConfig() const
{
    return Config;
}

////////////////////////////////////////////////////////////////////////////////

void CreateNbsService(const NKikimrConfig::TNbsConfig& config)
{
    NbsService = std::make_shared<TNbsService>(config);
}

void StartNbsService()
{
    if (NbsService) {
        NbsService->Start();
    }
}

void StopNbsService()
{
    if (NbsService) {
        NbsService->Stop();
    }
}

TNbsServicePtr GetNbsService()
{
    return NbsService;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
