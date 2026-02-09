#include "bootstrap.h"

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

TNbsService::TNbsService()
{
    TLogSettings logSettings;
    logSettings.FiltrationLevel = static_cast<ELogPriority>(DefaultLogLevel);
    Logging = CreateLoggingService("console", logSettings);

    NVhost::InitVhostLog(Logging);
    VhostQueueFactory = NVhost::CreateVhostQueueFactory();
    VHostStats = std::make_shared<TVHostStatsSimple>();

    VhostServer = NVhost::CreateServer(
        Logging,
        VHostStats,
        NVhost::CreateVhostQueueFactory(),
        CreateDefaultDeviceHandlerFactory(),
        CreateDefaultVhostServerConfig(),
        VhostCallbacks);
}

void CreateNbsService()
{
    NbsService = std::make_shared<TNbsService>();
}

TNbsServicePtr GetNbsService()
{
    return NbsService;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
