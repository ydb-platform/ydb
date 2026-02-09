#pragma once


#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/vhost/public.h>


namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TNbsService {
    ILoggingServicePtr Logging;
    NVhost::IVhostQueueFactoryPtr VhostQueueFactory;
    NVhost::IServerPtr VhostServer;
    TVHostStatsSimplePtr VHostStats;
    NVhost::TVhostCallbacks VhostCallbacks;

    TNbsService();
};

using TNbsServicePtr = std::shared_ptr<TNbsService> ;

////////////////////////////////////////////////////////////////////////////////

void CreateNbsService();

TNbsServicePtr GetNbsService();

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
