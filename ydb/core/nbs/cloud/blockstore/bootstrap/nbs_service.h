#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/vhost/public.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/startable.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/logger/log.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TNbsService: public IStartable
{
    ILoggingServicePtr Logging;
    TLog Log;
    NVhost::IVhostQueueFactoryPtr VhostQueueFactory;
    NVhost::IServerPtr VhostServer;
    TVHostStatsSimplePtr VHostStats;
    NVhost::TVhostCallbacks VhostCallbacks;

    TNbsService();

    void Start() override;
    void Stop() override;
};

using TNbsServicePtr = std::shared_ptr<TNbsService>;

////////////////////////////////////////////////////////////////////////////////

TNbsServicePtr GetNbsService();

////////////////////////////////////////////////////////////////////////////////
}   // namespace NYdb::NBS::NBlockStore
