#pragma once

#include "host.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/host_stat.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/oracle_config.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

class IHostHealthPolicy
{
public:
    virtual ~IHostHealthPolicy() = default;

    virtual EHostHealth GetNewHealth(
        EHostHealth health,
        const THostErrorsInfo& stats,
        ui64 errorsTotalSize) const = 0;
};

std::unique_ptr<IHostHealthPolicy> CreateDefaultHostHealthPolicy(
    const TOracleConfig& config);

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
