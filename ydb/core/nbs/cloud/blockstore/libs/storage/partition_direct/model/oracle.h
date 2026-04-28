#pragma once

#include "host_stat.h"
#include "host_state.h"

#include <ydb/core/nbs/cloud/blockstore/config/public.h>

#include <util/generic/vector.h>

#include <span>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

enum class EHostStatus
{
    Online,
    Sufferer,
    Offline,
};

class TOracle
{
public:
    TOracle(
        TStorageConfigPtr storageConfig,
        IHostStateController* hostStateController,
        const TVector<THostStat>& stats,
        const TVector<THostState>& states);

    void Think(TInstant now);

    // Picks the best host (by lowest inflight count) out of the provided set
    // of hosts. Ties are broken uniformly at random.
    [[nodiscard]] ui8 SelectBestPBufferHost(
        std::span<const ui8> hostIndexes,
        EOperation operation) const;

private:
    const TStorageConfigPtr StorageConfig;

    IHostStateController* const HostStateController;
    const TVector<THostStat>& Stats;
    const TVector<THostState>& States;

    TVector<EHostStatus> Statuses;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
