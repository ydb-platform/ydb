#pragma once

#include "public.h"

#include "host.h"
#include "host_stat.h"
#include "host_state.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/config/public.h>

#include <util/generic/vector.h>

#include <span>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

enum class EHostHealth
{
    Online,
    Sufferer,
    Offline,
};

class IOracle
{
public:
    virtual ~IOracle() = default;

    [[nodiscard]] virtual THostIndex SelectBestPBufferHost(
        std::span<const THostIndex> hostIndexes,
        EOperation operation) const = 0;

    [[nodiscard]] virtual TDuration GetWriteHedgingDelay() const = 0;
    [[nodiscard]] virtual TDuration GetWriteRequestTimeout() const = 0;
    [[nodiscard]] virtual TDuration GetPBufferReplyTimeout() const = 0;
    [[nodiscard]] virtual EWriteMode GetWriteMode() const = 0;
};

class TOracle: public IOracle
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
    [[nodiscard]] THostIndex SelectBestPBufferHost(
        std::span<const THostIndex> hostIndexes,
        EOperation operation) const override;

    [[nodiscard]] TDuration GetWriteHedgingDelay() const override;
    [[nodiscard]] TDuration GetWriteRequestTimeout() const override;
    [[nodiscard]] TDuration GetPBufferReplyTimeout() const override;
    [[nodiscard]] EWriteMode GetWriteMode() const override;

private:
    const TStorageConfigPtr StorageConfig;

    IHostStateController* const HostStateController;
    const TVector<THostStat>& Stats;
    const TVector<THostState>& States;
    const TDuration DefaultWriteHedgingDelay;
    const TDuration DefaultWriteRequestTimeout;
    const TDuration DefaultPBufferReplyTimeout;
    const EWriteMode DefaultWriteMode;

    TVector<EHostHealth> Statuses;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
