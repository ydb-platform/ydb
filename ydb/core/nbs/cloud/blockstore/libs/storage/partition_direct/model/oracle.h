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
    TemporaryOffline,
    Offline,
};

class IOracle
{
public:
    virtual ~IOracle() = default;

    virtual void OnRequestStarted(
        THostIndex hostIndex,
        EOperation operation,
        TInstant now) = 0;
    virtual void OnRequestSucceeded(
        THostIndex hostIndex,
        EOperation operation,
        TInstant now,
        TDuration executionTime) = 0;
    virtual void OnRequestFailed(
        THostIndex hostIndex,
        EOperation operation,
        TInstant now) = 0;

    // Picks the best host (by lowest inflight count) out of the provided set
    // of hosts. Ties are broken uniformly at random.
    [[nodiscard]] virtual THostIndex SelectBestPBufferHost(
        std::span<const THostIndex> hostIndexes,
        EOperation operation) const = 0;

    [[nodiscard]] virtual TDuration GetWriteHedgingDelay() const = 0;
    [[nodiscard]] virtual TDuration GetWriteRequestTimeout() const = 0;
    [[nodiscard]] virtual TDuration GetPBufferReplyTimeout() const = 0;
    [[nodiscard]] virtual EWriteMode GetWriteMode() const = 0;

    [[nodiscard]] virtual TString Dump() const = 0;
};

class TOracle: public IOracle
{
public:
    TOracle(
        TStorageConfigPtr storageConfig,
        IHostStateController* hostStateController);

    void Think(TInstant now);

    void OnRequestStarted(
        THostIndex hostIndex,
        EOperation operation,
        TInstant now) override;
    void OnRequestSucceeded(
        THostIndex hostIndex,
        EOperation operation,
        TInstant now,
        TDuration executionTime) override;
    void OnRequestFailed(
        THostIndex hostIndex,
        EOperation operation,
        TInstant now) override;

    [[nodiscard]] THostIndex SelectBestPBufferHost(
        std::span<const THostIndex> hostIndexes,
        EOperation operation) const override;

    [[nodiscard]] TDuration GetWriteHedgingDelay() const override;
    [[nodiscard]] TDuration GetWriteRequestTimeout() const override;
    [[nodiscard]] TDuration GetPBufferReplyTimeout() const override;
    [[nodiscard]] EWriteMode GetWriteMode() const override;

    [[nodiscard]] TString Dump() const override;

private:
    const TStorageConfigPtr StorageConfig;

    IHostStateController* const HostStateController;
    const TDuration DefaultWriteHedgingDelay;
    const TDuration DefaultWriteRequestTimeout;
    const TDuration DefaultPBufferReplyTimeout;
    const EWriteMode DefaultWriteMode;

    TVector<THostStat> HostStatistics;
    TVector<THostState> HostStates;
    TVector<EHostHealth> HostsHealths;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
