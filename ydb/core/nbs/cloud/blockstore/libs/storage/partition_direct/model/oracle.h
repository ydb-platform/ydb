#pragma once

#include "public.h"

#include "disk_state_provider.h"
#include "host.h"
#include "host_health_policy.h"
#include "host_mask.h"
#include "host_stat.h"
#include "host_state.h"
#include "mon_model.h"
#include "time_predictor.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/config/public.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/backoff_delay_provider.h>

#include <util/generic/vector.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

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
    virtual void OnRequestCancelled(
        THostIndex hostIndex,
        EOperation operation,
        TInstant now) = 0;

    virtual void OnDDiskDisconnected(THostIndex hostIndex, TInstant now) = 0;
    virtual void OnDDiskConnected(THostIndex hostIndex, TInstant now) = 0;
    virtual void OnDDiskBroken(THostIndex hostIndex) = 0;

    virtual void OnHostRemoved(THostIndex hostIndex) = 0;

    virtual TDuration GetHostReconnectDelay(THostIndex hostIndex) = 0;

    // Picks the best host (by lowest inflight count) out of the provided set
    // of hosts. Ties are broken uniformly at random.
    [[nodiscard]] virtual THostIndex SelectBestPBufferHost(
        THostMask hosts,
        EOperation operation) const = 0;

    [[nodiscard]] virtual TDuration GetReadHedgingDelay(
        THostIndex host,
        EDataLocation dataLocation) const = 0;
    [[nodiscard]] virtual TDuration GetReadRequestTimeout() const = 0;

    // Chooses DirectWrite or IndirectWrite for this request.
    // Low load favours DirectWrite for latency. The disk-wide in-flight
    // write count is read from the disk-state provider.
    [[nodiscard]] virtual EWriteMode GetWriteMode() const = 0;
    [[nodiscard]] virtual TDuration GetWriteHedgingDelay(
        THostMask hosts,
        bool indirect) const = 0;
    [[nodiscard]] virtual TDuration GetWriteRequestTimeout() const = 0;
    [[nodiscard]] virtual TDuration GetIndirectWriteReplyTimeout() const = 0;

    [[nodiscard]] virtual TDuration GetFlushRequestCooldown(
        THostMask hosts) const = 0;
    [[nodiscard]] virtual TDuration GetFlushRequestTimeout() const = 0;

    [[nodiscard]] virtual TDuration GetEraseRequestTimeout() const = 0;

    [[nodiscard]] virtual const THostStat& GetHostStatistics(
        THostIndex hostIndex) const = 0;
    [[nodiscard]] virtual TString Dump() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TOracle: public IOracle
{
public:
    TOracle(
        TStorageConfigPtr storageConfig,
        IHostStateController* hostStateController,
        const TVector<EHostHealth>& hostHealths);
    ~TOracle() override;

    void Think(TInstant now);

    // IOracle implementation
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
    void OnRequestCancelled(
        THostIndex hostIndex,
        EOperation operation,
        TInstant now) override;

    void OnDDiskDisconnected(THostIndex hostIndex, TInstant now) override;
    void OnDDiskConnected(THostIndex hostIndex, TInstant now) override;
    [[nodiscard]] TDuration GetHostReconnectDelay(
        THostIndex hostIndex) override;
    // Device is permanently broken, so force the host offline.
    void OnDDiskBroken(THostIndex hostIndex) override;

    void OnHostRemoved(THostIndex hostIndex) override;

    [[nodiscard]] THostIndex SelectBestPBufferHost(
        THostMask hosts,
        EOperation operation) const override;

    [[nodiscard]] TDuration GetReadHedgingDelay(
        THostIndex host,
        EDataLocation dataLocation) const override;
    [[nodiscard]] TDuration GetReadRequestTimeout() const override;

    [[nodiscard]] EWriteMode GetWriteMode() const override;
    [[nodiscard]] TDuration GetWriteHedgingDelay(
        THostMask hosts,
        bool indirect) const override;
    [[nodiscard]] TDuration GetWriteRequestTimeout() const override;
    [[nodiscard]] TDuration GetIndirectWriteReplyTimeout() const override;

    [[nodiscard]] TDuration GetFlushRequestCooldown(
        THostMask hosts) const override;
    [[nodiscard]] TDuration GetFlushRequestTimeout() const override;

    [[nodiscard]] TDuration GetEraseRequestTimeout() const override;

    [[nodiscard]] const THostStat& GetHostStatistics(
        THostIndex hostIndex) const override;

    // Returns the current group-wide state of a host.
    [[nodiscard]] EHostState GetHostState(THostIndex hostIndex) const;

    [[nodiscard]] TString Dump() const override;

    // The FastPath service that owns the disk-wide in-flight write count.
    // Wired from TDirectBlockGroup::Run after FastPath exists.
    void SetDiskStateProvider(IDiskStateProvider* diskStateProvider);

    // If necessary, adds hosts to make the hostIndex valid.
    void AddHostIfNeeded(THostIndex hostIndex);

    // Check if it's valid to QueryAddHost from HostStateController and do it.
    void MaybeQueryAddHost();

    [[nodiscard]] TVector<THostSnapshot> BuildHostStats(TInstant now) const;
    [[nodiscard]] size_t GetLatencyHistoryCapacity() const;

private:
    [[nodiscard]] TTimePredictor& AccessTimePredictor(EOperation operation);
    [[nodiscard]] const TTimePredictor& GetTimePredictor(
        EOperation operation) const;
    [[nodiscard]] size_t GetHostCount() const;

    const TStorageConfigPtr StorageConfig;
    const TOracleConfigPtr OracleConfig;

    IHostStateController* const HostStateController;
    const TDuration DefaultReadHedgingDelay;
    const TDuration DefaultReadRequestTimeout;
    const TDuration DefaultWriteHedgingDelay;
    const TDuration DefaultWriteRequestTimeout;
    const TDuration DefaultIndirectWriteReplyTimeout;
    const TDuration DefaultFlushRequestTimeout;
    const TDuration DefaultEraseRequestTimeout;
    const EWriteMode DefaultWriteMode;
    const size_t MaxInflightWritesForDirectWrite;

    IDiskStateProvider* DiskStateProvider = nullptr;
    TVector<THostStat> HostStatistics;
    TVector<THostState> HostStates;
    TVector<EHostHealth> HostsHealths;
    TVector<TBackoffDelayProvider> HostsReconnectDelays;
    TVector<TTimePredictor> TimePredictors;
    std::unique_ptr<IHostHealthPolicy> HealthPolicy;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
