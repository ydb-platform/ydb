#include "oracle.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>

#include <util/generic/size_literals.h>
#include <util/random/random.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

TDuration GetFromConfig(ui64 milliseconds, TDuration defaultValue)
{
    return milliseconds ? TDuration::MilliSeconds(milliseconds) : defaultValue;
}

ui32 GetFromConfig(ui32 value, ui32 defaultValue)
{
    return value ? value : defaultValue;
}

ui64 GetFromConfig(ui64 value, ui64 defaultValue)
{
    return value ? value : defaultValue;
}

THostState::EState StatusToState(EHostHealth status)
{
    switch (status) {
        case EHostHealth::Online:
        case EHostHealth::Sufferer:
            return THostState::EState::Enabled;
        case EHostHealth::Offline:
            return THostState::EState::Disabled;
    }
}

class TOracleConfig
{
public:
    explicit TOracleConfig(TStorageConfigPtr storageConfig)
        : StorageConfig(std::move(storageConfig))
    {}

    [[nodiscard]] TDuration GetMaxDurationBeforeGoingOffline() const
    {
        return GetFromConfig(
            StorageConfig->GetOracleConfig().GetMaxDurationBeforeGoingOffline(),
            TDuration::Seconds(10));
    }

    [[nodiscard]] ui32 GetMinErrorsCountBeforeGoingOffline() const
    {
        return GetFromConfig(
            StorageConfig->GetOracleConfig()
                .GetMinErrorsCountBeforeGoingOffline(),
            10);
    }

    [[nodiscard]] ui32 GetErrorsCountForGoingOffline() const
    {
        return GetFromConfig(
            StorageConfig->GetOracleConfig().GetErrorsCountForGoingOffline(),
            1000);
    }

    [[nodiscard]] ui64 GetErrorsTotalSizeForGoingOffline() const
    {
        return GetFromConfig(
            StorageConfig->GetOracleConfig()
                .GetErrorsTotalSizeForGoingOffline(),
            100_MB);
    }

private:
    TStorageConfigPtr StorageConfig;
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TOracle::TOracle(
    TStorageConfigPtr storageConfig,
    IHostStateController* hostStateController,
    const TVector<THostStat>& stats,
    const TVector<THostState>& states)
    : StorageConfig(std::move(storageConfig))
    , HostStateController(hostStateController)
    , Stats(stats)
    , States(states)
    , DefaultWriteHedgingDelay(StorageConfig->GetWriteHedgingDelay())
    , DefaultWriteRequestTimeout(StorageConfig->GetWriteRequestTimeout())
    , DefaultPBufferReplyTimeout(StorageConfig->GetPBufferReplyTimeout())
    , DefaultWriteMode(GetWriteModeFromProto(StorageConfig->GetWriteMode()))
{
    Y_ABORT_UNLESS(
        Stats.size() == States.size(),
        "Stats and States must have the same size");

    Statuses.resize(Stats.size());
    for (auto& status: Statuses) {
        status = EHostHealth::Online;
    }
}

void TOracle::Think(TInstant now)
{
    const TOracleConfig config(StorageConfig);

    TVector<EHostHealth> newStatuses(Statuses);

    for (size_t i = 0; i < Stats.size(); ++i) {
        auto errorsInfo = Stats[i].GetErrorsInfo(now);

        const bool hasSufferingSymptom = (errorsInfo.ErrorCount != 0);
        const bool hasOfflineSymptom =
            (hasSufferingSymptom) &&
            ((errorsInfo.ErrorCount >=
                  config.GetMinErrorsCountBeforeGoingOffline() &&
              errorsInfo.FromFirstError >
                  config.GetMaxDurationBeforeGoingOffline()) ||
             (errorsInfo.ErrorCount >=
              config.GetErrorsCountForGoingOffline()) ||
             (HostStateController->GetHostPBufferUsedSize(i) >=
              config.GetErrorsTotalSizeForGoingOffline()));

        newStatuses[i] = EHostHealth::Online;

        if (hasOfflineSymptom) {
            newStatuses[i] = EHostHealth::Offline;
        } else if (hasSufferingSymptom) {
            newStatuses[i] = EHostHealth::Sufferer;
        }
    }

    for (size_t i = 0; i < newStatuses.size(); ++i) {
        if (newStatuses[i] != Statuses[i]) {
            Statuses[i] = newStatuses[i];
            const auto newState = StatusToState(newStatuses[i]);
            if (newState != States[i].State) {
                HostStateController->SetHostState(i, newState);
            }
        }
    }
}

THostIndex TOracle::SelectBestPBufferHost(
    std::span<const THostIndex> hostIndexes,
    EOperation operation) const
{
    Y_ABORT_UNLESS(!hostIndexes.empty());

    auto getInflight = [this, operation](THostIndex hostIndex)
    {
        return Stats[hostIndex].InflightCount(operation);
    };

    // Pick the host with the lowest number of currently inflight requests of
    // the given operation type. Ties (multiple hosts with the same minimum
    // value) are broken uniformly at random via reservoir sampling, so the
    // load isn't always biased towards the first host in `hostIndexes`.
    THostIndex bestHostIndex = hostIndexes[0];
    size_t bestInflight = getInflight(bestHostIndex);
    size_t tieCount = 1;
    for (size_t i = 1; i < hostIndexes.size(); ++i) {
        const THostIndex hostIndex = hostIndexes[i];
        const size_t inflight = getInflight(hostIndex);
        if (inflight < bestInflight) {
            bestInflight = inflight;
            bestHostIndex = hostIndex;
            tieCount = 1;
        } else if (inflight == bestInflight) {
            ++tieCount;
            // Reservoir sampling: replace current best with probability
            // 1/tieCount so that, after the loop, every tied host has equal
            // probability of being chosen.
            if (RandomNumber<size_t>(tieCount) == 0) {
                bestHostIndex = hostIndex;
            }
        }
    }
    return bestHostIndex;
}

TDuration TOracle::GetWriteHedgingDelay() const
{
    return DefaultWriteHedgingDelay;
}

TDuration TOracle::GetWriteRequestTimeout() const
{
    return DefaultWriteRequestTimeout;
}

TDuration TOracle::GetPBufferReplyTimeout() const
{
    return DefaultPBufferReplyTimeout;
}

EWriteMode TOracle::GetWriteMode() const
{
    return DefaultWriteMode;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
