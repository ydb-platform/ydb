#include "oracle.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <util/generic/size_literals.h>
#include <util/random/random.h>
#include <util/string/builder.h>

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

EHostState HealthToState(EHostHealth health)
{
    switch (health) {
        case EHostHealth::Online:
        case EHostHealth::Sufferer:
            return EHostState::Online;
        case EHostHealth::TemporaryOffline:
            return EHostState::TemporaryOffline;
        case EHostHealth::Offline:
            return EHostState::Offline;
    }
}

class TOracleConfig
{
public:
    explicit TOracleConfig(TStorageConfigPtr storageConfig)
        : StorageConfig(std::move(storageConfig))
    {}

    [[nodiscard]] TDuration GetMaxDurationBeforeGoingTemporaryOffline() const
    {
        return GetFromConfig(
            StorageConfig->GetOracleConfig()
                .GetMaxDurationBeforeGoingTemporaryOffline(),
            TDuration::Seconds(10));
    }

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
    IHostStateController* hostStateController)
    : StorageConfig(std::move(storageConfig))
    , HostStateController(hostStateController)
    , DefaultWriteHedgingDelay(StorageConfig->GetWriteHedgingDelay())
    , DefaultWriteRequestTimeout(StorageConfig->GetWriteRequestTimeout())
    , DefaultPBufferReplyTimeout(StorageConfig->GetPBufferReplyTimeout())
    , DefaultWriteMode(GetWriteModeFromProto(StorageConfig->GetWriteMode()))
    , HostStatistics(DirectBlockGroupHostCount)
    , HostStates(DirectBlockGroupHostCount)
{
    HostsHealths.resize(HostStates.size());
    for (auto& healths: HostsHealths) {
        healths = EHostHealth::Online;
    }
}

void TOracle::Think(TInstant now)
{
    const TOracleConfig config(StorageConfig);

    for (THostIndex hostIndex = 0; hostIndex < HostStates.size(); ++hostIndex) {
        HostStates[hostIndex].PBufferUsedSize =
            HostStateController->GetHostPBufferUsedSize(hostIndex);
    }

    TVector<EHostHealth> newHostsHealths(HostsHealths);

    for (size_t i = 0; i < HostStatistics.size(); ++i) {
        auto errorsInfo = HostStatistics[i].GetErrorsInfo(now);

        const bool hasSufferingSymptom = (errorsInfo.ErrorCount != 0);
        const bool hasTemporaryOfflineSymptom =
            hasSufferingSymptom &&
            ((errorsInfo.ErrorCount >=
                  config.GetMinErrorsCountBeforeGoingOffline() &&
              errorsInfo.FromFirstError >
                  config.GetMaxDurationBeforeGoingTemporaryOffline()) ||
             (errorsInfo.ErrorCount >=
              config.GetErrorsCountForGoingOffline()) ||
             (HostStateController->GetHostPBufferUsedSize(i) >=
              config.GetErrorsTotalSizeForGoingOffline()));
        const bool hasOfflineSymptom =
            hasTemporaryOfflineSymptom &&
            (errorsInfo.FromFirstError >
             config.GetMaxDurationBeforeGoingOffline());

        if (hasOfflineSymptom) {
            newHostsHealths[i] = EHostHealth::Offline;
        } else if (hasTemporaryOfflineSymptom) {
            newHostsHealths[i] = EHostHealth::TemporaryOffline;
        } else if (hasSufferingSymptom) {
            newHostsHealths[i] = EHostHealth::Sufferer;
        } else {
            newHostsHealths[i] = EHostHealth::Online;
        }
    }

    for (size_t i = 0; i < newHostsHealths.size(); ++i) {
        if (newHostsHealths[i] != HostsHealths[i]) {
            HostsHealths[i] = newHostsHealths[i];
            const auto oldState = HostStates[i].State;
            const auto newState = HealthToState(newHostsHealths[i]);
            if (oldState != newState) {
                HostStates[i].State = newState;
                HostStateController->SetHostState(i, oldState, newState);
            }
        }
    }
}

void TOracle::OnRequestStarted(
    THostIndex hostIndex,
    EOperation operation,
    TInstant now)
{
    Y_UNUSED(now);
    HostStatistics[hostIndex].OnRequest(operation);
}

void TOracle::OnRequestSucceeded(
    THostIndex hostIndex,
    EOperation operation,
    TInstant now,
    TDuration executionTime)
{
    HostStatistics[hostIndex].OnSuccess(now, executionTime, operation);
}

void TOracle::OnRequestFailed(
    THostIndex hostIndex,
    EOperation operation,
    TInstant now)
{
    HostStatistics[hostIndex].OnError(now, operation);
}

THostIndex TOracle::SelectBestPBufferHost(
    std::span<const THostIndex> hostIndexes,
    EOperation operation) const
{
    Y_ABORT_UNLESS(!hostIndexes.empty());

    auto getInflight = [this, operation](THostIndex hostIndex)
    {
        return HostStatistics[hostIndex].InflightCount(operation);
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

TString TOracle::Dump() const
{
    TStringBuilder sb;
    for (size_t i = 0; i < HostStates.size(); ++i) {
        sb << " H" << i << ": " << HostStates[i].DebugPrint() << " "
           << HostStatistics[i].DebugPrint() << "\n";
    }
    return sb;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
