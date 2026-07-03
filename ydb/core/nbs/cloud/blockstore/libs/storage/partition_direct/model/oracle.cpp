#include "oracle.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>

#include <util/generic/size_literals.h>
#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

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

    [[nodiscard]] ui32 GetTimePredictionHistorySize() const
    {
        return GetFromConfig(
            StorageConfig->GetOracleConfig().GetTimePredictionHistorySize(),
            100);
    }

    [[nodiscard]] ui32 GetTimePredictionNthFromEnd() const
    {
        return GetFromConfig(
            StorageConfig->GetOracleConfig().GetTimePredictionNthFromEnd(),
            1);
    }

private:
    TStorageConfigPtr StorageConfig;
};

////////////////////////////////////////////////////////////////////////////////

TOracle::TOracle(
    TStorageConfigPtr storageConfig,
    IHostStateController* hostStateController)
    : StorageConfig(std::move(storageConfig))
    , OracleConfig(std::make_shared<TOracleConfig>(StorageConfig))
    , HostStateController(hostStateController)
    , DefaultReadHedgingDelay(StorageConfig->GetReadHedgingDelay())
    , DefaultReadRequestTimeout(StorageConfig->GetReadRequestTimeout())
    , DefaultWriteHedgingDelay(StorageConfig->GetWriteHedgingDelay())
    , DefaultWriteRequestTimeout(StorageConfig->GetWriteRequestTimeout())
    , DefaultIndirectWriteReplyTimeout(
          StorageConfig->GetIndirectWriteReplyTimeout())
    , DefaultFlushRequestTimeout(StorageConfig->GetFlushRequestTimeout())
    , DefaultEraseRequestTimeout(StorageConfig->GetEraseRequestTimeout())
    , DefaultWriteMode(GetWriteModeFromProto(StorageConfig->GetWriteMode()))
    , HostStatistics(DirectBlockGroupHostCount)
    , HostStates(DirectBlockGroupHostCount)
    , TimePredictors(
          OperationCount,
          TTimePredictor(
              OracleConfig->GetTimePredictionHistorySize(),
              OracleConfig->GetTimePredictionNthFromEnd()))
{
    HostsHealths.resize(HostStates.size());
    for (auto& healths: HostsHealths) {
        healths = EHostHealth::Online;
    }
}

TOracle::~TOracle() = default;

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

        const bool hasSufferingSymptom =
            (errorsInfo.ConsecutiveErrorCount != 0);
        const bool hasTemporaryOfflineSymptom =
            hasSufferingSymptom &&
            ((errorsInfo.ConsecutiveErrorCount >=
                  config.GetMinErrorsCountBeforeGoingOffline() &&
              errorsInfo.FromFirstError >
                  config.GetMaxDurationBeforeGoingTemporaryOffline()) ||
             (errorsInfo.ConsecutiveErrorCount >=
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
    AccessTimePredictor(operation).Add(hostIndex, executionTime);
    HostStatistics[hostIndex].OnSuccess(now, executionTime, operation);
}

void TOracle::OnRequestFailed(
    THostIndex hostIndex,
    EOperation operation,
    TInstant now)
{
    HostStatistics[hostIndex].OnError(now, operation);
}

void TOracle::OnRequestCancelled(
    THostIndex hostIndex,
    EOperation operation,
    TInstant now)
{
    HostStatistics[hostIndex].OnCancelled(now, operation);
}

THostIndex TOracle::SelectBestPBufferHost(
    THostMask hosts,
    EOperation operation) const
{
    Y_ABORT_UNLESS(!hosts.Empty());

    auto getInflight = [this, operation](THostIndex hostIndex)
    {
        return HostStatistics[hostIndex].InflightCount(operation);
    };

    // Pick the host with the lowest number of currently inflight requests of
    // the given operation type. Ties (multiple hosts with the same minimum
    // value) are broken uniformly at random via reservoir sampling, so the
    // load isn't always biased towards the first host in `hostIndexes`.
    THostIndex bestHostIndex = InvalidHostIndex;
    size_t bestInflight = 0;
    size_t tieCount = 0;
    for (auto host: hosts) {
        const size_t inflight = getInflight(host);
        if (bestHostIndex == InvalidHostIndex || inflight < bestInflight) {
            bestInflight = inflight;
            bestHostIndex = host;
            tieCount = 1;
        } else if (inflight == bestInflight) {
            ++tieCount;
            // Reservoir sampling: replace current best with probability
            // 1/tieCount so that, after the loop, every tied host has equal
            // probability of being chosen.
            if (RandomNumber<size_t>(tieCount) == 0) {
                bestHostIndex = host;
            }
        }
    }
    return bestHostIndex;
}

TDuration TOracle::GetReadHedgingDelay(
    THostIndex host,
    EDataLocation dataLocation) const
{
    TDuration result;

    switch (dataLocation) {
        case EDataLocation::DDisk: {
            result = GetTimePredictor(EOperation::ReadFromDDisk).Predict(host);
            break;
        }
        case EDataLocation::PBuffer: {
            result =
                GetTimePredictor(EOperation::ReadFromPBuffer).Predict(host);
            break;
        }
    }
    return result != TDuration::Zero() ? result : DefaultReadHedgingDelay;
}

TDuration TOracle::GetReadRequestTimeout() const
{
    return DefaultReadRequestTimeout;
}

TDuration TOracle::GetWriteHedgingDelay(THostMask hosts, bool indirect) const
{
    TDuration result = GetTimePredictor(
                           indirect ? EOperation::WriteToManyPBuffers
                                    : EOperation::WriteToPBuffer)
                           .Predict(hosts);
    return result != TDuration::Zero() ? result : DefaultWriteHedgingDelay;
}

TDuration TOracle::GetWriteRequestTimeout() const
{
    return DefaultWriteRequestTimeout;
}

TDuration TOracle::GetIndirectWriteReplyTimeout() const
{
    return DefaultIndirectWriteReplyTimeout;
}

TDuration TOracle::GetFlushRequestTimeout() const
{
    return DefaultFlushRequestTimeout;
}

TDuration TOracle::GetEraseRequestTimeout() const
{
    return DefaultEraseRequestTimeout;
}

EWriteMode TOracle::GetWriteMode() const
{
    return DefaultWriteMode;
}

const THostStat& TOracle::GetHostStatistics(THostIndex hostIndex) const
{
    return HostStatistics[hostIndex];
}

TString TOracle::Dump() const
{
    TStringBuilder sb;
    for (size_t i = 0; i < HostStates.size(); ++i) {
        sb << " H" << i << ": " << HostStates[i].DebugPrint() << " "
           << HostStatistics[i].DebugPrint() << "\n";
    }

    for (size_t i = 0; i < OperationCount; ++i) {
        auto op = static_cast<EOperation>(i);
        sb << " " << ToString(op) << ": ";
        for (THostIndex h = 0; h < HostStates.size(); ++h) {
            sb << FormatDuration(GetTimePredictor(op).Predict(h)) << " ";
        }
        sb << "\n";
    }
    return sb;
}

TTimePredictor& TOracle::AccessTimePredictor(EOperation operation)
{
    return TimePredictors[static_cast<size_t>(operation)];
}

const TTimePredictor& TOracle::GetTimePredictor(EOperation operation) const
{
    return TimePredictors[static_cast<size_t>(operation)];
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
