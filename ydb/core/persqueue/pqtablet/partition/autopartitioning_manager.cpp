#include "autopartitioning_manager.h"

#include <ydb/core/persqueue/common/partition_id.h>
#include <ydb/core/persqueue/public/partition_key_range/partition_key_range.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h> // TODO move to pubcli or common

namespace NKikimr::NPQ {

using TSlidingWindow = NSlidingWindow::TSlidingWindow<NSlidingWindow::TSumOperation<ui64>>;

namespace {
    void DoCleanUp(std::unordered_map<TString, TSlidingWindow>& writtenBytes) {
        auto now = TInstant::Now();

        for (auto it = writtenBytes.begin(); it != writtenBytes.end();) {
            auto& counter = it->second;
            counter.Update(now);

            if (0 == counter.GetValue()) {
                it = writtenBytes.erase(it);
            } else {
                 ++it;
            }
        }
    }
}

class TNoneAutopartitioningManager : public IAutopartitioningManager {
public:
    TNoneAutopartitioningManager(const NKikimrPQ::TPQTabletConfig& config) {
        Y_UNUSED(config);
    }

    void OnWrite(const TString& sourceId, ui64 size) override {
        Y_UNUSED(sourceId);
        Y_UNUSED(size);
    }

    void CleanUp() override {
    }

    std::optional<TString> SplitBoundary() override {
        return std::nullopt;
    }

    NKikimrPQ::EScaleStatus GetScaleStatus(NKikimrPQ::EScaleStatus currentState) override {
        return currentState;
    }

    void UpdateConfig(const NKikimrPQ::TPQTabletConfig& config) override {
        Y_UNUSED(config);
    }

    std::vector<std::pair<TString, ui64>> GetWrittenBytes() override {
        return {};
    }
};

class TSupportiveAutopartitioningManager : public IAutopartitioningManager {
public:
    TSupportiveAutopartitioningManager(const NKikimrPQ::TPQTabletConfig& config) {
        Y_UNUSED(config);
    }

    void OnWrite(const TString& sourceId, ui64 size) override {
        auto now = TInstant::Now();

        auto it = WrittenBytes.find(sourceId);
        if (it == WrittenBytes.end()) {
            auto [i,_] = WrittenBytes.emplace(sourceId, TSlidingWindow(TDuration::Minutes(1), 6));
            it = i;
        }

        auto& counter = it->second;
        counter.Update(size, now);
    }

    void CleanUp() override {
        DoCleanUp(WrittenBytes);
    }

    std::optional<TString> SplitBoundary() override {
        return std::nullopt;
    }

    NKikimrPQ::EScaleStatus GetScaleStatus(NKikimrPQ::EScaleStatus /*currentState*/) override {
        return NKikimrPQ::EScaleStatus::NORMAL;
    }

    void UpdateConfig(const NKikimrPQ::TPQTabletConfig& config) override {
        Y_UNUSED(config);
    }

    std::vector<std::pair<TString, ui64>> GetWrittenBytes() override {
        auto  now = TInstant::Now();
        std::vector<std::pair<TString, ui64>> result;

        for (auto& [sourceId, counter] : WrittenBytes) {
            counter.Update(now);
            if (counter.GetValue() > 0) {
                result.emplace_back(sourceId, counter.GetValue());
            }
        }

        return result;
    }

private:
    // SourceId -> SlidingWindow
    std::unordered_map<TString, TSlidingWindow> WrittenBytes;
};

class TAutopartitioningManager : public IAutopartitioningManager {
    static constexpr size_t Precision = 100;
public:
    TAutopartitioningManager(const NKikimrPQ::TPQTabletConfig& config, ui32 partitionId)
        : Config(config)
        , PartitionId(partitionId)
    {
        RecreateSumWrittenBytes();
    }

    void OnWrite(const TString& sourceId, ui64 size) override  {
        auto now = TInstant::Now();

        SumWrittenBytes->Update(size, now);

        TString sourceIdHash = sourceId // Kinesis protocol use empty sourceId
            ? AsKeyBound(Hash(NSourceIdEncoding::Decode(sourceId)))
            : "";

        auto it = WrittenBytes.find(sourceIdHash);
        if (it == WrittenBytes.end()) {
            auto [i,_] = WrittenBytes.emplace(sourceIdHash, TSlidingWindow(TDuration::Minutes(1), 6));
            it = i;
        }

        auto& counter = it->second;
        counter.Update(size, now);

        SourceIdCounter.Use(sourceIdHash, now);
    }

    void CleanUp() override {
        DoCleanUp(WrittenBytes);
    }

    std::optional<TString> SplitBoundary() override {
        CleanUp();

        if (WrittenBytes.size() < 2) {
            return std::nullopt;
        }

        std::vector<std::pair<TString, ui64>> sorted;
        sorted.reserve(WrittenBytes.size());
        for (const auto& [sourceIdHash, counter] : WrittenBytes) {
            sorted.emplace_back(sourceIdHash, counter.GetValue());
        }

        std::sort(sorted.begin(), sorted.end(), [](const auto& lhs, const auto& rhs) {
            return lhs < rhs;
        });

        auto* partition = FindIfPtr(Config.GetPartitions(), [&](const auto& v) {
            return v.GetPartitionId() == PartitionId;
        });

        AFL_ENSURE(partition)("p", PartitionId);

        const auto& keyRange = partition->GetKeyRange();

        auto inRange = [&](const TString& sourceIdHash) {
            if (sourceIdHash.empty()) {
                return false;
            }
            if (keyRange.HasFromBound() && sourceIdHash < keyRange.GetFromBound()) {
                return false;
            }
            if (keyRange.HasToBound() && sourceIdHash >= keyRange.GetToBound()) {
                return false;
            }
            return true;
        };

        ui64 lWrittenBytes = 0, rWrittenBytes = 0, oWrittenBytes = 0;
        ssize_t i = 0, j = sorted.size() - 1;
        TString* lastLeft = nullptr, *lastRight = nullptr;
        while (i <= j) {
            auto& lhs = sorted[i];
            auto& rhs = sorted[j];

            if (!inRange(lhs.first)) {
                oWrittenBytes += lhs.second;
                ++i;
            } else if (!inRange(rhs.first)) {
                oWrittenBytes += rhs.second;
                --j;
            } else if (lWrittenBytes < rWrittenBytes) {
                lWrittenBytes += lhs.second;
                ++i;
                lastLeft = &lhs.first;
            } else {
                rWrittenBytes += rhs.second;
                --j;
                lastRight = &rhs.first;
            }
        }

        if (oWrittenBytes >= lWrittenBytes || oWrittenBytes >= rWrittenBytes) {
            // The volume of entries in the partition with the SourceID manually linked to the partition is significant.
            // We divide the partition in half.
            return MiddleOf(keyRange.GetFromBound(), keyRange.GetToBound());
        }

        return MiddleOf(*lastLeft, *lastRight);
    }

    NKikimrPQ::EScaleStatus GetScaleStatus(NKikimrPQ::EScaleStatus /*currentState*/) override {
        const auto writeSpeedUsagePercent = SumWrittenBytes->GetValue() * 100.0 / Config.GetPartitionStrategy().GetScaleThresholdSeconds() / Config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond();
        const auto sourceIdWindow = TDuration::Seconds(std::min<ui32>(5, Config.GetPartitionStrategy().GetScaleThresholdSeconds()));
        const auto sourceIdCount = SourceIdCounter.Count(TInstant::Now() - sourceIdWindow);
        const auto canSplit = sourceIdCount > 1 || (sourceIdCount == 1 && SourceIdCounter.LastValue().empty() /* kinesis */);

        // LOG_D("TPartition::CheckScaleStatus"
        //         << " splitMergeAvgWriteBytes# " << SumWrittenBytes->GetValue()
        //         << " writeSpeedUsagePercent# " << writeSpeedUsagePercent
        //         << " scaleThresholdSeconds# " << Config.GetPartitionStrategy().GetScaleThresholdSeconds()
        //         << " totalPartitionWriteSpeed# " << Config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond()
        //         << " sourceIdCount=" << sourceIdCount
        //         << " canSplit=" << canSplit
        // );

        auto splitEnabled = Config.GetPartitionStrategy().GetPartitionStrategyType() == ::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT
            || Config.GetPartitionStrategy().GetPartitionStrategyType() == ::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT_AND_MERGE;

        auto mergeEnabled = Config.GetPartitionStrategy().GetPartitionStrategyType() == ::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT_AND_MERGE;

        if (splitEnabled && canSplit && writeSpeedUsagePercent >= Config.GetPartitionStrategy().GetScaleUpPartitionWriteSpeedThresholdPercent()) {
            // LOG_D("TPartition::CheckScaleStatus NEED_SPLIT.");
            return NKikimrPQ::EScaleStatus::NEED_SPLIT;
        } else if (mergeEnabled && writeSpeedUsagePercent <= Config.GetPartitionStrategy().GetScaleDownPartitionWriteSpeedThresholdPercent()) {
            // LOG_D("TPartition::CheckScaleStatus NEED_MERGE."
            //        << " writeSpeedUsagePercent: " << writeSpeedUsagePercent);
            return NKikimrPQ::EScaleStatus::NEED_MERGE;
        }
        return NKikimrPQ::EScaleStatus::NORMAL;
    }

    void UpdateConfig(const NKikimrPQ::TPQTabletConfig& config) override {
        if (config.GetPartitionStrategy().GetScaleThresholdSeconds() != SumWrittenBytes->GetDuration().Seconds()) {
            RecreateSumWrittenBytes();
        }
    }

    std::vector<std::pair<TString, ui64>> GetWrittenBytes() override {
        return {};
    }

    void RecreateSumWrittenBytes() {
        SumWrittenBytes.ConstructInPlace(TDuration::Seconds(Config.GetPartitionStrategy().GetScaleThresholdSeconds()), Precision);
    }

private:
    const NKikimrPQ::TPQTabletConfig& Config;
    const ui32 PartitionId;

    TMaybe<TSlidingWindow> SumWrittenBytes;
    // SourceIdHash -> SlidingWindow
    std::unordered_map<TString, TSlidingWindow> WrittenBytes;
    TLastCounter SourceIdCounter;
    TInstant LastCleanUp;
};



IAutopartitioningManager* CreateAutopartitioningManager(const NKikimrPQ::TPQTabletConfig& config, const TPartitionId& partitionId) {
    auto withAutopartitioning = SplitMergeEnabled(config) && !MirroringEnabled(config);
    if (!withAutopartitioning) {
        return new TNoneAutopartitioningManager(config);
    }

    if (partitionId.IsSupportivePartition()) {
        return new TSupportiveAutopartitioningManager(config);
    }

    return new TAutopartitioningManager(config, partitionId.OriginalPartitionId);
}

}
