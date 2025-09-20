#include "autopartitioning_manager.h"

#include <ydb/core/persqueue/public/partition_key_range/partition_key_range.h>
#include <ydb/core/persqueue/public/utils.h>

namespace NKikimr::NPQ {

class TNoneAutopartitioningManager : public IAutopartitioningManager {
public:
    TNoneAutopartitioningManager(const NKikimrPQ::TPQTabletConfig& config) {
        Y_UNUSED(config);
    }

    void OnWrite(const TString& sourceId, ui64 size) override {
        Y_UNUSED(sourceId);
        Y_UNUSED(size);
    }
    std::optional<TString> SplitBoundary() override {
        return std::nullopt;
    }

    NKikimrPQ::EScaleStatus GetScaleStatus(const NActors::TActorContext& ctx) override {
        Y_UNUSED(ctx);
        return NKikimrPQ::EScaleStatus::NORMAL;
    }

    void UpdateConfig(const NKikimrPQ::TPQTabletConfig& config) override {
        Y_UNUSED(config);
    }
};

class TAutopartitioningManager : public IAutopartitioningManager {
    using TSlidingWindow = NSlidingWindow::TSlidingWindow<NSlidingWindow::TSumOperation<ui64>>;

    static constexpr size_t Precision = 100;
public:
    TAutopartitioningManager(const NKikimrPQ::TPQTabletConfig& config)
        : Config(config)
    {
        RecreateSumWrittenBytes();
    }

    void OnWrite(const TString& sourceId, ui64 size) override  {
        auto now = TInstant::Now();

        SumWrittenBytes->Update(size, now);

        /* TODO hash of sourceId*/
        TString sourceIdHash = sourceId;
        auto it = WrittenBytes.find(sourceIdHash);
        if (it == WrittenBytes.end()) {
            auto [i,_] = WrittenBytes.emplace(sourceIdHash, TSlidingWindow(TDuration::Minutes(1), 6));
            it = i;
        }

        auto& counter = it->second;
        counter.Update(size, now);

        SourceIdCounter.Use(sourceIdHash, now);
    }

    void CleanUp() {
        auto now = TInstant::Now();
        for (auto it = WrittenBytes.begin(); it != WrittenBytes.end(); ++it) {
            auto& counter = it->second;
            counter.Update(now);

            if (0 == counter.GetValue()) {
            it = WrittenBytes.erase(it);
            }
        }
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

        ui64 lWrittenBytes = 0, rWrittenBytes = 0;
        size_t i = 0, j = sorted.size() - 1;
        bool lastIsLeft = false;
        TString* lastLeft, *lastRight;
        while (i < j) {
            auto& lhs = sorted[i];
            auto& rhs = sorted[j];

            if (lWrittenBytes < rWrittenBytes) {
                lWrittenBytes += lhs.second;
                ++i;
                lastIsLeft = true;
                lastLeft = &lhs.first;
            } else {
                rWrittenBytes += rhs.second;
                --j;
                lastIsLeft = false;
                lastRight = &rhs.first;
            }
        }

        return MiddleOf(*lastLeft, *lastRight);
    }

    NKikimrPQ::EScaleStatus GetScaleStatus(const NActors::TActorContext& ctx) override {
        const auto writeSpeedUsagePercent = SumWrittenBytes->GetValue() * 100.0 / Config.GetPartitionStrategy().GetScaleThresholdSeconds() / Config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond();
        const auto sourceIdWindow = TDuration::Seconds(std::min<ui32>(5, Config.GetPartitionStrategy().GetScaleThresholdSeconds()));
        const auto sourceIdCount = SourceIdCounter.Count(ctx.Now() - sourceIdWindow);
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

    void RecreateSumWrittenBytes() {
        SumWrittenBytes.ConstructInPlace(TDuration::Seconds(Config.GetPartitionStrategy().GetScaleThresholdSeconds()), Precision);
    }

private:
    const NKikimrPQ::TPQTabletConfig& Config;

    TMaybe<TSlidingWindow> SumWrittenBytes;
    // SoureIdHash -> SlidingWindow
    std::unordered_map<TString, TSlidingWindow> WrittenBytes;
    TLastCounter SourceIdCounter;

};

IAutopartitioningManager* CreateAutopartitioningManager(const NKikimrPQ::TPQTabletConfig& config, bool supportive) {
    return !supportive && SplitMergeEnabled(config) && !MirroringEnabled(config)
        ? static_cast<IAutopartitioningManager*>(new TAutopartitioningManager(config))
        : static_cast<IAutopartitioningManager*>(new TNoneAutopartitioningManager(config));
}

}
