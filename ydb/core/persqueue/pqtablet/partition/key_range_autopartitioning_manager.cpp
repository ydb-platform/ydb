#include "key_range_autopartitioning_manager.h"
#include "sourceid.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/persqueue/pqtablet/common/logging.h>
#include <ydb/core/persqueue/public/partition_key_range/key_range_split.h>
#include <ydb/core/persqueue/public/utils.h>

#include <library/cpp/time_provider/time_provider.h>


#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::PERSQUEUE

namespace NKikimr::NPQ {

namespace {

using TSlidingWindow = NSlidingWindow::TSlidingWindow<NSlidingWindow::TSumOperation<ui64>>;

class TKeyRangeAutopartitioningManager : public IAutopartitioningManager, public TLogPrefix {
    static constexpr size_t Precision = 100;
    static constexpr size_t MaxSketchCount = 10;
    static constexpr TDuration SplitBoundaryTtl = TDuration::Seconds(1);

public:
    TKeyRangeAutopartitioningManager(const NKikimrPQ::TPQTabletConfig& config, ui32 partitionId, const TSourceIdStorage& sourceIds)
        : Config(config)
        , PartitionId(partitionId)
        , Sources(sourceIds)
        , KeyTypes(MakePartitionKeyTypes(config))
    {
        RecreateWindows();
    }

    TStructuredMessage LogPrefix() const override {
        return YDB_LOG_CREATE_MESSAGE(
            {"className", "TKeyRangeAutopartitioningManager"},
            {"partition", PartitionId});
    }

    void OnWrite(const TString& sourceId, ui64 bytes, ui64 messages, const TString& key) override {
        Y_UNUSED(sourceId);
        Y_UNUSED(messages);
        const auto now = TAppData::TimeProvider->Now();
        WrittenBytes->Update(bytes, now);
        WrittenKeys->Add(key, bytes, now);
    }

    void AddKeysStats(ETag tag, const NPQ::TPartitioningKeysManager& keysManager) override {
        Y_UNUSED(tag);
        Y_UNUSED(keysManager);
    }

    void CleanUp() override {
    }

    NKikimrPQ::EScaleStatus GetScaleStatus(NKikimrPQ::EScaleStatus currentState) override {
        Y_UNUSED(currentState);
        const auto now = TAppData::TimeProvider->Now();
        const auto writeLoadPercent = GetWriteLoadPercent(now);
        const bool overloaded = writeLoadPercent >= Config.GetPartitionStrategy().GetScaleUpPartitionWriteSpeedThresholdPercent();
        if (CanSplit() && overloaded && GetSplitBoundary(now)) {
            LOG_D("TKeyRangeAutopartitioningManager::GetScaleStatus NEED_SPLIT", {"writeLoadPercent", writeLoadPercent});
            return NKikimrPQ::EScaleStatus::NEED_SPLIT;
        }
        return NKikimrPQ::EScaleStatus::NORMAL;
    }

    std::optional<TString> SplitBoundary() override {
        return SplitBoundaryCache;
    }

    TAutopartitioningManagerSnapshot GetSnapshot() override {
        return {};
    }

    void UpdateConfig(const NKikimrPQ::TPQTabletConfig& config) override {
        if (config.GetPartitionStrategy().GetScaleThresholdSeconds() != WrittenBytes->GetDuration().Seconds()) {
            RecreateWindows();
        }
    }

private:
    TDuration GetWindow() const {
        return TDuration::Seconds(Max<ui32>(1, Config.GetPartitionStrategy().GetScaleThresholdSeconds()));
    }

    bool CanSplit() const {
        const auto& strategy = Config.GetPartitionStrategy();
        const auto strategyType = strategy.GetPartitionStrategyType();
        return (strategyType == NKikimrPQ::TPQTabletConfig::CAN_SPLIT || strategyType == NKikimrPQ::TPQTabletConfig::CAN_SPLIT_AND_MERGE)
            && Config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond()
            && strategy.GetScaleThresholdSeconds();
    }

    double GetWriteLoadPercent(TInstant now) {
        WrittenBytes->Update(now);
        return WrittenBytes->GetValue() * 100.0 / Config.GetPartitionStrategy().GetScaleThresholdSeconds()
            / Config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond();
    }

    void RecreateWindows() {
        WrittenBytes.ConstructInPlace(GetWindow(), Precision);
        WrittenKeys.ConstructInPlace(KeyTypes, Max<size_t>(1, Min<size_t>(MaxSketchCount, GetWindow().Seconds())), GetWindow());
    }

    std::optional<TString> GetSplitBoundary(TInstant now) {
        if (SplitBoundaryTime + SplitBoundaryTtl <= now) {
            SplitBoundaryCache = PlanSplitBoundary(now);
            SplitBoundaryTime = now;
        }
        return SplitBoundaryCache;
    }

    std::optional<TString> PlanSplitBoundary(TInstant now) {
        const TKeyRangeWriteStats stats{
            .Sources = GetRegisteredSources(),
            .Keys = WrittenKeys->GetKeys(now),
        };
        const auto plan = PlanKeyRangeSplit(GetPartitionKeyRange(), KeyTypes, stats);
        if (plan.empty()) {
            return std::nullopt;
        }

        LOG_D("TKeyRangeAutopartitioningManager::PlanSplitBoundary",
            {"leftBytes", plan[0].EstimatedBytes},
            {"leftSources", plan[0].Sources.size()},
            {"rightBytes", plan[1].EstimatedBytes},
            {"rightSources", plan[1].Sources.size()});
        return plan[1].Range.FromBound->GetBuffer();
    }

    TPartitionKeyRange GetPartitionKeyRange() const {
        const auto* partition = GetPartitionConfig(Config, PartitionId);
        AFL_ENSURE(partition)("partition_id", PartitionId);
        return TPartitionKeyRange::Parse(partition->GetKeyRange());
    }

    TVector<TKeyRangeSource> GetRegisteredSources() const {
        TVector<TKeyRangeSource> sources;
        for (const auto& [sourceId, sourceIdInfo] : Sources.GetInMemorySourceIds()) {
            if (sourceIdInfo.KeyRange) {
                sources.push_back({.SourceId = sourceId, .KeyRange = *sourceIdInfo.KeyRange});
            }
        }
        return sources;
    }

    const NKikimrPQ::TPQTabletConfig& Config;
    const ui32 PartitionId;
    const TSourceIdStorage& Sources;
    const TVector<NScheme::TTypeInfo> KeyTypes;

    TMaybe<TKeyRangeKeysCollector> WrittenKeys;
    TMaybe<TSlidingWindow> WrittenBytes;

    std::optional<TString> SplitBoundaryCache;
    TInstant SplitBoundaryTime;
};

} // anonymous

std::unique_ptr<IAutopartitioningManager> CreateKeyRangeAutopartitioningManager(const NKikimrPQ::TPQTabletConfig& config, ui32 partitionId,
    const TSourceIdStorage& sourceIds)
{
    return std::make_unique<TKeyRangeAutopartitioningManager>(config, partitionId, sourceIds);
}

}
