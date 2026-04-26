#include "autopartitioning_manager.h"

#include <memory>
#include <type_traits>

#include <ydb/core/base/feature_flags.h>
#include <ydb/core/persqueue/common/last_counter.h>
#include <ydb/core/persqueue/common/partition_id.h>
#include <ydb/core/persqueue/pqtablet/common/logging.h>
#include <ydb/core/persqueue/public/partition_key_range/partition_key_range.h>
#include <ydb/core/persqueue/common/partitioning_keys_manager.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h> // TODO move to pubcli or common

#include <util/generic/strbuf.h>

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

    void OnWrite(const TString& sourceId, ui64 size, const TString& key = "") override {
        Y_UNUSED(sourceId);
        Y_UNUSED(size);
        Y_UNUSED(key);
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

    TAutopartitioningManagerSnapshot GetSnapshot() override {
        return {};
    }

    void AddKeysStats(const TString& tag, const NPQ::TPartitioningKeysManager& keysManager) override {
        Y_UNUSED(tag);
        Y_UNUSED(keysManager);
    }
};

class TWindowedAutopartitioningManager : public IAutopartitioningManager {
    static constexpr size_t Precision = 100;
public:
    TWindowedAutopartitioningManager(const NKikimrPQ::TPQTabletConfig& config, ui32 partitionId, ui64 maxUsagePerSec, const TString& tag = "bytes")
        : Config(config)
        , Tag(tag)
        , PartitionId(partitionId),
        KeysManager(
            Min<size_t>(DEFAULT_NUM_SKETCHES, config.GetPartitionStrategy().GetScaleThresholdSeconds()),
            TDuration::Seconds(config.GetPartitionStrategy().GetScaleThresholdSeconds())),
        MaxUsagePerSec(maxUsagePerSec)
    {
        RecreateSumMetric();
    }

    void OnWrite(const TString& sourceId, ui64 delta, const TString& key = "") override  {
        PQ_LOG_D("TAutopartitioningManager::OnWrite"
            << " sourceId# " << sourceId
            << " delta# " << delta
            << " key# " << key
            << " isEnabled# " << IsEnabled()
            << " SumMetric# " << SumMetric->GetValue()
            << " Tag# " << Tag);
        if (!IsEnabled()) {
            return;
        }

        TString decodedSourceId = "";
        if (sourceId.size() > 0) {
            if (NPQ::NSourceIdEncoding::IsValidEncoded(sourceId)) {
                decodedSourceId = NPQ::NSourceIdEncoding::Decode(sourceId);
            } else {
                decodedSourceId = sourceId;
            }
        }

        auto key128 = NKikimr::NPQ::AsInt<TUint128>(key);
        auto now = TInstant::Now();

        SumMetric->Update(delta, now);

#ifdef _win_
        NKikimr::NPQ::TUint128 sourceIdHash = decodedSourceId.size() > 0 ? (double)Hash(decodedSourceId) : 0.;
#else
        auto sourceIdHash = decodedSourceId.size() > 0 ? Hash(decodedSourceId) : 0;
#endif
        TString sourceIdHashStr = decodedSourceId.size() > 0 // Kinesis protocol use empty sourceId
            ? AsKeyBound(sourceIdHash)
            : "";

        auto it = PerSourceMetrics.find(sourceIdHashStr);
        if (it == PerSourceMetrics.end()) {
            auto [i, _] = PerSourceMetrics.emplace(sourceIdHashStr, TSlidingWindow(TDuration::Minutes(1), 6));
            it = i;
        }

        auto& counter = it->second;
        counter.Update(delta, now);

        SourceIdCounter.Use(sourceIdHashStr, now);

        const auto& keyRange = GetKeyRange();

        if (key && IsInKeyRange(key, keyRange)) {
            KeysManager.Add(key128, delta);
        } else if (decodedSourceId.size() > 0 && IsInKeyRange(sourceIdHashStr, keyRange)) {
            KeysManager.Add(sourceIdHash, delta);
        }
    }

    void CleanUp() override {
        DoCleanUp(PerSourceMetrics);
    }

    std::optional<TString> SplitBoundary() override {
        if (!IsEnabled()) {
            return std::nullopt;
        }

        CleanUp();

        if (PerSourceMetrics.size() < 2) {
            return std::nullopt;
        }

        if (AppData()->FeatureFlags.GetEnableTopicPartitionSplitBasedOnKllSketch()) {
            auto medianKey = KeysManager.GetMedianKey();
            if (medianKey) {
                return AsKeyBound(medianKey);
            }

            auto* partition = GetPartition();
            const auto& keyRange = partition->GetKeyRange();

            PQ_LOG_D(
                TStringBuilder()
                << "TAutopartitioningManager::SplitBoundary KLL sketch enabled, no median key found, will split by middle of key range");
            return MiddleOf(keyRange.GetFromBound(), keyRange.GetToBound());
        }

        return GetSplitBoundaryBasedOnSourceIds();
    }

    NKikimrPQ::EScaleStatus GetScaleStatus(NKikimrPQ::EScaleStatus /*currentState*/) override {
        if (!IsEnabled()) {
            return NKikimrPQ::EScaleStatus::NORMAL;
        }

        auto now = TInstant::Now();
        const auto usagePercent = SumMetric->GetValue() * 100.0 / Config.GetPartitionStrategy().GetScaleThresholdSeconds() / MaxUsagePerSec;
        const auto sourceIdWindow = TDuration::Seconds(std::min<ui32>(5, Config.GetPartitionStrategy().GetScaleThresholdSeconds()));
        const auto sourceIdCount = SourceIdCounter.Count(now - sourceIdWindow);
        auto canSplit = sourceIdCount > 1 || (sourceIdCount == 1 && SourceIdCounter.LastValue().empty() /* kinesis */);
        if (AppData()->FeatureFlags.GetEnableTopicPartitionSplitBasedOnKllSketch()) {
            canSplit = canSplit || KeysManager.MoreThanOneKey(now - TDuration::Seconds(Config.GetPartitionStrategy().GetScaleThresholdSeconds()));
        }

        auto splitEnabled = Config.GetPartitionStrategy().GetPartitionStrategyType() == ::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT
            || Config.GetPartitionStrategy().GetPartitionStrategyType() == ::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT_AND_MERGE;
        auto mergeEnabled = Config.GetPartitionStrategy().GetPartitionStrategyType() == ::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT_AND_MERGE;

        PQ_LOG_D("TPartition::CheckScaleStatus"
            << " splitMergeAvgWriteBytes# " << SumMetric->GetValue()
            << " usagePercent# " << usagePercent
            << " scaleThresholdSeconds# " << Config.GetPartitionStrategy().GetScaleThresholdSeconds()
            << " totalPartitionWriteSpeed# " << MaxUsagePerSec
            << " sourceIdCount=" << sourceIdCount
            << " canSplit=" << canSplit
            << " tag# " << Tag
            << " verdict=" << (splitEnabled && canSplit && usagePercent >= Config.GetPartitionStrategy().GetScaleUpPartitionWriteSpeedThresholdPercent() ? "NEED_SPLIT" : "NORMAL")
        );

        auto shouldSplit = usagePercent
                >= Config.GetPartitionStrategy().GetScaleUpPartitionWriteSpeedThresholdPercent();

        if (splitEnabled && canSplit && shouldSplit) {
            PQ_LOG_D("TPartition::CheckScaleStatus NEED_SPLIT.");
            return NKikimrPQ::EScaleStatus::NEED_SPLIT;
        } else if (mergeEnabled && usagePercent <= Config.GetPartitionStrategy().GetScaleDownPartitionWriteSpeedThresholdPercent()) {
            PQ_LOG_D("TPartition::CheckScaleStatus NEED_MERGE."
                << " usagePercent: " << usagePercent);
            return NKikimrPQ::EScaleStatus::NEED_MERGE;
        }
        return NKikimrPQ::EScaleStatus::NORMAL;
    }

    void UpdateConfig(const NKikimrPQ::TPQTabletConfig& config) override {
        if (config.GetPartitionStrategy().GetScaleThresholdSeconds() != SumMetric->GetDuration().Seconds()) {
            RecreateSumMetric();
        }
    }

    TAutopartitioningManagerSnapshot GetSnapshot() override {
        return {
            .PerSourceMetrics = GetSerializedMetrics(),
            .KeysManagers = {
                {Tag, KeysManager},
            },
        };
    }

    void RecreateSumMetric() {
        SumMetric.ConstructInPlace(TDuration::Seconds(Config.GetPartitionStrategy().GetScaleThresholdSeconds()), Precision);
    }

    void AddKeysStats(const TString& tag, const NPQ::TPartitioningKeysManager& keysManager) override {
        if (tag != Tag) {
            return;
        }

        KeysManager.Merge(keysManager);
    }

protected:
    bool IsEnabled() {
        return MaxUsagePerSec > 0;
    }

    bool IsInKeyRange(const TString& key, const auto& keyRange) {
        if (key.empty()) {
            return false;
        }
        if (keyRange.HasFromBound() && key < keyRange.GetFromBound()) {
            return false;
        }
        if (keyRange.HasToBound() && key >= keyRange.GetToBound()) {
            return false;
        }
        return true;
    }

    std::vector<std::pair<TString, ui64>> GetSerializedMetrics() {
        auto  now = TInstant::Now();
        std::vector<std::pair<TString, ui64>> result;

        for (auto& [sourceIdHash, counter] : PerSourceMetrics) {
            counter.Update(now);
            if (counter.GetValue() > 0) {
                result.emplace_back(sourceIdHash, counter.GetValue());
            }
        }

        return result;
    }
    
    std::string GetSplitBoundaryBasedOnSourceIds() {
        std::vector<std::pair<TString, ui64>> sorted;
        sorted.reserve(PerSourceMetrics.size());
        for (const auto& [sourceIdHash, counter] : PerSourceMetrics) {
            sorted.emplace_back(sourceIdHash, counter.GetValue());
        }

        std::sort(sorted.begin(), sorted.end(), [](const auto& lhs, const auto& rhs) {
            return lhs < rhs;
        });

        auto* partition = GetPartition();
        const auto& keyRange = partition->GetKeyRange();

        ui64 lQuota = 0, rQuota = 0, oQuota = 0;
        ssize_t i = 0, j = sorted.size() - 1;
        TString* lastLeft = nullptr, *lastRight = nullptr;
        while (i <= j) {
            auto& lhs = sorted[i];
            auto& rhs = sorted[j];

            if (!IsInKeyRange(lhs.first, keyRange)) {
                oQuota += lhs.second;
                ++i;
            } else if (!IsInKeyRange(rhs.first, keyRange)) {
                oQuota += rhs.second;
                --j;
            } else if (lQuota < rQuota) {
                lQuota += lhs.second;
                ++i;
                lastLeft = &lhs.first;
            } else {
                rQuota += rhs.second;
                --j;
                lastRight = &rhs.first;
            }
        }

        if (oQuota >= lQuota || oQuota >= rQuota) {
            // The volume of entries in the partition with the SourceID manually linked to the partition is significant.
            // We divide the partition in half.
            return MiddleOf(keyRange.GetFromBound(), keyRange.GetToBound());
        }

        return MiddleOf(*lastLeft, *lastRight);
    }

    const NKikimrPQ::TPQTabletConfig_TPartition* GetPartition() {
        auto partition = FindIfPtr(Config.GetPartitions(), [&](const auto& v) {
            return v.GetPartitionId() == PartitionId;
        });
        AFL_ENSURE(partition)("p", PartitionId);
        return partition;
    }

    const NKikimrPQ::TPartitionKeyRange& GetKeyRange() {
        if (KeyRange) {
            return *KeyRange;
        }

        auto partition = GetPartition();
        KeyRange = partition->GetKeyRange();
        return *KeyRange;
    }

    static constexpr auto DEFAULT_NUM_SKETCHES = 100;

    const NKikimrPQ::TPQTabletConfig& Config;
    const TString Tag;
    const ui32 PartitionId;
    std::optional<NKikimrPQ::TPartitionKeyRange> KeyRange;

    TMaybe<TSlidingWindow> SumMetric;
    // SourceIdHash -> SlidingWindow
    std::unordered_map<TString, TSlidingWindow> PerSourceMetrics;
    NPQ::TPartitioningKeysManager KeysManager;
    TLastCounter<TString> SourceIdCounter;
    TInstant LastCleanUp;
    ui64 MaxUsagePerSec;
};

class TBytesAutopartitioningManager : public TWindowedAutopartitioningManager {
public:
    TBytesAutopartitioningManager(const NKikimrPQ::TPQTabletConfig& config, ui32 partitionId, const TString& tag = "bytes")
        : TWindowedAutopartitioningManager(config, partitionId, config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond(), tag) {
    }

    void UpdateConfig(const NKikimrPQ::TPQTabletConfig& config) override {
        MaxUsagePerSec = config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond();
        this->TWindowedAutopartitioningManager::UpdateConfig(config);
    }
};

class TMessagesAutopartitioningManager : public TWindowedAutopartitioningManager {
public:
    TMessagesAutopartitioningManager(const NKikimrPQ::TPQTabletConfig& config, ui32 partitionId, const TString& tag = "messages")
        : TWindowedAutopartitioningManager(config, partitionId, config.GetPartitionConfig().GetWriteSpeedInMessagesPerSecond(), tag) {
    }

    void UpdateConfig(const NKikimrPQ::TPQTabletConfig& config) override {
        MaxUsagePerSec = config.GetPartitionConfig().GetWriteSpeedInMessagesPerSecond();
        this->TWindowedAutopartitioningManager::UpdateConfig(config);
    }
};

class TStatefulAutopartitioningManager : public IAutopartitioningManager {
private:
    static constexpr auto TAG_BYTES = "bytes";
    static constexpr auto TAG_MESSAGES = "messages";

    enum class EState : ui8 {
        NORMAL = 0,
        SPLIT_BY_MESSAGES = 1,
        SPLIT_BY_BYTES = 2,
        ANY_SPLIT = SPLIT_BY_MESSAGES | SPLIT_BY_BYTES,
    };

    void MoveTo(EState state) {
        using TStateBits = std::underlying_type_t<EState>;
        State = static_cast<EState>(static_cast<TStateBits>(State) | static_cast<TStateBits>(state));
    }

    void MoveFrom(EState state) {
        using TStateBits = std::underlying_type_t<EState>;
        if (IsInState(state)) {
            State = static_cast<EState>(static_cast<TStateBits>(State) ^ static_cast<TStateBits>(state));
        }
    }

    bool IsInState(EState state) {
        using TStateBits = std::underlying_type_t<EState>;
        return static_cast<TStateBits>(State) & static_cast<TStateBits>(state);
    }

    IAutopartitioningManager& GetAutopartitioningManager(const TString& tag) {
        return *AutopartitioningManagers.at(tag);
    }

public:
    TStatefulAutopartitioningManager(const NKikimrPQ::TPQTabletConfig& config, ui32 partitionId) {
        AutopartitioningManagers.emplace(
            TAG_BYTES,
            std::make_unique<TBytesAutopartitioningManager>(
                config,
                partitionId,
                TAG_BYTES));
        AutopartitioningManagers.emplace(
            TAG_MESSAGES,
            std::make_unique<TMessagesAutopartitioningManager>(
                config,
                partitionId,
                TAG_MESSAGES));
    }

    void OnWrite(const TString& sourceId, ui64 size, const TString& key = "") override {
        for (auto& [tag, autopartitioningManager] : AutopartitioningManagers) {
            autopartitioningManager->OnWrite(sourceId, size, key);
        }
    }

    void CleanUp() override {
        for (auto& [tag, autopartitioningManager] : AutopartitioningManagers) {
            autopartitioningManager->CleanUp();
        }
    }

    NKikimrPQ::EScaleStatus GetScaleStatus(NKikimrPQ::EScaleStatus currentState) override {
        auto scaleStatusMessages = GetAutopartitioningManager(TAG_MESSAGES).GetScaleStatus(currentState);
        if (scaleStatusMessages == NKikimrPQ::EScaleStatus::NEED_SPLIT) {
            MoveTo(EState::SPLIT_BY_MESSAGES);
        } else {
            MoveFrom(EState::SPLIT_BY_MESSAGES);
        }

        auto scaleStatusBytes = GetAutopartitioningManager(TAG_BYTES).GetScaleStatus(currentState);
        if (scaleStatusBytes == NKikimrPQ::EScaleStatus::NEED_SPLIT) {
            MoveTo(EState::SPLIT_BY_BYTES);
        } else {
            MoveFrom(EState::SPLIT_BY_BYTES);
        }

        return (scaleStatusMessages == NKikimrPQ::EScaleStatus::NEED_SPLIT || scaleStatusBytes == NKikimrPQ::EScaleStatus::NEED_SPLIT) ?
            NKikimrPQ::EScaleStatus::NEED_SPLIT :
            NKikimrPQ::EScaleStatus::NORMAL;
    }

    std::optional<TString> SplitBoundary() override {
        if (State == EState::NORMAL) {
            return std::nullopt;
        }

        using TStateBits = std::underlying_type_t<EState>;
        if ((static_cast<TStateBits>(State) & static_cast<TStateBits>(EState::SPLIT_BY_MESSAGES)) != 0) {
            return GetAutopartitioningManager(TAG_MESSAGES).SplitBoundary();
        }

        return GetAutopartitioningManager(TAG_BYTES).SplitBoundary();
    }

    TAutopartitioningManagerSnapshot GetSnapshot() override {
        auto bytesAutopartitioningManagerSnapshot = GetAutopartitioningManager(TAG_BYTES).GetSnapshot();
        auto messagesAutopartitioningManagerSnapshot = GetAutopartitioningManager(TAG_MESSAGES).GetSnapshot();

        bytesAutopartitioningManagerSnapshot.KeysManagers.merge(
            messagesAutopartitioningManagerSnapshot.KeysManagers);

        return {
            .PerSourceMetrics = std::move(bytesAutopartitioningManagerSnapshot.PerSourceMetrics),
            .KeysManagers = std::move(bytesAutopartitioningManagerSnapshot.KeysManagers),
        };
    }

    void AddKeysStats(const TString& tag, const NPQ::TPartitioningKeysManager& keysManager) override {
        auto it = AutopartitioningManagers.find(tag);
        if (it == AutopartitioningManagers.end()) {
            return;
        }

        it->second->AddKeysStats(tag, keysManager);
    }

    void UpdateConfig(const NKikimrPQ::TPQTabletConfig& config) override {
        for (auto& [_, autopartitioningManager] : AutopartitioningManagers) {
            autopartitioningManager->UpdateConfig(config);
        }
    }

private:
    std::unordered_map<TString, std::unique_ptr<IAutopartitioningManager>> AutopartitioningManagers;
    EState State = EState::NORMAL;
};

class TSupportiveAutopartitioningManager : public IAutopartitioningManager {
public:
    TSupportiveAutopartitioningManager(const NKikimrPQ::TPQTabletConfig& config, ui32 partitionId) : AutopartitioningManager(config, partitionId) {
    }

    void OnWrite(const TString& sourceId, ui64 size, const TString& key = "") override {
        AutopartitioningManager.OnWrite(sourceId, size, key);
    }

    void CleanUp() override {
        AutopartitioningManager.CleanUp();
    }

    std::optional<TString> SplitBoundary() override {
        return std::nullopt;
    }

    NKikimrPQ::EScaleStatus GetScaleStatus(NKikimrPQ::EScaleStatus /*currentState*/) override {
        return NKikimrPQ::EScaleStatus::NORMAL;
    }

    void UpdateConfig(const NKikimrPQ::TPQTabletConfig& config) override {
        AutopartitioningManager.UpdateConfig(config);
    }

    TAutopartitioningManagerSnapshot GetSnapshot() override {
        return {};
    }

    void AddKeysStats(const TString& tag, const NPQ::TPartitioningKeysManager& keysManager) override {
        Y_UNUSED(tag);
        Y_UNUSED(keysManager);
    }

private:
    TStatefulAutopartitioningManager AutopartitioningManager;
};

IAutopartitioningManager* CreateAutopartitioningManager(const NKikimrPQ::TPQTabletConfig& config, const TPartitionId& partitionId) {
    auto withAutopartitioning = SplitMergeEnabled(config) && !MirroringEnabled(config);
    if (!withAutopartitioning) {
        return new TNoneAutopartitioningManager(config);
    }

    if (partitionId.IsSupportivePartition()) {
        return new TSupportiveAutopartitioningManager(config, partitionId.OriginalPartitionId);
    }

    return new TStatefulAutopartitioningManager(config, partitionId.OriginalPartitionId);
}

}
