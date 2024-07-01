#pragma once
#include "ro_controller.h"
#include <ydb/core/tx/columnshard/blobs_action/abstract/blob_set.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/common/tablet_id.h>
#include <ydb/core/tx/columnshard/engines/writer/write_controller.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NYDBTest::NColumnShard {

class TController: public TReadOnlyController {
private:
    using TBase = TReadOnlyController;
    YDB_ACCESSOR_DEF(std::optional<TDuration>, LagForCompactionBeforeTierings);
    YDB_ACCESSOR(std::optional<TDuration>, GuaranteeIndexationInterval, TDuration::Zero());
    YDB_ACCESSOR(std::optional<TDuration>, PeriodicWakeupActivationPeriod, std::nullopt);
    YDB_ACCESSOR(std::optional<TDuration>, StatsReportInterval, std::nullopt);
    YDB_ACCESSOR(std::optional<ui64>, GuaranteeIndexationStartBytesLimit, 0);
    YDB_ACCESSOR(std::optional<TDuration>, OptimizerFreshnessCheckDuration, TDuration::Zero());
    EOptimizerCompactionWeightControl CompactionControl = EOptimizerCompactionWeightControl::Force;
protected:
    virtual ::NKikimr::NColumnShard::TBlobPutResult::TPtr OverrideBlobPutResultOnCompaction(const ::NKikimr::NColumnShard::TBlobPutResult::TPtr original, const NOlap::TWriteActionsCollection& actions) const override;
    virtual TDuration GetLagForCompactionBeforeTierings(const TDuration def) const override {
        return LagForCompactionBeforeTierings.value_or(def);
    }

    virtual bool IsBackgroundEnabled(const EBackground id) const override {
        TGuard<TMutex> g(Mutex);
        return !DisabledBackgrounds.contains(id);
    }

    virtual void DoOnTabletInitCompleted(const ::NKikimr::NColumnShard::TColumnShard& shard) override;
    virtual void DoOnTabletStopped(const ::NKikimr::NColumnShard::TColumnShard& shard) override;
    virtual void DoOnAfterGCAction(const ::NKikimr::NColumnShard::TColumnShard& shard, const NOlap::IBlobsGCAction& action) override;

    virtual bool DoOnWriteIndexComplete(const NOlap::TColumnEngineChanges& changes, const ::NKikimr::NColumnShard::TColumnShard& shard) override;
    virtual TDuration GetGuaranteeIndexationInterval(const TDuration defaultValue) const override {
        return GuaranteeIndexationInterval.value_or(defaultValue);
    }
    TDuration GetPeriodicWakeupActivationPeriod(const TDuration defaultValue) const override {
        return PeriodicWakeupActivationPeriod.value_or(defaultValue);
    }
    TDuration GetStatsReportInterval(const TDuration defaultValue) const override {
        return StatsReportInterval.value_or(defaultValue);
    }
    virtual ui64 GetGuaranteeIndexationStartBytesLimit(const ui64 defaultValue) const override {
        return GuaranteeIndexationStartBytesLimit.value_or(defaultValue);
    }
    virtual TDuration GetOptimizerFreshnessCheckDuration(const TDuration defaultValue) const override {
        return OptimizerFreshnessCheckDuration.value_or(defaultValue);
    }
    virtual EOptimizerCompactionWeightControl GetCompactionControl() const override {
        return CompactionControl;
    }

public:
    const TAtomicCounter& GetIndexWriteControllerBrokeCount() const {
        return IndexWriteControllerBrokeCount;
    }
    virtual ui64 GetReduceMemoryIntervalLimit(const ui64 def) const override {
        return OverrideReduceMemoryIntervalLimit.value_or(def);
    }
    virtual ui64 GetRejectMemoryIntervalLimit(const ui64 def) const override {
        return OverrideRejectMemoryIntervalLimit.value_or(def);
    }
    bool IsTrivialLinks() const;
    TCheckContext CheckInvariants() const;

    ui32 GetShardActualsCount() const {
        TGuard<TMutex> g(Mutex);
        return ShardActuals.size();
    }

    void DisableBackground(const EBackground id) {
        TGuard<TMutex> g(Mutex);
        DisabledBackgrounds.emplace(id);
    }

    void EnableBackground(const EBackground id) {
        TGuard<TMutex> g(Mutex);
        DisabledBackgrounds.erase(id);
    }

    std::vector<ui64> GetShardActualIds() const {
        TGuard<TMutex> g(Mutex);
        std::vector<ui64> result;
        for (auto&& i : ShardActuals) {
            result.emplace_back(i.first);
        }
        return result;
    }

    std::vector<ui64> GetPathIds(const ui64 tabletId) const;

    void SetExpectedShardsCount(const ui32 value) {
        ExpectedShardsCount = value;
    }
    void SetCompactionControl(const EOptimizerCompactionWeightControl value) {
        CompactionControl = value;
    }

    bool HasPKSortingOnly() const;
    bool HasCompactions() const {
        return Compactions.Val();
    }
};

}
