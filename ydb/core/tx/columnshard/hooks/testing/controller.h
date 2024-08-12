#pragma once
#include "ro_controller.h"
#include <ydb/core/tx/columnshard/blobs_action/abstract/blob_set.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/common/tablet_id.h>
#include <ydb/core/tx/columnshard/engines/writer/write_controller.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <util/string/join.h>

namespace NKikimr::NYDBTest::NColumnShard {

class TController: public TReadOnlyController {
private:
    using TBase = TReadOnlyController;
    YDB_ACCESSOR_DEF(std::optional<TDuration>, RequestsTracePingCheckPeriod);
    YDB_ACCESSOR_DEF(std::optional<TDuration>, LagForCompactionBeforeTierings);
    YDB_ACCESSOR(std::optional<TDuration>, GuaranteeIndexationInterval, TDuration::Zero());
    YDB_ACCESSOR(std::optional<TDuration>, PeriodicWakeupActivationPeriod, std::nullopt);
    YDB_ACCESSOR(std::optional<TDuration>, StatsReportInterval, std::nullopt);
    YDB_ACCESSOR(std::optional<ui64>, GuaranteeIndexationStartBytesLimit, 0);
    YDB_ACCESSOR(std::optional<TDuration>, OptimizerFreshnessCheckDuration, TDuration::Zero());
    YDB_ACCESSOR_DEF(std::optional<TDuration>, CompactionActualizationLag);
    YDB_ACCESSOR_DEF(std::optional<TDuration>, TasksActualizationLag);
    EOptimizerCompactionWeightControl CompactionControl = EOptimizerCompactionWeightControl::Force;

    YDB_ACCESSOR(std::optional<ui64>, OverrideReduceMemoryIntervalLimit, 1024);
    YDB_ACCESSOR_DEF(std::optional<ui64>, OverrideRejectMemoryIntervalLimit);

    std::optional<TDuration> ReadTimeoutClean;
    std::optional<ui32> ExpectedShardsCount;

    THashMap<ui64, const ::NKikimr::NColumnShard::TColumnShard*> ShardActuals;
    THashMap<TString, THashMap<NOlap::TUnifiedBlobId, THashSet<NOlap::TTabletId>>> RemovedBlobIds;
    TMutex Mutex;

    YDB_ACCESSOR(bool, IndexWriteControllerEnabled, true);
    mutable TAtomicCounter IndexWriteControllerBrokeCount;
    std::set<EBackground> DisabledBackgrounds;

    TMutex ActiveTabletsMutex;
    std::set<ui64> ActiveTablets;

    class TBlobInfo {
    private:
        const NOlap::TUnifiedBlobId BlobId;
        std::optional<ui64> OwnerTabletId;
        THashSet<ui64> SharedTabletIdsFromShared;
        THashSet<ui64> SharedTabletIdsFromOwner;
    public:
        TBlobInfo(const NOlap::TUnifiedBlobId& blobId)
            : BlobId(blobId)
        {

        }
        void AddOwner(const ui64 tabletId) {
            if (!OwnerTabletId) {
                OwnerTabletId = tabletId;
            } else {
                AFL_VERIFY(*OwnerTabletId == tabletId);
            }
        }

        void AddSharingFromOwner(const ui64 tabletId) {
            SharedTabletIdsFromOwner.emplace(tabletId);
        }
        void AddSharingFromShared(const ui64 tabletId) {
            SharedTabletIdsFromShared.emplace(tabletId);
        }
        void Check() const {
            AFL_VERIFY(OwnerTabletId);
            AFL_VERIFY(SharedTabletIdsFromShared == SharedTabletIdsFromOwner)("blob_id", BlobId.ToStringNew())("shared", JoinSeq(",", SharedTabletIdsFromShared))("owned", JoinSeq(",", SharedTabletIdsFromOwner));
        }

        void DebugString(const TString& delta, TStringBuilder& sb) const {
            if (OwnerTabletId) {
                sb << delta << "O: " << *OwnerTabletId << Endl;
            }
            if (SharedTabletIdsFromShared.size()) {
                sb << delta << "S: " << JoinSeq(",", SharedTabletIdsFromShared) << Endl;
            }
        }
    };

    class TCheckContext {
    private:
        THashMap<TString, THashMap<NOlap::TUnifiedBlobId, TBlobInfo>> Infos;
    public:
        void Check() const {
            for (auto&& i : Infos) {
                for (auto&& b : i.second) {
                    b.second.Check();
                }
            }
        }

        TString DebugString() const {
            TStringBuilder sb;
            for (auto&& i : Infos) {
                sb << i.first << Endl;
                for (auto&& b : i.second) {
                    sb << "    " << b.first << Endl;
                    b.second.DebugString("        ", sb);
                }
            }
            return sb;
        }

        void AddCategories(const ui64 tabletId, THashMap<TString, NOlap::TBlobsCategories>&& categories) {
            for (auto&& s : categories) {
                for (auto it = s.second.GetDirect().GetIterator(); it.IsValid(); ++it) {
                    Infos[s.first].emplace(it.GetBlobId(), it.GetBlobId()).first->second.AddOwner((ui64)it.GetTabletId());
                }
                for (auto it = s.second.GetBorrowed().GetIterator(); it.IsValid(); ++it) {
                    Infos[s.first].emplace(it.GetBlobId(), it.GetBlobId()).first->second.AddOwner((ui64)it.GetTabletId());
                    Infos[s.first].emplace(it.GetBlobId(), it.GetBlobId()).first->second.AddSharingFromShared((ui64)tabletId);
                }
                for (auto it = s.second.GetSharing().GetIterator(); it.IsValid(); ++it) {
                    Infos[s.first].emplace(it.GetBlobId(), it.GetBlobId()).first->second.AddOwner(tabletId);
                    Infos[s.first].emplace(it.GetBlobId(), it.GetBlobId()).first->second.AddSharingFromOwner((ui64)it.GetTabletId());
                    if (it.GetTabletId() == (NOlap::TTabletId)tabletId) {
                        Infos[s.first].emplace(it.GetBlobId(), it.GetBlobId()).first->second.AddSharingFromShared((ui64)tabletId);
                    }
                }
            }
        }
    };

    void CheckInvariants(const ::NKikimr::NColumnShard::TColumnShard& shard, TCheckContext& context) const;

    THashSet<TString> SharingIds;
protected:
    virtual ::NKikimr::NColumnShard::TBlobPutResult::TPtr OverrideBlobPutResultOnCompaction(const ::NKikimr::NColumnShard::TBlobPutResult::TPtr original, const NOlap::TWriteActionsCollection& actions) const override;
    virtual TDuration GetLagForCompactionBeforeTierings(const TDuration def) const override {
        return LagForCompactionBeforeTierings.value_or(def);
    }

    virtual TDuration GetPingCheckPeriod(const TDuration def) const override {
        return RequestsTracePingCheckPeriod.value_or(def);
    }

    virtual TDuration GetCompactionActualizationLag(const TDuration def) const override {
        return CompactionActualizationLag.value_or(def);
    }


    virtual bool IsBackgroundEnabled(const EBackground id) const override {
        TGuard<TMutex> g(Mutex);
        return !DisabledBackgrounds.contains(id);
    }

    virtual TDuration GetActualizationTasksLag(const TDuration d) const override {
        return TasksActualizationLag.value_or(d);
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
    virtual TDuration GetReadTimeoutClean(const TDuration def) override {
        return ReadTimeoutClean.value_or(def);
    }
    virtual EOptimizerCompactionWeightControl GetCompactionControl() const override {
        return CompactionControl;
    }

    virtual void DoOnDataSharingFinished(const ui64 /*tabletId*/, const TString& sessionId) override {
        TGuard<TMutex> g(Mutex);
        AFL_VERIFY(SharingIds.erase(sessionId));
    }
    virtual void DoOnDataSharingStarted(const ui64 /*tabletId*/, const TString& sessionId) override {
        // dont check here. on finish only
        TGuard<TMutex> g(Mutex);
        SharingIds.emplace(sessionId);
    }

public:
    virtual TDuration GetRemovedPortionLivetime(const TDuration /*def*/) const override {
        return TDuration::Zero();
    }
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
    void SetReadTimeoutClean(const TDuration d) {
        ReadTimeoutClean = d;
    }

    bool HasPKSortingOnly() const;

    void OnSwitchToWork(const ui64 tabletId) override {
        TGuard<TMutex> g(ActiveTabletsMutex);
        ActiveTablets.emplace(tabletId);
    }

    void OnCleanupActors(const ui64 tabletId) override {
        TGuard<TMutex> g(ActiveTabletsMutex);
        ActiveTablets.erase(tabletId);
    }

    ui64 GetActiveTabletsCount() const {
        TGuard<TMutex> g(ActiveTabletsMutex);
        return ActiveTablets.size();
    }

    bool IsActiveTablet(const ui64 tabletId) const {
        TGuard<TMutex> g(ActiveTabletsMutex);
        return ActiveTablets.contains(tabletId);
    }
};

}
