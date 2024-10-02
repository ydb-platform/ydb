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
    YDB_ACCESSOR_DEF(std::optional<TDuration>, OverrideRequestsTracePingCheckPeriod);
    YDB_ACCESSOR_DEF(std::optional<TDuration>, OverrideLagForCompactionBeforeTierings);
    YDB_ACCESSOR(std::optional<TDuration>, OverrideGuaranteeIndexationInterval, TDuration::Zero());
    YDB_ACCESSOR(std::optional<TDuration>, OverridePeriodicWakeupActivationPeriod, std::nullopt);
    YDB_ACCESSOR(std::optional<TDuration>, OverrideStatsReportInterval, std::nullopt);
    YDB_ACCESSOR(std::optional<ui64>, OverrideGuaranteeIndexationStartBytesLimit, 0);
    YDB_ACCESSOR(std::optional<TDuration>, OverrideOptimizerFreshnessCheckDuration, TDuration::Zero());
    YDB_ACCESSOR_DEF(std::optional<TDuration>, OverrideCompactionActualizationLag);
    YDB_ACCESSOR_DEF(std::optional<TDuration>, OverrideTasksActualizationLag);
    YDB_ACCESSOR_DEF(std::optional<TDuration>, OverrideReadTimeoutClean);
    EOptimizerCompactionWeightControl CompactionControl = EOptimizerCompactionWeightControl::Force;

    YDB_ACCESSOR(std::optional<ui64>, OverrideReduceMemoryIntervalLimit, 1024);
    YDB_ACCESSOR_DEF(std::optional<ui64>, OverrideRejectMemoryIntervalLimit);

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
    virtual TDuration DoGetLagForCompactionBeforeTierings(const TDuration def) const override {
        return OverrideLagForCompactionBeforeTierings.value_or(def);
    }

    virtual TDuration DoGetPingCheckPeriod(const TDuration def) const override {
        return OverrideRequestsTracePingCheckPeriod.value_or(def);
    }

    virtual TDuration DoGetCompactionActualizationLag(const TDuration def) const override {
        return OverrideCompactionActualizationLag.value_or(def);
    }


    virtual bool IsBackgroundEnabled(const EBackground id) const override {
        TGuard<TMutex> g(Mutex);
        return !DisabledBackgrounds.contains(id);
    }

    virtual TDuration DoGetActualizationTasksLag(const TDuration d) const override {
        return OverrideTasksActualizationLag.value_or(d);
    }

    virtual void DoOnTabletInitCompleted(const ::NKikimr::NColumnShard::TColumnShard& shard) override;
    virtual void DoOnTabletStopped(const ::NKikimr::NColumnShard::TColumnShard& shard) override;
    virtual void DoOnAfterGCAction(const ::NKikimr::NColumnShard::TColumnShard& shard, const NOlap::IBlobsGCAction& action) override;

    virtual bool DoOnWriteIndexComplete(const NOlap::TColumnEngineChanges& changes, const ::NKikimr::NColumnShard::TColumnShard& shard) override;
    virtual TDuration DoGetGuaranteeIndexationInterval(const TDuration defaultValue) const override {
        return OverrideGuaranteeIndexationInterval.value_or(defaultValue);
    }
    virtual TDuration DoGetPeriodicWakeupActivationPeriod(const TDuration defaultValue) const override {
        return OverridePeriodicWakeupActivationPeriod.value_or(defaultValue);
    }
    virtual TDuration DoGetStatsReportInterval(const TDuration defaultValue) const override {
        return OverrideStatsReportInterval.value_or(defaultValue);
    }
    virtual ui64 DoGetGuaranteeIndexationStartBytesLimit(const ui64 defaultValue) const override {
        return OverrideGuaranteeIndexationStartBytesLimit.value_or(defaultValue);
    }
    virtual TDuration DoGetOptimizerFreshnessCheckDuration(const TDuration defaultValue) const override {
        return OverrideOptimizerFreshnessCheckDuration.value_or(defaultValue);
    }
    virtual TDuration DoGetReadTimeoutClean(const TDuration def) const override {
        return OverrideReadTimeoutClean.value_or(def);
    }
    virtual ui64 DoGetReduceMemoryIntervalLimit(const ui64 def) const override {
        return OverrideReduceMemoryIntervalLimit.value_or(def);
    }
    virtual ui64 DoGetRejectMemoryIntervalLimit(const ui64 def) const override {
        return OverrideRejectMemoryIntervalLimit.value_or(def);
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
    const TAtomicCounter& GetIndexWriteControllerBrokeCount() const {
        return IndexWriteControllerBrokeCount;
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
