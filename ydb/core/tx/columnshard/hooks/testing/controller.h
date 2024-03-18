#pragma once
#include <ydb/core/tx/columnshard/blobs_action/abstract/blob_set.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/common/tablet_id.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <util/string/join.h>

namespace NKikimr::NYDBTest::NColumnShard {

class TController: public ICSController {
private:
    YDB_READONLY(TAtomicCounter, FilteredRecordsCount, 0);
    YDB_READONLY(TAtomicCounter, Compactions, 0);
    YDB_READONLY(TAtomicCounter, Indexations, 0);
    YDB_READONLY(TAtomicCounter, IndexesSkippingOnSelect, 0);
    YDB_READONLY(TAtomicCounter, IndexesApprovedOnSelect, 0);
    YDB_READONLY(TAtomicCounter, IndexesSkippedNoData, 0);
    YDB_READONLY(TAtomicCounter, TieringUpdates, 0);
    YDB_READONLY(TAtomicCounter, ActualizationsCount, 0);
    YDB_READONLY(TAtomicCounter, ActualizationRefreshSchemeCount, 0);
    YDB_READONLY(TAtomicCounter, ActualizationRefreshTieringCount, 0);
    YDB_ACCESSOR(std::optional<TDuration>, GuaranteeIndexationInterval, TDuration::Zero());
    YDB_ACCESSOR(std::optional<TDuration>, PeriodicWakeupActivationPeriod, std::nullopt);
    YDB_ACCESSOR(std::optional<TDuration>, StatsReportInterval, std::nullopt);
    YDB_ACCESSOR(std::optional<ui64>, GuaranteeIndexationStartBytesLimit, 0);
    YDB_ACCESSOR(std::optional<TDuration>, OptimizerFreshnessCheckDuration, TDuration::Zero());
    EOptimizerCompactionWeightControl CompactionControl = EOptimizerCompactionWeightControl::Force;
    std::optional<TDuration> ReadTimeoutClean;
    std::optional<ui32> ExpectedShardsCount;

    THashMap<ui64, const ::NKikimr::NColumnShard::TColumnShard*> ShardActuals;
    THashMap<TString, THashMap<NOlap::TUnifiedBlobId, THashSet<NOlap::TTabletId>>> RemovedBlobIds;
    TMutex Mutex;

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
    virtual void OnPortionActualization(const NOlap::TPortionInfo& /*info*/) override {
        ActualizationsCount.Inc();
    }
    virtual void OnActualizationRefreshScheme() override {
        ActualizationRefreshSchemeCount.Inc();
    }
    virtual void OnActualizationRefreshTiering() override {
        ActualizationRefreshTieringCount.Inc();
    }
    virtual void DoOnTabletInitCompleted(const ::NKikimr::NColumnShard::TColumnShard& shard) override;
    virtual void DoOnTabletStopped(const ::NKikimr::NColumnShard::TColumnShard& shard) override;
    virtual void DoOnAfterGCAction(const ::NKikimr::NColumnShard::TColumnShard& shard, const NOlap::IBlobsGCAction& action) override;

    virtual bool DoOnAfterFilterAssembling(const std::shared_ptr<arrow::RecordBatch>& batch) override;
    virtual bool DoOnStartCompaction(std::shared_ptr<NOlap::TColumnEngineChanges>& changes) override;
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
    void OnTieringModified(const std::shared_ptr<NKikimr::NColumnShard::TTiersManager>& /*tiers*/) override {
        TieringUpdates.Inc();
    }

    virtual void DoOnDataSharingFinished(const ui64 /*tabletId*/, const TString& sessionId) override {
        TGuard<TMutex> g(Mutex);
        AFL_VERIFY(SharingIds.erase(sessionId));
        if (SharingIds.empty()) {
            CheckInvariants();
        }
    }
    virtual void DoOnDataSharingStarted(const ui64 /*tabletId*/, const TString& sessionId) override {
        TGuard<TMutex> g(Mutex);
        if (SharingIds.empty()) {
            CheckInvariants();
        }
        SharingIds.emplace(sessionId);
    }

public:
    bool IsTrivialLinks() const;
    TCheckContext CheckInvariants() const;

    ui32 GetShardActualsCount() const {
        TGuard<TMutex> g(Mutex);
        return ShardActuals.size();
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

    virtual void OnIndexSelectProcessed(const std::optional<bool> result) override {
        if (!result) {
            IndexesSkippedNoData.Inc();
        } else if (*result) {
            IndexesApprovedOnSelect.Inc();
        } else {
            IndexesSkippingOnSelect.Inc();
        }
    }
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
    bool HasCompactions() const {
        return Compactions.Val();
    }
};

}
