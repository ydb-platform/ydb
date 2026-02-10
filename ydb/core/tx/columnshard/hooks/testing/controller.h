#pragma once
#include "ro_controller.h"

#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/blob_set.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/common/tablet_id.h>
#include <ydb/core/tx/columnshard/engines/writer/write_controller.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/wrappers/unavailable_storage.h>

#include <util/string/join.h>

namespace NKikimr::NYDBTest::NColumnShard {

class TController: public TReadOnlyController {
private:
    using TBase = TReadOnlyController;
    YDB_ACCESSOR_DEF(std::optional<TDuration>, OverrideUsedSnapshotLivetime);
    YDB_ACCESSOR_DEF(std::optional<TDuration>, OverrideStalenessLivetimePing);
    YDB_ACCESSOR_DEF(std::optional<TDuration>, OverrideLagForCompactionBeforeTierings);
    YDB_ACCESSOR(std::optional<TDuration>, OverrideGuaranteeIndexationInterval, TDuration::Zero());
    YDB_ACCESSOR(std::optional<TDuration>, OverridePeriodicWakeupActivationPeriod, std::nullopt);
    YDB_ACCESSOR(std::optional<TDuration>, OverrideStatsReportInterval, std::nullopt);
    YDB_ACCESSOR(std::optional<ui64>, OverrideGuaranteeIndexationStartBytesLimit, 0);
    YDB_ACCESSOR(std::optional<TDuration>, OverrideOptimizerFreshnessCheckDuration, TDuration::Zero());
    YDB_ACCESSOR_DEF(std::optional<TDuration>, OverrideCompactionActualizationLag);
    YDB_ACCESSOR_DEF(std::optional<TDuration>, OverrideTasksActualizationLag);
    YDB_ACCESSOR_DEF(std::optional<TDuration>, OverrideMaxReadStaleness);
    YDB_ACCESSOR(std::optional<ui64>, OverrideMemoryLimitForPortionReading, 100);
    YDB_ACCESSOR(std::optional<ui64>, OverrideLimitForPortionsMetadataAsk, 1);
    YDB_ACCESSOR(std::optional<NOlap::NSplitter::TSplitSettings>, OverrideBlobSplitSettings, NOlap::NSplitter::TSplitSettings::BuildForTests());
    YDB_FLAG_ACCESSOR(ExternalStorageUnavailable, false);

    YDB_ACCESSOR_DEF(std::optional<NKikimrProto::EReplyStatus>, OverrideBlobPutResultOnWriteValue);

    EOptimizerCompactionWeightControl CompactionControl = EOptimizerCompactionWeightControl::Force;

    std::optional<ui32> ExpectedShardsCount;

    THashMap<ui64, const ::NKikimr::NColumnShard::TColumnShard*> ShardActuals;
    THashMap<TString, THashMap<NOlap::TUnifiedBlobId, THashSet<NOlap::TTabletId>>> RemovedBlobIds;
    TMutex Mutex;

    YDB_ACCESSOR(bool, IndexWriteControllerEnabled, true);
    mutable TAtomicCounter IndexWriteControllerBrokeCount;
    std::set<EBackground> DisabledBackgrounds;

    TMutex ActiveTabletsMutex;

    bool ForcedGenerateInternalPathId = true;

    using TInternalPathId = NKikimr::NColumnShard::TInternalPathId;
    using TSchemeShardLocalPathId = NKikimr::NColumnShard::TSchemeShardLocalPathId;
    using TUnifiedPathId = NKikimr::NColumnShard::TUnifiedPathId;

    class TPathIdTranslator: public NOlap::IPathIdTranslator {
        THashMap<TInternalPathId, std::set<TSchemeShardLocalPathId>> InternalToSchemeShardLocal;
        THashMap<TSchemeShardLocalPathId, TInternalPathId> SchemeShardLocalToInternal;

    public:
        void AddPathId(const TUnifiedPathId& pathId) {
            AFL_VERIFY(InternalToSchemeShardLocal[pathId.InternalPathId].emplace(pathId.SchemeShardLocalPathId).second);
            AFL_VERIFY(SchemeShardLocalToInternal.emplace(pathId.SchemeShardLocalPathId, pathId.InternalPathId).second);
        }
        void DeletePathId(const TUnifiedPathId& pathId) {
            SchemeShardLocalToInternal.erase(pathId.SchemeShardLocalPathId);
            auto it = InternalToSchemeShardLocal.find(pathId.InternalPathId);
            if (it == InternalToSchemeShardLocal.end()) {
                return;
            }
            it->second.erase(pathId.SchemeShardLocalPathId);
            if (it->second.empty()) {
                InternalToSchemeShardLocal.erase(it);
            }
        }

    public:
        THashSet<TInternalPathId> GetInternalPathIds() const {
            THashSet<TInternalPathId> result;
            for (const auto& [internalPathId, schemeShardLocalPathId] : InternalToSchemeShardLocal) {
                result.emplace(internalPathId);
            }
            return result;
        }
        THashSet<TSchemeShardLocalPathId> GetSchemeShardLocalPathIds() const {
            THashSet<TSchemeShardLocalPathId> result;
            for (const auto& [internalPathId, schemeShardLocalPathIds] : InternalToSchemeShardLocal) {
                for (const auto& schemeShardLocalPathId: schemeShardLocalPathIds) {
                    AFL_VERIFY(result.insert(schemeShardLocalPathId).second);
                }
            }
            return result;
        }

    public:   //NOlap::IPathIdTranslator
        virtual std::optional<std::set<TSchemeShardLocalPathId>> ResolveSchemeShardLocalPathIdsOptional(
            const TInternalPathId internalPathId) const override {
            if (const auto* p = InternalToSchemeShardLocal.FindPtr(internalPathId)) {
                return { *p };
            }
            return std::nullopt;
        }
        virtual std::optional<TInternalPathId> ResolveInternalPathIdOptional(
            const TSchemeShardLocalPathId schemeShardLocalPathId, const bool withTabletPathId) const override {
            if (const auto* p = SchemeShardLocalToInternal.FindPtr(schemeShardLocalPathId)) {
                return { *p };
            }
            AFL_VERIFY(!withTabletPathId);
            return std::nullopt;
        }
    };
    THashMap<ui64, std::shared_ptr<TPathIdTranslator>> ActiveTablets;

    THashMap<TString, std::shared_ptr<NOlap::NDataLocks::ILock>> ExternalLocks;

    class TBlobInfo {
    private:
        const NOlap::TUnifiedBlobId BlobId;
        std::optional<ui64> OwnerTabletId;
        THashSet<ui64> SharedTabletIdsFromShared;
        THashSet<ui64> SharedTabletIdsFromOwner;

    public:
        TBlobInfo(const NOlap::TUnifiedBlobId& blobId)
            : BlobId(blobId) {
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
            AFL_VERIFY(SharedTabletIdsFromShared == SharedTabletIdsFromOwner)("blob_id", BlobId.ToStringNew())(
                "shared", JoinSeq(",", SharedTabletIdsFromShared))("owned", JoinSeq(",", SharedTabletIdsFromOwner));
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

    std::optional<TString> RestartOnLocalDbTxCommitted;

protected:
    virtual const NOlap::NSplitter::TSplitSettings& DoGetBlobSplitSettings(const NOlap::NSplitter::TSplitSettings& defaultValue) const override {
        if (OverrideBlobSplitSettings) {
            return *OverrideBlobSplitSettings;
        } else {
            return defaultValue;
        }
    }
    virtual ::NKikimr::NColumnShard::TBlobPutResult::TPtr OverrideBlobPutResultOnCompaction(
        const ::NKikimr::NColumnShard::TBlobPutResult::TPtr original, const NOlap::TWriteActionsCollection& actions) const override;

    virtual ui64 DoGetLimitForPortionsMetadataAsk(const ui64 defaultValue) const override {
        return OverrideLimitForPortionsMetadataAsk.value_or(defaultValue);
    }

    virtual ui64 DoGetMemoryLimitScanPortion(const ui64 defaultValue) const override {
        return OverrideMemoryLimitForPortionReading.value_or(defaultValue);
    }

    virtual TDuration DoGetLagForCompactionBeforeTierings(const TDuration def) const override {
        return OverrideLagForCompactionBeforeTierings.value_or(def);
    }

    virtual TDuration DoGetUsedSnapshotLivetime(const TDuration def) const override {
        return OverrideUsedSnapshotLivetime.value_or(def);
    }
    virtual std::optional<TDuration> DoGetStalenessLivetimePing() const override {
        return OverrideStalenessLivetimePing;
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
    virtual TDuration DoGetMaxReadStaleness(const TDuration def) const override {
        return OverrideMaxReadStaleness.value_or(def);
    }
    virtual ui64 DoGetMetadataRequestSoftMemoryLimit(const ui64 /* def */) const override {
        return 0;
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

    virtual THashMap<TString, std::shared_ptr<NOlap::NDataLocks::ILock>> GetExternalDataLocks() const override {
        TGuard<TMutex> g(Mutex);
        return ExternalLocks;
    }

    virtual NWrappers::NExternalStorage::IExternalStorageOperator::TPtr GetStorageOperatorOverride(
        const ::NKikimr::NColumnShard::NTiers::TExternalStorageId& /*storageId*/) const override {
        if (ExternalStorageUnavailableFlag) {
            return std::make_shared<NWrappers::NExternalStorage::TUnavailableExternalStorageOperator>(
                "unavailable", "disabled by test controller");
        }
        return nullptr;
    }

public:
    virtual bool CheckPortionsToMergeOnCompaction(const ui64 /*memoryAfterAdd*/, const ui32 currentSubsetsCount) override {
        return currentSubsetsCount > 1;
    }

    virtual NKikimrProto::EReplyStatus OverrideBlobPutResultOnWrite(const NKikimrProto::EReplyStatus originalStatus) const override {
        return OverrideBlobPutResultOnWriteValue.value_or(originalStatus);
    }

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

    bool IsBackgroundEnable(const EBackground id) {
        TGuard<TMutex> g(Mutex);
        return !DisabledBackgrounds.contains(id);
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

    void SetExpectedShardsCount(const ui32 value) {
        ExpectedShardsCount = value;
    }
    void SetCompactionControl(const EOptimizerCompactionWeightControl value) {
        CompactionControl = value;
    }

    void RegisterLock(const TString& name, const std::shared_ptr<NOlap::NDataLocks::ILock>& lock) {
        TGuard<TMutex> g(Mutex);
        AFL_VERIFY(ExternalLocks.emplace(name, lock).second)("name", name);
    }

    void UnregisterLock(const TString& name) {
        TGuard<TMutex> g(Mutex);
        AFL_VERIFY(ExternalLocks.erase(name))("name", name);
    }

    bool HasPKSortingOnly() const;

    void OnSwitchToWork(const ui64 tabletId) override {
        TGuard<TMutex> g(ActiveTabletsMutex);
        ActiveTablets.emplace(tabletId, std::make_shared<TPathIdTranslator>());
    }

    void OnCleanupActors(const ui64 tabletId) override {
        TGuard<TMutex> g(ActiveTabletsMutex);
        ActiveTablets.erase(tabletId);
    }

    THashMap<ui64, std::shared_ptr<TPathIdTranslator>> GetActiveTablets() const {
        TGuard<TMutex> g(ActiveTabletsMutex);
        return ActiveTablets;
    }

    bool IsActiveTablet(const ui64 tabletId) const {
        TGuard<TMutex> g(ActiveTabletsMutex);
        return ActiveTablets.contains(tabletId);
    }

    void SetRestartOnLocalTxCommitted(std::optional<TString> txInfo) {
        RestartOnLocalDbTxCommitted = std::move(txInfo);
    }

    const std::shared_ptr<TPathIdTranslator> GetPathIdTranslator(const ui64 tabletId) {
        TGuard<TMutex> g(ActiveTabletsMutex);
        const auto* tablet = ActiveTablets.FindPtr(tabletId);
        if (!tablet) {
            return nullptr;
        }
        return *tablet;
    }

    virtual void OnAfterLocalTxCommitted(
        const NActors::TActorContext& ctx, const ::NKikimr::NColumnShard::TColumnShard& shard, const TString& txInfo) override;

    virtual void OnAddPathId(const ui64 tabletId, const TUnifiedPathId& pathId) override {
        TGuard<TMutex> g(ActiveTabletsMutex);
        auto* tablet = ActiveTablets.FindPtr(tabletId);
        AFL_VERIFY(tablet);
        (*tablet)->AddPathId(pathId);
    }
    virtual void OnDeletePathId(const ui64 tabletId, const TUnifiedPathId& pathId) override {
        auto* tablet = ActiveTablets.FindPtr(tabletId);
        AFL_VERIFY(tablet);
        (*tablet)->DeletePathId(pathId);
    }

    virtual bool IsForcedGenerateInternalPathId() const override {
        return ForcedGenerateInternalPathId;
    }

    void SetForcedGenerateInternalPathId(const bool value) {
        ForcedGenerateInternalPathId = value;
    }
};

}   // namespace NKikimr::NYDBTest::NColumnShard
