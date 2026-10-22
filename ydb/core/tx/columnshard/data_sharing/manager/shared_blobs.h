#pragma once
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/blob_set.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/common.h>
#include <ydb/core/tx/columnshard/common/tablet_id.h>

#include <ydb/library/accessor/accessor.h>

#include <util/generic/algorithm.h>

#include <optional>

namespace NKikimr::NOlap::NDataSharing {

class TStorageSharedBlobsManager {
private:
    const TString StorageId;
    const TTabletId SelfTabletId;
    THashMap<TUnifiedBlobId, TTabletId> BorrowedBlobIds;   // blobId -> owned by tabletId
    TTabletsByBlob SharedBlobIds;   // blobId -> shared with tabletIds
    THashMap<TLogoBlobID, ui32> ForeignGroups;

    void RefreshForeignGroup(const TUnifiedBlobId& blob) {
        if (blob.GetLogoBlobId().TabletID() == static_cast<ui64>(SelfTabletId)) {
            return;
        }
        if (BorrowedBlobIds.contains(blob) || SharedBlobIds.Find(blob)) {
            const auto [it, inserted] = ForeignGroups.emplace(blob.GetLogoBlobId(), blob.GetDsGroup());
            AFL_VERIFY(inserted || it->second == blob.GetDsGroup());
        } else {
            ForeignGroups.erase(blob.GetLogoBlobId());
        }
    }

    bool CheckRemoveBlobId(const TTabletId tabletId, const TUnifiedBlobId& blobId, TBlobsCategories& blobs) const {
        const THashSet<TTabletId>* shared = SharedBlobIds.Find(blobId);
        bool doRemove = false;
        if (shared) {
            auto itTablet = shared->find(tabletId);
            AFL_VERIFY(itTablet != shared->end());
            if (shared->size() == 1) {
                doRemove = true;
            }
            blobs.AddSharing(tabletId, blobId);
        } else {
            doRemove = true;
        }
        if (doRemove) {
            auto it = BorrowedBlobIds.find(blobId);
            if (it != BorrowedBlobIds.end()) {
                AFL_VERIFY(it->second != tabletId);
                blobs.AddBorrowed(it->second, blobId);
            } else {
                blobs.AddDirect(tabletId, blobId);
            }
        }
        return doRemove;
    }

public:
    TStorageSharedBlobsManager(const TString& storageId, const TTabletId tabletId)
        : StorageId(storageId)
        , SelfTabletId(tabletId)
    {
    }

    bool HasBlobsInRange(const ui32 channel, const ui32 from, const ui32 to) const {
        const auto matches = [&](const auto& blob) {
            const auto& id = blob.first.GetLogoBlobId();
            return id.TabletID() == static_cast<ui64>(SelfTabletId) && id.Channel() == channel && id.Generation() >= from &&
                   id.Generation() < to;
        };
        return AnyOf(BorrowedBlobIds, matches) || AnyOf(SharedBlobIds, matches);
    }

    bool IsTrivialLinks() const {
        return BorrowedBlobIds.empty() && SharedBlobIds.IsEmpty();
    }

    bool HasBlobsForGroups(const THashSet<ui32>& groups) const {
        const auto inGroups = [&groups](const auto& blob) {
            return groups.contains(blob.first.GetDsGroup());
        };
        return AnyOf(BorrowedBlobIds, inGroups) || AnyOf(SharedBlobIds, inGroups);
    }

    // Portion metadata stores bare LogoBlobIDs. Foreign IDs must not use our channel history.
    // Update this index with sharing links, so metadata loads only perform a hash lookup.
    std::optional<ui32> GetForeignGroup(const TLogoBlobID& blob) const {
        const auto it = ForeignGroups.find(blob);
        return it == ForeignGroups.end() ? std::nullopt : std::optional<ui32>(it->second);
    }

    std::optional<TTabletId> GetBorrowedOwner(const TUnifiedBlobId& blob) const {
        const auto it = BorrowedBlobIds.find(blob);
        return it == BorrowedBlobIds.end() ? std::nullopt : std::optional<TTabletId>(it->second);
    }

    template <class TVisitor>
    void VisitSharedBlobs(TVisitor&& visitor) const {
        for (auto it = SharedBlobIds.GetIterator(); it.IsValid(); ++it) {
            if (!visitor(it.GetBlobId(), it.GetTabletId())) {
                return;
            }
        }
    }

    TTabletId GetSelfTabletId() const {
        return SelfTabletId;
    }

    TBlobsCategories GetBlobCategories() const {
        TBlobsCategories result(SelfTabletId);
        for (auto&& i : BorrowedBlobIds) {
            result.AddBorrowed(i.second, i.first);
        }
        for (auto i = SharedBlobIds.GetIterator(); i.IsValid(); ++i) {
            result.AddSharing(i.GetTabletId(), i.GetBlobId());
        }
        return result;
    }

    TBlobsCategories BuildRemoveCategories(const TTabletsByBlob& blobs) const {
        TBlobsCategories result(SelfTabletId);
        for (auto it = blobs.GetIterator(); it.IsValid(); ++it) {
            CheckRemoveBlobId(it.GetTabletId(), it.GetBlobId(), result);
        }
        return result;
    }

    TBlobsCategories BuildStoreCategories(const THashSet<TUnifiedBlobId>& blobIds) const {
        TBlobsCategories result(SelfTabletId);
        for (auto&& i : blobIds) {
            auto* tabletIds = SharedBlobIds.Find(i);
            auto it = BorrowedBlobIds.find(i);
            bool borrowed = false;
            bool direct = false;
            bool shared = false;
            if (it != BorrowedBlobIds.end()) {
                result.AddBorrowed(it->second, i);
                borrowed = true;
            } else if (!tabletIds) {
                result.AddDirect(SelfTabletId, i);
                direct = true;
            }
            if (tabletIds) {
                for (auto&& t : *tabletIds) {
                    result.AddSharing(t, i);
                    shared = true;
                }
            }
            AFL_VERIFY((borrowed ? 1 : 0) + (direct ? 1 : 0) + (shared ? 1 : 0) == 1)("b", borrowed)("d", direct)("s", shared)("blob_id", i.ToStringNew());
        }
        return result;
    }

    void RemoveSharedBlobsDB(NTabletFlatExecutor::TTransactionContext& txc, const TTabletsByBlob& blobIds);

    void RemoveSharedBlobs(const TTabletsByBlob& blobIds) {
        for (auto i = blobIds.GetIterator(); i.IsValid(); ++i) {
            AFL_VERIFY(SharedBlobIds.Remove(i.GetTabletId(), i.GetBlobId()));
            RefreshForeignGroup(i.GetBlobId());
        }
    }

    void WriteSharedBlobsDB(NTabletFlatExecutor::TTransactionContext& txc, const TTabletsByBlob& blobIds);

    [[nodiscard]] bool AddSharedBlobs(const TTabletsByBlob& blobIds) {
        bool result = true;
        for (auto i = blobIds.GetIterator(); i.IsValid(); ++i) {
            if (!SharedBlobIds.Add(i.GetTabletId(), i.GetBlobId())) {
                result = false;
            }
            RefreshForeignGroup(i.GetBlobId());
        }
        return result;
    }

    void WriteBorrowedBlobsDB(NTabletFlatExecutor::TTransactionContext& txc, const TTabletByBlob& blobIds);

    void AddBorrowedBlobs(const TTabletByBlob& blobIds) {
        for (auto&& i : blobIds) {
            if (i.second == SelfTabletId) {
                continue;
            }
            auto infoInsert = BorrowedBlobIds.emplace(i.first, i.second);
            if (!infoInsert.second) {
                AFL_VERIFY(infoInsert.first->second == i.second)("before", infoInsert.first->second)("after", i.second);
            }
            RefreshForeignGroup(i.first);
        }
    }

    void CASBorrowedBlobsDB(NTabletFlatExecutor::TTransactionContext& txc, const TTabletId tabletIdFrom, const TTabletId tabletIdTo,
        const THashSet<TUnifiedBlobId>& blobIds);

    void CASBorrowedBlobs(const TTabletId tabletIdFrom, const TTabletId tabletIdTo, const THashSet<TUnifiedBlobId>& blobIds);

    [[nodiscard]] bool UpsertSharedBlobOnLoad(const TUnifiedBlobId& blobId, const TTabletId tabletId) {
        const bool inserted = SharedBlobIds.Add(tabletId, blobId);
        RefreshForeignGroup(blobId);
        return inserted;
    }

    [[nodiscard]] bool UpsertBorrowedBlobOnLoad(const TUnifiedBlobId& blobId, const TTabletId ownerTabletId) {
        AFL_VERIFY(ownerTabletId != SelfTabletId);
        const bool inserted = BorrowedBlobIds.emplace(blobId, ownerTabletId).second;
        RefreshForeignGroup(blobId);
        return inserted;
    }

    void Clear() {
        ForeignGroups.clear();
        SharedBlobIds.Clear();
        BorrowedBlobIds.clear();
    }

    void OnTransactionExecuteAfterCleaning(const TBlobsCategories& removeTask, NTable::TDatabase& db);
    void OnTransactionCompleteAfterCleaning(const TBlobsCategories& removeTask);
};

class TSharedBlobsManager {
private:
    const TTabletId SelfTabletId;
    THashMap<TString, std::shared_ptr<TStorageSharedBlobsManager>> Storages;
    TAtomicCounter ExternalModificationsCount;

public:
    TSharedBlobsManager(const TTabletId tabletId)
        : SelfTabletId(tabletId)
    {
    }

    void StartExternalModification() {
        ExternalModificationsCount.Inc();
    }

    void FinishExternalModification() {
        AFL_VERIFY(ExternalModificationsCount.Dec() >= 0);
    }

    bool HasExternalModifications() const {
        return ExternalModificationsCount.Val();
    }

    bool IsTrivialLinks() const {
        for (auto&& i : Storages) {
            if (!i.second->IsTrivialLinks()) {
                return false;
            }
        }
        return true;
    }

    THashMap<TString, TBlobsCategories> GetBlobCategories() const {
        THashMap<TString, TBlobsCategories> result;
        for (auto&& i : Storages) {
            result.emplace(i.first, i.second->GetBlobCategories());
        }
        return result;
    }

    TTabletId GetSelfTabletId() const {
        return SelfTabletId;
    }

    void WriteSharedBlobsDB(NTabletFlatExecutor::TTransactionContext& txc, const THashMap<TString, TTabletsByBlob>& blobIds) {
        for (auto&& i : blobIds) {
            GetStorageManagerGuarantee(i.first)->WriteSharedBlobsDB(txc, i.second);
        }
    }

    void AddSharingBlobs(const THashMap<TString, TTabletsByBlob>& blobIds) {
        for (auto&& i : blobIds) {
            Y_UNUSED(GetStorageManagerGuarantee(i.first)->AddSharedBlobs(i.second));
        }
    }

    void WriteBorrowedBlobsDB(NTabletFlatExecutor::TTransactionContext& txc, const THashMap<TString, TTabletByBlob>& blobIds) {
        for (auto&& i : blobIds) {
            GetStorageManagerGuarantee(i.first)->WriteBorrowedBlobsDB(txc, i.second);
        }
    }

    void AddBorrowedBlobs(const THashMap<TString, TTabletByBlob>& blobIds) {
        for (auto&& i : blobIds) {
            GetStorageManagerGuarantee(i.first)->AddBorrowedBlobs(i.second);
        }
    }

    std::shared_ptr<TStorageSharedBlobsManager> GetStorageManagerOptional(const TString& storageId) const {
        auto it = Storages.find(storageId);
        if (it == Storages.end()) {
            return nullptr;
        }
        return it->second;
    }

    std::shared_ptr<TStorageSharedBlobsManager> GetStorageManagerVerified(const TString& storageId) const {
        auto it = Storages.find(storageId);
        AFL_VERIFY(it != Storages.end())("storage_id", storageId);
        return it->second;
    }

    std::shared_ptr<TStorageSharedBlobsManager> GetStorageManagerGuarantee(const TString& storageId) {
        auto it = Storages.find(storageId);
        if (it == Storages.end()) {
            it = Storages.emplace(storageId, std::make_shared<TStorageSharedBlobsManager>(storageId, SelfTabletId)).first;
        }
        return it->second;
    }

    bool LoadIdempotency(NTable::TDatabase& database);
};

}   // namespace NKikimr::NOlap::NDataSharing
