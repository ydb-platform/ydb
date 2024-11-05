#pragma once
#include <ydb/core/tx/columnshard/common/tablet_id.h>

#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/blob_set.h>
#include <ydb/core/tx/columnshard/data_sharing/common/session/common.h>
#include <ydb/core/tx/columnshard/data_sharing/initiator/controller/abstract.h>
#include <ydb/core/tx/columnshard/data_sharing/protos/links.pb.h>
#include <ydb/library/conclusion/result.h>

namespace NKikimr::NColumnShard {
class TColumnShard;
}

namespace NKikimr::NOlap {
class TColumnEngineForLogs;
}

namespace NKikimr::NOlap::NDataSharing {

class TStorageSharedBlobsManager;
class TSharedBlobsManager;

namespace NEvents {
class TPathIdData;
}

class TBlobOwnerRemap {
private:
    YDB_READONLY_DEF(TTabletId, From);
    YDB_READONLY_DEF(TTabletId, To);
public:
    TBlobOwnerRemap(const TTabletId from, const TTabletId to)
        : From(from)
        , To(to) {

    }

    bool operator==(const TBlobOwnerRemap& item) const {
        return From == item.From && To == item.To;
    }
};

class TStorageTabletTask {
private:
    TTabletId TabletId;
    TString StorageId;
    THashMap<TUnifiedBlobId, TBlobOwnerRemap> RemapOwner;
    TTabletByBlob InitOwner;
    TTabletsByBlob AddSharingLinks;
    TTabletsByBlob RemoveSharingLinks;
public:
    TStorageTabletTask(const TString& storageId, const TTabletId tabletId)
        : TabletId(tabletId)
        , StorageId(storageId) {

    }

    NKikimrColumnShardDataSharingProto::TStorageTabletTask SerializeToProto() const {
        NKikimrColumnShardDataSharingProto::TStorageTabletTask result;
        result.SetTabletId((ui64)TabletId);
        result.SetStorageId(StorageId);
        *result.MutableInitOwner() = InitOwner.SerializeToProto();
        *result.MutableAddSharingLinks() = AddSharingLinks.SerializeToProto();
        *result.MutableRemoveSharingLinks() = RemoveSharingLinks.SerializeToProto();

        for (auto&& i : RemapOwner) {
            auto* remapProto = result.AddRemapOwner();
            remapProto->SetBlobId(i.first.ToStringNew());
            remapProto->SetFrom((ui64)i.second.GetFrom());
            remapProto->SetTo((ui64)i.second.GetTo());
        }

        return result;
    }

    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TStorageTabletTask& proto) {
        StorageId = proto.GetStorageId();
        if (!StorageId) {
            return TConclusionStatus::Fail("empty storage id");
        }
        TabletId = (TTabletId)proto.GetTabletId();
        if (!(ui64)TabletId) {
            return TConclusionStatus::Fail("empty tablet id for storage task");
        }
        {
            auto parse = InitOwner.DeserializeFromProto(proto.GetInitOwner());
            if (!parse) {
                return parse;
            }
        }
        {
            auto parse = AddSharingLinks.DeserializeFromProto(proto.GetAddSharingLinks());
            if (!parse) {
                return parse;
            }
        }
        {
            auto parse = RemoveSharingLinks.DeserializeFromProto(proto.GetRemoveSharingLinks());
            if (!parse) {
                return parse;
            }
        }
        for (auto&& i : proto.GetRemapOwner()) {
            auto parse = TUnifiedBlobId::BuildFromString(i.GetBlobId(), nullptr);
            if (!parse) {
                return parse;
            }
            RemapOwner.emplace(*parse, TBlobOwnerRemap((TTabletId)i.GetFrom(), (TTabletId)i.GetTo()));
        }
        return TConclusionStatus::Success();
    }

    void ApplyForDB(NTabletFlatExecutor::TTransactionContext& txc, const std::shared_ptr<TStorageSharedBlobsManager>& manager) const;

    void ApplyForRuntime(const std::shared_ptr<TStorageSharedBlobsManager>& manager) const;

    const TString GetStorageId() const {
        return StorageId;
    }

    void AddRemapOwner(const TUnifiedBlobId& blobId, const TTabletId from, const TTabletId to) {
//        AFL_VERIFY(to != TabletId);
        AFL_VERIFY(RemapOwner.emplace(blobId, TBlobOwnerRemap(from, to)).second);
    }

    void AddInitOwner(const TUnifiedBlobId& blobId, const TTabletId to) {
//        AFL_VERIFY(to != TabletId);
        AFL_VERIFY(InitOwner->emplace(blobId, to).second);
    }

    void AddLink(const TUnifiedBlobId& blobId, const TTabletId tabletId) {
//        AFL_VERIFY(tabletId != TabletId);
        AFL_VERIFY(AddSharingLinks.Add(tabletId, blobId));
    }

    void RemoveLink(const TUnifiedBlobId& blobId, const TTabletId tabletId) {
        AFL_VERIFY(RemoveSharingLinks.Add(tabletId, blobId));
    }

    void Merge(const TStorageTabletTask& from) {
        AFL_VERIFY(TabletId == from.TabletId);
        for (auto&& i : from.InitOwner) {
            auto info = InitOwner->emplace(i.first, i.second);
            if (!info.second) {
                AFL_VERIFY(info.first->second == i.second);
            }
        }
        for (auto&& i : from.RemapOwner) {
            auto info = RemapOwner.emplace(i.first, i.second);
            if (!info.second) {
                AFL_VERIFY(info.first->second == i.second);
            }
        }
        AddSharingLinks.Add(from.AddSharingLinks);
        RemoveSharingLinks.Add(from.RemoveSharingLinks);
    }
};

class TTaskForTablet {
private:
    YDB_READONLY(TTabletId, TabletId, (TTabletId)0);
    THashMap<TString, TStorageTabletTask> TasksByStorage;
public:
    TTaskForTablet(const TTabletId tabletId)
        : TabletId(tabletId) 
    {
    }

    void Merge(const TTaskForTablet& from) {
        for (auto&& i : from.TasksByStorage) {
            auto it = TasksByStorage.find(i.first);
            if (it == TasksByStorage.end()) {
                TasksByStorage.emplace(i.first, i.second);
            } else {
                it->second.Merge(i.second);
            }
        }
    }

    void ApplyForDB(NTabletFlatExecutor::TTransactionContext& txc, const std::shared_ptr<TSharedBlobsManager>& manager) const;

    void ApplyForRuntime(const std::shared_ptr<TSharedBlobsManager>& manager) const;

    void AddStorage(TStorageTabletTask&& info) {
        auto storageId = info.GetStorageId();
        TasksByStorage.emplace(storageId, std::move(info));
    }

    const TStorageTabletTask* GetStorageTasksOptional(const TString& storageId) const {
        auto it = TasksByStorage.find(storageId);
        if (it == TasksByStorage.end()) {
            return nullptr;
        }
        return &it->second;
    }

    const TStorageTabletTask& GetStorageTasksGuarantee(const TString& storageId) {
        auto it = TasksByStorage.find(storageId);
        if (it == TasksByStorage.end()) {
            it = TasksByStorage.emplace(storageId, TStorageTabletTask(storageId, TabletId)).first;
        }
        return it->second;
    }

    NKikimrColumnShardDataSharingProto::TTaskForTablet SerializeToProto() const {
        NKikimrColumnShardDataSharingProto::TTaskForTablet result;
        for (auto&& i : TasksByStorage) {
            *result.AddTasksByStorage() = i.second.SerializeToProto();
        }
        return result;
    }

    TConclusionStatus DeserializeFromProto(const NKikimrColumnShardDataSharingProto::TTaskForTablet& proto) {
        for (auto&& i : proto.GetTasksByStorage()) {
            TStorageTabletTask sTask("", TTabletId(0));
            auto parse = sTask.DeserializeFromProto(i);
            if (!parse) {
                return parse;
            }
            const TString storageId = sTask.GetStorageId();
            AFL_VERIFY(TasksByStorage.emplace(storageId, std::move(sTask)).second);
        }
        return TConclusionStatus::Success();
    }

    TConclusion<std::unique_ptr<NTabletFlatExecutor::ITransaction>> BuildModificationTransaction(NColumnShard::TColumnShard* self, const TTabletId initiator,
        const TString& sessionId, const ui64 packIdx, const std::shared_ptr<TTaskForTablet>& selfPtr);

};

class TBlobSharing {
private:
    const TString StorageId;
    TUnifiedBlobId BlobId;
    std::optional<TTabletId> Borrowed;
    THashSet<TTabletId> Shared;
public:
    TBlobSharing(const TString& storageId, const TUnifiedBlobId& blobId)
        : StorageId(storageId)
        , BlobId(blobId)
    {

    }
    void AddShared(const TTabletId tabletId) {
        AFL_VERIFY(Shared.emplace(tabletId).second);
    }
    void AddBorrowed(const TTabletId tabletId) {
        AFL_VERIFY(!Borrowed);
        Borrowed = tabletId;
    }

    THashMap<TTabletId, TStorageTabletTask> BuildTabletTasksOnCopy(const TTransferContext& context, const TTabletId selfTabletId, const TString& storageId) const {
        auto toTabletId = context.GetDestinationTabletId();
        THashMap<TTabletId, TStorageTabletTask> result;
        const TTabletId ownerTabletId = Borrowed.value_or(selfTabletId);
        if (Borrowed) {
            AFL_VERIFY(Shared.empty());
        }
        if (ownerTabletId != toTabletId) {
            {
                TStorageTabletTask task(storageId, ownerTabletId);
                task.AddLink(BlobId, toTabletId);
                task.AddLink(BlobId, selfTabletId);
                AFL_VERIFY(result.emplace(ownerTabletId, std::move(task)).second);
            }
            {
                TStorageTabletTask task(storageId, toTabletId);
                task.AddInitOwner(BlobId, ownerTabletId);
                AFL_VERIFY(result.emplace(toTabletId, std::move(task)).second);
            }
        }
        return result;
    }

    THashMap<TTabletId, TStorageTabletTask> BuildTabletTasksOnMove(const TTransferContext& context, const TTabletId selfTabletId, const TString& storageId) const {
        THashMap<TTabletId, TStorageTabletTask> result;
        auto& movedTabletId = context.GetSourceTabletIds();
        auto toTabletId = context.GetDestinationTabletId();
        if (Borrowed) {
            AFL_VERIFY(Shared.empty());
            if (movedTabletId.contains(*Borrowed)) {
                {
                    TStorageTabletTask task(storageId, toTabletId);
                    task.AddLink(BlobId, selfTabletId);
                    task.AddLink(BlobId, toTabletId);
                    AFL_VERIFY(result.emplace(toTabletId, std::move(task)).second);
                }
                {
                    TStorageTabletTask task(storageId, selfTabletId);
                    task.AddRemapOwner(BlobId, *Borrowed, toTabletId);
                    AFL_VERIFY(result.emplace(selfTabletId, std::move(task)).second);
                }
                {
                    TStorageTabletTask task(storageId, *Borrowed);
                    task.RemoveLink(BlobId, selfTabletId);
                    AFL_VERIFY(result.emplace(*Borrowed, std::move(task)).second);
                }
            } else if (toTabletId == *Borrowed) {
            } else {
                {
                    TStorageTabletTask task(storageId, *Borrowed);
                    task.AddLink(BlobId, toTabletId);
                    AFL_VERIFY(result.emplace(*Borrowed, std::move(task)).second);
                }
                {
                    TStorageTabletTask task(storageId, toTabletId);
                    task.AddInitOwner(BlobId, *Borrowed);
                    AFL_VERIFY(result.emplace(toTabletId, std::move(task)).second);
                }
            }
        } else {
            for (auto&& i : Shared) {
                if (movedTabletId.contains(i) && i != selfTabletId) {
                    continue;
                }

                if (i != selfTabletId) {
                    TStorageTabletTask task(StorageId, i);
                    task.AddRemapOwner(BlobId, selfTabletId, toTabletId);
                    auto info = result.emplace(i, task);
                    if (!info.second) {
                        info.first->second.Merge(task);
                    }
                }

                {
                    TStorageTabletTask task(StorageId, selfTabletId);
                    task.RemoveLink(BlobId, i);
                    auto info = result.emplace(selfTabletId, task);
                    if (!info.second) {
                        info.first->second.Merge(task);
                    }
                }

                {
                    TStorageTabletTask task(StorageId, toTabletId);
                    task.AddLink(BlobId, i);
                    auto info = result.emplace(toTabletId, task);
                    if (!info.second) {
                        info.first->second.Merge(task);
                    }
                }
            }
            {
                TStorageTabletTask task(storageId, toTabletId);
                task.AddLink(BlobId, selfTabletId);
                task.AddLink(BlobId, toTabletId);
                auto info = result.emplace(toTabletId, task);
                if (!info.second) {
                    info.first->second.Merge(task);
                }
            }
            {
                TStorageTabletTask task(storageId, selfTabletId);
                task.AddInitOwner(BlobId, toTabletId);
                auto info = result.emplace(selfTabletId, task);
                if (!info.second) {
                    info.first->second.Merge(task);
                }
            }
        }
        return result;
    }
};

}