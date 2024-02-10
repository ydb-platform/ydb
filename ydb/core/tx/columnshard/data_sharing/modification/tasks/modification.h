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
};

class TStorageTabletTask {
private:
    TString StorageId;
    THashMap<TUnifiedBlobId, TBlobOwnerRemap> RemapOwner;
    TTabletByBlob InitOwner;
    TTabletsByBlob AddSharingLinks;
    TTabletsByBlob RemoveSharingLinks;
public:
    TStorageTabletTask(const TString& storageId)
        : StorageId(storageId) {

    }

    NKikimrColumnShardDataSharingProto::TStorageTabletTask SerializeToProto() const {
        NKikimrColumnShardDataSharingProto::TStorageTabletTask result;
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
        AFL_VERIFY(RemapOwner.emplace(blobId, TBlobOwnerRemap(from, to)).second);
    }

    void AddInitOwner(const TUnifiedBlobId& blobId, const TTabletId to) {
        AFL_VERIFY(InitOwner->emplace(blobId, to).second);
    }

    void AddLink(const TUnifiedBlobId& blobId, const TTabletId tabletId) {
        AFL_VERIFY(AddSharingLinks.Add(tabletId, blobId));
    }

    void RemoveLink(const TUnifiedBlobId& blobId, const TTabletId tabletId) {
        AFL_VERIFY(RemoveSharingLinks.Add(tabletId, blobId));
    }

    void Merge(const TStorageTabletTask& from) {
        for (auto&& i : from.InitOwner) {
            AFL_VERIFY(InitOwner->emplace(i.first, i.second).second);
        }
        for (auto&& i : from.RemapOwner) {
            AFL_VERIFY(RemapOwner.emplace(i.first, i.second).second);
        }
        AFL_VERIFY(AddSharingLinks.Add(from.AddSharingLinks));
        AFL_VERIFY(RemoveSharingLinks.Add(from.RemoveSharingLinks));
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
            it = TasksByStorage.emplace(storageId, storageId).first;
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
            TStorageTabletTask sTask("");
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

    THashMap<TTabletId, TStorageTabletTask> BuildTabletTasksOnCopy(const TTransferContext& context, const TTabletId& selfTabletId, const TString& storageId) const {
        auto toTabletId = context.GetDestinationTabletId();
        THashMap<TTabletId, TStorageTabletTask> result;
        if (Borrowed) {
            AFL_VERIFY(Shared.empty());
            {
                TStorageTabletTask task(storageId);
                task.AddLink(BlobId, toTabletId);
                AFL_VERIFY(result.emplace(*Borrowed, std::move(task)).second);
            }
            {
                TStorageTabletTask task(storageId);
                task.AddInitOwner(BlobId, *Borrowed);
                AFL_VERIFY(result.emplace(toTabletId, std::move(task)).second);
            }
        } else {
            {
                TStorageTabletTask task(storageId);
                task.AddLink(BlobId, toTabletId);
                AFL_VERIFY(result.emplace(selfTabletId, std::move(task)).second);
            }
            {
                TStorageTabletTask task(storageId);
                task.AddInitOwner(BlobId, selfTabletId);
                AFL_VERIFY(result.emplace(toTabletId, std::move(task)).second);
            }
        }
        return result;
    }

    THashMap<TTabletId, TStorageTabletTask> BuildTabletTasksOnMove(const TTransferContext& context, const TTabletId& selfTabletId, const TString& storageId) const {
        THashMap<TTabletId, TStorageTabletTask> result;
        auto& movedTabletId = context.GetSourceTabletIds();
        auto toTabletId = context.GetDestinationTabletId();
        if (Borrowed) {
            AFL_VERIFY(Shared.empty());
            if (movedTabletId.contains(*Borrowed)) {
                {
                    TStorageTabletTask task(storageId);
                    task.AddLink(BlobId, selfTabletId);
                    AFL_VERIFY(result.emplace(toTabletId, std::move(task)).second);
                }
                {
                    TStorageTabletTask task(storageId);
                    task.AddRemapOwner(BlobId, *Borrowed, toTabletId);
                    AFL_VERIFY(result.emplace(selfTabletId, std::move(task)).second);
                }
                {
                    TStorageTabletTask task(storageId);
                    task.RemoveLink(BlobId, selfTabletId);
                    AFL_VERIFY(result.emplace(*Borrowed, std::move(task)).second);
                }
            } else {
                {
                    TStorageTabletTask task(storageId);
                    task.AddLink(BlobId, toTabletId);
                    AFL_VERIFY(result.emplace(*Borrowed, std::move(task)).second);
                }
                {
                    TStorageTabletTask task(storageId);
                    task.AddInitOwner(BlobId, *Borrowed);
                    AFL_VERIFY(result.emplace(toTabletId, std::move(task)).second);
                }
            }
        } else {
            for (auto&& i : Shared) {
                if (movedTabletId.contains(i)) {
                    continue;
                }
                {
                    TStorageTabletTask task(StorageId);
                    task.AddRemapOwner(BlobId, selfTabletId, toTabletId);
                    AFL_VERIFY(result.emplace(i, std::move(task)).second);
                }
                {
                    TStorageTabletTask task(StorageId);
                    task.RemoveLink(BlobId, i);
                    AFL_VERIFY(result.emplace(selfTabletId, std::move(task)).second);
                }
            }
            {
                TStorageTabletTask task(storageId);
                task.AddLink(BlobId, selfTabletId);
                AFL_VERIFY(result.emplace(toTabletId, std::move(task)).second);
            }
            {
                TStorageTabletTask task(storageId);
                task.AddInitOwner(BlobId, toTabletId);
                AFL_VERIFY(result.emplace(selfTabletId, std::move(task)).second);
            }
        }
        return result;
    }
};

}