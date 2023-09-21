#pragma once

#include <ydb/core/tx/columnshard/blobs_action/abstract/storage.h>
#include <ydb/core/tx/columnshard/blob_manager.h>
#include <ydb/core/tx/columnshard/blob_cache.h>

namespace NKikimr::NOlap {

class TMemoryStorage {
private:
    THashMap<TUnifiedBlobId, TString> Data;
    THashMap<TUnifiedBlobId, TString> DataWriting;
    THashSet<TUnifiedBlobId> DataForRemove;
    TMutex Mutex;
public:
    std::optional<TString> Read(const TUnifiedBlobId& id) {
        TGuard<TMutex> g(Mutex);
        auto it = Data.find(id);
        if (it == Data.end()) {
            return {};
        } else {
            return it->second;
        }
    }

    void DeclareDataForRemove(const TUnifiedBlobId& id) {
        TGuard<TMutex> g(Mutex);
        DataForRemove.emplace(id);
    }

    void StartWriting(const TUnifiedBlobId& id, const TString& data) {
        TGuard<TMutex> g(Mutex);
        Y_VERIFY(DataWriting.emplace(id, data).second);
    }

    void CommitWriting(const TUnifiedBlobId& id) {
        TGuard<TMutex> g(Mutex);
        auto it = DataWriting.find(id);
        Y_VERIFY(it != DataWriting.end());
        Y_VERIFY(Data.emplace(id, it->second).second);
        DataWriting.erase(it);
    }

    TMemoryStorage() = default;
};

class TMemoryWriteAction: public IBlobsWritingAction {
private:
    using TBase = IBlobsWritingAction;
    const std::shared_ptr<TMemoryStorage> Storage;
protected:
    virtual void DoSendWriteBlobRequest(const TString& data, const TUnifiedBlobId& blobId) override {
        Storage->StartWriting(blobId, data);
        TActorContext::AsActorContext().Send(TActorContext::AsActorContext().SelfID, std::make_unique<TEvBlobStorage::TEvPutResult>(
            NKikimrProto::EReplyStatus::OK, blobId.GetLogoBlobId(), TStorageStatusFlags(), 0, 0));
    }

    virtual void DoOnBlobWriteResult(const TUnifiedBlobId& blobId, const NKikimrProto::EReplyStatus status) override {
        Y_VERIFY(status == NKikimrProto::EReplyStatus::OK);
        Storage->CommitWriting(blobId);
    }

    virtual void DoOnExecuteTxBeforeWrite(NColumnShard::TColumnShard& /*self*/, NColumnShard::TBlobManagerDb& /*dbBlobs*/) override {
        return;
    }

    virtual void DoOnCompleteTxBeforeWrite(NColumnShard::TColumnShard& /*self*/) override {
        return;
    }

    virtual void DoOnExecuteTxAfterWrite(NColumnShard::TColumnShard& /*self*/, NColumnShard::TBlobManagerDb& /*dbBlobs*/, const bool /*success*/) override {

    }
    virtual void DoOnCompleteTxAfterWrite(NColumnShard::TColumnShard& /*self*/) override {

    }
public:
    virtual bool NeedDraftTransaction() const override {
        return true;
    }

    virtual TUnifiedBlobId AllocateNextBlobId(const TString& /*data*/) override {
        return TUnifiedBlobId();
//        return BlobBatch.AllocateNextBlobId(data);
    }

    TMemoryWriteAction(const TString& storageId, const std::shared_ptr<TMemoryStorage>& storage)
        : TBase(storageId)
        , Storage(storage)
    {

    }
};

class TMemoryDeclareRemovingAction: public IBlobsDeclareRemovingAction {
private:
    using TBase = IBlobsDeclareRemovingAction;
    const std::shared_ptr<TMemoryStorage> Storage;
protected:
    virtual void DoDeclareRemove(const TUnifiedBlobId& /*blobId*/) {

    }

    virtual void DoOnExecuteTxAfterRemoving(NColumnShard::TColumnShard& /*self*/, NColumnShard::TBlobManagerDb& /*dbBlobs*/, const bool /*success*/) {
        for (auto&& i : GetDeclaredBlobs()) {
            Storage->DeclareDataForRemove(i);
        }
    }
    virtual void DoOnCompleteTxAfterRemoving(NColumnShard::TColumnShard& /*self*/) {

    }
public:

    TMemoryDeclareRemovingAction(const TString& storageId, const std::shared_ptr<TMemoryStorage>& storage)
        : TBase(storageId)
        , Storage(storage) {

    }
};

class TMemoryReadingAction: public IBlobsReadingAction {
private:
    using TBase = IBlobsReadingAction;
    const std::shared_ptr<TMemoryStorage> Storage;
protected:
    virtual void DoStartReading(const THashMap<TUnifiedBlobId, THashSet<TBlobRange>>& ranges) override {
        for (auto&& i : ranges) {
            auto data = Storage->Read(i.first);
            for (auto&& r : i.second) {
                if (!data) {
                    TActorContext::AsActorContext().Send(TActorContext::AsActorContext().SelfID,
                        new NBlobCache::TEvBlobCache::TEvReadBlobRangeResult(r, NKikimrProto::EReplyStatus::NODATA, ""));
                } else {
                    Y_VERIFY(r.Offset + r.Size <= data->size());
                    TActorContext::AsActorContext().Send(TActorContext::AsActorContext().SelfID,
                        new NBlobCache::TEvBlobCache::TEvReadBlobRangeResult(r, NKikimrProto::EReplyStatus::OK, data->substr(r.Offset, r.Size)));
                }
            }
        }
    }
public:

    TMemoryReadingAction(const TString& storageId, const std::shared_ptr<TMemoryStorage>& storage)
        : TBase(storageId)
        , Storage(storage)
    {

    }
};

class TMemoryOperator: public IBlobsStorageOperator {
private:
    using TBase = IBlobsStorageOperator;
    std::shared_ptr<TMemoryStorage> Storage;
protected:
    virtual std::shared_ptr<IBlobsDeclareRemovingAction> DoStartDeclareRemovingAction() override {
        return std::make_shared<TMemoryDeclareRemovingAction>(GetStorageId(), Storage);
    }
    virtual std::shared_ptr<IBlobsWritingAction> DoStartWritingAction() override {
        return std::make_shared<TMemoryWriteAction>(GetStorageId(), Storage);
    }
    virtual std::shared_ptr<IBlobsReadingAction> DoStartReadingAction() override {
        return std::make_shared<TMemoryReadingAction>(GetStorageId(), Storage);
    }
public:
    TMemoryOperator(const TString& storageId)
        : TBase(storageId)
    {
        Storage = std::make_shared<TMemoryStorage>();
    }
};

}
