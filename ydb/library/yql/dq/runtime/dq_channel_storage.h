#pragma once

#include <yql/essentials/utils/chunked_buffer.h>
#include <yql/essentials/minikql/computation/mkql_spiller.h>

#include <util/generic/buffer.h>
#include <util/generic/yexception.h>
#include <util/generic/hash.h>

#include <library/cpp/threading/future/core/future.h>


namespace NYql::NDq {

class TDqChannelStorageException : public yexception {
};

class IDqChannelStorage : public TSimpleRefCount<IDqChannelStorage> {
public:
    using TPtr = TIntrusivePtr<IDqChannelStorage>;

public:
    virtual ~IDqChannelStorage() = default;

    virtual bool IsEmpty() = 0;
    virtual bool IsFull() = 0;

    // methods Put/Get can throw `TDqChannelStorageException`

    // Data should be owned by `blob` argument since the Put() call is actually asynchronous
    virtual void Put(ui64 blobId, TChunkedBuffer&& blob, ui64 cookie = 0) = 0;

    // TODO: there is no way for client to delete blob.
    // It is better to replace Get() with Pull() which will delete blob after read
    // (current clients read each blob exactly once)
    // Get() will return false if data is not ready yet. Client should repeat Get() in this case
    virtual bool Get(ui64 blobId, TBuffer& data, ui64 cookie = 0)  = 0;
};

// Adapter to make ISpiller work as IDqChannelStorage
class TSpillerToChannelStorageAdapter : public IDqChannelStorage {
public:
    TSpillerToChannelStorageAdapter(NKikimr::NMiniKQL::ISpiller::TPtr spiller);
    
    bool IsEmpty() override;
    bool IsFull() override;
    void Put(ui64 blobId, TChunkedBuffer&& blob, ui64 cookie = 0) override;
    bool Get(ui64 blobId, TBuffer& data, ui64 cookie = 0) override;

private:
    void UpdatePendingOperations();
    
    NKikimr::NMiniKQL::ISpiller::TPtr Spiller_;
    
    // BlobId -> SpillerKey mapping
    std::unordered_map<ui64, NKikimr::NMiniKQL::ISpiller::TKey> BlobKeys_;
    
    // Pending operations
    std::unordered_map<ui64, NThreading::TFuture<NKikimr::NMiniKQL::ISpiller::TKey>> WritingBlobs_;
    std::unordered_map<ui64, NThreading::TFuture<std::optional<NYql::TChunkedBuffer>>> LoadingBlobs_;
    
    // State tracking
    ui64 StoredBlobsCount_ = 0;
    ui64 WritingBlobsCount_ = 0;
    ui64 LoadingBlobsCount_ = 0;
};

} // namespace NYql::NDq
