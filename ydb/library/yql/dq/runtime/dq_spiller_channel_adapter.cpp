#include <ydb/library/yql/dq/runtime/dq_channel_storage.h>

namespace NYql::NDq {

TSpillerToChannelStorageAdapter::TSpillerToChannelStorageAdapter(NKikimr::NMiniKQL::ISpiller::TPtr spiller)
    : Spiller_(spiller)
{}

bool TSpillerToChannelStorageAdapter::IsEmpty() {
    // Update pending operations first
    UpdatePendingOperations();
    
    return StoredBlobsCount_ == 0 && WritingBlobsCount_ == 0 && LoadingBlobsCount_ == 0;
}

bool TSpillerToChannelStorageAdapter::IsFull() {
    // For simplicity, we don't implement full check for spiller adapter
    // This would require knowledge of spiller's internal limits
    return false;
}

void TSpillerToChannelStorageAdapter::Put(ui64 blobId, TChunkedBuffer&& blob, ui64 cookie) {
    Y_UNUSED(cookie);
    
    if (WritingBlobs_.contains(blobId) || BlobKeys_.contains(blobId)) {
        throw TDqChannelStorageException() << "Blob " << blobId << " is already being written or exists";
    }
    
    auto future = Spiller_->Put(std::move(blob));
    WritingBlobs_[blobId] = future;
    WritingBlobsCount_++;
}

bool TSpillerToChannelStorageAdapter::Get(ui64 blobId, TBuffer& data, ui64 cookie) {
    Y_UNUSED(cookie);
    
    UpdatePendingOperations();
    
    // Check if blob is still being written
    if (auto it = WritingBlobs_.find(blobId); it != WritingBlobs_.end()) {
        if (!it->second.HasValue()) {
            return false; // Still writing
        }
        // Move from writing to stored
        auto key = it->second.GetValue();
        BlobKeys_[blobId] = key;
        WritingBlobs_.erase(it);
        WritingBlobsCount_--;
        StoredBlobsCount_++;
    }
    
    // Check if we're already loading this blob
    if (auto it = LoadingBlobs_.find(blobId); it != LoadingBlobs_.end()) {
        if (!it->second.HasValue()) {
            return false; // Still loading
        }
        auto result = it->second.GetValue();
        LoadingBlobs_.erase(it);
        LoadingBlobsCount_--;
        
        if (result) {
            // Convert TChunkedBuffer to TBuffer
            data.Clear();
            data.Reserve(result->Size());
            for (const auto& chunk : result->Chunks()) {
                data.Append(chunk.data(), chunk.size());
            }
            // Remove from stored blobs
            BlobKeys_.erase(blobId);
            StoredBlobsCount_--;
            return true;
        } else {
            throw TDqChannelStorageException() << "Blob " << blobId << " not found in spiller";
        }
    }
    
    // Check if we have this blob key
    auto keyIt = BlobKeys_.find(blobId);
    if (keyIt == BlobKeys_.end()) {
        throw TDqChannelStorageException() << "Blob " << blobId << " not found";
    }
    
    // Start loading the blob
    auto future = Spiller_->Get(keyIt->second);
    LoadingBlobs_[blobId] = future;
    LoadingBlobsCount_++;
    
    if (future.HasValue()) {
        auto result = future.GetValue();
        LoadingBlobs_.erase(blobId);
        LoadingBlobsCount_--;
        
        if (result) {
            // Convert TChunkedBuffer to TBuffer
            data.Clear();
            data.Reserve(result->Size());
            for (const auto& chunk : result->Chunks()) {
                data.Append(chunk.data(), chunk.size());
            }
            BlobKeys_.erase(blobId);
            StoredBlobsCount_--;
            return true;
        } else {
            throw TDqChannelStorageException() << "Blob " << blobId << " not found in spiller";
        }
    }
    
    return false; // Loading started, will be ready later
}

void TSpillerToChannelStorageAdapter::UpdatePendingOperations() {
    // Update writing operations
    for (auto it = WritingBlobs_.begin(); it != WritingBlobs_.end();) {
        if (it->second.HasValue()) {
            auto key = it->second.GetValue();
            BlobKeys_[it->first] = key;
            StoredBlobsCount_++;
            WritingBlobsCount_--;
            it = WritingBlobs_.erase(it);
        } else {
            ++it;
        }
    }
    
    // Update loading operations
    for (auto it = LoadingBlobs_.begin(); it != LoadingBlobs_.end();) {
        if (it->second.HasValue()) {
            // Loading completed, but we'll handle the result in Get() method
            ++it;
        } else {
            ++it;
        }
    }
}

} // namespace NYql::NDq
