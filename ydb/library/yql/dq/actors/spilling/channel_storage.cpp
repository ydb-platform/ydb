#include "channel_storage.h"

#include "channel_storage_actor.h"

#include <yql/essentials/utils/yql_panic.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/buffer.h>
#include <util/generic/map.h>
#include <util/generic/set.h>


namespace NYql::NDq {

using namespace NActors;

namespace {


constexpr ui32 MAX_INFLIGHT_BLOBS_COUNT = 10;
constexpr ui64 MAX_INFLIGHT_BLOBS_SIZE = 50_MB;

class TDqChannelStorage : public IDqChannelStorage {
    struct TWritingBlobInfo {
        ui64 BlobSize_;
        NThreading::TFuture<NKikimr::NMiniKQL::ISpiller::TKey> WriteFuture_;
        NKikimr::NMiniKQL::ISpiller::TKey SpillerKey_;
        bool KeyReady_ = false;
    };
    
    struct TLoadingBlobInfo {
        NThreading::TFuture<std::optional<NYql::TChunkedBuffer>> LoadFuture_;
    };
    
public:
    // Only constructor - takes spiller from outside
    TDqChannelStorage(ui64 channelId, NKikimr::NMiniKQL::ISpiller::TPtr spiller,
        TIntrusivePtr<TSpillingTaskCounters> spillingTaskCounters)
        : ChannelId_(channelId)
        , Spiller_(spiller)
        , SpillingTaskCounters_(spillingTaskCounters)
        , StoredBlobsCount_(0)
        , WritingBlobsTotalSize_(0)
    {}

    ~TDqChannelStorage() = default;

    bool IsEmpty() override {
        UpdateWriteStatus();
        return WritingBlobs_.empty() && StoredBlobsCount_ == 0 && LoadingBlobs_.empty();
    }

    bool IsFull() override {
        UpdateWriteStatus();
        return WritingBlobs_.size() > MAX_INFLIGHT_BLOBS_COUNT || WritingBlobsTotalSize_ > MAX_INFLIGHT_BLOBS_SIZE;
    }

    void Put(ui64 blobId, TChunkedBuffer&& blob, ui64 cookie = 0) override {
        Y_UNUSED(cookie);
        if (WritingBlobs_.contains(blobId)) {
            throw TDqChannelStorageException() << "Blob " << blobId << " is already being written";
        }

        ui64 blobSize = blob.Size();
        auto future = Spiller_->Put(std::move(blob));
        
        WritingBlobs_[blobId] = TWritingBlobInfo{blobSize, future, 0, false};
        WritingBlobsTotalSize_ += blobSize;

        if (SpillingTaskCounters_) {
            SpillingTaskCounters_->ChannelWriteBytes += blobSize;
        }
    }

    bool Get(ui64 blobId, TBuffer& blob, ui64 cookie = 0) override {
        Y_UNUSED(cookie);
        UpdateWriteStatus();
        
        // Check if blob is still being written
        if (auto it = WritingBlobs_.find(blobId); it != WritingBlobs_.end()) {
            if (!it->second.WriteFuture_.HasValue()) {
                return false; // Still writing
            }
            // Get the spiller key and mark as ready
            if (!it->second.KeyReady_) {
                it->second.SpillerKey_ = it->second.WriteFuture_.GetValue();
                it->second.KeyReady_ = true;
            }
            // Move to stored blobs
            BlobKeys_[blobId] = it->second.SpillerKey_;
            WritingBlobsTotalSize_ -= it->second.BlobSize_;
            WritingBlobs_.erase(it);
            StoredBlobsCount_++;
        }

        // Check if we're already loading this blob
        if (auto it = LoadingBlobs_.find(blobId); it != LoadingBlobs_.end()) {
            if (!it->second.LoadFuture_.HasValue()) {
                return false; // Still loading
            }
            auto result = it->second.LoadFuture_.GetValue();
            LoadingBlobs_.erase(it);
            if (result) {
                // Convert TChunkedBuffer to TBuffer
                blob.Clear();
                blob.Reserve(result->Size());
                for (const auto& chunk : result->Chunks()) {
                    blob.Append(chunk.data(), chunk.size());
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
        LoadingBlobs_[blobId] = TLoadingBlobInfo{future};
        
        if (future.HasValue()) {
            auto result = future.GetValue();
            LoadingBlobs_.erase(blobId);
            if (result) {
                // Convert TChunkedBuffer to TBuffer
                blob.Clear();
                blob.Reserve(result->Size());
                for (const auto& chunk : result->Chunks()) {
                    blob.Append(chunk.data(), chunk.size());
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

private:
    void UpdateWriteStatus() {
        for (auto it = WritingBlobs_.begin(); it != WritingBlobs_.end();) {
            if (it->second.WriteFuture_.HasValue()) {
                if (!it->second.KeyReady_) {
                    it->second.SpillerKey_ = it->second.WriteFuture_.GetValue();
                    it->second.KeyReady_ = true;
                }
                BlobKeys_[it->first] = it->second.SpillerKey_;
                WritingBlobsTotalSize_ -= it->second.BlobSize_;
                StoredBlobsCount_++;
                it = WritingBlobs_.erase(it);
            } else {
                ++it;
            }
        }
    }

private:
    const ui64 ChannelId_;
    NKikimr::NMiniKQL::ISpiller::TPtr Spiller_;
    TIntrusivePtr<TSpillingTaskCounters> SpillingTaskCounters_;
    
    // BlobId -> SpillerKey mapping for stored blobs
    std::unordered_map<ui64, NKikimr::NMiniKQL::ISpiller::TKey> BlobKeys_;
    // BlobId -> loading info
    std::unordered_map<ui64, TLoadingBlobInfo> LoadingBlobs_;
    // BlobId -> writing info
    std::unordered_map<ui64, TWritingBlobInfo> WritingBlobs_;
    
    ui64 WritingBlobsTotalSize_ = 0;
    ui64 StoredBlobsCount_ = 0;
};



} // anonymous namespace


IDqChannelStorage::TPtr CreateDqChannelStorage(TTxId txId, ui64 channelId,
    TWakeUpCallback wakeUpCallback,
    TErrorCallback errorCallback,
    TIntrusivePtr<TSpillingTaskCounters> spillingTaskCounters,
    TActorSystem* actorSystem)
{
    // Legacy function - creates own spilling actor. This should be deprecated.
    // For now, we need to create a spiller here, but this breaks the new architecture
    Y_UNUSED(txId);
    Y_UNUSED(wakeUpCallback);
    Y_UNUSED(errorCallback);
    Y_UNUSED(actorSystem);
    Y_ABORT("CreateDqChannelStorage with actor creation is deprecated. Use CreateDqChannelStorageWithSharedSpiller instead.");
}

IDqChannelStorage::TPtr CreateDqChannelStorageWithSharedSpiller(ui64 channelId,
    NKikimr::NMiniKQL::ISpiller::TPtr sharedSpiller,
    TIntrusivePtr<TSpillingTaskCounters> spillingTaskCounters)
{
    return new TDqChannelStorage(channelId, sharedSpiller, spillingTaskCounters);
}

} // namespace NYql::NDq
