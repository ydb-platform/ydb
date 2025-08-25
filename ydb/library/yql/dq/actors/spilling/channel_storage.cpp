#include "channel_storage.h"

#include <yql/essentials/utils/yql_panic.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/buffer.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/stream/mem.h>


namespace NYql::NDq {

namespace {


constexpr ui32 MAX_INFLIGHT_BLOBS_COUNT = 10;
constexpr ui64 MAX_INFLIGHT_BLOBS_SIZE = 50_MB;

class TDqChannelStorage : public IDqChannelStorage {
    struct TWritingBlobInfo {
        ui64 BlobSize_;
        NThreading::TFuture<IDqSpiller::TKey> BlobKey_;
    };
public:
    TDqChannelStorage(TTxId txId, ui64 channelId, IDqSpiller::TPtr spiller)
    : Spiller_(spiller)
    {
        Y_UNUSED(txId);
        Y_UNUSED(channelId);
    }

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
        UpdateWriteStatus();

        ui64 blobSize = blob.Size();

        auto future = Spiller_->Put(std::move(blob));

        WritingBlobs_.emplace(blobId, TWritingBlobInfo{blobSize, std::move(future)});
        WritingBlobsTotalSize_ += blobSize;
    }

    bool Get(ui64 blobId, TBuffer& blob, ui64 cookie = 0) override {
        Y_UNUSED(cookie);
        UpdateWriteStatus();

        auto keyIt = StoredBlobsKeysMapping_.find(blobId);
        Y_ENSURE(keyIt != StoredBlobsKeysMapping_.end());

        const auto it = LoadingBlobs_.find(blobId);
        if (it == LoadingBlobs_.end()) {
            auto future = Spiller_->Extract(keyIt->second);

            LoadingBlobs_.emplace(blobId, std::move(future));
            return false;
        }
        if (!it->second.HasValue()) return false;

        auto tmp = it->second.ExtractValue();
        blob.Resize(tmp->Size());
        TMemoryOutput output(blob.Data(), blob.Size());
        // TODO: get rid of redundant Copy
        // Add interface that returns TBuffer
        tmp->CopyTo(output);
        LoadingBlobs_.erase(it);
        StoredBlobsKeysMapping_.erase(keyIt);
        --StoredBlobsCount_;

        return true;
    }

private:
    void UpdateWriteStatus() {
        for (auto it = WritingBlobs_.begin(); it != WritingBlobs_.end();) {
            if (it->second.BlobKey_.HasValue()) {
                WritingBlobsTotalSize_ -= it->second.BlobSize_;
                ++StoredBlobsCount_;
                it = WritingBlobs_.erase(it);
                StoredBlobsKeysMapping_[it->first] = it->second.BlobKey_.ExtractValue();
            } else {
                ++it;
            }
        }
    }

private:
    IDqSpiller::TPtr Spiller_;

    std::unordered_map<ui64, NThreading::TFuture<std::optional<TChunkedBuffer>>> LoadingBlobs_;
    std::unordered_map<ui64, TWritingBlobInfo> WritingBlobs_;
    std::unordered_map<ui64, ui64> StoredBlobsKeysMapping_;
    ui64 WritingBlobsTotalSize_ = 0;

    ui64 StoredBlobsCount_ = 0;
};

} // anonymous namespace


IDqChannelStorage::TPtr CreateDqChannelStorage(TTxId txId, ui64 channelId, IDqSpiller::TPtr spiller)
{
    return new TDqChannelStorage(txId, channelId, spiller);
}

} // namespace NYql::NDq
