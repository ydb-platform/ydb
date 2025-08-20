#include "dq_spiller.h"


#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/buffer.h>
#include <util/generic/map.h>
#include <util/generic/set.h>

namespace NYql::NDq {

namespace {

constexpr ui32 MAX_INFLIGHT_BLOBS_COUNT = 10;
constexpr ui64 MAX_INFLIGHT_BLOBS_SIZE = 50_MB;

class TDqChannelSpillerAdapter {
    struct TWritingBlobInfo {
        ui64 BlobSize_;
        NThreading::TFuture<IDqSpiller::TKey> KeyFuture_;
    };
public:
    TDqChannelSpillerAdapter(IDqSpiller::TPtr spiller)
    : Spiller_(spiller) { }

    bool IsEmpty() {
        UpdateWriteStatus();

        return WritingBlobs_.empty() && StoredBlobsCount_ == 0 && !LoadingBlob_.has_value();
    }

    bool IsFull() {
        UpdateWriteStatus();

        return WritingBlobs_.size() > MAX_INFLIGHT_BLOBS_COUNT || WritingBlobsTotalSize_ > MAX_INFLIGHT_BLOBS_SIZE;
    }

    void Push(TChunkedBuffer&& blob) {
        UpdateWriteStatus();

        ui64 blobSize = blob.Size();
        auto keyFuture = Spiller_->Put(std::move(blob));
        ui64 blobId = BlobId_++;

        WritingBlobs_.emplace(blobId, TWritingBlobInfo{blobSize, std::move(keyFuture)});
        WritingBlobsTotalSize_ += blobSize;
    }

    bool Pop(TBuffer& blob) {
        UpdateWriteStatus();

        if (LoadingBlob_.has_value()) {
            if (!LoadingBlob_->HasValue()) {
                return false;
            }

            blob = std::move(*LoadingBlob_->ExtractValue());
            --StoredBlobsCount_;
            LoadingBlob_ = std::nullopt;
            return true;
        }

        Y_ENSURE(StoredBlobsCount_ > 0, "Internal Logic Error");
        Y_ENSURE(StoredBlobs_.size() > 0, "Internal Logic Error");

        LoadingBlob_ = Spiller_->Extract(StoredBlobs_.begin()->second);

        return false;
    }

private:
    void UpdateWriteStatus() {
        for (auto it = WritingBlobs_.begin(); it != WritingBlobs_.end();) {
            if (it->second.KeyFuture_.HasValue()) {
                StoredBlobs_[it->first] = it->second.KeyFuture_.GetValue();
                WritingBlobsTotalSize_ -= it->second.BlobSize_;
                ++StoredBlobsCount_;
                it = WritingBlobs_.erase(it);
            } else {
                ++it;
            }
        }
    }

private:
    IDqSpiller::TPtr Spiller_;

    std::optional<NThreading::TFuture<std::optional<TBuffer>>> LoadingBlob_ = std::nullopt;
    // BlobId -> future with some additional info
    std::unordered_map<ui64, TWritingBlobInfo> WritingBlobs_;
    ui64 WritingBlobsTotalSize_ = 0;

    ui64 BlobId_ = 0;
    ui64 StoredBlobsCount_ = 0;

    std::map<ui64, IDqSpiller::TKey> StoredBlobs_;

};

} // anonymous namespace

} // namespace NYql::NDq
