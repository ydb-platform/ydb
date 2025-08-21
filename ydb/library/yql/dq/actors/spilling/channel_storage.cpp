#include "channel_storage.h"

#include <yql/essentials/utils/yql_panic.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/buffer.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/queue.h>


namespace NYql::NDq {

constexpr ui32 MAX_INFLIGHT_BLOBS_COUNT = 10;
constexpr ui64 MAX_INFLIGHT_BLOBS_SIZE = 50_MB;

class TDqChannelStorage : public IDqChannelStorage {
    struct TWritingBlobInfo {
        ui64 BlobSize_;
        NThreading::TFuture<IDqSpiller::TKey> KeyFuture_;
    };
public:
    TDqChannelStorage(IDqSpiller::TPtr spiller)
    : Spiller_(spiller) { }

    bool IsEmpty() override {
        UpdateWriteStatus();

        return WritingBlobs_.empty() && StoredBlobsCount_ == 0 && !LoadingBlob_.has_value();
    }

    bool IsFull() override {
        UpdateWriteStatus();

        return WritingBlobs_.size() > MAX_INFLIGHT_BLOBS_COUNT || WritingBlobsTotalSize_ > MAX_INFLIGHT_BLOBS_SIZE;
    }

    void Push(TChunkedBuffer&& blob) override {
        UpdateWriteStatus();

        ui64 blobSize = blob.Size();
        auto keyFuture = Spiller_->Put(std::move(blob));

        ui64 blobId = BlobId_++;
        WritingBlobs_.emplace(blobId, TWritingBlobInfo{blobSize, std::move(keyFuture)});
        WritingBlobsTotalSize_ += blobSize;
        
        BlobIdQueue_.push(blobId);
    }

    bool Pop(TBuffer& blob) override {
        UpdateWriteStatus();

        if (LoadingBlob_.has_value()) {
            if (!LoadingBlob_->HasValue()) {
                return false;
            }

            auto optionalChunkedBuffer = LoadingBlob_->ExtractValue();
            if (optionalChunkedBuffer.has_value()) {
                // Конвертируем TChunkedBuffer в TBuffer
                const auto& chunkedBuffer = *optionalChunkedBuffer;
                blob.Resize(chunkedBuffer.Size());
                chunkedBuffer.FillBuffer(blob.Data());
                --StoredBlobsCount_;
                LoadingBlob_ = std::nullopt;
                return true;
            } else {
                LoadingBlob_ = std::nullopt;
                return false;
            }
        }

        // Проверяем есть ли готовые блобы в FIFO порядке
        if (StoredBlobsCount_ == 0 || StoredBlobs_.empty()) {
            return false;
        }

        // Берем первый блоб из очереди (FIFO)
        if (BlobIdQueue_.empty()) {
            return false;
        }

        ui64 nextBlobId = BlobIdQueue_.front();
        auto it = StoredBlobs_.find(nextBlobId);
        if (it == StoredBlobs_.end()) {
            return false;
        }

        BlobIdQueue_.pop();
        LoadingBlob_ = Spiller_->Extract(it->second);
        StoredBlobs_.erase(it);

        return false; // Данные еще не готовы, нужно будет вызвать Get снова
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

    std::optional<NThreading::TFuture<std::optional<TChunkedBuffer>>> LoadingBlob_ = std::nullopt;
    // BlobId -> future with some additional info
    std::unordered_map<ui64, TWritingBlobInfo> WritingBlobs_;
    ui64 WritingBlobsTotalSize_ = 0;

    ui64 StoredBlobsCount_ = 0;

    std::unordered_map<ui64, IDqSpiller::TKey> StoredBlobs_;
    TQueue<ui64> BlobIdQueue_; // Очередь для FIFO порядка
    ui64 BlobId_ = 0;

};

// Фабричная функция для создания адаптера
IDqChannelStorage::TPtr CreateDqChannelSpillerAdapter(IDqSpiller::TPtr spiller);


} // namespace NYql::NDq
