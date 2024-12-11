#include "channel_storage.h"

#include "channel_storage_actor.h"

#include <ydb/library/yql/utils/yql_panic.h>
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
        NThreading::TFuture<void> IsBlobWrittenFuture_;
    };
public:
    TDqChannelStorage(TTxId txId, ui64 channelId, TWakeUpCallback&& wakeUpCallback, TErrorCallback&& errorCallback, 
        TIntrusivePtr<TSpillingTaskCounters> spillingTaskCounters, TActorSystem* actorSystem)
    : ActorSystem_(actorSystem)
    {
        ChannelStorageActor_ = CreateDqChannelStorageActor(txId, channelId, std::move(wakeUpCallback), std::move(errorCallback), spillingTaskCounters, actorSystem);
        ChannelStorageActorId_ = ActorSystem_->Register(ChannelStorageActor_->GetActor());
    }

    ~TDqChannelStorage() {
        ActorSystem_->Send(ChannelStorageActorId_, new TEvents::TEvPoison);
    }

    bool IsEmpty() override {
        UpdateWriteStatus();

        return WritingBlobs_.empty() && StoredBlobsCount_ == 0 && LoadingBlobs_.empty();
    }

    bool IsFull() override {
        UpdateWriteStatus();

        return WritingBlobs_.size() > MAX_INFLIGHT_BLOBS_COUNT || WritingBlobsTotalSize_ > MAX_INFLIGHT_BLOBS_SIZE;
    }

    void Put(ui64 blobId, TRope&& blob, ui64 cookie = 0) override {
        UpdateWriteStatus();

        auto promise = NThreading::NewPromise<void>();
        auto future = promise.GetFuture();

        ui64 blobSize = blob.size();

        ActorSystem_->Send(ChannelStorageActorId_, new TEvDqChannelSpilling::TEvPut(blobId, std::move(blob), std::move(promise)), /*flags*/0, cookie);

        WritingBlobs_.emplace(blobId, TWritingBlobInfo{blobSize, std::move(future)});
        WritingBlobsTotalSize_ += blobSize;
    }

    bool Get(ui64 blobId, TBuffer& blob, ui64 cookie = 0) override {
        UpdateWriteStatus();

        const auto it = LoadingBlobs_.find(blobId);
        // If we didn't request loading blob from spilling -> request it
        if (it == LoadingBlobs_.end()) {
            auto promise = NThreading::NewPromise<TBuffer>();
            auto future = promise.GetFuture();
            ActorSystem_->Send(ChannelStorageActorId_, new TEvDqChannelSpilling::TEvGet(blobId, std::move(promise)), /*flags*/0, cookie);

            LoadingBlobs_.emplace(blobId, std::move(future));
            return false;
        }
        // If we requested loading blob, but it's not loaded -> wait
        if (!it->second.HasValue()) return false;

        blob = std::move(it->second.ExtractValue());
        LoadingBlobs_.erase(it);
        --StoredBlobsCount_;

        return true;
    }

private:
    void UpdateWriteStatus() {
        for (auto it = WritingBlobs_.begin(); it != WritingBlobs_.end();) {
            if (it->second.IsBlobWrittenFuture_.HasValue()) {
                WritingBlobsTotalSize_ -= it->second.BlobSize_;
                ++StoredBlobsCount_;
                it = WritingBlobs_.erase(it);
            } else {
                ++it;
            }
        }
    }

private:
    IDqChannelStorageActor* ChannelStorageActor_;
    TActorId ChannelStorageActorId_;
    TActorSystem *ActorSystem_;

    // BlobId -> future with requested blob
    std::unordered_map<ui64, NThreading::TFuture<TBuffer>> LoadingBlobs_;
    // BlobId -> future with some additional info
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
    return new TDqChannelStorage(txId, channelId, std::move(wakeUpCallback), std::move(errorCallback), spillingTaskCounters, actorSystem);
}

} // namespace NYql::NDq
