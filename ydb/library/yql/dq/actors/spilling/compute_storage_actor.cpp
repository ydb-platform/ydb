#include "compute_storage_actor.h"

#include "spilling.h"
#include "spilling_file.h"

#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>


namespace NYql::NDq {

using namespace NActors;

namespace {

#define LOG_D(s) \
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId_ << ". " << s)
#define LOG_I(s) \
    LOG_INFO_S(*TlsActivationContext,  NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ". " << s)
#define LOG_E(s) \
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId_ << ". " << s)
#define LOG_C(s) \
    LOG_CRIT_S(*TlsActivationContext,  NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ". " << s)
#define LOG_W(s) \
    LOG_WARN_S(*TlsActivationContext,  NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ". " << s)
#define LOG_T(s) \
    LOG_TRACE_S(*TlsActivationContext,  NKikimrServices::KQP_COMPUTE, "TxId: " << TxId_ << ". " << s)

class TDqComputeStorageActor : public NActors::TActorBootstrapped<TDqComputeStorageActor>,
                               public IDqComputeStorageActor
{
    using TBase = TActorBootstrapped<TDqComputeStorageActor>;
    struct TWritingBlobInfo {
        ui64 Size;
        NThreading::TPromise<IDqComputeStorageActor::TKey> BlobIdPromise;
        TInstant OpBegin;
    };

    struct TLoadingBlobInfo {
        bool RemoveAfterRead;
        NThreading::TPromise<std::optional<TRope>> BlobPromise;
        TInstant OpBegin;
    };
    // void promise that completes when block is removed
    using TDeletingBlobInfo = NThreading::TPromise<void>;
public:
    TDqComputeStorageActor(TTxId txId, const TString& spillerName, TWakeUpCallback wakeupCallback, TErrorCallback errorCallback,
        TIntrusivePtr<TSpillingTaskCounters> spillingTaskCounters)
        : TxId_(txId),
        SpillerName_(spillerName),
        WakeupCallback_(wakeupCallback),
        ErrorCallback_(errorCallback),
        SpillingTaskCounters_(spillingTaskCounters)
    {
    }

    void Bootstrap() {
        auto spillingActor = CreateDqLocalFileSpillingActor(TxId_, SpillerName_,
            SelfId(), false);
        SpillingActorId_ = Register(spillingActor);
        Become(&TDqComputeStorageActor::WorkState);
    }

    static constexpr char ActorName[] = "DQ_COMPUTE_STORAGE";

    IActor* GetActor() override {
        return this;
    }

protected:

    void FailWithError(const TString& error) {
        if (!ErrorCallback_) Y_ABORT("Error: %s", error.c_str());

        LOG_E("Error: " << error);
        ErrorCallback_(error);
        SendInternal(SpillingActorId_, new TEvents::TEvPoison);
        PassAway();
    }

    void SendInternal(const TActorId& recipient, IEventBase* ev, TEventFlags flags = IEventHandle::FlagTrackDelivery) {
        if (!Send(recipient, ev, flags)) FailWithError("Event was not sent");
    }

private:
    STATEFN(WorkState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDqSpilling::TEvWriteResult, HandleWork);
            hFunc(TEvDqSpilling::TEvReadResult, HandleWork);
            hFunc(TEvDqSpilling::TEvError, HandleWork);
            hFunc(TEvGet, HandleWork);
            hFunc(TEvPut, HandleWork);
            hFunc(TEvDelete, HandleWork);
            hFunc(TEvents::TEvPoison, HandleWork);
            default:
                Y_ABORT("TDqComputeStorageActor::WorkState unexpected event type: %" PRIx32 " event: %s",
                    ev->GetTypeRewrite(),
                    ev->ToString().data());
        }
    }

    void HandleWork(TEvents::TEvPoison::TPtr&) {
        SendInternal(SpillingActorId_, new TEvents::TEvPoison);
        PassAway();
    }

    void HandleWork(TEvPut::TPtr& ev) {
        auto& msg = *ev->Get();
        ui64 size = msg.Blob_.size();
        auto opBegin = TInstant::Now();

        SendInternal(SpillingActorId_, new TEvDqSpilling::TEvWrite(NextBlobId, std::move(msg.Blob_)));

        auto writingBlobInfo = TWritingBlobInfo{size, std::move(msg.Promise_), opBegin};
        WritingBlobs_.emplace(NextBlobId, writingBlobInfo);
        WritingBlobsSize_ += size;

        ++NextBlobId;
    }

    void HandleWork(TEvGet::TPtr& ev) {
        auto& msg = *ev->Get();
        auto opBegin = TInstant::Now();

        if (!StoredBlobs_.contains(msg.Key_)) {
            msg.Promise_.SetValue(std::nullopt);
            return;
        }

        bool removeBlobAfterRead = msg.RemoveBlobAfterRead_;

        auto loadingBlobInfo = TLoadingBlobInfo{removeBlobAfterRead, std::move(msg.Promise_), opBegin};
        LoadingBlobs_.emplace(msg.Key_, std::move(loadingBlobInfo));

        SendInternal(SpillingActorId_, new TEvDqSpilling::TEvRead(msg.Key_, removeBlobAfterRead));
    }

    void HandleWork(TEvDelete::TPtr& ev) {
        auto& msg = *ev->Get();

        if (!StoredBlobs_.contains(msg.Key_)) {
            msg.Promise_.SetValue();
            return;
        }

        DeletingBlobs_.emplace(msg.Key_, std::move(msg.Promise_));

        SendInternal(SpillingActorId_, new TEvDqSpilling::TEvRead(msg.Key_, true));
    }

    void HandleWork(TEvDqSpilling::TEvWriteResult::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_T("[TEvWriteResult] blobId: " << msg.BlobId);


        auto it = WritingBlobs_.find(msg.BlobId);
        if (it == WritingBlobs_.end()) {
            FailWithError(TStringBuilder() << "[TEvWriteResult] Got unexpected TEvWriteResult, blobId: " << msg.BlobId);
            return;
        }

        auto& blobInfo = it->second;

        WritingBlobsSize_ -= blobInfo.Size;

        StoredBlobsCount_++;
        StoredBlobsSize_ += blobInfo.Size;

        if (SpillingTaskCounters_) {
            SpillingTaskCounters_->ComputeWriteBytes += blobInfo.Size;
            auto opDuration = TInstant::Now() - blobInfo.OpBegin;
            SpillingTaskCounters_->ComputeWriteTime += opDuration.MilliSeconds();
        }
        // complete future and wake up waiting compute node
        auto& promise = blobInfo.BlobIdPromise;
        promise.SetValue(msg.BlobId);

        StoredBlobs_.emplace(msg.BlobId);

        WritingBlobs_.erase(it);
        WakeupCallback_();
    }

    void HandleWork(TEvDqSpilling::TEvReadResult::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_T("[TEvReadResult] blobId: " << msg.BlobId << ", size: " << msg.Blob.size());

        // Deletion is read without fetching the results. So, after the deletion library sends TEvReadResult event
        // Check if the intention was to delete and complete correct future in this case.
        if (HandleDelete(msg.BlobId, msg.Blob.Size())) {
            WakeupCallback_();
            return;
        }

        auto it = LoadingBlobs_.find(msg.BlobId);
        if (it == LoadingBlobs_.end()) {
            FailWithError(TStringBuilder() << "[TEvReadResult] Got unexpected TEvReadResult, blobId: " << msg.BlobId);
            return;
        }

        auto& blobInfo = it->second;

        if (SpillingTaskCounters_) {
            auto opDuration = TInstant::Now() - blobInfo.OpBegin;
            SpillingTaskCounters_->ComputeReadTime += opDuration.MilliSeconds();
        }

        if (blobInfo.RemoveAfterRead) {
            UpdateStatsAfterBlobDeletion(msg.Blob.Size(), msg.BlobId);
        }

        TRope res(TString(reinterpret_cast<const char*>(msg.Blob.Data()), msg.Blob.Size()));

        auto& promise = blobInfo.BlobPromise;
        promise.SetValue(std::move(res));

        LoadingBlobs_.erase(it);

        WakeupCallback_();
    }

    void HandleWork(TEvDqSpilling::TEvError::TPtr& ev) {
        auto& msg = *ev->Get();
        FailWithError(TStringBuilder() << "[TEvError] " << msg.Message);
    }

    bool HandleDelete(TKey blobId, ui64 size) {
        auto it = DeletingBlobs_.find(blobId);
        if (it == DeletingBlobs_.end()) {
            return false;
        }

        UpdateStatsAfterBlobDeletion(size, blobId);

        auto& promise = it->second;
        promise.SetValue();
        DeletingBlobs_.erase(it);
        return true;
    }

    void UpdateStatsAfterBlobDeletion(ui64 size, TKey blobId) {
        StoredBlobsCount_--;
        StoredBlobsSize_ -= size;
        StoredBlobs_.erase(blobId);
    }

protected:
    const TTxId TxId_;
    TActorId SpillingActorId_;

    TMap<TKey, TWritingBlobInfo> WritingBlobs_;
    ui64 WritingBlobsSize_ = 0;

    ui32 StoredBlobsCount_ = 0;
    ui64 StoredBlobsSize_ = 0;

    TMap<TKey, TLoadingBlobInfo> LoadingBlobs_;

    TMap<TKey, TDeletingBlobInfo> DeletingBlobs_;

    TKey NextBlobId = 0;

    TString SpillerName_;

    bool IsInitialized_ = false;
    TWakeUpCallback WakeupCallback_;
    TErrorCallback ErrorCallback_;
    TIntrusivePtr<TSpillingTaskCounters> SpillingTaskCounters_;

    TSet<TKey> StoredBlobs_;
};

} // anonymous namespace

IDqComputeStorageActor* CreateDqComputeStorageActor(TTxId txId, const TString& spillerName, TWakeUpCallback wakeupCallback, 
    TErrorCallback errorCallback, TIntrusivePtr<TSpillingTaskCounters> spillingTaskCounters) {
    return new TDqComputeStorageActor(txId, spillerName, wakeupCallback, errorCallback, spillingTaskCounters);
}

} // namespace NYql::NDq 
