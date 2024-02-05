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
    // size + promise with key
    using TWritingBlobInfo = std::pair<ui64, NThreading::TPromise<IDqComputeStorageActor::TKey>>;
    // remove after read + promise with blob
    using TLoadingBlobInfo = std::pair<bool, NThreading::TPromise<TRope>>;
    // void promise that completes when block is removed
    using TDeletingBlobInfo = NThreading::TPromise<void>;
public:
    TDqComputeStorageActor(TTxId txId, const TString& spillerName, std::function<void()> wakeupCallback, TActorSystem* actorSystem)
        : TxId_(txId),
        ActorSystem_(actorSystem),
        SpillerName_(spillerName),
        WakeupCallback_(wakeupCallback)
    {
    }

    void Bootstrap() {
        Become(&TDqComputeStorageActor::WorkState);
    }

    static constexpr char ActorName[] = "DQ_COMPUTE_STORAGE";

    IActor* GetActor() override {
        return this;
    }

    NThreading::TFuture<IDqComputeStorageActor::TKey> Put(TRope&& blob) override {
        InitializeIfNot();
        // Use lock to prevent race when state is changed on event processing and on Put call
        std::lock_guard lock(Mutex_);

        FailOnError();

        ui64 size = blob.size();

        ActorSystem_->Send(SpillingActorId_, new TEvDqSpilling::TEvWrite(NextBlobId, std::move(blob)));

        auto it = WritingBlobs_.emplace(NextBlobId, std::make_pair(size, NThreading::NewPromise<IDqComputeStorageActor::TKey>())).first;
        WritingBlobsSize_ += size;

        ++NextBlobId;

        auto& promise = it->second.second;

        return promise.GetFuture();
    }

    std::optional<NThreading::TFuture<TRope>> Get(IDqComputeStorageActor::TKey blobId) override {
        return GetInternal(blobId, false);
    }

    std::optional<NThreading::TFuture<TRope>> Extract(IDqComputeStorageActor::TKey blobId) override {
        return GetInternal(blobId, true);
    }

    NThreading::TFuture<void> Delete(IDqComputeStorageActor::TKey blobId) override {
        InitializeIfNot();
        // Use lock to prevent race when state is changed on event processing and on Delete call
        std::lock_guard lock(Mutex_);

        FailOnError();

        auto promise = NThreading::NewPromise<void>();
        auto future = promise.GetFuture();

        if (!StoredBlobs_.contains(blobId)) {
            promise.SetValue();
            return future;
        }

        DeletingBlobs_.emplace(blobId, std::move(promise));

        ActorSystem_->Send(SpillingActorId_, new TEvDqSpilling::TEvRead(blobId, true));

        return future;
    }

protected:
    std::optional<NThreading::TFuture<TRope>>GetInternal(IDqComputeStorageActor::TKey blobId, bool removeAfterRead) {
        InitializeIfNot();
        // Use lock to prevent race when state is changed on event processing and on Get call
        std::lock_guard lock(Mutex_);

        FailOnError();

        if (!StoredBlobs_.contains(blobId)) return std::nullopt;

        TLoadingBlobInfo loadingblobInfo = std::make_pair(removeAfterRead, NThreading::NewPromise<TRope>());
        auto it = LoadingBlobs_.emplace(blobId, std::move(loadingblobInfo)).first;

        ActorSystem_->Send(SpillingActorId_, new TEvDqSpilling::TEvRead(blobId, false));

        auto& promise = it->second.second;
        return promise.GetFuture();
    }

    void PassAway() override {
        InitializeIfNot();
        ActorSystem_->Send(SpillingActorId_, new TEvents::TEvPoison);
        TBase::PassAway();
    }

    void FailOnError() {
        InitializeIfNot();
        if (Error_) {
            LOG_E("Error: " << *Error_);
            ActorSystem_->Send(SpillingActorId_, new TEvents::TEvPoison);
        }
    }

private:
    STATEFN(WorkState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDqSpilling::TEvWriteResult, HandleWork);
            hFunc(TEvDqSpilling::TEvReadResult, HandleWork);
            hFunc(TEvDqSpilling::TEvError, HandleWork);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                Y_ABORT("TDqComputeStorageActor::WorkState unexpected event type: %" PRIx32 " event: %s",
                    ev->GetTypeRewrite(),
                    ev->ToString().data());
        }
    }

    void HandleWork(TEvDqSpilling::TEvWriteResult::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_T("[TEvWriteResult] blobId: " << msg.BlobId);

        // Use lock to prevent race when state is changed on event processing and on Put call
        std::lock_guard lock(Mutex_);

        auto it = WritingBlobs_.find(msg.BlobId);
        if (it == WritingBlobs_.end()) {
            LOG_E("Got unexpected TEvWriteResult, blobId: " << msg.BlobId);

            Error_ = "Internal error";

            ActorSystem_->Send(SpillingActorId_, new TEvents::TEvPoison);
            return;
        }

        auto& [size, promise] = it->second;

        WritingBlobsSize_ -= size;

        StoredBlobsCount_++;
        StoredBlobsSize_ += size;

        StoredBlobs_.insert(msg.BlobId);

        // complete future and wake up waiting compute node
        promise.SetValue(msg.BlobId);

        WritingBlobs_.erase(it);
        WakeupCallback_();
    }

    void HandleWork(TEvDqSpilling::TEvReadResult::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_T("[TEvReadResult] blobId: " << msg.BlobId << ", size: " << msg.Blob.size());

        // Use lock to prevent race when state is changed on event processing and on Put call
        std::lock_guard lock(Mutex_);

        // Deletion is read without fetching the results. So, after the deletion library sends TEvReadResult event
        // Check if the intention was to delete and complete correct future in this case.
        if (HandleDelete(msg.BlobId, msg.Blob.Size())) {
            WakeupCallback_();
            return;
        }

        auto it = LoadingBlobs_.find(msg.BlobId);
        if (it == LoadingBlobs_.end()) {
            LOG_E("Got unexpected TEvReadResult, blobId: " << msg.BlobId);

            Error_ = "Internal error";

            ActorSystem_->Send(SpillingActorId_, new TEvents::TEvPoison);
            return;
        }

        bool removedAfterRead = it->second.first;
        if (removedAfterRead) {
            UpdateStatsAfterBlobDeletion(msg.BlobId, msg.Blob.Size());
        }

        TRope res(TString(reinterpret_cast<const char*>(msg.Blob.Data()), msg.Blob.Size()));

        auto& promise = it->second.second;
        promise.SetValue(std::move(res));

        LoadingBlobs_.erase(it);

        WakeupCallback_();
    }

    void HandleWork(TEvDqSpilling::TEvError::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_D("[TEvError] " << msg.Message);

        Error_.ConstructInPlace(msg.Message);
    }

    bool HandleDelete(IDqComputeStorageActor::TKey blobId, ui64 size) {
        auto it = DeletingBlobs_.find(blobId);
        if (it == DeletingBlobs_.end()) {
            return false;
        }

        UpdateStatsAfterBlobDeletion(blobId, size);

        auto& promise = it->second;
        promise.SetValue();
        DeletingBlobs_.erase(it);
        return true;
    }

    void UpdateStatsAfterBlobDeletion(IDqComputeStorageActor::TKey blobId, ui64 size) {
        StoredBlobsCount_--;
        StoredBlobsSize_ -= size;
        StoredBlobs_.erase(blobId);
    }

    // It's illegal to initialize an inner actor in the actor's ctor. Because in this case ctx will not be initialized because it's initialized afger Bootstrap event.
    // But also it's not possible to initialize inner actor in the bootstrap function because in this case Put/Get may be called before the Bootstrap -> inner worker will be uninitialized.
    // In current implementation it's still possible to leave inner actor uninitialized that is why it's planned to split this class into Actor part + non actor part
    void InitializeIfNot() {
        if (IsInitialized_) return;
        auto spillingActor = CreateDqLocalFileSpillingActor(TxId_, SpillerName_,
            SelfId(), false);
        SpillingActorId_ = Register(spillingActor);

        IsInitialized_ = true;
    }


    protected:
    const TTxId TxId_;
    TActorId SpillingActorId_;
    TActorSystem* ActorSystem_;

    TMap<IDqComputeStorageActor::TKey, TWritingBlobInfo> WritingBlobs_;
    TSet<ui64> StoredBlobs_;
    ui64 WritingBlobsSize_ = 0;

    ui32 StoredBlobsCount_ = 0;
    ui64 StoredBlobsSize_ = 0;

    TMap<IDqComputeStorageActor::TKey, TLoadingBlobInfo> LoadingBlobs_;

    TMap<IDqComputeStorageActor::TKey, TDeletingBlobInfo> DeletingBlobs_;

    TMaybe<TString> Error_;

    IDqComputeStorageActor::TKey NextBlobId = 0;

    TString SpillerName_;

    bool IsInitialized_ = false;

    std::function<void()> WakeupCallback_;

    private:
    std::mutex Mutex_;

};

} // anonymous namespace

IDqComputeStorageActor* CreateDqComputeStorageActor(TTxId txId, const TString& spillerName, std::function<void()> wakeupCallback, TActorSystem* actorSystem) {
    return new TDqComputeStorageActor(txId, spillerName, wakeupCallback, actorSystem);
}

} // namespace NYql::NDq
