#include "compute_storage_actor.h"

#include "spilling.h"
#include "spilling_file.h"

#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/size_literals.h>

namespace NYql::NDq {

using namespace NActors;

std::atomic_int SpillerBlobId = 0;

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
public:
    TDqComputeStorageActor(TTxId txId)
        : TxId_(txId)  
    {}

    void Bootstrap() {
        Become(&TDqComputeStorageActor::WorkState);
    }

    static constexpr char ActorName[] = "DQ_COMPUTE_STORAGE";

    IActor* GetActor() override {
        return this;
    }

    NThreading::TFuture<IDqComputeStorageActor::TKey> Put(TRope&& blob) override {
        std::lock_guard lock(Mutex_);
        FailOnError();
        CreateSpiller();

        ui64 size = blob.size();
        ui64 NextBlobId = SpillerBlobId++;


        SelfId().Send(SpillingActorId_, new TEvDqSpilling::TEvWrite(NextBlobId, std::move(blob)));

        auto res = WritingBlobs_.emplace(NextBlobId, std::make_pair(size, NThreading::NewPromise<IDqComputeStorageActor::TKey>()));
        WritingBlobsSize_ += size;

        ++NextBlobId;

        return res.first->second.second.GetFuture();
    }

    std::optional<NThreading::TFuture<TRope>> Get(IDqComputeStorageActor::TKey blobId) override {
        std::lock_guard lock(Mutex_);
        FailOnError();
        CreateSpiller();

        if (!SavedBlobs_.contains(blobId)) return std::nullopt;

        auto res = LoadingBlobs_.emplace(blobId, NThreading::NewPromise<TRope>());

        SelfId().Send(SpillingActorId_, new TEvDqSpilling::TEvRead(blobId, false));

        return res.first->second.GetFuture();
    }

    std::optional<NThreading::TFuture<TRope>> Extract(IDqComputeStorageActor::TKey blobId) override {
        std::lock_guard lock(Mutex_);
        FailOnError();
        CreateSpiller();

        if (!SavedBlobs_.contains(blobId)) return std::nullopt;

        auto res = LoadingBlobs_.emplace(blobId, NThreading::NewPromise<TRope>());

        SelfId().Send(SpillingActorId_, new TEvDqSpilling::TEvRead(blobId, true));
        BlobsToRemoveAfterRead_.emplace(blobId);

        return res.first->second.GetFuture();
    }

    NThreading::TFuture<void> Delete(IDqComputeStorageActor::TKey blobId) override {
        std::lock_guard lock(Mutex_);
        FailOnError();
        CreateSpiller();

        auto promise = NThreading::NewPromise<void>();

        promise.SetValue();


        if (!SavedBlobs_.contains(blobId)) promise.GetFuture();

        // TODO: actual delete
        return promise.GetFuture();
    }

protected:
    void PassAway() override {
        SelfId().Send(SpillingActorId_, new TEvents::TEvPoison);
        TBase::PassAway();
    }

    void FailOnError() {
        if (Error_) {
            LOG_E("Error: " << *Error_);
            SelfId().Send(SpillingActorId_, new TEvents::TEvPoison);
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
        std::lock_guard lock(Mutex_);
        auto& msg = *ev->Get();
        LOG_T("[TEvWriteResult] blobId: " << msg.BlobId);

        auto it = WritingBlobs_.find(msg.BlobId);
        if (it == WritingBlobs_.end()) {
            LOG_E("Got unexpected TEvWriteResult, blobId: " << msg.BlobId);

            Error_ = "Internal error";

            SelfId().Send(SpillingActorId_, new TEvents::TEvPoison);
            return;
        }

        auto& blob = it->second;
        // complete future and wake up waiting compute node
        blob.second.SetValue(msg.BlobId);
        ui64 size = blob.first;
        WritingBlobsSize_ -= size;
        WritingBlobs_.erase(it);

        StoredBlobsCount_++;
        StoredBlobsSize_ += size;

        SavedBlobs_.insert(msg.BlobId);
    }

    void HandleWork(TEvDqSpilling::TEvReadResult::TPtr& ev) {
        std::lock_guard lock(Mutex_);
        auto& msg = *ev->Get();
        LOG_T("[TEvReadResult] blobId: " << msg.BlobId << ", size: " << msg.Blob.size());

        auto it = LoadingBlobs_.find(msg.BlobId);
        if (it == LoadingBlobs_.end()) {
            LOG_E("Got unexpected TEvReadResult, blobId: " << msg.BlobId);

            Error_ = "Internal error";

            SelfId().Send(SpillingActorId_, new TEvents::TEvPoison);
            return;
        }

        if (BlobsToRemoveAfterRead_.contains(msg.BlobId)) {
            StoredBlobsCount_--;
            StoredBlobsSize_ -= msg.Blob.Size();
            BlobsToRemoveAfterRead_.erase(msg.BlobId);
            SavedBlobs_.erase(msg.BlobId);
        }

        TRope res(std::move(msg.Blob.Data()));

        it->second.SetValue(std::move(res));

        LoadingBlobs_.erase(it);
    }

    void HandleWork(TEvDqSpilling::TEvError::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_D("[TEvError] " << msg.Message);

        Error_.ConstructInPlace(msg.Message);
    }

    void CreateSpiller() {
        if (IsCreated) return;
        auto spillingActor = CreateDqLocalFileSpillingActor(TxId_, TStringBuilder() << "ComputeId: " << 1998,
            SelfId(), true);
        SpillingActorId_ = Register(spillingActor);

        IsCreated = true;
    }

    protected:
    const TTxId TxId_;
    TActorId SpillingActorId_;

    TMap<IDqComputeStorageActor::TKey, std::pair<ui64, NThreading::TPromise<IDqComputeStorageActor::TKey>>> WritingBlobs_;
    TSet<ui64> SavedBlobs_;
    ui64 WritingBlobsSize_ = 0;

    ui32 StoredBlobsCount_ = 0;
    ui64 StoredBlobsSize_ = 0;

    TMap<IDqComputeStorageActor::TKey, NThreading::TPromise<TRope>> LoadingBlobs_;
    TSet<IDqComputeStorageActor::TKey> BlobsToRemoveAfterRead_;

    TMaybe<TString> Error_;

    // IDqComputeStorageActor::TKey NextBlobId = 0;

    bool IsCreated = false;

    std::mutex Mutex_;

};

} // anonymous namespace

IDqComputeStorageActor* CreateDqComputeStorageActor(TTxId txId) {
    return new TDqComputeStorageActor(txId);
}

} // namespace NYql::NDq
