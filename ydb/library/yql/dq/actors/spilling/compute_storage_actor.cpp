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
        auto spillingActor = CreateDqLocalFileSpillingActor(TxId_, TStringBuilder() << "ComputeId: " << 1998,
            SelfId(), true);
        SpillingActorId_ = Register(spillingActor);
        Become(&TDqComputeStorageActor::WorkState);
    }

    static constexpr char ActorName[] = "DQ_COMPUTE_STORAGE";

    IActor* GetActor() override {
        return this;
    }

    NThreading::TFuture<IDqComputeStorageActor::TKey> Put(TRope&& blob) override {
        FailOnError();

        // TODO: timeout
        // TODO: limit inflight events

        ui64 size = blob.size();

        Send(SpillingActorId_, new TEvDqSpilling::TEvWrite(blobId, std::move(blob)));

        WritingBlobs_.emplace(blobId, size);
        WritingBlobsSize_ += size;
    }

    bool Get(ui64 blobId) override {
        FailOnError();

        auto loadedIt = LoadedBlobs_.find(blobId);
        if (loadedIt != LoadedBlobs_.end()) {
            YQL_ENSURE(loadedIt->second.size() != 0);
            blob.Swap(loadedIt->second);
            LoadedBlobs_.erase(loadedIt);
            return true;
        }

        auto result = LoadingBlobs_.emplace(blobId);
        if (result.second) {
            Send(SpillingActorId_, new TEvDqSpilling::TEvRead(blobId, true));
        }

        return false;
    }

protected:
    void PassAway() override {
        Send(SpillingActorId_, new TEvents::TEvPoison);
        TBase::PassAway();
    }

    void FailOnError() {
        if (Error_) {
            LOG_E("Error: " << *Error_);
            // TODO think about exception
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

        auto it = WritingBlobs_.find(msg.BlobId);
        if (it == WritingBlobs_.end()) {
            LOG_E("Got unexpected TEvWriteResult, blobId: " << msg.BlobId);

            Error_ = "Internal error";

            Send(SpillingActorId_, new TEvents::TEvPoison);
            return;
        }

        ui64 size = it->second;
        WritingBlobsSize_ -= size;
        WritingBlobs_.erase(it);

        StoredBlobsCount_++;
        StoredBlobsSize_ += size;
    }

    void HandleWork(TEvDqSpilling::TEvReadResult::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_T("[TEvReadResult] blobId: " << msg.BlobId << ", size: " << msg.Blob.size());

        if (LoadingBlobs_.erase(msg.BlobId) != 1) {
            LOG_E("[TEvReadResult] unexpected, blobId: " << msg.BlobId << ", size: " << msg.Blob.size());
            return;
        }

        LoadedBlobs_[msg.BlobId].Swap(msg.Blob);
        YQL_ENSURE(LoadedBlobs_[msg.BlobId].size() != 0);

        if (LoadedBlobs_.size() == 1) {
            WakeUp_();
        }
    }

    void HandleWork(TEvDqSpilling::TEvError::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_D("[TEvError] " << msg.Message);

        Error_.ConstructInPlace(msg.Message);
    }

    protected:
    const TTxId TxId_;
    TActorId SpillingActorId_;

    TMap<ui64, ui64> WritingBlobs_; // blobId -> blobSize
    ui64 WritingBlobsSize_ = 0;

    ui32 StoredBlobsCount_ = 0;
    ui64 StoredBlobsSize_ = 0;

    TSet<ui64> LoadingBlobs_;
    TMap<ui64, TBuffer> LoadedBlobs_;

    TMaybe<TString> Error_;
};

} // anonymous namespace

IDqComputeStorageActor* CreateDqComputeStorageActor(TTxId txId) {
    return new TDqComputeStorageActor(txId);
}

} // namespace NYql::NDq
