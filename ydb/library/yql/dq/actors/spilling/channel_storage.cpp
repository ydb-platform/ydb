#include "channel_storage.h"
#include "spilling.h"
#include "spilling_file.h"

#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <util/generic/buffer.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/size_literals.h>


namespace NYql::NDq {

using namespace NActors;

#define LOG_D(s) \
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId_ << ", channelId: " << ChannelId_ << ". " << s)
#define LOG_I(s) \
    LOG_INFO_S(*TlsActivationContext,  NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", channelId: " << ChannelId << ". " << s)
#define LOG_E(s) \
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId_ << ", channelId: " << ChannelId_ << ". " << s)
#define LOG_C(s) \
    LOG_CRIT_S(*TlsActivationContext,  NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", channelId: " << ChannelId << ". " << s)
#define LOG_W(s) \
    LOG_WARN_S(*TlsActivationContext,  NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", channelId: " << ChannelId << ". " << s)
#define LOG_T(s) \
    LOG_TRACE_S(*TlsActivationContext,  NKikimrServices::KQP_COMPUTE, "TxId: " << TxId_ << ", channelId: " << ChannelId_ << ". " << s)

namespace {

constexpr ui32 MAX_INFLIGHT_BLOBS_COUNT = 10;
constexpr ui64 MAX_INFLIGHT_BLOBS_SIZE = 50_MB;

class TDqChannelStorageActor : public TActorBootstrapped<TDqChannelStorageActor> {
    using TBase = TActorBootstrapped<TDqChannelStorageActor>;

public:
    TDqChannelStorageActor(TTxId txId, ui64 channelId, IDqChannelStorage::TWakeUpCallback&& wakeUp)
        : TxId_(txId)
        , ChannelId_(channelId)
        , WakeUp_(std::move(wakeUp)) {}

    void Bootstrap() {
        auto spillingActor = CreateDqLocalFileSpillingActor(TxId_, TStringBuilder() << "ChannelId: " << ChannelId_,
            SelfId(), true);
        SpillingActorId_ = Register(spillingActor);

        Become(&TDqChannelStorageActor::WorkState);
    }

    static constexpr char ActorName[] = "DQ_CHANNEL_STORAGE";

protected:
    void PassAway() override {
        Send(SpillingActorId_, new TEvents::TEvPoison);
        TBase::PassAway();
    }

private:
    STATEFN(WorkState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDqSpilling::TEvWriteResult, HandleWork);
            hFunc(TEvDqSpilling::TEvReadResult, HandleWork);
            hFunc(TEvDqSpilling::TEvError, HandleWork);
            default:
                Y_ABORT("TDqChannelStorageActor::WorkState unexpected event type: %" PRIx32 " event: %s",
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

public:
    [[nodiscard]]
    const TMaybe<TString>& GetError() const {
        return Error_;
    }

    bool IsEmpty() const {
        return WritingBlobs_.empty() && StoredBlobsCount_ == 0 && LoadedBlobs_.empty();
    }

    bool IsFull() const {
        return WritingBlobs_.size() > MAX_INFLIGHT_BLOBS_COUNT || WritingBlobsSize_ > MAX_INFLIGHT_BLOBS_SIZE;
    }

    void Put(ui64 blobId, TRope&& blob) {
        FailOnError();

        // TODO: timeout
        // TODO: limit inflight events

        ui64 size = blob.size();

        Send(SpillingActorId_, new TEvDqSpilling::TEvWrite(blobId, std::move(blob)));

        WritingBlobs_.emplace(blobId, size);
        WritingBlobsSize_ += size;
    }

    bool Get(ui64 blobId, TBuffer& blob) {
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

    void Terminate() {
        PassAway();
    }

private:
    void FailOnError() {
        if (Error_) {
            LOG_E("Error: " << *Error_);
            ythrow TDqChannelStorageException() << "TxId: " << TxId_ << ", channelId: " << ChannelId_
                << ", error: " << *Error_;
        }
    }

private:
    const TTxId TxId_;
    const ui64 ChannelId_;
    IDqChannelStorage::TWakeUpCallback WakeUp_;
    TActorId SpillingActorId_;

    TMap<ui64, ui64> WritingBlobs_; // blobId -> blobSize
    ui64 WritingBlobsSize_ = 0;

    ui32 StoredBlobsCount_ = 0;
    ui64 StoredBlobsSize_ = 0;

    TSet<ui64> LoadingBlobs_;
    TMap<ui64, TBuffer> LoadedBlobs_;

    TMaybe<TString> Error_;
};


class TDqChannelStorage : public IDqChannelStorage {
public:
    TDqChannelStorage(TTxId txId, ui64 channelId, TWakeUpCallback&& wakeUp, const TActorContext& ctx) {
        SelfActor_ = new TDqChannelStorageActor(txId, channelId, std::move(wakeUp));
        ctx.RegisterWithSameMailbox(SelfActor_);
    }

    ~TDqChannelStorage() {
        SelfActor_->Terminate();
    }

    bool IsEmpty() const override {
        return SelfActor_->IsEmpty();
    }

    bool IsFull() const override {
        return SelfActor_->IsFull();
    }

    void Put(ui64 blobId, TRope&& blob) override {
        SelfActor_->Put(blobId, std::move(blob));
    }

    bool Get(ui64 blobId, TBuffer& blob) override {
        return SelfActor_->Get(blobId, blob);
    }

private:
    TDqChannelStorageActor* SelfActor_;
};

} // anonymous namespace

IDqChannelStorage::TPtr CreateDqChannelStorage(TTxId txId, ui64 channelId, IDqChannelStorage::TWakeUpCallback wakeUp,
    const TActorContext& ctx)
{
    return new TDqChannelStorage(txId, channelId, std::move(wakeUp), ctx);
}

} // namespace NYql::NDq
