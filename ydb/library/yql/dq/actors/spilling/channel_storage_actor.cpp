#include "channel_storage_actor.h"

#include "spilling.h"
#include "spilling_file.h"

#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/size_literals.h>
#include <util/generic/guid.h>

namespace NYql::NDq {

using namespace NActors;

namespace {

#define LOG_D(s) \
    LOG_DEBUG_S(*ActorSystem_, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId_ << ", channelId: " << ChannelId_ << ". " << s);

#define LOG_I(s) \
    LOG_INFO_S(*ActorSystem_,  NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", channelId: " << ChannelId << ". " << s);

#define LOG_E(s) \
    LOG_ERROR_S(*ActorSystem_, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId_ << ", channelId: " << ChannelId_ << ". " << s);

#define LOG_C(s) \
    LOG_CRIT_S(*ActorSystem_,  NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", channelId: " << ChannelId << ". " << s);

#define LOG_W(s) \
    LOG_WARN_S(*ActorSystem_,  NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", channelId: " << ChannelId << ". " << s);

#define LOG_T(s) \
    LOG_TRACE_S(*ActorSystem_,  NKikimrServices::KQP_COMPUTE, "TxId: " << TxId_ << ", channelId: " << ChannelId_ << ". " << s); 

class TDqChannelStorageActor : public IDqChannelStorageActor,
                               public NActors::TActorBootstrapped<TDqChannelStorageActor>
{
    using TBase = TActorBootstrapped<TDqChannelStorageActor>;

    struct TWritingBlobInfo {
        ui64 Size;
        NThreading::TPromise<void> SavePromise;
        TInstant OpBegin;
    };

    struct TLoadingBlobInfo {
        NThreading::TPromise<TBuffer> BlobPromise;
        TInstant OpBegin;
    };
public:

    TDqChannelStorageActor(TTxId txId, ui64 channelId, TWakeUpCallback&& wakeUpCallback, TErrorCallback&& errorCallback,
        TIntrusivePtr<TSpillingTaskCounters> spillingTaskCounters, TActorSystem* actorSystem)
        : TxId_(txId)
        , ChannelId_(channelId)
        , WakeUpCallback_(std::move(wakeUpCallback))
        , ErrorCallback_(std::move(errorCallback))
        , SpillingTaskCounters_(spillingTaskCounters)
        , ActorSystem_(actorSystem)
    {}

    void Bootstrap() {
        auto spillingActor = CreateDqLocalFileSpillingActor(TxId_, TStringBuilder() << "ChannelId: " << ChannelId_ << "_" << CreateGuidAsString(),
            SelfId(), true);
        SpillingActorId_ = Register(spillingActor);
        Become(&TDqChannelStorageActor::WorkState);
    }

    static constexpr char ActorName[] = "DQ_CHANNEL_STORAGE";

    IActor* GetActor() override {
        return this;
    }

protected:
    void FailWithError(const TString& error) {
        if (!ErrorCallback_) Y_ABORT("Error: %s", error.c_str());

        LOG_E("Error: " << error);
        ErrorCallback_(TStringBuilder() << "[Channel spilling]" << error);
        SendInternal(SpillingActorId_, new TEvents::TEvPoison);
        PassAway();
    }

    void SendInternal(const TActorId& recipient, IEventBase* ev, TEventFlags flags = IEventHandle::FlagTrackDelivery) {
        bool isSent = Send(recipient, ev, flags);
        Y_ABORT_UNLESS(isSent, "Event was not sent");
    }

private:

    STATEFN(WorkState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDqSpilling::TEvWriteResult, HandleWork);
            hFunc(TEvDqSpilling::TEvReadResult, HandleWork);
            hFunc(TEvDqSpilling::TEvError, HandleWork);
            hFunc(TEvDqChannelSpilling::TEvGet, HandleWork);
            hFunc(TEvDqChannelSpilling::TEvPut, HandleWork);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                Y_ABORT("TDqChannelStorageActor::WorkState unexpected event type: %" PRIx32 " event: %s",
                    ev->GetTypeRewrite(),
                    ev->ToString().data());
        }
    }



    void HandleWork(TEvDqChannelSpilling::TEvGet::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_T("[TEvGet] blobId: " << msg.BlobId_);

        auto opBegin = TInstant::Now();
 
        auto loadingBlobInfo = TLoadingBlobInfo{std::move(msg.Promise_), opBegin};
        LoadingBlobs_.emplace(msg.BlobId_, std::move(loadingBlobInfo));

        SendInternal(SpillingActorId_, new TEvDqSpilling::TEvRead(msg.BlobId_));
    }

    void HandleWork(TEvDqChannelSpilling::TEvPut::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_T("[TEvPut] blobId: " << msg.BlobId_);

        auto opBegin = TInstant::Now();

        auto writingBlobInfo = TWritingBlobInfo{msg.Blob_.size(), std::move(msg.Promise_), opBegin};
        WritingBlobs_.emplace(msg.BlobId_, std::move(writingBlobInfo));

        SendInternal(SpillingActorId_, new TEvDqSpilling::TEvWrite(msg.BlobId_, std::move(msg.Blob_)));
    }

    void HandleWork(TEvDqSpilling::TEvWriteResult::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_T("[TEvWriteResult] blobId: " << msg.BlobId);

        const auto it = WritingBlobs_.find(msg.BlobId);
        if (it == WritingBlobs_.end()) {
            FailWithError(TStringBuilder() << "[TEvWriteResult] Got unexpected TEvWriteResult, blobId: " << msg.BlobId);
            return;
        }

        auto& blobInfo = it->second;

        if (SpillingTaskCounters_) {
            SpillingTaskCounters_->ChannelWriteBytes += blobInfo.Size;
            auto opDuration = TInstant::Now() - blobInfo.OpBegin;
            SpillingTaskCounters_->ChannelWriteTime += opDuration.MilliSeconds();
        }
        // Complete the future
        blobInfo.SavePromise.SetValue();
        WritingBlobs_.erase(it);

        WakeUpCallback_();
    }

    void HandleWork(TEvDqSpilling::TEvReadResult::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_T("[TEvReadResult] blobId: " << msg.BlobId << ", size: " << msg.Blob.size());

        const auto it = LoadingBlobs_.find(msg.BlobId);
        if (it == LoadingBlobs_.end()) {
            FailWithError(TStringBuilder() << "[TEvReadResult] Got unexpected TEvReadResult, blobId: " << msg.BlobId);
            return;
        }

        auto& blobInfo = it->second;

        if (SpillingTaskCounters_) {
            auto opDuration = TInstant::Now() - blobInfo.OpBegin;
            SpillingTaskCounters_->ChannelReadTime += opDuration.MilliSeconds();
        }

        blobInfo.BlobPromise.SetValue(std::move(msg.Blob));
        LoadingBlobs_.erase(it);

        WakeUpCallback_();
    }

    void HandleWork(TEvDqSpilling::TEvError::TPtr& ev) {
        auto& msg = *ev->Get();
        FailWithError(TStringBuilder() << "[TEvError] " << msg.Message);
    }

    void PassAway() override {
        SendInternal(SpillingActorId_, new TEvents::TEvPoison);
        TBase::PassAway();
    }


private:
    const TTxId TxId_;
    const ui64 ChannelId_;

    TWakeUpCallback WakeUpCallback_;
    TErrorCallback ErrorCallback_;
    TIntrusivePtr<TSpillingTaskCounters> SpillingTaskCounters_;
    TActorId SpillingActorId_;

    // BlobId -> blob size + promise that blob is saved
    std::unordered_map<ui64, TWritingBlobInfo> WritingBlobs_;
    
    // BlobId -> promise with requested blob
    std::unordered_map<ui64, TLoadingBlobInfo> LoadingBlobs_;

    TActorSystem* ActorSystem_;
};

} // anonymous namespace

IDqChannelStorageActor* CreateDqChannelStorageActor(TTxId txId, ui64 channelId,
    TWakeUpCallback&& wakeUpCallback,
    TErrorCallback&& errorCallback,
    TIntrusivePtr<TSpillingTaskCounters> spillingTaskCounters,
    NActors::TActorSystem* actorSystem)
{
    return new TDqChannelStorageActor(txId, channelId, std::move(wakeUpCallback), std::move(errorCallback), spillingTaskCounters, actorSystem);
}

} // namespace NYql::NDq
