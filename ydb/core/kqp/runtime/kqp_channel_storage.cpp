#include "kqp_channel_storage.h"
#include "kqp_spilling.h"
#include "kqp_spilling_file.h"

#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <util/generic/buffer.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/size_literals.h>


namespace NKikimr::NKqp {

using namespace NActors;
using namespace NYql;
using namespace NYql::NDq;

#define LOG(...) do { if (Y_UNLIKELY(LogFunc)) { LogFunc(__VA_ARGS__); } } while (0)

#define LOG_D(s) \
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", channelId: " << ChannelId << ". " << s)
#define LOG_I(s) \
    LOG_INFO_S(*TlsActivationContext,  NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", channelId: " << ChannelId << ". " << s)
#define LOG_E(s) \
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", channelId: " << ChannelId << ". " << s)
#define LOG_C(s) \
    LOG_CRIT_S(*TlsActivationContext,  NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", channelId: " << ChannelId << ". " << s)
#define LOG_W(s) \
    LOG_WARN_S(*TlsActivationContext,  NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", channelId: " << ChannelId << ". " << s)
#define LOG_T(s) \
    LOG_TRACE_S(*TlsActivationContext,  NKikimrServices::KQP_COMPUTE, "TxId: " << TxId << ", channelId: " << ChannelId << ". " << s)

namespace {

constexpr ui32 MAX_INFLIGHT_BLOBS_COUNT = 10;
constexpr ui64 MAX_INFLIGHT_BLOBS_SIZE = 50_MB;

class TKqpChannelStorageActor : public TActorBootstrapped<TKqpChannelStorageActor> {
    using TBase = TActorBootstrapped<TKqpChannelStorageActor>;

public:
    TKqpChannelStorageActor(ui64 txId, ui64 channelId, IDqChannelStorage::TWakeUpCallback&& wakeUp)
        : TxId(txId)
        , ChannelId(channelId)
        , WakeUp(std::move(wakeUp)) {}

    void Bootstrap() {
        auto spillingActor = CreateKqpLocalFileSpillingActor(TxId, TStringBuilder() << "ChannelId: " << ChannelId,
            SelfId(), true);
        SpillingActorId = Register(spillingActor);

        Become(&TKqpChannelStorageActor::WorkState);
    }

    static constexpr char ActorName[] = "KQP_CHANNEL_STORAGE";

protected:
    void PassAway() override {
        Send(SpillingActorId, new TEvents::TEvPoison);
        TBase::PassAway();
    }

private:
    STATEFN(WorkState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKqpSpilling::TEvWriteResult, HandleWork);
            hFunc(TEvKqpSpilling::TEvReadResult, HandleWork);
            hFunc(TEvKqpSpilling::TEvError, HandleWork);
            default:
                Y_FAIL("TKqpChannelStorageActor::WorkState unexpected event type: %" PRIx32 " event: %s",
                    ev->GetTypeRewrite(),
                    ev->ToString().data());
        }
    }

    void HandleWork(TEvKqpSpilling::TEvWriteResult::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_T("[TEvWriteResult] blobId: " << msg.BlobId);

        auto it = WritingBlobs.find(msg.BlobId);
        if (it == WritingBlobs.end()) {
            LOG_E("Got unexpected TEvWriteResult, blobId: " << msg.BlobId);

            Error = "Internal error";

            Send(SpillingActorId, new TEvents::TEvPoison);
            return;
        }

        ui64 size = it->second;
        WritingBlobsSize -= size;
        WritingBlobs.erase(it);

        StoredBlobsCount++;
        StoredBlobsSize += size;
    }

    void HandleWork(TEvKqpSpilling::TEvReadResult::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_T("[TEvReadResult] blobId: " << msg.BlobId << ", size: " << msg.Blob.size());

        if (LoadingBlobs.erase(msg.BlobId) != 1) {
            LOG_E("[TEvReadResult] unexpected, blobId: " << msg.BlobId << ", size: " << msg.Blob.size());
            return;
        }

        LoadedBlobs[msg.BlobId].Swap(msg.Blob);
        YQL_ENSURE(LoadedBlobs[msg.BlobId].size() != 0);

        if (LoadedBlobs.size() == 1) {
            WakeUp();
        }
    }

    void HandleWork(TEvKqpSpilling::TEvError::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_D("[TEvError] " << msg.Message);

        Error.ConstructInPlace(msg.Message);
    }

public:
    [[nodiscard]]
    const TMaybe<TString>& GetError() const {
        return Error;
    }

    bool IsEmpty() const {
        return WritingBlobs.empty() && StoredBlobsCount == 0 && LoadedBlobs.empty();
    }

    bool IsFull() const {
        return WritingBlobs.size() > MAX_INFLIGHT_BLOBS_COUNT || WritingBlobsSize > MAX_INFLIGHT_BLOBS_SIZE;
    }

    void Put(ui64 blobId, TRope&& blob) {
        FailOnError();

        // TODO: timeout
        // TODO: limit inflight events

        ui64 size = blob.size();

        Send(SpillingActorId, new TEvKqpSpilling::TEvWrite(blobId, std::move(blob)));

        WritingBlobs.emplace(blobId, size);
        WritingBlobsSize += size;
    }

    bool Get(ui64 blobId, TBuffer& blob) {
        FailOnError();

        auto loadedIt = LoadedBlobs.find(blobId);
        if (loadedIt != LoadedBlobs.end()) {
            YQL_ENSURE(loadedIt->second.size() != 0);
            blob.Swap(loadedIt->second);
            LoadedBlobs.erase(loadedIt);
            return true;
        }

        auto result = LoadingBlobs.emplace(blobId);
        if (result.second) {
            Send(SpillingActorId, new TEvKqpSpilling::TEvRead(blobId, true));
        }

        return false;
    }

    void Terminate() {
        PassAway();
    }

private:
    void FailOnError() {
        if (Error) {
            LOG_E("TxId: " << TxId << ", channelId: " << ChannelId << ", error: " << *Error);
            ythrow TDqChannelStorageException() << "TxId: " << TxId << ", channelId: " << ChannelId
                << ", error: " << *Error;
        }
    }

private:
    const ui64 TxId;
    const ui64 ChannelId;
    IDqChannelStorage::TWakeUpCallback WakeUp;
    TActorId SpillingActorId;

    TMap<ui64, ui64> WritingBlobs; // blobId -> blobSize
    ui64 WritingBlobsSize = 0;

    ui32 StoredBlobsCount = 0;
    ui64 StoredBlobsSize = 0;

    TSet<ui64> LoadingBlobs;
    TMap<ui64, TBuffer> LoadedBlobs;

    TMaybe<TString> Error;
};


class TKqpChannelStorage : public IDqChannelStorage {
public:
    TKqpChannelStorage(ui64 txId, ui64 channelId, TWakeUpCallback&& wakeUp, const TActorContext& ctx)
    {
        SelfActor = new TKqpChannelStorageActor(txId, channelId, std::move(wakeUp));
        ctx.RegisterWithSameMailbox(SelfActor);
    }

    ~TKqpChannelStorage() {
        SelfActor->Terminate();
    }

    bool IsEmpty() const override {
        return SelfActor->IsEmpty();
    }

    bool IsFull() const override {
        return SelfActor->IsFull();
    }

    void Put(ui64 blobId, TRope&& blob) override {
        SelfActor->Put(blobId, std::move(blob));
    }

    bool Get(ui64 blobId, TBuffer& blob) override {
        return SelfActor->Get(blobId, blob);
    }

private:
    TKqpChannelStorageActor* SelfActor;
};

} // anonymous namespace

IDqChannelStorage::TPtr CreateKqpChannelStorage(ui64 txId, ui64 channelId, IDqChannelStorage::TWakeUpCallback wakeUp,
    const TActorContext& ctx)
{
    return new TKqpChannelStorage(txId, channelId, std::move(wakeUp), ctx);
}

} // namespace NKikimr::NKqp
