#ifndef KIKIMR_DISABLE_S3_OPS

#include "defs.h"
#include "columnshard_impl.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/datashard/s3_common.h>
#include <ydb/core/wrappers/s3_wrapper.h>

namespace NKikimr::NColumnShard {

using NWrappers::TEvS3Wrapper;

namespace {

TString ExtractBlobPart(const NOlap::TBlobRange& blobRange, const TString& data) {
    return TString(&data[blobRange.Offset], blobRange.Size);
}

struct TS3Export {
    std::unique_ptr<TEvPrivate::TEvExport> Event;
    THashSet<TString> KeysToWrite;

    TS3Export() = default;

    explicit TS3Export(TAutoPtr<TEvPrivate::TEvExport> ev)
        : Event(ev.Release())
    {}

    TEvPrivate::TEvExport::TBlobDataMap& Blobs() {
        return Event->Blobs;
    }

    TUnifiedBlobId AddExported(const TString& bucket, const TUnifiedBlobId& srcBlob) {
        Event->SrcToDstBlobs[srcBlob] = TUnifiedBlobId(srcBlob, TUnifiedBlobId::S3_BLOB, bucket);
        return Event->SrcToDstBlobs[srcBlob];
    }
};

struct TS3Forget {
    std::unique_ptr<TEvPrivate::TEvForget> Event;
    THashSet<TString> KeysToDelete;

    TS3Forget() = default;

    explicit TS3Forget(TAutoPtr<TEvPrivate::TEvForget> ev)
        : Event(ev.Release())
    {}
};

// S3 objects need InitAPI() called frist. TS3User calls it in ctor.
struct TAwsContext : private NWrappers::TS3User {
    Aws::Client::ClientConfiguration Config;
    Aws::Auth::AWSCredentials Credentials;
    TActorId Client; // S3Wrapper should be created after API owner too

    void SetConfig(const NKikimrSchemeOp::TS3Settings& settings) {
        Config = NDataShard::ConfigFromSettings(settings);
        Credentials = NDataShard::CredentialsFromSettings(settings);
    }

    IActor* CreateS3Wrapper() const {
        return NWrappers::CreateS3Wrapper(Credentials, Config);
    }
};

}


class TS3Actor : public TActorBootstrapped<TS3Actor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_COLUMNSHARD_S3_ACTOR;
    }

    TS3Actor(ui64 tabletId, const TActorId& parent, const TString& tierName)
        : TabletId(tabletId)
        , ShardActor(parent)
        , TierName(tierName)
    {}

    void Bootstrap() {
        LOG_S_DEBUG("[S3] Starting actor for tier '" << TierName << "' at tablet " << TabletId);
        Become(&TThis::StateWait);
    }

    void Handle(TEvPrivate::TEvS3Settings::TPtr& ev) {
        auto& msg = *ev->Get();
        auto& endpoint = msg.Settings.GetEndpoint();
        Bucket = msg.Settings.GetBucket();

        LOG_S_DEBUG("[S3] Update settings for tier '" << TierName << "' endpoint '" << endpoint
            << "' bucket '" << Bucket << "' at tablet " << TabletId);

        if (endpoint.empty()) {
            LOG_S_ERROR("[S3] No endpoint in settings for tier '" << TierName << "' at tablet " << TabletId);
            return;
        }
        if (Bucket.empty()) {
            LOG_S_ERROR("[S3] No bucket in settings for tier '" << TierName << "' at tablet " << TabletId);
            return;
        }

        S3Ctx.SetConfig(msg.Settings);
        if (S3Ctx.Client) {
            Send(S3Ctx.Client, new TEvents::TEvPoisonPill);
            S3Ctx.Client = {};
        }
        S3Ctx.Client = this->RegisterWithSameMailbox(S3Ctx.CreateS3Wrapper());
    }

    void Handle(TEvPrivate::TEvExport::TPtr& ev) {
        auto& msg = *ev->Get();
        ui64 exportNo = msg.ExportNo;

        Y_VERIFY(!Exports.count(exportNo));
        Exports[exportNo] = TS3Export(ev->Release());
        auto& ex = Exports[exportNo];

        for (auto& [blobId, blob] : ex.Blobs()) {
            TString key = ex.AddExported(Bucket, blobId).GetS3Key();
            Y_VERIFY(!ExportingKeys.count(key)); // TODO

            ex.KeysToWrite.emplace(key);
            ExportingKeys[key] = exportNo;
            SendPutObject(key, std::move(blob.Data));
        }
    }

    void Handle(TEvPrivate::TEvForget::TPtr& ev) {
        ui64 forgetNo = ++ForgetNo;

        Forgets[forgetNo] = TS3Forget(ev->Release());
        auto& forget = Forgets[forgetNo];

        for (auto& evict : forget.Event->Evicted) {
            if (!evict.ExternBlob.IsS3Blob()) {
                LOG_S_ERROR("[S3] Forget not exported '" << evict.Blob.ToStringNew() << "' at tablet " << TabletId);
                continue;
            }

            TString key = evict.ExternBlob.GetS3Key();
            Y_VERIFY(!ForgettingKeys.count(key)); // TODO

            forget.KeysToDelete.emplace(key);
            ForgettingKeys[key] = forgetNo;
            SendDeleteObject(key);
        }
    }

    void Handle(TEvPrivate::TEvGetExported::TPtr& ev) {
        auto& evict = ev->Get()->Evicted;
        if (!evict.ExternBlob.IsS3Blob()) {
            LOG_S_ERROR("[S3] Get not exported '" << evict.Blob.ToStringNew() << "' at tablet " << TabletId);
            return;
        }

        TString key = evict.ExternBlob.GetS3Key();

        bool reading = ReadingKeys.count(key);
        ReadingKeys[key].emplace_back(ev->Release().Release());

        if (!reading) {
            ui64 blobSize = evict.ExternBlob.BlobSize();
            SendGetObject(key, {0, blobSize});
        } else {
            LOG_S_DEBUG("[S3] Outstanding get key '" << key << "' at tablet " << TabletId);
        }
    }

    // TODO: clean written blobs in failed export
    void Handle(TEvS3Wrapper::TEvPutObjectResponse::TPtr& ev) {
        Y_VERIFY(Initialized());

        auto& msg = *ev->Get();
        const auto& resultOutcome = msg.Result;

        TString errStr;
        if (!resultOutcome.IsSuccess()) {
            errStr = LogError("PutObjectResponse", resultOutcome.GetError(), !!msg.Key);
        }

        Y_VERIFY(msg.Key); // FIXME
        TString key = *msg.Key;

        LOG_S_DEBUG("[S3] PutObjectResponse '" << key << "' at tablet " << TabletId);

        if (!ExportingKeys.count(key)) {
            LOG_S_DEBUG("[S3] PutObjectResponse for unknown key '" << key << "' at tablet " << TabletId);
            return;
        }

        ui64 exportNo = ExportingKeys[key];
        ExportingKeys.erase(key);

        if (!Exports.count(exportNo)) {
            LOG_S_DEBUG("[S3] PutObjectResponse for unknown export with key '" << key << "' at tablet " << TabletId);
            return;
        }

        auto& ex = Exports[exportNo];
        ex.KeysToWrite.erase(key);
        Y_VERIFY(ex.Event->DstActor == ShardActor);

        if (!errStr.empty()) {
            ex.Event->Status = NKikimrProto::ERROR;
            ex.Event->ErrorStrings.emplace(key, errStr);
            Send(ShardActor, ex.Event.release());
            Exports.erase(exportNo);
        } else if (ex.KeysToWrite.empty()) {
            ex.Event->Status = NKikimrProto::OK;
            Send(ShardActor, ex.Event.release());
            Exports.erase(exportNo);
        }
    }

    void Handle(TEvS3Wrapper::TEvDeleteObjectResponse::TPtr& ev) {
        Y_VERIFY(Initialized());

        auto& msg = *ev->Get();
        const auto& resultOutcome = msg.Result;

        TString errStr;
        if (!resultOutcome.IsSuccess()) {
            errStr = LogError("DeleteObjectResponse", resultOutcome.GetError(), !!msg.Key);
        }

        Y_VERIFY(msg.Key); // FIXME
        TString key = *msg.Key;

        LOG_S_DEBUG("[S3] DeleteObjectResponse '" << key << "' at tablet " << TabletId);

        if (!ForgettingKeys.count(key)) {
            LOG_S_DEBUG("[S3] DeleteObjectResponse for unknown key '" << key << "' at tablet " << TabletId);
            return;
        }

        ui64 forgetNo = ForgettingKeys[key];
        ForgettingKeys.erase(key);

        if (!Forgets.count(forgetNo)) {
            LOG_S_DEBUG("[S3] DeleteObjectResponse for unknown forget with key '" << key << "' at tablet " << TabletId);
            return;
        }

        auto& forget = Forgets[forgetNo];
        forget.KeysToDelete.erase(key);

        if (!errStr.empty()) {
            forget.Event->Status = NKikimrProto::ERROR;
            forget.Event->ErrorStr = errStr;
            Send(ShardActor, forget.Event.release());
            Forgets.erase(forgetNo);
        } else if (forget.KeysToDelete.empty()) {
            forget.Event->Status = NKikimrProto::OK;
            Send(ShardActor, forget.Event.release());
            Forgets.erase(forgetNo);
        }
    }

    void Handle(TEvS3Wrapper::TEvGetObjectResponse::TPtr& ev) {
        Y_VERIFY(Initialized());

        auto& msg = *ev->Get();
        const auto& key = msg.Key;
        const auto& data = msg.Body;
        const auto& resultOutcome = msg.Result;

        TString errStr;
        if (!resultOutcome.IsSuccess()) {
            errStr = LogError("GetObjectResponse", resultOutcome.GetError(), !!key);
        }

        if (!key || key->empty()) {
            LOG_S_ERROR("[S3] no key in GetObjectResponse at tablet " << TabletId << ": " << errStr);
            return; // nothing to do without key
        }

        if (!ReadingKeys.count(*key)) {
            LOG_S_ERROR("[S3] no reading keys for key " << *key << " at tablet " << TabletId);
            return; // nothing to do without events
        }

        // TODO: CheckETag

        LOG_S_DEBUG("GetObjectResponse '" << *key << "', size: " << data.size() << " at tablet " << TabletId);

        auto status = errStr.empty() ? NKikimrProto::OK : NKikimrProto::ERROR;

        for (const auto& ev : ReadingKeys[*key]) {
            auto result = std::make_unique<TEvColumnShard::TEvReadBlobRangesResult>(TabletId);

            for (const auto& blobRange : ev->BlobRanges) {
                if (data.size() < blobRange.Offset + blobRange.Size) {
                    LOG_S_ERROR("GetObjectResponse '" << *key << "', data size: " << data.size()
                        << " is too small for blob range {" << blobRange.Offset << "," << blobRange.Size << "}"
                        << " at tablet " << TabletId);
                    status = NKikimrProto::ERROR;
                }

                auto* res = result->Record.AddResults();
                auto* resRange = res->MutableBlobRange();
                resRange->SetBlobId(blobRange.BlobId.ToStringNew());
                resRange->SetOffset(blobRange.Offset);
                resRange->SetSize(blobRange.Size);
                res->SetStatus(status);

                if (status == NKikimrProto::OK) {
                    res->SetData(ExtractBlobPart(blobRange, data));
                }
            }

            Send(ev->DstActor, result.release(), 0, ev->DstCookie);
        }
        ReadingKeys.erase(*key);
    }

private:
    ui64 TabletId;
    TActorId ShardActor;
    TAwsContext S3Ctx;
    TString TierName;
    TString Bucket;
    ui64 ForgetNo{};
    THashMap<ui64, TS3Export> Exports;
    THashMap<ui64, TS3Forget> Forgets;
    THashMap<TString, ui64> ExportingKeys;
    THashMap<TString, ui64> ForgettingKeys;
    THashMap<TString, std::vector<std::unique_ptr<TEvPrivate::TEvGetExported>>> ReadingKeys;

    STATEFN(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvS3Settings, Handle);
            hFunc(TEvPrivate::TEvExport, Handle);
            hFunc(TEvPrivate::TEvForget, Handle);
            hFunc(TEvPrivate::TEvGetExported, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
            hFunc(TEvS3Wrapper::TEvPutObjectResponse, Handle);
            hFunc(TEvS3Wrapper::TEvDeleteObjectResponse, Handle);
            hFunc(TEvS3Wrapper::TEvGetObjectResponse, Handle);
#if 0
            hFunc(TEvS3Wrapper::TEvHeadObjectResponse, Handle);
#endif
            default:
                break;
        }
    }

    bool Initialized() const {
        return (bool)S3Ctx.Client;
    }

    void PassAway() override {
        if (S3Ctx.Client) {
            Send(S3Ctx.Client, new TEvents::TEvPoisonPill());
            S3Ctx.Client = {};
        }
        TActor::PassAway();
    }

    void SendPutObject(const TString& key, TString&& data) const {
        auto request = Aws::S3::Model::PutObjectRequest()
            .WithBucket(Bucket)
            .WithKey(key)
            .WithStorageClass(Aws::S3::Model::StorageClass::STANDARD_IA);
#if 0
        Aws::Map<Aws::String, Aws::String> metadata;
        metadata.emplace("Content-Type", "application/x-compressed");
        request.SetMetadata(std::move(metadata));
#endif
        LOG_S_DEBUG("[S3] PutObjectRequest key '" << key << "' at tablet " << TabletId);
        Send(S3Ctx.Client, new TEvS3Wrapper::TEvPutObjectRequest(request, std::move(data)));
    }

    void SendHeadObject(const TString& key) const {
        auto request = Aws::S3::Model::HeadObjectRequest()
            .WithBucket(Bucket)
            .WithKey(key);

        LOG_S_DEBUG("[S3] HeadObjectRequest key '" << key << "' at tablet " << TabletId);
        Send(S3Ctx.Client, new TEvS3Wrapper::TEvHeadObjectRequest(request));
    }

    void SendGetObject(const TString& key, const std::pair<ui64, ui64>& range) {
        auto request = Aws::S3::Model::GetObjectRequest()
            .WithBucket(Bucket)
            .WithKey(key)
            .WithRange(TStringBuilder() << "bytes=" << range.first << "-" << range.second);

        LOG_S_DEBUG("[S3] GetObjectRequest key '" << key << "' at tablet " << TabletId);
        Send(S3Ctx.Client, new TEvS3Wrapper::TEvGetObjectRequest(request));
    }

    void SendDeleteObject(const TString& key) const {
        auto request = Aws::S3::Model::DeleteObjectRequest()
            .WithBucket(Bucket)
            .WithKey(key);

        Send(S3Ctx.Client, new TEvS3Wrapper::TEvDeleteObjectRequest(request));
    }

    TString LogError(const TString& responseType, const Aws::S3::S3Error& error, bool hasKey) const {
        TString errStr = TString(error.GetExceptionName()) + " " + error.GetMessage();
        if (errStr.empty() && !hasKey) {
            errStr = responseType + " with no key";
        }

        LOG_S_NOTICE("[S3] Error in " << responseType << " at tablet " << TabletId << ": " << errStr);
        return errStr;
    }
};

IActor* CreateS3Actor(ui64 tabletId, const TActorId& parent, const TString& tierName) {
    return new TS3Actor(tabletId, parent, tierName);
}

}

#endif
