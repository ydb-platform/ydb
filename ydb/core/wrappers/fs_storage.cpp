#include "fs_storage.h"

#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/client/AWSError.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/S3Errors.h>

#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/system/fs.h>
#include <util/random/random.h>

#include <type_traits>

namespace NKikimr::NWrappers::NExternalStorage {

#define FS_LOG(verbose, stream) \
    do { \
        if (verbose) { \
            LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::FS_WRAPPER, stream); \
        } else { \
            LOG_INFO_S(*TlsActivationContext, NKikimrServices::FS_WRAPPER, stream); \
        } \
    } while (false)

namespace {

class TFsOperationActor : public NActors::TActorBootstrapped<TFsOperationActor> {
private:
    TString BasePath;
    bool Verbose;
    const TReplyAdapterContainer& ReplyAdapter;

    struct TMultipartUploadSession {
        TString Key;
        TFile File;
        ui64 TotalSize = 0;

        TMultipartUploadSession(const TString& key)
            : Key(key)
            , File(key, CreateAlways | WrOnly | ForAppend)
        {
            File.Flock(LOCK_EX);
        }
    };

    THashMap<TString, std::unique_ptr<TMultipartUploadSession>> ActiveUploads;

    template<typename TEvResponse>
    struct RequiresKey : std::true_type {};

    template<>
    struct RequiresKey<TEvListObjectsResponse> : std::false_type {};

    template<>
    struct RequiresKey<TEvDeleteObjectsResponse> : std::false_type {};

    template<typename TEvResponse>
    static constexpr bool HasKeyConstructor() {
        return RequiresKey<TEvResponse>::value;
    }

    template<typename TEvResponse>
    void ReplySuccess(const NActors::TActorId& sender, const std::optional<TString>& key) {
        typename TEvResponse::TAwsResult awsResult;
        Aws::Utils::Outcome<typename TEvResponse::TAwsResult, Aws::S3::S3Error> outcome(std::move(awsResult));

        std::unique_ptr<TEvResponse> response;
        if constexpr (HasKeyConstructor<TEvResponse>()) {
            Y_ENSURE(key, "Key is required for this response type");
            response = std::make_unique<TEvResponse>(*key, std::move(outcome));
        } else {
            response = std::make_unique<TEvResponse>(std::move(outcome));
        }
        ReplyAdapter.Reply(sender, std::move(response));
    }

    template<typename TEvResponse>
    void ReplyError(const NActors::TActorId& sender, const std::optional<TString>& key, const TString& errorMessage) {
        Aws::Client::AWSError<Aws::S3::S3Errors> awsError(
            Aws::S3::S3Errors::INTERNAL_FAILURE,
            "FsStorageError",
            errorMessage,
            false // not retryable for FS errors
        );
        Aws::S3::S3Error error(std::move(awsError));
        Aws::Utils::Outcome<typename TEvResponse::TAwsResult, Aws::S3::S3Error> outcome(std::move(error));

        std::unique_ptr<TEvResponse> response;
        if constexpr (HasKeyConstructor<TEvResponse>()) {
            Y_ENSURE(key, "Key is required for this response type");
            response = std::make_unique<TEvResponse>(*key, std::move(outcome));
        } else {
            response = std::make_unique<TEvResponse>(std::move(outcome));
        }
        ReplyAdapter.Reply(sender, std::move(response));
    }

public:
    TFsOperationActor(const TString& basePath, bool verbose, const TReplyAdapterContainer& replyAdapter)
        : BasePath(basePath)
        , Verbose(verbose)
        , ReplyAdapter(replyAdapter)
    {
        FS_LOG(Verbose, "TFsOperationActor created: BasePath# " << BasePath);
    }

    ~TFsOperationActor() {
        CleanupActiveSessions();
    }

    void Bootstrap() {
        FS_LOG(Verbose, "TFsOperationActor Bootstrap called");
        Become(&TThis::StateWork);
    }

    void PassAway() override {
        CleanupActiveSessions();
        NActors::TActorBootstrapped<TFsOperationActor>::PassAway();
    }

private:
    void CleanupActiveSessions() {
        if (!ActiveUploads.empty()) {
            FS_LOG(Verbose, "TFsOperationActor: cleaning up " << ActiveUploads.size() << " active MPU sessions");
            for (auto& [uploadId, session] : ActiveUploads) {
                try {
                    session->File.Flush();
                    session->File.Close();
                    FS_LOG(Verbose, "TFsOperationActor: closed MPU session: uploadId# " << uploadId);
                } catch (const std::exception& ex) {
                    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FS_WRAPPER,
                        "Failed to close MPU session: uploadId# " << uploadId << ", error# " << ex.what());
                }
            }
            ActiveUploads.clear();
        }
    }

public:

    STATEFN(StateWork) {
        FS_LOG(Verbose, "TFsOperationActor StateWork received event type# " << ev->GetTypeRewrite());
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPutObjectRequest, Handle);
            hFunc(TEvGetObjectRequest, Handle);
            hFunc(TEvHeadObjectRequest, Handle);
            hFunc(TEvDeleteObjectRequest, Handle);
            hFunc(TEvCheckObjectExistsRequest, Handle);
            hFunc(TEvListObjectsRequest, Handle);
            hFunc(TEvDeleteObjectsRequest, Handle);
            hFunc(TEvCreateMultipartUploadRequest, Handle);
            hFunc(TEvUploadPartRequest, Handle);
            hFunc(TEvCompleteMultipartUploadRequest, Handle);
            hFunc(TEvAbortMultipartUploadRequest, Handle);
            hFunc(TEvUploadPartCopyRequest, Handle);
            sFunc(NActors::TEvents::TEvPoison, PassAway);
            default:
                LOG_WARN_S(*TlsActivationContext, NKikimrServices::FS_WRAPPER,
                    "TFsOperationActor StateWork received unknown event type# " << ev->GetTypeRewrite());
        }
    }

    void Handle(TEvPutObjectRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const auto& body = ev->Get()->Body;
        const TString key = TString(request.GetKey().data(), request.GetKey().size());

        FS_LOG(Verbose, "FS PutObject: key# " << key << ", size# " << body.size());

        try {
            WriteFile(key, body);
            ReplySuccess<TEvPutObjectResponse>(ev->Sender, key);
        } catch (const std::exception& ex) {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FS_WRAPPER,
                "FS PutObject failed: key# " << key << ", error# " << ex.what());
            ReplyError<TEvPutObjectResponse>(ev->Sender, key, ex.what());
        }
    }

    void Handle(TEvGetObjectRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());

        LOG_WARN_S(*TlsActivationContext, NKikimrServices::FS_WRAPPER,
            "FS GetObject: not implemented");
        ReplyError<TEvGetObjectResponse>(ev->Sender, key, "Not implemented");
    }

    void Handle(TEvHeadObjectRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());

        LOG_WARN_S(*TlsActivationContext, NKikimrServices::FS_WRAPPER,
            "FS HeadObject: not implemented");
        ReplyError<TEvHeadObjectResponse>(ev->Sender, key, "Not implemented");
    }

    void Handle(TEvDeleteObjectRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());
        LOG_WARN_S(*TlsActivationContext, NKikimrServices::FS_WRAPPER,
            "FS DeleteObject: not implemented");
        ReplyError<TEvDeleteObjectResponse>(ev->Sender, key, "Not implemented");
    }

    void Handle(TEvCheckObjectExistsRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());
        LOG_WARN_S(*TlsActivationContext, NKikimrServices::FS_WRAPPER,
            "FS CheckObjectExists: not implemented");
        ReplyError<TEvCheckObjectExistsResponse>(ev->Sender, key, "Not implemented");
    }

    void Handle(TEvListObjectsRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString prefix = TString(request.GetPrefix().data(), request.GetPrefix().size());
        LOG_WARN_S(*TlsActivationContext, NKikimrServices::FS_WRAPPER,
            "FS ListObjects: prefix# " << prefix << ", not implemented");
        ReplyError  <TEvListObjectsResponse>(ev->Sender, std::nullopt, "Not implemented");
    }

    void Handle(TEvDeleteObjectsRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        LOG_WARN_S(*TlsActivationContext, NKikimrServices::FS_WRAPPER,
            "FS DeleteObjects: not implemented, objects count# " << request.GetDelete().GetObjects().size());
        ReplyError<TEvDeleteObjectsResponse>(ev->Sender, std::nullopt, "Not implemented");
    }

    void Handle(TEvCreateMultipartUploadRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());

        try {
            TString uploadId = TStringBuilder() << key << "_" << RandomNumber<ui64>();
            TFsPath fsPath(key);
            fsPath.Parent().MkDirs();

            auto session = std::make_unique<TMultipartUploadSession>(key);
            ActiveUploads[uploadId] = std::move(session);

            FS_LOG(Verbose, "FS CreateMultipartUpload: key# " << key << ", uploadId# " << uploadId
                << ", file opened with exclusive lock");

            Aws::S3::Model::CreateMultipartUploadResult awsResult;
            awsResult.SetKey(request.GetKey());
            awsResult.SetUploadId(uploadId.c_str());

            Aws::Utils::Outcome<Aws::S3::Model::CreateMultipartUploadResult, Aws::S3::S3Error> outcome(std::move(awsResult));
            auto response = std::make_unique<TEvCreateMultipartUploadResponse>(key, std::move(outcome));
            ReplyAdapter.Reply(ev->Sender, std::move(response));
        } catch (const std::exception& ex) {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FS_WRAPPER,
                "FS CreateMultipartUpload failed: key# " << key << ", error# " << ex.what());
            ReplyError<TEvCreateMultipartUploadResponse>(ev->Sender, key, ex.what());
        }
    }

    void Handle(TEvUploadPartRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const auto& body = ev->Get()->Body;
        const TString key = TString(request.GetKey().data(), request.GetKey().size());
        const TString uploadId = TString(request.GetUploadId().data(), request.GetUploadId().size());
        const int partNumber = request.GetPartNumber();

        FS_LOG(Verbose, "FS UploadPart: key# " << key << ", uploadId# " << uploadId
            << ", part# " << partNumber << ", size# " << body.size());

        try {
            auto it = ActiveUploads.find(uploadId);
            if (it == ActiveUploads.end()) {
                throw yexception() << "Upload session not found: uploadId# " << uploadId;
            }

            auto& session = it->second;

            session->File.Write(body.data(), body.size());
            session->TotalSize += body.size();

            FS_LOG(Verbose, "FS UploadPart written under lock: uploadId# " << uploadId
                << ", part# " << partNumber << ", total size# " << session->TotalSize);

            TString etag = TStringBuilder() << "\"part" << partNumber << "\"";

            Aws::S3::Model::UploadPartResult awsResult;
            awsResult.SetETag(etag.c_str());

            Aws::Utils::Outcome<Aws::S3::Model::UploadPartResult, Aws::S3::S3Error> outcome(std::move(awsResult));
            auto response = std::make_unique<TEvUploadPartResponse>(key, std::move(outcome));
            ReplyAdapter.Reply(ev->Sender, std::move(response));
        } catch (const std::exception& ex) {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FS_WRAPPER,
                "FS UploadPart failed: key# " << key << ", uploadId# " << uploadId << ", error# " << ex.what());
            ReplyError<TEvUploadPartResponse>(ev->Sender, key, ex.what());
        }
    }

    void Handle(TEvCompleteMultipartUploadRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());
        const TString uploadId = TString(request.GetUploadId().data(), request.GetUploadId().size());

        FS_LOG(Verbose, "FS CompleteMultipartUpload: key# " << key << ", uploadId# " << uploadId);

        try {
            auto it = ActiveUploads.find(uploadId);
            if (it == ActiveUploads.end()) {
                throw yexception() << "Upload session not found: uploadId# " << uploadId;
            }

            auto& session = it->second;

            session->File.Flush();
            session->File.Close();

            FS_LOG(Verbose, "FS CompleteMultipartUpload: uploadId# " << uploadId
                << ", total size# " << session->TotalSize << ", file closed, lock released");

            ActiveUploads.erase(it);

            Aws::S3::Model::CompleteMultipartUploadResult awsResult;
            awsResult.SetKey(request.GetKey());
            TString etag = "\"completed\"";
            awsResult.SetETag(etag.c_str());

            Aws::Utils::Outcome<Aws::S3::Model::CompleteMultipartUploadResult, Aws::S3::S3Error> outcome(std::move(awsResult));
            auto response = std::make_unique<TEvCompleteMultipartUploadResponse>(key, std::move(outcome));
            ReplyAdapter.Reply(ev->Sender, std::move(response));
        } catch (const std::exception& ex) {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FS_WRAPPER,
                "FS CompleteMultipartUpload failed: key# " << key << ", uploadId# " << uploadId << ", error# " << ex.what());
            ReplyError<TEvCompleteMultipartUploadResponse>(ev->Sender, key, ex.what());
        }
    }

    void Handle(TEvAbortMultipartUploadRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());
        const TString uploadId = TString(request.GetUploadId().data(), request.GetUploadId().size());

        FS_LOG(Verbose, "FS AbortMultipartUpload: key# " << key << ", uploadId# " << uploadId);

        try {
            auto it = ActiveUploads.find(uploadId);
            if (it == ActiveUploads.end()) {
                FS_LOG(Verbose, "FS AbortMultipartUpload: session not found (already closed?): uploadId# " << uploadId);
            } else {
                auto& session = it->second;
                TString filePath = session->Key;
                session->File.Close();
                ActiveUploads.erase(it);
                NFs::Remove(filePath);

                FS_LOG(Verbose, "FS AbortMultipartUpload: uploadId# " << uploadId
                    << ", file deleted, lock released");
            }

            ReplySuccess<TEvAbortMultipartUploadResponse>(ev->Sender, key);
        } catch (const std::exception& ex) {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FS_WRAPPER,
                "FS AbortMultipartUpload failed: key# " << key << ", uploadId# " << uploadId << ", error# " << ex.what());
            ReplyError<TEvAbortMultipartUploadResponse>(ev->Sender, key, ex.what());
        }
    }

    void Handle(TEvUploadPartCopyRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());

        LOG_WARN_S(*TlsActivationContext, NKikimrServices::FS_WRAPPER,
            "FS UploadPartCopy: not implemented");
        ReplyError<TEvUploadPartCopyResponse>(ev->Sender, key, "Not implemented");
    }

private:
    void WriteFile(const TString& path, const TStringBuf& data, bool isAppend = false) {
        TFsPath fsPath(path);
        fsPath.Parent().MkDirs();

        auto flags = CreateAlways | WrOnly;
        if (isAppend) {
            flags = OpenAlways | WrOnly | ForAppend;
        }
        TFile file(path, flags);
        file.Flock(LOCK_EX);
        file.Write(data.data(), data.size());
        file.Flush();
        file.Close();
    }
};

} // anonymous namespace

TFsExternalStorage::TFsExternalStorage(const TString& basePath, bool verbose)
    : BasePath(basePath)
    , Verbose(verbose)
{
    InitReplyAdapter(nullptr);
    LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::FS_WRAPPER,
        "TFsExternalStorage created: BasePath# " << BasePath << ", Verbose# " << Verbose);
}

TFsExternalStorage::~TFsExternalStorage()
{
    Shutdown();
}

void TFsExternalStorage::EnsureActor() const {
    if (ActorCreated) {
        return;
    }

    auto actor = new TFsOperationActor(BasePath, Verbose, ReplyAdapter);
    OperationActorId = TlsActivationContext->AsActorContext().Register(
        actor, TMailboxType::HTSwap, AppData()->IOPoolId);
    ActorCreated = true;

    FS_LOG(Verbose, "TFsExternalStorage: Created persistent actor# " << OperationActorId);
}

void TFsExternalStorage::Shutdown() {
    if (ActorCreated && TlsActivationContext) {
        FS_LOG(Verbose, "TFsExternalStorage: Shutting down actor# " << OperationActorId);
        TlsActivationContext->AsActorContext().Send(OperationActorId, new NActors::TEvents::TEvPoison());
        ActorCreated = false;
    }
}

void TFsExternalStorage::Execute(TEvPutObjectRequest::TPtr& ev) const {
    EnsureActor();
    TlsActivationContext->AsActorContext().Send(ev->Forward(OperationActorId));
}

void TFsExternalStorage::Execute(TEvGetObjectRequest::TPtr& ev) const {
    EnsureActor();
    TlsActivationContext->AsActorContext().Send(ev->Forward(OperationActorId));
}

void TFsExternalStorage::Execute(TEvHeadObjectRequest::TPtr& ev) const {
    EnsureActor();
    TlsActivationContext->AsActorContext().Send(ev->Forward(OperationActorId));
}

void TFsExternalStorage::Execute(TEvDeleteObjectRequest::TPtr& ev) const {
    EnsureActor();
    TlsActivationContext->AsActorContext().Send(ev->Forward(OperationActorId));
}

void TFsExternalStorage::Execute(TEvCheckObjectExistsRequest::TPtr& ev) const {
    EnsureActor();
    TlsActivationContext->AsActorContext().Send(ev->Forward(OperationActorId));
}

void TFsExternalStorage::Execute(TEvListObjectsRequest::TPtr& ev) const {
    EnsureActor();
    TlsActivationContext->AsActorContext().Send(ev->Forward(OperationActorId));
}

void TFsExternalStorage::Execute(TEvDeleteObjectsRequest::TPtr& ev) const {
    EnsureActor();
    TlsActivationContext->AsActorContext().Send(ev->Forward(OperationActorId));
}

void TFsExternalStorage::Execute(TEvCreateMultipartUploadRequest::TPtr& ev) const {
    EnsureActor();
    TlsActivationContext->AsActorContext().Send(ev->Forward(OperationActorId));
}

void TFsExternalStorage::Execute(TEvUploadPartRequest::TPtr& ev) const {
    EnsureActor();
    TlsActivationContext->AsActorContext().Send(ev->Forward(OperationActorId));
}

void TFsExternalStorage::Execute(TEvCompleteMultipartUploadRequest::TPtr& ev) const {
    EnsureActor();
    TlsActivationContext->AsActorContext().Send(ev->Forward(OperationActorId));
}

void TFsExternalStorage::Execute(TEvAbortMultipartUploadRequest::TPtr& ev) const {
    EnsureActor();
    TlsActivationContext->AsActorContext().Send(ev->Forward(OperationActorId));
}

void TFsExternalStorage::Execute(TEvUploadPartCopyRequest::TPtr& ev) const {
    EnsureActor();
    TlsActivationContext->AsActorContext().Send(ev->Forward(OperationActorId));
}

#undef FS_LOG

} // NKikimr::NWrappers::NExternalStorage
