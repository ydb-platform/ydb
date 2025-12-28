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
#include <util/generic/guid.h>

#include <type_traits>

namespace NKikimr::NWrappers::NExternalStorage {

#define FS_LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FS_WRAPPER, stream)
#define FS_LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::FS_WRAPPER, stream)
#define FS_LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FS_WRAPPER, stream)
#define FS_LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::FS_WRAPPER, stream)
#define FS_LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FS_WRAPPER, stream)

namespace {

class TFsOperationActor : public TActorBootstrapped<TFsOperationActor> {
private:
    const TString BasePath;

    struct TMultipartUploadSession {
        const TString Key;
        TFile File;
        ui64 TotalSize = 0;

        explicit TMultipartUploadSession(const TString& key)
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
        this->Send(sender, response.release());
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
        this->Send(sender, response.release());
    }

public:
    TFsOperationActor(const TString& basePath)
        : BasePath(basePath)
    {
    }

    ~TFsOperationActor() {
        CleanupActiveSessions();
    }

    void Bootstrap() {
        FS_LOG_T("TFsOperationActor Bootstrap called");
        Become(&TThis::StateWork);
    }

    void PassAway() override {
        CleanupActiveSessions();
        NActors::TActorBootstrapped<TFsOperationActor>::PassAway();
    }

private:
    void CleanupActiveSessions() {
        FS_LOG_T("TFsOperationActor: cleaning up"
            << ": active MPU sessions# " << ActiveUploads.size());
        for (auto& [uploadId, session] : ActiveUploads) {
            try {
                const TString filePath = session->Key;
                session->File.Close();
                NFs::Remove(filePath);

                FS_LOG_T("TFsOperationActor: closed and deleted incomplete file"
                    << ": uploadId# " << uploadId
                    << ", file# " << filePath);
            } catch (const std::exception& ex) {
                FS_LOG_W("Failed to cleanup MPU session"
                    << ": uploadId# " << uploadId
                    << ", error# " << ex.what());
            }
        }
        ActiveUploads.clear();
    }

public:

    STATEFN(StateWork) {
        FS_LOG_D("TFsOperationActor StateWork received event type"
            << ": type# " << ev->GetTypeRewrite());
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
                FS_LOG_W("TFsOperationActor StateWork received unknown event type"
                    << ": type# " << ev->GetTypeRewrite());
        }
    }

    TString GetIncompletePath(const TString& key) {
        return TStringBuilder() << key << ".incomplete";
    }

    void Handle(TEvPutObjectRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const auto& body = ev->Get()->Body;
        const TString key = TString(request.GetKey().data(), request.GetKey().size());

        FS_LOG_D("PutObject"
            << ": key# " << key
            << ", size# " << body.size());

        try {
            TFsPath fsPath(key);
            fsPath.Parent().MkDirs();

            TFile file(fsPath.GetPath(), CreateAlways | WrOnly);
            file.Flock(LOCK_EX);
            file.Write(body.data(), body.size());
            file.Flush();
            file.Close();
            ReplySuccess<TEvPutObjectResponse>(ev->Sender, key);
        } catch (const std::exception& ex) {
            FS_LOG_E("PutObject failed"
                << ": key# " << key
                << ", error# " << ex.what());
            ReplyError<TEvPutObjectResponse>(ev->Sender, key, ex.what());
        }
    }

    void Handle(TEvGetObjectRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());

        FS_LOG_W("GetObject: not implemented");
        ReplyError<TEvGetObjectResponse>(ev->Sender, key, "Not implemented");
    }

    void Handle(TEvHeadObjectRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());

        FS_LOG_W("HeadObject: not implemented");
        ReplyError<TEvHeadObjectResponse>(ev->Sender, key, "Not implemented");
    }

    void Handle(TEvDeleteObjectRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());
        FS_LOG_W("DeleteObject: not implemented");
        ReplyError<TEvDeleteObjectResponse>(ev->Sender, key, "Not implemented");
    }

    void Handle(TEvCheckObjectExistsRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());
        FS_LOG_W("CheckObjectExists: not implemented");
        ReplyError<TEvCheckObjectExistsResponse>(ev->Sender, key, "Not implemented");
    }

    void Handle(TEvListObjectsRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString prefix = TString(request.GetPrefix().data(), request.GetPrefix().size());
        FS_LOG_W("ListObjects"
            << ": prefix# " << prefix
            << ", not implemented");
        ReplyError<TEvListObjectsResponse>(ev->Sender, std::nullopt, "Not implemented");
    }

    void Handle(TEvDeleteObjectsRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        FS_LOG_W("DeleteObjects: not implemented, objects count# " << request.GetDelete().GetObjects().size());
        ReplyError<TEvDeleteObjectsResponse>(ev->Sender, std::nullopt, "Not implemented");
    }

    void Handle(TEvCreateMultipartUploadRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = GetIncompletePath(TString(request.GetKey().data(), request.GetKey().size()));

        FS_LOG_D("CreateMultipartUpload"
            << ": key# " << key);

        try {
            const TString uploadId = TStringBuilder() << key << "_" << CreateGuidAsString();
            TFsPath fsPath(key);
            fsPath.Parent().MkDirs();

            auto session = std::make_unique<TMultipartUploadSession>(key);
            ActiveUploads[uploadId] = std::move(session);

            FS_LOG_I("CreateMultipartUpload"
                << ": key# " << key
                << ", uploadId# " << uploadId
                << ", file opened with exclusive lock");

            Aws::S3::Model::CreateMultipartUploadResult awsResult;
            awsResult.SetKey(request.GetKey());
            awsResult.SetUploadId(uploadId.c_str());

            Aws::Utils::Outcome<Aws::S3::Model::CreateMultipartUploadResult, Aws::S3::S3Error> outcome(std::move(awsResult));
            auto response = std::make_unique<TEvCreateMultipartUploadResponse>(key, std::move(outcome));
            this->Send(ev->Sender, response.release());
        } catch (const std::exception& ex) {
            FS_LOG_E("CreateMultipartUpload failed"
                << ": key# " << key
                << ", error# " << ex.what());
            ReplyError<TEvCreateMultipartUploadResponse>(ev->Sender, key, ex.what());
        }
    }

    void Handle(TEvUploadPartRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const auto& body = ev->Get()->Body;
        const TString key = GetIncompletePath(TString(request.GetKey().data(), request.GetKey().size()));
        const TString uploadId = TString(request.GetUploadId().data(), request.GetUploadId().size());
        const int partNumber = request.GetPartNumber();

        FS_LOG_D("UploadPart"
            << ": key# " << key
            << ", uploadId# " << uploadId
            << ", part# " << partNumber
            << ", size# " << body.size());

        try {
            auto it = ActiveUploads.find(uploadId);
            if (it == ActiveUploads.end()) {
                // If the UploadId is not found in ActiveUploads,
                // it means that a restart has occurred and all parts have started being written again
                // so we can simply create a new session.
                if (partNumber != 1) {
                    throw yexception() << TStringBuilder()
                        << "Cannot create new upload session for part " << partNumber
                        << " (uploadId: " << uploadId << "). Session must start with part 1.";
                }
                it = ActiveUploads.emplace(uploadId, std::make_unique<TMultipartUploadSession>(key)).first;
            }

            auto& session = it->second;

            session->File.Write(body.data(), body.size());
            session->TotalSize += body.size();

            FS_LOG_I("UploadPart: written under lock"
                << ": uploadId# " << uploadId
                << ", part# " << partNumber
                << ", total size# " << session->TotalSize);

            const TString etag = TStringBuilder() << "\"part" << partNumber << "\"";

            Aws::S3::Model::UploadPartResult awsResult;
            awsResult.SetETag(etag.c_str());

            Aws::Utils::Outcome<Aws::S3::Model::UploadPartResult, Aws::S3::S3Error> outcome(std::move(awsResult));
            auto response = std::make_unique<TEvUploadPartResponse>(key, std::move(outcome));
            this->Send(ev->Sender, response.release());
        } catch (const std::exception& ex) {
            FS_LOG_E("UploadPart failed"
                << ": key# " << key
                << ", uploadId# " << uploadId
                << ", error# " << ex.what());
            ReplyError<TEvUploadPartResponse>(ev->Sender, key, ex.what());
        }
    }

    void Handle(TEvCompleteMultipartUploadRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());
        const TString incompleteKey = GetIncompletePath(key);
        const TString uploadId = TString(request.GetUploadId().data(), request.GetUploadId().size());

        FS_LOG_D("CompleteMultipartUpload"
            << ": key# " << key
            << ", uploadId# " << uploadId);

        try {
            auto it = ActiveUploads.find(uploadId);
            if (it == ActiveUploads.end()) {
                // Upload session not found - likely due to actor restart
                // Return retryable error to force datashard to retry with cleared uploadId
                Aws::Client::AWSError<Aws::S3::S3Errors> awsError(
                    Aws::S3::S3Errors::INTERNAL_FAILURE,
                    "OperationAborted",
                    TStringBuilder() << "Upload session not found: uploadId# " << uploadId,
                    true // retryable
                );
                Aws::S3::S3Error error(std::move(awsError));
                Aws::Utils::Outcome<Aws::S3::Model::CompleteMultipartUploadResult, Aws::S3::S3Error> outcome(std::move(error));
                auto response = std::make_unique<TEvCompleteMultipartUploadResponse>(key, std::move(outcome));
                this->Send(ev->Sender, response.release());
                return;
            }

            auto& session = it->second;
            session->File.Flush();
            session->File.Close();

            NFs::Rename(incompleteKey, key);

            FS_LOG_T("CompleteMultipartUpload"
                << ": uploadId# " << uploadId
                << ", total size# " << session->TotalSize
                << ", file mv from# " << incompleteKey << " to# " << key);

            ActiveUploads.erase(it);

            Aws::S3::Model::CompleteMultipartUploadResult awsResult;
            awsResult.SetKey(request.GetKey());
            const TString etag = "\"completed\"";
            awsResult.SetETag(etag.c_str());

            Aws::Utils::Outcome<Aws::S3::Model::CompleteMultipartUploadResult, Aws::S3::S3Error> outcome(std::move(awsResult));
            auto response = std::make_unique<TEvCompleteMultipartUploadResponse>(key, std::move(outcome));
            this->Send(ev->Sender, response.release());
        } catch (const std::exception& ex) {
            FS_LOG_E("CompleteMultipartUpload failed: key# " << key << ", uploadId# " << uploadId << ", error# " << ex.what());
            ReplyError<TEvCompleteMultipartUploadResponse>(ev->Sender, key, ex.what());
        }
    }

    void Handle(TEvAbortMultipartUploadRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());
        const TString uploadId = TString(request.GetUploadId().data(), request.GetUploadId().size());

        FS_LOG_D("AbortMultipartUpload"
            << ": key# " << key
            << ", uploadId# " << uploadId);

        try {
            auto it = ActiveUploads.find(uploadId);
            if (it == ActiveUploads.end()) {
                FS_LOG_W("AbortMultipartUpload"
                    << ": session not found"
                    << ": uploadId# " << uploadId);
            } else {
                auto& session = it->second;
                const TString filePath = session->Key;
                session->File.Close();
                ActiveUploads.erase(it);
                NFs::Remove(filePath);

                FS_LOG_I("AbortMultipartUpload"
                    << ": uploadId# " << uploadId
                    << ", file deleted, lock released");
            }

            ReplySuccess<TEvAbortMultipartUploadResponse>(ev->Sender, key);
        } catch (const std::exception& ex) {
            FS_LOG_E("AbortMultipartUpload failed"
                << ": key# " << key
                << ", uploadId# " << uploadId
                << ", error# " << ex.what());
            ReplyError<TEvAbortMultipartUploadResponse>(ev->Sender, key, ex.what());
        }
    }

    void Handle(TEvUploadPartCopyRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());

        FS_LOG_W("UploadPartCopy: not implemented");
        ReplyError<TEvUploadPartCopyResponse>(ev->Sender, key, "Not implemented");
    }
};

} // anonymous namespace

TFsExternalStorage::TFsExternalStorage(const TString& basePath)
    : BasePath(basePath)
{
}

TFsExternalStorage::~TFsExternalStorage() {
    Shutdown();
}

void TFsExternalStorage::EnsureActor() const {
    if (ActorCreated) {
        return;
    }

    auto actor = new TFsOperationActor(BasePath);
    OperationActorId = TlsActivationContext->AsActorContext().Register(
        actor, TMailboxType::HTSwap, AppData()->IOPoolId);
    ActorCreated = true;

    FS_LOG_I("TFsExternalStorage: Created persistent actor"
        << ": OperationActorId# " << OperationActorId);
}

void TFsExternalStorage::Shutdown() {
    if (ActorCreated && TlsActivationContext) {
        FS_LOG_I("TFsExternalStorage: Shutting down actor"
            << ": OperationActorId# " << OperationActorId);
        TlsActivationContext->AsActorContext().Send(OperationActorId, new NActors::TEvents::TEvPoison());
        ActorCreated = false;
    }
}

template <typename TEvPtr>
void TFsExternalStorage::ExecuteImpl(TEvPtr& ev) const {
    EnsureActor();
    TlsActivationContext->AsActorContext().Send(ev->Forward(OperationActorId));
}

void TFsExternalStorage::Execute(TEvPutObjectRequest::TPtr& ev) const {
    ExecuteImpl(ev);
}

void TFsExternalStorage::Execute(TEvGetObjectRequest::TPtr& ev) const {
    ExecuteImpl(ev);
}

void TFsExternalStorage::Execute(TEvHeadObjectRequest::TPtr& ev) const {
    ExecuteImpl(ev);
}

void TFsExternalStorage::Execute(TEvDeleteObjectRequest::TPtr& ev) const {
    ExecuteImpl(ev);
}

void TFsExternalStorage::Execute(TEvCheckObjectExistsRequest::TPtr& ev) const {
    ExecuteImpl(ev);
}

void TFsExternalStorage::Execute(TEvListObjectsRequest::TPtr& ev) const {
    ExecuteImpl(ev);
}

void TFsExternalStorage::Execute(TEvDeleteObjectsRequest::TPtr& ev) const {
    ExecuteImpl(ev);
}

void TFsExternalStorage::Execute(TEvCreateMultipartUploadRequest::TPtr& ev) const {
    ExecuteImpl(ev);
}

void TFsExternalStorage::Execute(TEvUploadPartRequest::TPtr& ev) const {
    ExecuteImpl(ev);
}

void TFsExternalStorage::Execute(TEvCompleteMultipartUploadRequest::TPtr& ev) const {
    ExecuteImpl(ev);
}

void TFsExternalStorage::Execute(TEvAbortMultipartUploadRequest::TPtr& ev) const {
    ExecuteImpl(ev);
}

void TFsExternalStorage::Execute(TEvUploadPartCopyRequest::TPtr& ev) const {
    ExecuteImpl(ev);
}

} // NKikimr::NWrappers::NExternalStorage
