#include "fs_storage.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/base/appdata.h>

#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/system/fs.h>
#include <util/folder/dirut.h>

namespace NKikimr::NWrappers::NExternalStorage {

namespace {

class TFsOperationActor : public NActors::TActorBootstrapped<TFsOperationActor> {
private:
    TString BasePath;
    bool Verbose;
    const TReplyAdapterContainer& ReplyAdapter;

    TString MakeFullPath(const TString& key) const {
        TFsPath basePath(BasePath);
        TFsPath fullPath = basePath / key;
        return fullPath.GetPath();
    }

    void EnsureDirectory(const TString& filePath) {
        size_t pos = filePath.find_last_of('/');
        if (pos != TString::npos) {
            TString dirPath = filePath.substr(0, pos);
            if (!dirPath.empty()) {
                MakePathIfNotExist(dirPath.c_str());
            }
        }
    }

    template<typename TEvResponse>
    void ReplySuccess(const NActors::TActorId& sender, const TString& key) {
        typename TEvResponse::TAwsResult awsResult;
        Aws::Utils::Outcome<typename TEvResponse::TAwsResult, Aws::S3::S3Error> outcome(std::move(awsResult));
        
        auto response = std::make_unique<TEvResponse>(key, std::move(outcome));
        ReplyAdapter.Reply(sender, std::move(response));
    }

    template<typename TEvResponse>
    void ReplyError(const NActors::TActorId& sender, const TString& key, const TString& errorMessage) {
        Y_UNUSED(errorMessage);
        Aws::Utils::Outcome<typename TEvResponse::TAwsResult, Aws::S3::S3Error> outcome;
        
        auto response = std::make_unique<TEvResponse>(key, std::move(outcome));
        ReplyAdapter.Reply(sender, std::move(response));
    }

public:
    TFsOperationActor(const TString& basePath, bool verbose, const TReplyAdapterContainer& replyAdapter)
        : BasePath(basePath)
        , Verbose(verbose)
        , ReplyAdapter(replyAdapter)
    {
        if (Verbose) {
            LOG_INFO_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                "TFsOperationActor created: BasePath# " << BasePath);
        }
    }

    void Bootstrap() {
        if (Verbose) {
            LOG_INFO_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                "TFsOperationActor Bootstrap called");
        }
        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        if (Verbose) {
            LOG_INFO_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                "TFsOperationActor StateWork received event type# " << ev->GetTypeRewrite());
        }
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
                LOG_WARN_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                    "TFsOperationActor StateWork received unknown event type# " << ev->GetTypeRewrite());
        }
    }

    void Handle(TEvPutObjectRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const auto& body = ev->Get()->Body;
        const TString key = TString(request.GetKey().data(), request.GetKey().size());

        LOG_ERROR_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
            "FS PutObject START: key# " << key << ", size# " << body.size());

        if (Verbose) {
            LOG_INFO_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                "FS PutObject: key# " << key << ", size# " << body.size());
        }

        try {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                "FS PutObject: creating TFsPath for key# " << key);
            TFsPath fsPath(key);
            
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                "FS PutObject: fsPath.GetPath()# " << fsPath.GetPath() << ", Parent()# " << fsPath.Parent().GetPath());
            
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                "FS PutObject: calling MkDirs() on parent# " << fsPath.Parent().GetPath());
            fsPath.Parent().MkDirs();

            auto flags = CreateAlways | WrOnly;
            // if (isAppend) {
            //     flags = OpenAlways | WrOnly | ForAppend;
            // }
            TFile file(key, flags);
            file.Flock(LOCK_EX);
            file.Write(body.data(), body.size());
            file.Close();

            ReplySuccess<TEvPutObjectResponse>(ev->Sender, key);
        } catch (const std::exception& ex) {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                "FS PutObject failed: key# " << key << ", error# " << ex.what());
            ReplyError<TEvPutObjectResponse>(ev->Sender, key, ex.what());
        }
    }

    void Handle(TEvGetObjectRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());
        
        if (Verbose) {
            LOG_INFO_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                "FS GetObject: key# " << key);
        }

        try {
            TString fullPath = MakeFullPath(key);
            
            if (!TFsPath(fullPath).Exists()) {
                std::pair<ui64, ui64> range(0, 0);
                Aws::Utils::Outcome<Aws::S3::Model::GetObjectResult, Aws::S3::S3Error> outcome;
                TString emptyBody;
                auto response = std::make_unique<TEvGetObjectResponse>(key, range, std::move(outcome), std::move(emptyBody));
                ReplyAdapter.Reply(ev->Sender, std::move(response));
                return;
            }

            TFileInput file(fullPath);
            TString body = file.ReadAll();

            Aws::S3::Model::GetObjectResult awsResult;
            Aws::Utils::Outcome<Aws::S3::Model::GetObjectResult, Aws::S3::S3Error> outcome(std::move(awsResult));
            
            std::pair<ui64, ui64> range(0, body.size() > 0 ? body.size() - 1 : 0);
            auto response = std::make_unique<TEvGetObjectResponse>(key, range, std::move(outcome), std::move(body));
            ReplyAdapter.Reply(ev->Sender, std::move(response));

        } catch (const std::exception& ex) {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                "FS GetObject failed: key# " << key << ", error# " << ex.what());
            std::pair<ui64, ui64> range(0, 0);
            Aws::Utils::Outcome<Aws::S3::Model::GetObjectResult, Aws::S3::S3Error> outcome;
            TString emptyBody;
            auto response = std::make_unique<TEvGetObjectResponse>(key, range, std::move(outcome), std::move(emptyBody));
            ReplyAdapter.Reply(ev->Sender, std::move(response));
        }
    }

    void Handle(TEvHeadObjectRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());
        
        if (Verbose) {
            LOG_INFO_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                "FS HeadObject: key# " << key);
        }

        try {
            TString fullPath = MakeFullPath(key);
            
            if (!TFsPath(fullPath).Exists()) {
                ReplyError<TEvHeadObjectResponse>(ev->Sender, key, "File not found");
                return;
            }

            ReplySuccess<TEvHeadObjectResponse>(ev->Sender, key);
        } catch (const std::exception& ex) {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                "FS HeadObject failed: key# " << key << ", error# " << ex.what());
            ReplyError<TEvHeadObjectResponse>(ev->Sender, key, ex.what());
        }
    }

    void Handle(TEvDeleteObjectRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());
        
        if (Verbose) {
            LOG_INFO_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                "FS DeleteObject: key# " << key);
        }

        try {
            TString fullPath = MakeFullPath(key);
            
            if (TFsPath(fullPath).Exists()) {
                NFs::Remove(fullPath);
            }

            ReplySuccess<TEvDeleteObjectResponse>(ev->Sender, key);
        } catch (const std::exception& ex) {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                "FS DeleteObject failed: key# " << key << ", error# " << ex.what());
            ReplyError<TEvDeleteObjectResponse>(ev->Sender, key, ex.what());
        }
    }

    void Handle(TEvCheckObjectExistsRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());
        
        if (Verbose) {
            LOG_INFO_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                "FS CheckObjectExists: key# " << key);
        }

        try {
            TString fullPath = MakeFullPath(key);
            
            if (!TFsPath(fullPath).Exists()) {
                ReplyError<TEvCheckObjectExistsResponse>(ev->Sender, key, "File not found");
                return;
            }

            ReplySuccess<TEvCheckObjectExistsResponse>(ev->Sender, key);
        } catch (const std::exception& ex) {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                "FS CheckObjectExists failed: key# " << key << ", error# " << ex.what());
            ReplyError<TEvCheckObjectExistsResponse>(ev->Sender, key, ex.what());
        }
    }

    void Handle(TEvListObjectsRequest::TPtr& ev) {
        Y_UNUSED(ev);
        LOG_WARN_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
            "FS ListObjects: not implemented yet");
    }

    void Handle(TEvDeleteObjectsRequest::TPtr& ev) {
        Y_UNUSED(ev);
        LOG_WARN_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
            "FS DeleteObjects: not implemented yet");
    }

    void Handle(TEvCreateMultipartUploadRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());
        
        if (Verbose) {
            LOG_INFO_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                "FS CreateMultipartUpload: key# " << key);
        }

        try {
            TString uploadId = TStringBuilder() << key << "_" << TInstant::Now().MicroSeconds();
            
            if (Verbose) {
                LOG_INFO_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                    "FS CreateMultipartUpload: generated uploadId# " << uploadId);
            }
            
            Aws::S3::Model::CreateMultipartUploadResult awsResult;
            awsResult.SetUploadId(uploadId.c_str());
            awsResult.SetKey(request.GetKey());
            
            Aws::Utils::Outcome<Aws::S3::Model::CreateMultipartUploadResult, Aws::S3::S3Error> outcome(std::move(awsResult));
            auto response = std::make_unique<TEvCreateMultipartUploadResponse>(key, std::move(outcome));
            ReplyAdapter.Reply(ev->Sender, std::move(response));
        } catch (const std::exception& ex) {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                "FS CreateMultipartUpload failed: key# " << key << ", error# " << ex.what());
            ReplyError<TEvCreateMultipartUploadResponse>(ev->Sender, key, ex.what());
        }
    }

    void Handle(TEvUploadPartRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const auto& body = ev->Get()->Body;
        const TString key = TString(request.GetKey().data(), request.GetKey().size());
        const int partNumber = request.GetPartNumber();
        
        if (Verbose) {
            LOG_INFO_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                "FS UploadPart: key# " << key << ", part# " << partNumber << ", size# " << body.size());
        }

        try {
            TString partPath = TStringBuilder() << MakeFullPath(key) << ".part" << partNumber;
            EnsureDirectory(partPath);
            
            TFileOutput file(partPath);
            file.Write(body.data(), body.size());
            file.Finish();

            TString etag = TStringBuilder() << "\"part" << partNumber << "\"";
            
            Aws::S3::Model::UploadPartResult awsResult;
            awsResult.SetETag(etag.c_str());
            
            Aws::Utils::Outcome<Aws::S3::Model::UploadPartResult, Aws::S3::S3Error> outcome(std::move(awsResult));
            auto response = std::make_unique<TEvUploadPartResponse>(key, std::move(outcome));
            ReplyAdapter.Reply(ev->Sender, std::move(response));
        } catch (const std::exception& ex) {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                "FS UploadPart failed: key# " << key << ", error# " << ex.what());
            ReplyError<TEvUploadPartResponse>(ev->Sender, key, ex.what());
        }
    }

    void Handle(TEvCompleteMultipartUploadRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());
        const TString uploadId = TString(request.GetUploadId().data(), request.GetUploadId().size());
        
        if (Verbose) {
            LOG_INFO_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                "FS CompleteMultipartUpload: key# " << key << ", uploadId# " << uploadId);
        }

        try {
            TString finalPath = MakeFullPath(key);
            EnsureDirectory(finalPath);
            TFileOutput finalFile(finalPath);

            int partNumber = 1;
            int totalParts = 0;
            while (true) {
                TString partPath = TStringBuilder() << finalPath << ".part" << partNumber;
                if (!TFsPath(partPath).Exists()) {
                    break;
                }

                TFileInput partFile(partPath);
                TString partData = partFile.ReadAll();
                finalFile.Write(partData.data(), partData.size());

                if (Verbose) {
                    LOG_INFO_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                        "FS CompleteMultipartUpload: merged part# " << partNumber << ", size# " << partData.size());
                }

                NFs::Remove(partPath);
                ++partNumber;
                ++totalParts;
            }

            finalFile.Finish();
            
            if (Verbose) {
                LOG_INFO_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                    "FS CompleteMultipartUpload: completed, total parts# " << totalParts);
            }
            
            Aws::S3::Model::CompleteMultipartUploadResult awsResult;
            awsResult.SetKey(request.GetKey());
            TString etag = "\"completed\"";
            awsResult.SetETag(etag.c_str());
            
            Aws::Utils::Outcome<Aws::S3::Model::CompleteMultipartUploadResult, Aws::S3::S3Error> outcome(std::move(awsResult));
            auto response = std::make_unique<TEvCompleteMultipartUploadResponse>(key, std::move(outcome));
            ReplyAdapter.Reply(ev->Sender, std::move(response));
        } catch (const std::exception& ex) {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                "FS CompleteMultipartUpload failed: key# " << key << ", error# " << ex.what());
            ReplyError<TEvCompleteMultipartUploadResponse>(ev->Sender, key, ex.what());
        }
    }

    void Handle(TEvAbortMultipartUploadRequest::TPtr& ev) {
        const auto& request = ev->Get()->GetRequest();
        const TString key = TString(request.GetKey().data(), request.GetKey().size());
        
        if (Verbose) {
            LOG_INFO_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                "FS AbortMultipartUpload: key# " << key);
        }

        try {
            TString basePath = MakeFullPath(key);
            
            int partNumber = 1;
            while (true) {
                TString partPath = TStringBuilder() << basePath << ".part" << partNumber;
                if (!TFsPath(partPath).Exists()) {
                    break;
                }
                NFs::Remove(partPath);
                ++partNumber;
            }

            ReplySuccess<TEvAbortMultipartUploadResponse>(ev->Sender, key);
        } catch (const std::exception& ex) {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
                "FS AbortMultipartUpload failed: key# " << key << ", error# " << ex.what());
            ReplyError<TEvAbortMultipartUploadResponse>(ev->Sender, key, ex.what());
        }
    }

    void Handle(TEvUploadPartCopyRequest::TPtr& ev) {
        Y_UNUSED(ev);
        LOG_WARN_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
            "FS UploadPartCopy: not implemented yet");
    }
};

} // anonymous namespace

TFsExternalStorage::TFsExternalStorage(const TString& basePath, bool verbose)
    : BasePath(basePath)
    , Verbose(true)
{
    Y_UNUSED(verbose);
    LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
        "TFsExternalStorage created: BasePath# " << BasePath << ", Verbose# " << Verbose);
}

TFsExternalStorage::~TFsExternalStorage()
{
}

void TFsExternalStorage::Execute(TEvPutObjectRequest::TPtr& ev) const {
    LOG_INFO_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
        "TFsExternalStorage::Execute(TEvPutObjectRequest) called, BasePath# " << BasePath);
    
    auto actor = new TFsOperationActor(BasePath, Verbose, ReplyAdapter);
    auto actorId = TlsActivationContext->AsActorContext().Register(actor, TMailboxType::HTSwap, AppData()->IOPoolId);
    TlsActivationContext->AsActorContext().Send(ev->Forward(actorId));
}

void TFsExternalStorage::Execute(TEvGetObjectRequest::TPtr& ev) const {
    LOG_INFO_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
        "TFsExternalStorage::Execute(TEvGetObjectRequest) called");
    
    auto actor = new TFsOperationActor(BasePath, Verbose, ReplyAdapter);
    auto actorId = TlsActivationContext->AsActorContext().Register(actor, TMailboxType::HTSwap, AppData()->IOPoolId);
    TlsActivationContext->AsActorContext().Send(ev->Forward(actorId));
}

void TFsExternalStorage::Execute(TEvHeadObjectRequest::TPtr& ev) const {
    auto actor = new TFsOperationActor(BasePath, Verbose, ReplyAdapter);
    auto actorId = TlsActivationContext->AsActorContext().Register(actor, TMailboxType::HTSwap, AppData()->IOPoolId);
    TlsActivationContext->AsActorContext().Send(ev->Forward(actorId));
}

void TFsExternalStorage::Execute(TEvDeleteObjectRequest::TPtr& ev) const {
    auto actor = new TFsOperationActor(BasePath, Verbose, ReplyAdapter);
    auto actorId = TlsActivationContext->AsActorContext().Register(actor, TMailboxType::HTSwap, AppData()->IOPoolId);
    TlsActivationContext->AsActorContext().Send(ev->Forward(actorId));
}

void TFsExternalStorage::Execute(TEvCheckObjectExistsRequest::TPtr& ev) const {
    auto actor = new TFsOperationActor(BasePath, Verbose, ReplyAdapter);
    auto actorId = TlsActivationContext->AsActorContext().Register(actor, TMailboxType::HTSwap, AppData()->IOPoolId);
    TlsActivationContext->AsActorContext().Send(ev->Forward(actorId));
}

void TFsExternalStorage::Execute(TEvListObjectsRequest::TPtr& ev) const {
    auto actor = new TFsOperationActor(BasePath, Verbose, ReplyAdapter);
    auto actorId = TlsActivationContext->AsActorContext().Register(actor, TMailboxType::HTSwap, AppData()->IOPoolId);
    TlsActivationContext->AsActorContext().Send(ev->Forward(actorId));
}

void TFsExternalStorage::Execute(TEvDeleteObjectsRequest::TPtr& ev) const {
    auto actor = new TFsOperationActor(BasePath, Verbose, ReplyAdapter);
    auto actorId = TlsActivationContext->AsActorContext().Register(actor, TMailboxType::HTSwap, AppData()->IOPoolId);
    TlsActivationContext->AsActorContext().Send(ev->Forward(actorId));
}

void TFsExternalStorage::Execute(TEvCreateMultipartUploadRequest::TPtr& ev) const {
    LOG_INFO_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
        "TFsExternalStorage::Execute(TEvCreateMultipartUploadRequest) called");
    
    auto actor = new TFsOperationActor(BasePath, Verbose, ReplyAdapter);
    auto actorId = TlsActivationContext->AsActorContext().Register(actor, TMailboxType::HTSwap, AppData()->IOPoolId);
    TlsActivationContext->AsActorContext().Send(ev->Forward(actorId));
}

void TFsExternalStorage::Execute(TEvUploadPartRequest::TPtr& ev) const {
    LOG_INFO_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
        "TFsExternalStorage::Execute(TEvUploadPartRequest) called");
    
    auto actor = new TFsOperationActor(BasePath, Verbose, ReplyAdapter);
    auto actorId = TlsActivationContext->AsActorContext().Register(actor, TMailboxType::HTSwap, AppData()->IOPoolId);
    TlsActivationContext->AsActorContext().Send(ev->Forward(actorId));
}

void TFsExternalStorage::Execute(TEvCompleteMultipartUploadRequest::TPtr& ev) const {
    LOG_INFO_S(*TlsActivationContext, NKikimrServices::S3_WRAPPER,
        "TFsExternalStorage::Execute(TEvCompleteMultipartUploadRequest) called");
    
    auto actor = new TFsOperationActor(BasePath, Verbose, ReplyAdapter);
    auto actorId = TlsActivationContext->AsActorContext().Register(actor, TMailboxType::HTSwap, AppData()->IOPoolId);
    TlsActivationContext->AsActorContext().Send(ev->Forward(actorId));
}

void TFsExternalStorage::Execute(TEvAbortMultipartUploadRequest::TPtr& ev) const {
    auto actor = new TFsOperationActor(BasePath, Verbose, ReplyAdapter);
    auto actorId = TlsActivationContext->AsActorContext().Register(actor, TMailboxType::HTSwap, AppData()->IOPoolId);
    TlsActivationContext->AsActorContext().Send(ev->Forward(actorId));
}

void TFsExternalStorage::Execute(TEvUploadPartCopyRequest::TPtr& ev) const {
    auto actor = new TFsOperationActor(BasePath, Verbose, ReplyAdapter);
    auto actorId = TlsActivationContext->AsActorContext().Register(actor, TMailboxType::HTSwap, AppData()->IOPoolId);
    TlsActivationContext->AsActorContext().Send(ev->Forward(actorId));
}

} // NKikimr::NWrappers::NExternalStorage
