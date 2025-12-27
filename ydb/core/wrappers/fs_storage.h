#pragma once

#include "abstract.h"

namespace NKikimr::NWrappers::NExternalStorage {

class TFsExternalStorage: public IExternalStorageOperator {
private:
    TString BasePath;
    bool Verbose = true;
    mutable NActors::TActorId OperationActorId;
    mutable bool ActorCreated = false;

    void EnsureActor() const;
    void Shutdown();

public:
    TFsExternalStorage(const TString& basePath, bool verbose = true);
    ~TFsExternalStorage();

    virtual void Execute(TEvCheckObjectExistsRequest::TPtr& ev) const override;
    virtual void Execute(TEvListObjectsRequest::TPtr& ev) const override;
    virtual void Execute(TEvGetObjectRequest::TPtr& ev) const override;
    virtual void Execute(TEvHeadObjectRequest::TPtr& ev) const override;
    virtual void Execute(TEvPutObjectRequest::TPtr& ev) const override;
    virtual void Execute(TEvDeleteObjectRequest::TPtr& ev) const override;
    virtual void Execute(TEvDeleteObjectsRequest::TPtr& ev) const override;
    virtual void Execute(TEvCreateMultipartUploadRequest::TPtr& ev) const override;
    virtual void Execute(TEvUploadPartRequest::TPtr& ev) const override;
    virtual void Execute(TEvCompleteMultipartUploadRequest::TPtr& ev) const override;
    virtual void Execute(TEvAbortMultipartUploadRequest::TPtr& ev) const override;
    virtual void Execute(TEvUploadPartCopyRequest::TPtr& ev) const override;
};

} // NKikimr::NWrappers::NExternalStorage
