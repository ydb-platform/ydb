#pragma once

#include "abstract.h"

namespace NKikimr::NWrappers::NExternalStorage {

class TFsExternalStorage: public IExternalStorageOperator {
private:
    TString BasePath;
    mutable NActors::TActorId OperationActorId;
    mutable bool ActorCreated = false;

    void EnsureActor() const;
    void Shutdown();

    template <typename TEvPtr>
    void ExecuteImpl(TEvPtr& ev) const;

public:
    explicit TFsExternalStorage(const TString& basePath);
    ~TFsExternalStorage() override;

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
