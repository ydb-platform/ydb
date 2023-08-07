#pragma once
#include <ydb/core/base/events.h>
#include <ydb/core/wrappers/events/abstract.h>
#include <ydb/core/wrappers/events/common.h>
#include <ydb/core/wrappers/events/delete_objects.h>
#include <ydb/core/wrappers/events/list_objects.h>
#include <ydb/core/wrappers/events/object_exists.h>
#include <util/generic/ptr.h>

namespace NKikimr::NWrappers {

struct TEvExternalStorage {
    using TEvListObjectsRequest = NExternalStorage::TEvListObjectsRequest;
    using TEvListObjectsResponse = NExternalStorage::TEvListObjectsResponse;
    using TEvGetObjectRequest = NExternalStorage::TEvGetObjectRequest;
    using TEvGetObjectResponse = NExternalStorage::TEvGetObjectResponse;
    using TEvHeadObjectRequest = NExternalStorage::TEvHeadObjectRequest;
    using TEvHeadObjectResponse = NExternalStorage::TEvHeadObjectResponse;
    using TEvPutObjectRequest = NExternalStorage::TEvPutObjectRequest;
    using TEvPutObjectResponse = NExternalStorage::TEvPutObjectResponse;
    using TEvDeleteObjectRequest = NExternalStorage::TEvDeleteObjectRequest;
    using TEvDeleteObjectResponse = NExternalStorage::TEvDeleteObjectResponse;
    using TEvDeleteObjectsRequest = NExternalStorage::TEvDeleteObjectsRequest;
    using TEvDeleteObjectsResponse = NExternalStorage::TEvDeleteObjectsResponse;
    using TEvCreateMultipartUploadRequest = NExternalStorage::TEvCreateMultipartUploadRequest;
    using TEvCreateMultipartUploadResponse = NExternalStorage::TEvCreateMultipartUploadResponse;
    using TEvUploadPartRequest = NExternalStorage::TEvUploadPartRequest;
    using TEvUploadPartResponse = NExternalStorage::TEvUploadPartResponse;
    using TEvCompleteMultipartUploadRequest = NExternalStorage::TEvCompleteMultipartUploadRequest;
    using TEvCompleteMultipartUploadResponse = NExternalStorage::TEvCompleteMultipartUploadResponse;
    using TEvAbortMultipartUploadRequest = NExternalStorage::TEvAbortMultipartUploadRequest;
    using TEvAbortMultipartUploadResponse = NExternalStorage::TEvAbortMultipartUploadResponse;
    using TEvCheckObjectExistsRequest = NExternalStorage::TEvCheckObjectExistsRequest;
    using TEvCheckObjectExistsResponse = NExternalStorage::TEvCheckObjectExistsResponse;
    using TEvUploadPartCopyRequest = NExternalStorage::TEvUploadPartCopyRequest;
    using TEvUploadPartCopyResponse = NExternalStorage::TEvUploadPartCopyResponse;
};

namespace NExternalStorage {

class IExternalStorageOperator {
public:
    using TPtr = std::shared_ptr<IExternalStorageOperator>;
    virtual ~IExternalStorageOperator() = default;

    virtual void Execute(TEvListObjectsRequest::TPtr& ev) const = 0;
    virtual void Execute(TEvGetObjectRequest::TPtr& ev) const = 0;
    virtual void Execute(TEvHeadObjectRequest::TPtr& ev) const = 0;
    virtual void Execute(TEvPutObjectRequest::TPtr& ev) const = 0;
    virtual void Execute(TEvDeleteObjectRequest::TPtr& ev) const = 0;
    virtual void Execute(TEvDeleteObjectsRequest::TPtr& ev) const = 0;
    virtual void Execute(TEvCreateMultipartUploadRequest::TPtr& ev) const = 0;
    virtual void Execute(TEvUploadPartRequest::TPtr& ev) const = 0;
    virtual void Execute(TEvCompleteMultipartUploadRequest::TPtr& ev) const = 0;
    virtual void Execute(TEvAbortMultipartUploadRequest::TPtr& ev) const = 0;
    virtual void Execute(TEvCheckObjectExistsRequest::TPtr& ev) const = 0;
    virtual void Execute(TEvUploadPartCopyRequest::TPtr& ev) const = 0;
};

class IExternalStorageConfig {
private:
    class TStoragesCollection;

protected:
    virtual IExternalStorageOperator::TPtr DoConstructStorageOperator() const = 0;
    virtual TString DoGetStorageId() const = 0;

    TString GetStorageId() const {
        return DoGetStorageId();
    }
public:
    using TPtr = std::shared_ptr<IExternalStorageConfig>;
    virtual ~IExternalStorageConfig() = default;
    IExternalStorageOperator::TPtr ConstructStorageOperator() const;
    static IExternalStorageConfig::TPtr Construct(const NKikimrSchemeOp::TS3Settings& settings);
};
} // NExternalStorage

using IExternalStorageConfig = NExternalStorage::IExternalStorageConfig;

} // NKikimr::NWrappers
