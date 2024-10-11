#pragma once

#include "common.h"

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/DeleteObjectsRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/DeleteObjectsResult.h>

namespace NKikimr::NWrappers::NExternalStorage {

class TEvDeleteObjectsRequest: public TGenericRequest<TEvDeleteObjectsRequest, EvDeleteObjectsRequest, Aws::S3::Model::DeleteObjectsRequest> {
private:
    using TBase = TGenericRequest<TEvDeleteObjectsRequest, EvDeleteObjectsRequest, Aws::S3::Model::DeleteObjectsRequest>;
public:
    using TBase::TBase;
};

class TEvDeleteObjectsResponse: public TGenericResponse<TEvDeleteObjectsResponse, EvDeleteObjectsResponse, Aws::S3::Model::DeleteObjectsResult> {
private:
    using TBase = TGenericResponse<TEvDeleteObjectsResponse, EvDeleteObjectsResponse, Aws::S3::Model::DeleteObjectsResult>;
public:
    using TBase::TBase;
};

}
