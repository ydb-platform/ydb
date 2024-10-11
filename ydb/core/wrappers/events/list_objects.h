#pragma once

#include "common.h"

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/ListObjectsRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/ListObjectsResult.h>

namespace NKikimr::NWrappers::NExternalStorage {

class TEvListObjectsRequest: public TGenericRequest<TEvListObjectsRequest, EvListObjectsRequest, Aws::S3::Model::ListObjectsRequest> {
private:
    using TBase = TGenericRequest<TEvListObjectsRequest, EvListObjectsRequest, Aws::S3::Model::ListObjectsRequest>;
public:
    using TBase::TBase;
};

class TEvListObjectsResponse: public TGenericResponse<TEvListObjectsResponse, EvListObjectsResponse, Aws::S3::Model::ListObjectsResult> {
private:
    using TBase = TGenericResponse<TEvListObjectsResponse, EvListObjectsResponse, Aws::S3::Model::ListObjectsResult>;
public:
    using TBase::TBase;
};

}
