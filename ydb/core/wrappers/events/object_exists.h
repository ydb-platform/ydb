#pragma once

#include "common.h"

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/HeadObjectRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/HeadObjectResult.h>

namespace NKikimr::NWrappers::NExternalStorage {

class TEvCheckObjectExistsRequest: public TGenericRequest<TEvCheckObjectExistsRequest, EvCheckObjectExistsRequest, Aws::S3::Model::HeadObjectRequest> {
private:
    using TBase = TGenericRequest<TEvCheckObjectExistsRequest, EvCheckObjectExistsRequest, Aws::S3::Model::HeadObjectRequest>;
public:
    using TBase::TBase;
};

class TEvCheckObjectExistsResponse: public TGenericResponse<TEvCheckObjectExistsResponse, EvCheckObjectExistsResponse, Aws::S3::Model::HeadObjectResult> {
private:
    using TBase = TGenericResponse<TEvCheckObjectExistsResponse, EvCheckObjectExistsResponse, Aws::S3::Model::HeadObjectResult>;
public:
    using TBase::TBase;

    bool IsExists() const {
        return Result.IsSuccess();
    }
};

}
