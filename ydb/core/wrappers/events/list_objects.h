#pragma once
#include "abstract.h"
#include "common.h"

#include <ydb/core/base/events.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/S3Errors.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/ListObjectsRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/ListObjectsResult.h>
#include <ydb/library/actors/core/event_local.h>
#include <util/generic/ptr.h>

namespace NKikimr::NWrappers::NExternalStorage {

    class TEvListObjectsRequest: public TGenericRequest<TEvListObjectsRequest, EvListObjectsRequest, Aws::S3::Model::ListObjectsRequest> {
    private:
        using TBase = TGenericRequest<TEvListObjectsRequest, EvListObjectsRequest, Aws::S3::Model::ListObjectsRequest>;
    public:
        using TBase::TBase;
    };

    class TEvListObjectsResponse: public TBaseGenericResponse<TEvListObjectsResponse, EvListObjectsResponse, Aws::S3::Model::ListObjectsResult> {
    private:
        using TBase = TBaseGenericResponse<TEvListObjectsResponse, EvListObjectsResponse, Aws::S3::Model::ListObjectsResult>;
    public:
        using TBase::TBase;
    };

}
