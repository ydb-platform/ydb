#pragma once
#include "abstract.h"
#include "common.h"

#include <ydb/core/base/events.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/S3Errors.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/DeleteObjectsRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/DeleteObjectsResult.h>
#include <ydb/library/actors/core/event_local.h>
#include <util/generic/ptr.h>

namespace NKikimr::NWrappers::NExternalStorage {

    class TEvDeleteObjectsRequest: public TGenericRequest<TEvDeleteObjectsRequest, EvDeleteObjectsRequest, Aws::S3::Model::DeleteObjectsRequest> {
    private:
        using TBase = TGenericRequest<TEvDeleteObjectsRequest, EvDeleteObjectsRequest, Aws::S3::Model::DeleteObjectsRequest>;
    public:
        using TBase::TBase;
    };

    class TEvDeleteObjectsResponse: public TBaseGenericResponse<TEvDeleteObjectsResponse, EvDeleteObjectsResponse, Aws::S3::Model::DeleteObjectsResult> {
    private:
        using TBase = TBaseGenericResponse<TEvDeleteObjectsResponse, EvDeleteObjectsResponse, Aws::S3::Model::DeleteObjectsResult>;
    public:
        using TBase::TBase;
    };

}
