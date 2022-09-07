#pragma once
#include "abstract.h"
#include "common.h"

#include <ydb/core/base/events.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/S3Errors.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/ListObjectsRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/ListObjectsResult.h>
#include <library/cpp/actors/core/event_local.h>
#include <util/generic/ptr.h>

namespace NKikimr::NWrappers::NExternalStorage {

    class TEvListObjectsRequest: public TEventLocal<TEvListObjectsRequest, EvListObjectsRequest> {
    public:
        using TRequest = Aws::S3::Model::ListObjectsRequest;
    private:
        TRequest Request;
    public:
        IRequestContext::TPtr GetRequestContext() const {
            return nullptr;
        }
        const TRequest& GetRequest() const {
            return Request;
        }
        TRequest* operator->() {
            return &Request;
        }
    };
    class TEvListObjectsResponse: public TEventLocal<TEvListObjectsResponse, EvListObjectsResponse> {
    public:
        using TResult = Aws::S3::Model::ListObjectsResult;
        using TOutcome = Aws::Utils::Outcome<TResult, Aws::S3::S3Error>;
        using TKey = std::optional<TString>;
    private:
        TOutcome Outcome;
    public:
        TEvListObjectsResponse(const TOutcome& result)
            : Outcome(result)
        {

        }
        bool IsSuccess() const {
            return Outcome.IsSuccess();
        }
        const TResult& GetResult() const {
            return Outcome.GetResult();
        }
        const TResult* operator->() const {
            return &Outcome.GetResult();
        }
    };

}
