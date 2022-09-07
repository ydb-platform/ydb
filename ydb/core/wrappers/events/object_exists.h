#pragma once
#include "abstract.h"
#include "common.h"

#include <ydb/core/base/events.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/HeadObjectRequest.h>
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/HeadObjectResult.h>
#include <library/cpp/actors/core/event_local.h>
#include <util/generic/ptr.h>

namespace NKikimr::NWrappers::NExternalStorage {

class TEvCheckObjectExistsRequest: public TEventLocal<TEvCheckObjectExistsRequest, EvCheckObjectExistsRequest> {
public:
    using TRequest = Aws::S3::Model::HeadObjectRequest;
private:
    TRequest Request;
    IRequestContext::TPtr RequestContext;
public:
    TEvCheckObjectExistsRequest(const TRequest& request, IRequestContext::TPtr requestContext)
        : Request(request)
        , RequestContext(requestContext) {

    }
    IRequestContext::TPtr GetRequestContext() const {
        return RequestContext;
    }
    const TRequest& GetRequest() const {
        return Request;
    }
    TRequest* operator->() {
        return &Request;
    }
};

class TEvCheckObjectExistsResponse: public TEventLocal<TEvCheckObjectExistsResponse, EvCheckObjectExistsResponse> {
public:
    using TRequest = Aws::S3::Model::HeadObjectRequest;
    using TResult = Aws::S3::Model::HeadObjectResult;
    using TOutcome = Aws::Utils::Outcome<TResult, Aws::S3::S3Error>;
    using TKey = std::optional<TString>;
private:
    IRequestContext::TPtr RequestContext;
    const bool IsExistsFlag;
public:
    TEvCheckObjectExistsResponse(const TRequest& /*request*/, const TOutcome& outcome, IRequestContext::TPtr requestContext)
        : RequestContext(requestContext)
        , IsExistsFlag(outcome.IsSuccess() && !outcome.GetResult().GetDeleteMarker()) {

    }
    bool IsExists() const {
        return IsExistsFlag;
    }
    template <class T>
    std::shared_ptr<T> GetRequestContextAs() const {
        return dynamic_pointer_cast<T>(RequestContext);
    }
};
}
