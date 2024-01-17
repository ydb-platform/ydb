#pragma once
#include "abstract.h"
#include "s3_out.h"

#include <ydb/library/actors/core/event_local.h>

#include <ydb/core/base/events.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/ptr.h>

namespace NKikimr::NWrappers::NExternalStorage {

template <typename TDerived, ui32 EventType, typename T>
struct TGenericRequest: public NActors::TEventLocal<TDerived, EventType> {
private:
    IRequestContext::TPtr RequestContext;
public:
    using TRequest = T;
    TRequest Request;
    IRequestContext::TPtr GetRequestContext() const {
        return RequestContext;
    }

    const TRequest& GetRequest() const {
        return Request;
    }

    TRequest& MutableRequest() {
        return Request;
    }

    explicit TGenericRequest(const TRequest& request)
        : Request(request) {
    }

    explicit TGenericRequest(const TRequest& request, IRequestContext::TPtr requestContext)
        : RequestContext(requestContext)
        , Request(request)
    {
    }

    TString ToString() const override {
        return TStringBuilder() << this->ToStringHeader() << " {"
            << " Request: " << Request
            << " }";
    }

    using TBase = TGenericRequest<TDerived, EventType, TRequest>;
};

template <typename TDerived, ui32 EventType, typename T>
struct TRequestWithBody: public TGenericRequest<TDerived, EventType, T> {
    using TGeneric = TGenericRequest<TDerived, EventType, T>;

    TString Body;

    explicit TRequestWithBody(const typename TGeneric::TRequest& request, TString&& body)
        : TGeneric(request)
        , Body(std::move(body)) {
    }

    TString ToString() const override {
        return TStringBuilder() << this->ToStringHeader() << " {"
            << " Request: " << this->Request
            << " Body: " << Body.size() << "b"
            << " }";
    }

    using TBase = TRequestWithBody<TDerived, EventType, T>;
};

template <typename TDerived, ui32 EventType, typename TAwsResultExt, typename U = TAwsResultExt>
struct TBaseGenericResponse: public NActors::TEventLocal<TDerived, EventType> {
private:
    using TBase = NActors::TEventLocal<TDerived, EventType>;
    IRequestContext::TPtr RequestContext;
public:
    using TOutcome = Aws::Utils::Outcome<TAwsResultExt, Aws::S3::S3Error>;
    using TResult = Aws::Utils::Outcome<U, Aws::S3::S3Error>;
    using TAwsResult = U;
    using TAwsOutcome = TResult;
    using TKey = std::optional<TString>;

    TResult Result;

    explicit TBaseGenericResponse(const TOutcome& outcome)
        : Result(TDerived::ResultFromOutcome(outcome)) {
    }

    explicit TBaseGenericResponse(const TOutcome& outcome, IRequestContext::TPtr requestContext)
        : RequestContext(requestContext)
        , Result(TDerived::ResultFromOutcome(outcome)) {
    }

    bool IsSuccess() const {
        return Result.IsSuccess();
    }
    const Aws::S3::S3Error& GetError() const {
        return Result.GetError();
    }
    const U& GetResult() const {
        return Result.GetResult();
    }

    template <class T>
    std::shared_ptr<T> GetRequestContextAs() const {
        return dynamic_pointer_cast<T>(RequestContext);
    }

    static TResult ResultFromOutcome(const TOutcome& outcome) {
        return outcome;
    }

    TString ToString() const override {
        return TStringBuilder() << this->ToStringHeader() << " {"
            << " Result: " << Result
            << " }";
    }
};

template <typename TDerived, ui32 EventType, typename TAwsResult, typename U = TAwsResult>
struct TGenericResponse: public TBaseGenericResponse<TDerived, EventType, TAwsResult, U> {
private:
    using TBase = TBaseGenericResponse<TDerived, EventType, TAwsResult, U>;
public:
    using TOutcome = typename TBase::TOutcome;
    using TResult = typename TBase::TResult;
    using TKey = std::optional<TString>;

    TKey Key;

    explicit TGenericResponse(const TKey& key, const TOutcome& outcome)
        : TBase(outcome)
        , Key(key) {
    }

    explicit TGenericResponse(const TKey& key, const TOutcome& outcome, IRequestContext::TPtr requestContext)
        : TBase(outcome, requestContext)
        , Key(key)
    {
    }

    TString ToString() const override {
        return TStringBuilder() << this->ToStringHeader() << " {"
            << " Key: " << (Key ? "null" : *Key)
            << " Result: " << TBase::Result
            << " }";
    }
};

template <typename TDerived, ui32 EventType, typename T, typename U>
struct TResponseWithBody: public TGenericResponse<TDerived, EventType, T, U> {
private:
    using TBase = TGenericResponse<TDerived, EventType, T, U>;
public:
    using TKey = typename TBase::TKey;

    TString Body;

    explicit TResponseWithBody(const TKey& key, const typename TBase::TOutcome& outcome)
        : TBase(key, outcome) {
    }

    explicit TResponseWithBody(const TKey& key, const typename TBase::TOutcome& outcome, TString&& body)
        : TBase(key, outcome)
        , Body(std::move(body)) {
    }

    TString ToString() const override {
        return TStringBuilder() << this->ToStringHeader() << " {"
            << " Key: " << (this->Key ? "null" : *this->Key)
            << " Result: " << this->Result
            << " Body: " << Body.size() << "b"
            << " }";
    }
};

#define DEFINE_REQUEST(name, base) \
        struct TEv##name##Request: public base<TEv##name##Request, Ev##name##Request, Aws::S3::Model::name##Request> { \
            using TBase::TBase; \
        }

#define DEFINE_GENERIC_REQUEST(name) \
        DEFINE_REQUEST(name, TGenericRequest)

#define DECLARE_GENERIC_RESPONSE(name) \
        struct TEv##name##Response: public TGenericResponse<TEv##name##Response, Ev##name##Response, Aws::S3::Model::name##Result> {\
        private:\
            using TBase = TGenericResponse<TEv##name##Response, Ev##name##Response, Aws::S3::Model::name##Result>;\
        public:\
            using TBase::TBase;

#define DECLARE_RESPONSE_WITH_BODY(name, result_t) \
        struct TEv##name##Response: public TResponseWithBody<TEv##name##Response, Ev##name##Response, Aws::S3::Model::name##Result, result_t> {\
        private:\
            using TBase = TResponseWithBody<TEv##name##Response, Ev##name##Response, Aws::S3::Model::name##Result, result_t>;\
        public:\
            using TBase::TBase;

#define DEFINE_GENERIC_RESPONSE(name) \
        DECLARE_GENERIC_RESPONSE(name) \
        }

#define DEFINE_GENERIC_REQUEST_RESPONSE(name) \
        DEFINE_GENERIC_REQUEST(name); \
        DEFINE_GENERIC_RESPONSE(name)

DEFINE_REQUEST(PutObject, TRequestWithBody);
DEFINE_GENERIC_RESPONSE(PutObject);

DEFINE_REQUEST(UploadPart, TRequestWithBody);
DEFINE_GENERIC_RESPONSE(UploadPart);

DEFINE_GENERIC_REQUEST_RESPONSE(HeadObject);
DEFINE_GENERIC_REQUEST_RESPONSE(DeleteObject);
DEFINE_GENERIC_REQUEST_RESPONSE(CreateMultipartUpload);
DEFINE_GENERIC_REQUEST_RESPONSE(CompleteMultipartUpload);
DEFINE_GENERIC_REQUEST_RESPONSE(AbortMultipartUpload);

DEFINE_GENERIC_REQUEST(UploadPartCopy);
DEFINE_GENERIC_RESPONSE(UploadPartCopy);

#undef DEFINE_REQUEST
#undef DEFINE_GENERIC_REQUEST
#undef DECLARE_GENERIC_RESPONSE
#undef DECLARE_RESPONSE_WITH_BODY
#undef DEFINE_GENERIC_RESPONSE
#undef DEFINE_GENERIC_REQUEST_RESPONSE

}
