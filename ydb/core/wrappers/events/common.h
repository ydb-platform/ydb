#pragma once

#include "abstract.h"
#include "s3_out.h"

#include <ydb/library/actors/core/event_local.h>

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

    explicit TGenericRequest(const TRequest& request, IRequestContext::TPtr requestContext = nullptr)
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
        , Body(std::move(body))
    {
    }

    TString ToString() const override {
        return TStringBuilder() << this->ToStringHeader() << " {"
            << " Request: " << this->Request
            << " Body: " << Body.size() << "b"
            << " }";
    }

    using TBase = TRequestWithBody<TDerived, EventType, T>;
};

template <typename TDerived, ui32 EventType, typename T, typename U = T, bool HasKey = true>
struct TGenericResponse: public NActors::TEventLocal<TDerived, EventType> {
private:
    IRequestContext::TPtr RequestContext;

public:
    using TOutcome = Aws::Utils::Outcome<T, Aws::S3::S3Error>;
    using TResult = Aws::Utils::Outcome<U, Aws::S3::S3Error>;
    using TAwsResult = U;
    using TKey = std::optional<TString>;

    TKey Key;
    TResult Result;

    explicit TGenericResponse(const TOutcome& outcome, IRequestContext::TPtr requestContext = nullptr)
        : RequestContext(requestContext)
        , Result(TDerived::ResultFromOutcome(outcome))
    {
    }

    explicit TGenericResponse(const TKey& key, const TOutcome& outcome, IRequestContext::TPtr requestContext = nullptr)
        : RequestContext(requestContext)
        , Key(key)
        , Result(TDerived::ResultFromOutcome(outcome))
    {
        static_assert(HasKey, "Object has no key");
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

    template <typename Type>
    std::shared_ptr<Type> GetRequestContextAs() const {
        Y_ABORT_UNLESS(RequestContext);
        return dynamic_pointer_cast<Type>(RequestContext);
    }

    static TResult ResultFromOutcome(const TOutcome& outcome) {
        return outcome;
    }

    virtual TString ToStringBody() const {
        auto result = TStringBuilder();
        if constexpr (HasKey) {
            result << " Key: " << (Key ? "null" : *Key);
        }
        result << " Result: " << Result;
        return result;
    }

    TString ToString() const override {
        return TStringBuilder() << this->ToStringHeader() << " {" << ToStringBody() << " }";
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
        : TBase(key, outcome)
    {
    }

    explicit TResponseWithBody(const TKey& key, const typename TBase::TOutcome& outcome, TString&& body)
        : TBase(key, outcome)
        , Body(std::move(body))
    {
    }

    TString ToStringBody() const override {
        return TStringBuilder()
            << TBase::ToStringBody()
            << " Body: " << Body.size() << "b";
    }
};

#define DEFINE_REQUEST(name, base) \
    struct TEv##name##Request: public base<TEv##name##Request, Ev##name##Request, Aws::S3::Model::name##Request> { \
        using TBase::TBase; \
    }

#define DEFINE_GENERIC_REQUEST(name) \
    DEFINE_REQUEST(name, TGenericRequest)

#define DECLARE_GENERIC_RESPONSE_K(name, hasKey) \
    struct TEv##name##Response: public TGenericResponse<TEv##name##Response, Ev##name##Response, Aws::S3::Model::name##Result, Aws::S3::Model::name##Result, hasKey> { \
    private: \
        using TBase = TGenericResponse<TEv##name##Response, Ev##name##Response, Aws::S3::Model::name##Result, Aws::S3::Model::name##Result, hasKey>; \
    public: \
        using TBase::TBase;

#define DEFINE_GENERIC_RESPONSE_K(name, hasKey) \
    DECLARE_GENERIC_RESPONSE_K(name, hasKey) \
    }

#define DEFINE_GENERIC_RESPONSE(name) \
    DEFINE_GENERIC_RESPONSE_K(name, true)

#define DEFINE_GENERIC_REQUEST_RESPONSE_K(name, hasKey) \
    DEFINE_GENERIC_REQUEST(name); \
    DEFINE_GENERIC_RESPONSE_K(name, hasKey)

#define DEFINE_GENERIC_REQUEST_RESPONSE(name) \
    DEFINE_GENERIC_REQUEST_RESPONSE_K(name, true)

DEFINE_REQUEST(PutObject, TRequestWithBody);
DEFINE_GENERIC_RESPONSE(PutObject);

DEFINE_REQUEST(UploadPart, TRequestWithBody);
DEFINE_GENERIC_RESPONSE(UploadPart);

DEFINE_GENERIC_REQUEST_RESPONSE(AbortMultipartUpload);
DEFINE_GENERIC_REQUEST_RESPONSE(CompleteMultipartUpload);
DEFINE_GENERIC_REQUEST_RESPONSE(CreateMultipartUpload);
DEFINE_GENERIC_REQUEST_RESPONSE(DeleteObject);
DEFINE_GENERIC_REQUEST_RESPONSE_K(DeleteObjects, false);
DEFINE_GENERIC_REQUEST_RESPONSE(HeadObject);
DEFINE_GENERIC_REQUEST_RESPONSE_K(ListObjects, false);
DEFINE_GENERIC_REQUEST_RESPONSE(UploadPartCopy);

#undef DEFINE_REQUEST
#undef DEFINE_GENERIC_REQUEST
#undef DECLARE_GENERIC_RESPONSE_K
#undef DEFINE_GENERIC_RESPONSE
#undef DEFINE_GENERIC_RESPONSE_K
#undef DEFINE_GENERIC_REQUEST_RESPONSE
#undef DEFINE_GENERIC_REQUEST_RESPONSE_K

}
