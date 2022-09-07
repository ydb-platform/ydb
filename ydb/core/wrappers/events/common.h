#pragma once
#include "abstract.h"
#include "s3_out.h"

#include <library/cpp/actors/core/event_local.h>

#include <ydb/core/base/events.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/generic/ptr.h>

namespace NKikimr::NWrappers::NExternalStorage {

template <typename TDerived, ui32 EventType, typename T>
struct TGenericRequest: public NActors::TEventLocal<TDerived, EventType> {
    using TRequest = T;
    TRequest Request;
    IRequestContext::TPtr GetRequestContext() const {
        return nullptr;
    }

    const TRequest& GetRequest() const {
        return Request;
    }

    explicit TGenericRequest(const TRequest& request)
        : Request(request) {
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

template <typename TDerived, ui32 EventType, typename T, typename U = T>
struct TGenericResponse: public NActors::TEventLocal<TDerived, EventType> {
    using TOutcome = Aws::Utils::Outcome<T, Aws::S3::S3Error>;
    using TResult = Aws::Utils::Outcome<U, Aws::S3::S3Error>;
    using TKey = std::optional<TString>;

    TKey Key;
    TResult Result;

    explicit TGenericResponse(const TKey& key, const TOutcome& outcome)
        : Key(key)
        , Result(TDerived::ResultFromOutcome(outcome)) {
    }

    static TResult ResultFromOutcome(const TOutcome& outcome) {
        return outcome;
    }

    TString ToString() const override {
        return TStringBuilder() << this->ToStringHeader() << " {"
            << " Key: " << (Key ? "null" : *Key)
            << " Result: " << Result
            << " }";
    }

    using TBase = TGenericResponse<TDerived, EventType, T, U>;
};

template <typename TDerived, ui32 EventType, typename T, typename U>
struct TResponseWithBody: public TGenericResponse<TDerived, EventType, T, U> {
    using TGeneric = TGenericResponse<TDerived, EventType, T, U>;
    using TKey = typename TGeneric::TKey;

    TString Body;

    explicit TResponseWithBody(const TKey& key, const typename TGeneric::TOutcome& outcome)
        : TGeneric(key, outcome) {
    }

    explicit TResponseWithBody(const TKey& key, const typename TGeneric::TOutcome& outcome, TString&& body)
        : TGeneric(key, outcome)
        , Body(std::move(body)) {
    }

    TString ToString() const override {
        return TStringBuilder() << this->ToStringHeader() << " {"
            << " Key: " << (this->Key ? "null" : *this->Key)
            << " Result: " << this->Result
            << " Body: " << Body.size() << "b"
            << " }";
    }

    using TBase = TResponseWithBody<TDerived, EventType, T, U>;
};

#define DEFINE_REQUEST(name, base) \
        struct TEv##name##Request: public base<TEv##name##Request, Ev##name##Request, Aws::S3::Model::name##Request> { \
            using TBase::TBase; \
        }

#define DEFINE_GENERIC_REQUEST(name) \
        DEFINE_REQUEST(name, TGenericRequest)

#define DECLARE_GENERIC_RESPONSE(name) \
        struct TEv##name##Response: public TGenericResponse<TEv##name##Response, Ev##name##Response, Aws::S3::Model::name##Result>

#define DECLARE_RESPONSE_WITH_BODY(name, result_t) \
        struct TEv##name##Response: public TResponseWithBody<TEv##name##Response, Ev##name##Response, Aws::S3::Model::name##Result, result_t>

#define DEFINE_GENERIC_RESPONSE(name) \
        DECLARE_GENERIC_RESPONSE(name) { \
            using TBase::TBase; \
        }

#define DEFINE_GENERIC_REQUEST_RESPONSE(name) \
        DEFINE_GENERIC_REQUEST(name); \
        DEFINE_GENERIC_RESPONSE(name)

DEFINE_GENERIC_REQUEST(GetObject);
DECLARE_RESPONSE_WITH_BODY(GetObject, Aws::String) {
    static TResult ResultFromOutcome(const TOutcome & outcome) {
        if (outcome.IsSuccess()) {
            return outcome.GetResult().GetETag();
        } else {
            return outcome.GetError();
        }
    }

    using TBase::TBase;
};

DEFINE_REQUEST(PutObject, TRequestWithBody);
DEFINE_GENERIC_RESPONSE(PutObject);

DEFINE_REQUEST(UploadPart, TRequestWithBody);
DEFINE_GENERIC_RESPONSE(UploadPart);

DEFINE_GENERIC_REQUEST_RESPONSE(HeadObject);
DEFINE_GENERIC_REQUEST_RESPONSE(DeleteObject);
DEFINE_GENERIC_REQUEST_RESPONSE(CreateMultipartUpload);
DEFINE_GENERIC_REQUEST_RESPONSE(CompleteMultipartUpload);
DEFINE_GENERIC_REQUEST_RESPONSE(AbortMultipartUpload);

#undef DEFINE_REQUEST
#undef DEFINE_GENERIC_REQUEST
#undef DECLARE_GENERIC_RESPONSE
#undef DECLARE_RESPONSE_WITH_BODY
#undef DEFINE_GENERIC_RESPONSE
#undef DEFINE_GENERIC_REQUEST_RESPONSE

}
