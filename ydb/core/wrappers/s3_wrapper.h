#pragma once 
 
#include "s3_out.h" 
 
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/auth/AWSCredentials.h> 
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core/include/aws/core/client/AWSClient.h> 
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/AbortMultipartUploadRequest.h> 
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/CreateMultipartUploadRequest.h> 
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/CompleteMultipartUploadRequest.h> 
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/GetObjectRequest.h> 
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/HeadObjectRequest.h> 
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/PutObjectRequest.h> 
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/UploadPartRequest.h> 
#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/S3Client.h> 
 
#include <ydb/core/base/events.h>
 
namespace NKikimr { 
namespace NWrappers { 
 
struct TS3User {
    TS3User();
    ~TS3User();
};

struct TEvS3Wrapper { 
    template <typename TDerived, ui32 EventType, typename T> 
    struct TGenericRequest: public TEventLocal<TDerived, EventType> { 
        using TRequest = T; 
        TRequest Request; 
 
        explicit TGenericRequest(const TRequest& request) 
            : Request(request) 
        { 
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
 
        using TBase = TRequestWithBody<TDerived, EventType, T>; 
    }; 
 
    template <typename TDerived, ui32 EventType, typename T, typename U = T> 
    struct TGenericResponse: public TEventLocal<TDerived, EventType> { 
        using TOutcome = Aws::Utils::Outcome<T, Aws::S3::S3Error>;
        using TResult = Aws::Utils::Outcome<U, Aws::S3::S3Error>;
 
        TResult Result; 
 
        explicit TGenericResponse(const TOutcome& outcome) 
            : Result(TDerived::ResultFromOutcome(outcome)) 
        { 
        } 
 
        static TResult ResultFromOutcome(const TOutcome& outcome) { 
            return outcome; 
        } 
 
        using TBase = TGenericResponse<TDerived, EventType, T, U>; 
    }; 
 
    template <typename TDerived, ui32 EventType, typename T, typename U> 
    struct TResponseWithBody: public TGenericResponse<TDerived, EventType, T, U> { 
        using TGeneric = TGenericResponse<TDerived, EventType, T, U>; 
 
        TString Body; 
 
        explicit TResponseWithBody(const typename TGeneric::TOutcome& outcome) 
            : TGeneric(outcome) 
        { 
        } 
 
        explicit TResponseWithBody(const typename TGeneric::TOutcome& outcome, TString&& body) 
            : TGeneric(outcome) 
            , Body(std::move(body)) 
        { 
        } 
 
        using TBase = TResponseWithBody<TDerived, EventType, T, U>; 
    }; 
 
    #define EV_REQUEST_RESPONSE(name) \ 
        Ev##name##Request, \ 
        Ev##name##Response 
 
    enum EEv { 
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_S3_WRAPPER), 
 
        EV_REQUEST_RESPONSE(GetObject), 
        EV_REQUEST_RESPONSE(HeadObject), 
        EV_REQUEST_RESPONSE(PutObject), 
        EV_REQUEST_RESPONSE(CreateMultipartUpload), 
        EV_REQUEST_RESPONSE(UploadPart), 
        EV_REQUEST_RESPONSE(CompleteMultipartUpload), 
        EV_REQUEST_RESPONSE(AbortMultipartUpload), 
 
        EvEnd, 
    }; 
 
    #undef EV_REQUEST_RESPONSE 
 
    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_S3_WRAPPER), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_S3_WRAPPER)"); 
 
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
        static TResult ResultFromOutcome(const TOutcome& outcome) { 
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
    DEFINE_GENERIC_REQUEST_RESPONSE(CreateMultipartUpload); 
    DEFINE_GENERIC_REQUEST_RESPONSE(CompleteMultipartUpload); 
    DEFINE_GENERIC_REQUEST_RESPONSE(AbortMultipartUpload); 
 
    #undef DEFINE_REQUEST 
    #undef DEFINE_GENERIC_REQUEST 
    #undef DECLARE_GENERIC_RESPONSE 
    #undef DECLARE_RESPONSE_WITH_BODY 
    #undef DEFINE_GENERIC_RESPONSE 
    #undef DEFINE_GENERIC_REQUEST_RESPONSE 
 
}; // TEvS3Wrapper 
 
IActor* CreateS3Wrapper(const Aws::Auth::AWSCredentials& credentials, const Aws::Client::ClientConfiguration& config); 
 
} // NWrappers 
} // NKikimr 
