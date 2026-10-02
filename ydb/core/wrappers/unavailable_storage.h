#pragma once

#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/events/abstract.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/log.h>

#include <type_traits>

namespace NKikimr::NWrappers::NExternalStorage {

inline Aws::S3::S3Error MakeServiceUnavailableError(const TString& exceptionName, const TString& reason) {
    return Aws::S3::S3Error(Aws::Client::AWSError<Aws::Client::CoreErrors>(
        Aws::Client::CoreErrors::SERVICE_UNAVAILABLE, exceptionName, reason, true));
}

template <class TResponse, class TRequest>
std::unique_ptr<TResponse> MakeErrorResponse(const TRequest& request, const Aws::S3::S3Error& error) {
    constexpr bool hasKey = requires { request.GetRequest().GetKey(); };
    if constexpr (std::is_same_v<TResponse, TEvGetObjectResponse>) {
        std::pair<ui64, ui64> range;
        AFL_VERIFY(TResponse::TryParseRange(TString(request.GetRequest().GetRange()), range))(
            "original", request.GetRequest().GetRange());
        return std::make_unique<TResponse>(TString(request.GetRequest().GetKey()), range, error);
    } else if constexpr (hasKey) {
        return std::make_unique<TResponse>(TString(request.GetRequest().GetKey()), error);
    } else {
        return std::make_unique<TResponse>(error);
    }
}

inline std::unique_ptr<NActors::IEventBase> MakeErrorResponse(NActors::IEventHandle& ev, const Aws::S3::S3Error& error) {
    switch (ev.GetTypeRewrite()) {
#define MAKE_ERROR_RESPONSE(NAME) \
        case TEv##NAME##Request::EventType: \
            return MakeErrorResponse<TEv##NAME##Response>(*ev.Get<TEv##NAME##Request>(), error);
        Y_FOR_EACH_S3_WRAPPER_OP(MAKE_ERROR_RESPONSE)
#undef MAKE_ERROR_RESPONSE
        default:
            return nullptr;
    }
}

class TUnavailableExternalStorageOperator: public IExternalStorageOperator {
private:
    const TString Exception;
    const TString Reason;

    template <class TResponse, class TRequestPtr>
    void ExecuteImpl(TRequestPtr& ev) const {
        ReplyAdapter.Reply(ev->Sender, MakeErrorResponse<TResponse>(
            *ev->Get(), MakeServiceUnavailableError(Exception, Reason)));
    }

    virtual TString DoDebugString() const override {
        return "type:UNAVAILABLE;";
    }

public:
    TUnavailableExternalStorageOperator(const TString& exceptionName, const TString& unavailabilityReason)
        : Exception(exceptionName)
        , Reason(unavailabilityReason) {
    }

#define DECLARE_EXECUTE(NAME) \
    virtual void Execute(TEv##NAME##Request::TPtr& ev) const override { \
        ExecuteImpl<TEv##NAME##Response>(ev); \
    }
    Y_FOR_EACH_S3_WRAPPER_OP(DECLARE_EXECUTE)
#undef DECLARE_EXECUTE
};

}   // namespace NKikimr::NWrappers::NExternalStorage
