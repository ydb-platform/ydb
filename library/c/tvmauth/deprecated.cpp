// DO_NOT_STYLE
#include "deprecated.h"

#include "src/exception.h"
#include "src/utils.h"

#include <library/cpp/tvmauth/src/service_impl.h>
#include <library/cpp/tvmauth/src/user_impl.h>
#include <library/cpp/tvmauth/src/utils.h>

using namespace NTvmAuth;
using namespace NTvmAuthC;

TA_EErrorCode TA_CreateServiceContext(
    uint32_t tvmId,
    const char* secretBase64,
    size_t secretBase64Size,
    const char* tvmKeysResponse,
    size_t tvmKeysResponseSize,
    TA_TServiceContext** context) {
    if ((tvmKeysResponse == nullptr && secretBase64 == nullptr) || context == nullptr)
        return TA_EC_INVALID_PARAM;

    return CatchExceptions([=]() -> TA_EErrorCode {
        if (secretBase64Size && tvmKeysResponse) {
            (*context) = reinterpret_cast<TA_TServiceContext*>(new TServiceContext::TImpl(
                TStringBuf(secretBase64, secretBase64Size),
                tvmId,
                TStringBuf(tvmKeysResponse, tvmKeysResponseSize)));
        } else if (secretBase64Size) {
            (*context) = reinterpret_cast<TA_TServiceContext*>(new TServiceContext::TImpl(
                TStringBuf(secretBase64, secretBase64Size)));
        } else {
            (*context) = reinterpret_cast<TA_TServiceContext*>(new TServiceContext::TImpl(
                tvmId,
                TStringBuf(tvmKeysResponse, tvmKeysResponseSize)));
        }

        return TA_EC_OK;
    });
}

TA_EErrorCode TA_DeleteServiceContext(
    TA_TServiceContext* context) {
    return CatchExceptions([=]() -> TA_EErrorCode {
        delete reinterpret_cast<TServiceContext::TImpl*>(context);
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_CheckServiceTicket(
    const TA_TServiceContext* context,
    const char* ticketBody,
    size_t ticketBodySize,
    TA_TCheckedServiceTicket** ticket) {
    if (context == nullptr || ticketBody == nullptr || ticket == nullptr)
        return TA_EC_INVALID_PARAM;

    return CatchExceptions([=]() -> TA_EErrorCode {
        (*ticket) = reinterpret_cast<TA_TCheckedServiceTicket*>(
            reinterpret_cast<const TServiceContext::TImpl*>(context)->Check(TStringBuf(ticketBody, ticketBodySize)).Release());
        return NTvmAuthC::NUtils::CppErrorCodeToC(NTvmAuthC::NUtils::Translate(*ticket)->GetStatus());
    });
}

TA_EErrorCode TA_SignCgiParamsForTvm(
    const TA_TServiceContext* context,
    const char* ts,
    size_t tsSize,
    const char* dst,
    size_t dstSize,
    const char* scopes,
    size_t scopesSize,
    char* sign,
    size_t* signSize,
    size_t maxSignatureSize) {
    if (context == nullptr || scopes == nullptr || sign == nullptr || signSize == nullptr)
        return TA_EC_INVALID_PARAM;

    return CatchExceptions([=]() -> TA_EErrorCode {
        const TString signedParams = reinterpret_cast<const TServiceContext::TImpl*>(context)->SignCgiParamsForTvm(
            TStringBuf(ts, tsSize),
            TStringBuf(dst, dstSize),
            TStringBuf(scopes, scopesSize));
        (*signSize) = signedParams.size();
        if (maxSignatureSize < *signSize) {
            return TA_EC_SMALL_BUFFER;
        }
        strcpy(sign, signedParams.c_str());
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_CreateUserContext(
    TA_EBlackboxEnv env,
    const char* tvmKeysResponse,
    size_t tvmKeysResponseSize,
    TA_TUserContext** context) {
    if (tvmKeysResponse == nullptr || context == nullptr)
        return TA_EC_INVALID_PARAM;

    return CatchExceptions([=]() -> TA_EErrorCode {
        (*context) = reinterpret_cast<TA_TUserContext*>(
            new TUserContext::TImpl(NTvmAuth::EBlackboxEnv(int(env)),
                                    TStringBuf(tvmKeysResponse, tvmKeysResponseSize)));
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_DeleteUserContext(
    TA_TUserContext* context) {
    return CatchExceptions([=]() -> TA_EErrorCode {
        delete reinterpret_cast<TUserContext::TImpl*>(context);
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_CheckUserTicket(
    const TA_TUserContext* context,
    const char* ticketBody,
    size_t ticketBodySize,
    TA_TCheckedUserTicket** ticket) {
    if (context == nullptr || ticket == nullptr)
        return TA_EC_INVALID_PARAM;

    return CatchExceptions([=]() -> TA_EErrorCode {
        (*ticket) = reinterpret_cast<TA_TCheckedUserTicket*>(
            reinterpret_cast<const TUserContext::TImpl*>(context)->Check(TStringBuf(ticketBody, ticketBodySize)).Release());
        return NTvmAuthC::NUtils::CppErrorCodeToC(NTvmAuthC::NUtils::Translate(*ticket)->GetStatus());
    });
}
