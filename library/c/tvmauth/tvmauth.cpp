// DO_NOT_STYLE
#include "tvmauth.h"

#include "src/exception.h"
#include "src/utils.h"

#include <library/cpp/tvmauth/ticket_status.h>
#include <library/cpp/tvmauth/version.h>
#include <library/cpp/tvmauth/src/service_impl.h>
#include <library/cpp/tvmauth/src/user_impl.h>
#include <library/cpp/tvmauth/src/utils.h>

#include <util/string/cast.h>

using namespace NTvmAuth;
using namespace NTvmAuthC;

const char* TA_BlackboxTvmIdProd = "222";
const char* TA_BlackboxTvmIdTest = "224";
const char* TA_BlackboxTvmIdProdYateam = "223";
const char* TA_BlackboxTvmIdTestYateam = "225";
const char* TA_BlackboxTvmIdStress = "226";
const char* TA_BlackboxTvmIdMimino = "239";

const char* TA_ErrorCodeToString(enum TA_EErrorCode code) {
    switch (code) {
        case TA_EC_OK:
            return "libtvmauth.so: OK";
        case TA_EC_DEPRECATED:
            return "libtvmauth.so: Deprecated function";
        case TA_EC_EMPTY_TVM_KEYS:
            return "libtvmauth.so: Empty TVM keys";
        case TA_EC_EXPIRED_TICKET:
            return "libtvmauth.so: Expired ticket";
        case TA_EC_INVALID_BLACKBOX_ENV:
            return "libtvmauth.so: Invalid BlackBox environment";
        case TA_EC_INVALID_DST:
            return "libtvmauth.so: Invalid ticket destination";
        case TA_EC_INVALID_PARAM:
            return "libtvmauth.so: Invalid function parameter";
        case TA_EC_INVALID_TICKET_TYPE:
            return "libtvmauth.so: Invalid ticket type";
        case TA_EC_MALFORMED_TICKET:
            return "libtvmauth.so: Malformed ticket";
        case TA_EC_MALFORMED_TVM_KEYS:
            return "libtvmauth.so: Malformed TVM keys";
        case TA_EC_MALFORMED_TVM_SECRET:
            return "libtvmauth.so: Malformed TVM secret: it is empty or invalid base64url";
        case TA_EC_MISSING_KEY:
            return "libtvmauth.so: Context does not have required key to check ticket: public keys are too old";
        case TA_EC_NOT_ALLOWED:
            return "libtvmauth.so: Not allowed method";
        case TA_EC_SIGN_BROKEN:
            return "libtvmauth.so: Invalid ticket signature";
        case TA_EC_SMALL_BUFFER:
            return "libtvmauth.so: Small buffer";
        case TA_EC_UNEXPECTED_ERROR:
            return "libtvmauth.so: Unexpected error";
        case TA_EC_UNSUPPORTED_VERSION:
            return "libtvmauth.so: Unsupported ticket version";
        case TA_EC_BROKEN_TVM_CLIENT_SETTINGS:
            return "libtvmauth.so: TVM settings are broken";
        case TA_EC_PERMISSION_DENIED_TO_CACHE_DIR:
            return "libtvmauth.so: Permission denied to cache dir";
        case TA_EC_FAILED_TO_START_TVM_CLIENT:
            return "libtvmauth.so: TvmClient failed to start with some reason (need to check logs)";
    }

    return "libtvmauth.so: Unknown error";
}

TA_EErrorCode TA_DeleteServiceTicket(TA_TCheckedServiceTicket* ticket) {
    return CatchExceptions([=]() -> TA_EErrorCode {
        delete NTvmAuthC::NUtils::Translate(ticket);
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_GetServiceTicketSrc(
    const TA_TCheckedServiceTicket* ticket,
    uint32_t* srcTvmId) {
    if (ticket == nullptr || srcTvmId == nullptr)
        return TA_EC_INVALID_PARAM;

    return CatchExceptions([=]() -> TA_EErrorCode {
        (*srcTvmId) = reinterpret_cast<const TCheckedServiceTicket::TImpl*>(ticket)->GetSrc();
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_GetServiceTicketDebugInfo(
    const struct TA_TCheckedServiceTicket* ticket,
    char* debugInfo,
    size_t* debugInfoSize,
    size_t maxDebugInfoSize) {
    if (ticket == nullptr || debugInfoSize == nullptr)
        return TA_EC_INVALID_PARAM;

    return CatchExceptions([=]() -> TA_EErrorCode {
        const TString debugInfoStroka = reinterpret_cast<const TCheckedServiceTicket::TImpl*>(ticket)->DebugInfo();
        (*debugInfoSize) = debugInfoStroka.size();
        if (debugInfo == nullptr) {
            return TA_EC_INVALID_PARAM;
        }
        if (maxDebugInfoSize < *debugInfoSize) {
            return TA_EC_SMALL_BUFFER;
        }
        strcpy(debugInfo, debugInfoStroka.c_str());
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_GetServiceTicketIssuerUid(
    const TA_TCheckedServiceTicket* ticket,
        uint64_t* uid)
{
    if (ticket == nullptr || uid == nullptr)
        return TA_EC_INVALID_PARAM;

    return CatchExceptions([=]() -> TA_EErrorCode {
        TMaybe<TUid> u = reinterpret_cast<const TCheckedServiceTicket::TImpl*>(ticket)->GetIssuerUid();
        *uid = u ? *u : 0;
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_RemoveTicketSignature(
    const char* ticketBody,
    size_t ticketBodySize,
    const char** logableTicket,
    size_t* logableTicketSize) {
    if (ticketBody == nullptr || logableTicket == nullptr || logableTicketSize == nullptr)
        return TA_EC_INVALID_PARAM;

    return CatchExceptions([=]() -> TA_EErrorCode {
        TStringBuf tempLogableTicket = NTvmAuth::NUtils::RemoveTicketSignature(TStringBuf(ticketBody, ticketBodySize));
        (*logableTicket) = tempLogableTicket.data();
        (*logableTicketSize) = tempLogableTicket.size();
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_DeleteUserTicket(
    TA_TCheckedUserTicket* ticket) {
    if (ticket == nullptr)
        return TA_EC_INVALID_PARAM;

    return CatchExceptions([=]() -> TA_EErrorCode {
        delete NTvmAuthC::NUtils::Translate(ticket);
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_GetUserTicketUidsCount(
    const TA_TCheckedUserTicket* ticket,
    size_t* count) {
    if (ticket == nullptr || count == nullptr)
        return TA_EC_INVALID_PARAM;

    return CatchExceptions([=]() -> TA_EErrorCode {
        (*count) = reinterpret_cast<const TCheckedUserTicket::TImpl*>(ticket)->GetUids().size();
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_GetUserTicketUid(
    const TA_TCheckedUserTicket* ticket,
    size_t idx,
    uint64_t* uid) {
    if (ticket == nullptr || uid == nullptr)
        return TA_EC_INVALID_PARAM;

    return CatchExceptions([=]() -> TA_EErrorCode {
        (*uid) = reinterpret_cast<const TCheckedUserTicket::TImpl*>(ticket)->GetUids()[idx];
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_GetUserTicketDefaultUid(
    const TA_TCheckedUserTicket* ticket,
    uint64_t* uid) {
    if (ticket == nullptr || uid == nullptr)
        return TA_EC_INVALID_PARAM;

    return CatchExceptions([=]() -> TA_EErrorCode {
        (*uid) = reinterpret_cast<const TCheckedUserTicket::TImpl*>(ticket)->GetDefaultUid();
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_GetUserTicketScopesCount(
    const TA_TCheckedUserTicket* ticket,
    size_t* count) {
    if (ticket == nullptr || count == nullptr)
        return TA_EC_INVALID_PARAM;

    return CatchExceptions([=]() -> TA_EErrorCode {
        (*count) = reinterpret_cast<const TCheckedUserTicket::TImpl*>(ticket)->GetScopes().size();
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_GetUserTicketScope(
    const TA_TCheckedUserTicket* ticket,
    size_t idx,
    const char** scope) {
    if (ticket == nullptr || scope == nullptr)
        return TA_EC_INVALID_PARAM;

    return CatchExceptions([=]() -> TA_EErrorCode {
        (*scope) = reinterpret_cast<const TCheckedUserTicket::TImpl*>(ticket)->GetScopes().at(idx).data();
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_HasUserTicketScope(
    const struct TA_TCheckedUserTicket* ticket,
    const char* scope,
    size_t scopeSize,
    int* checkingResult) {
    if (ticket == nullptr || scope == nullptr)
        return TA_EC_INVALID_PARAM;
    return CatchExceptions([=]() -> TA_EErrorCode {
        (*checkingResult) = reinterpret_cast<const TCheckedUserTicket::TImpl*>(ticket)->HasScope(
            TStringBuf(scope, scopeSize)) ? 1 : 0;
        return TA_EC_OK;
    });
}

TA_EErrorCode TA_GetUserTicketDebugInfo(
    const struct TA_TCheckedUserTicket* ticket,
    char* debugInfo,
    size_t* debugInfoSize,
    size_t maxDebugInfoSize) {
    if (ticket == nullptr || debugInfoSize == nullptr)
        return TA_EC_INVALID_PARAM;

    return CatchExceptions([=]() -> TA_EErrorCode {
        const TString debugInfoStroka = reinterpret_cast<const TCheckedUserTicket::TImpl*>(ticket)->DebugInfo();
        (*debugInfoSize) = debugInfoStroka.size();
        if (debugInfo == nullptr) {
            return TA_EC_INVALID_PARAM;
        }
        if (maxDebugInfoSize < *debugInfoSize) {
            return TA_EC_SMALL_BUFFER;
        }
        strcpy(debugInfo, debugInfoStroka.c_str());
        return TA_EC_OK;
    });
}

const char* TA_LibVersion() {
    return LibVersion().data();
}

static_assert(0 == TA_EC_OK, "");
static_assert(1 == TA_EC_DEPRECATED, "");
static_assert(2 == TA_EC_EMPTY_TVM_KEYS, "");
static_assert(3 == TA_EC_EXPIRED_TICKET, "");
static_assert(4 == TA_EC_INVALID_BLACKBOX_ENV, "");
static_assert(5 == TA_EC_INVALID_DST, "");
static_assert(6 == TA_EC_INVALID_PARAM, "");
static_assert(7 == TA_EC_INVALID_TICKET_TYPE, "");
static_assert(8 == TA_EC_MALFORMED_TICKET, "");
static_assert(9 == TA_EC_MALFORMED_TVM_KEYS, "");
static_assert(10 == TA_EC_MALFORMED_TVM_SECRET, "");
static_assert(11 == TA_EC_MISSING_KEY, "");
static_assert(12 == TA_EC_NOT_ALLOWED, "");
static_assert(13 == TA_EC_SIGN_BROKEN, "");
static_assert(14 == TA_EC_SMALL_BUFFER, "");
static_assert(15 == TA_EC_UNEXPECTED_ERROR, "");
static_assert(16 == TA_EC_UNSUPPORTED_VERSION, "");
static_assert(17 == TA_EC_BROKEN_TVM_CLIENT_SETTINGS, "");
static_assert(18 == TA_EC_PERMISSION_DENIED_TO_CACHE_DIR, "");
static_assert(19 == TA_EC_FAILED_TO_START_TVM_CLIENT, "");

static_assert(0 == TA_BE_PROD, "");
static_assert(1 == TA_BE_TEST, "");
static_assert(2 == TA_BE_PROD_YATEAM, "");
static_assert(3 == TA_BE_TEST_YATEAM, "");
static_assert(4 == TA_BE_STRESS, "");
