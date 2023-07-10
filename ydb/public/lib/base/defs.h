#pragma once

/// @note It must not depend on anything except util, library and propobufs
#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>
#include <util/stream/output.h>

namespace NKikimr {

enum class EDataReqStatusExcerpt {
    Unknown, // must not happen
    Complete, // request success
    InProgress, // request success, but result is not ready yet
    RejectedForNow, // request rejected for some reason, guarantied to have no meaningful side effects
    LostInSpaceAndTime, // smth happened with request, we don't know what (i.e. timeout), side effects (successful execution inclusive) possible
    Error, // error with request
    InternalError, // smth weird, report to
};

inline const char* EDataReqStatusExcerptStr(EDataReqStatusExcerpt status) {
    switch (status) {
    case EDataReqStatusExcerpt::Complete:
        return "Complete";
    case EDataReqStatusExcerpt::InProgress:
        return "In progress";
    case EDataReqStatusExcerpt::RejectedForNow:
        return "Rejected";
    case EDataReqStatusExcerpt::LostInSpaceAndTime:
        return "Lost in space and time";
    case EDataReqStatusExcerpt::Error:
        return "Request error";
    case EDataReqStatusExcerpt::InternalError:
        return "Internal error";
    default:
        return "Unknown error";
    }
}

namespace NTxProxy {
#define TXUSERPROXY_RESULT_STATUS_MAP(XX) \
    XX(Unknown, 0) \
    XX(WrongRequest, 1) \
    XX(EmptyAffectedSet, 2) \
    XX(NotImplemented, 3) \
    XX(ResolveError, 4) \
    XX(AccessDenied, 5) \
    XX(DomainLocalityError, 6) \
    \
    XX(ProxyNotReady, 16) \
    XX(ProxyAccepted, 17) \
    XX(ProxyResolved, 18) \
    XX(ProxyPrepared, 19) \
    XX(ProxyShardNotAvailable, 20) \
    XX(ProxyShardTryLater, 21) \
    XX(ProxyShardOverloaded, 22) \
    XX(ProxyShardUnknown, 23) \
    \
    XX(CoordinatorDeclined, 32) \
    XX(CoordinatorOutdated, 33) \
    XX(CoordinatorAborted, 34) \
    XX(CoordinatorPlanned, 35) \
    XX(CoordinatorUnknown, 36) \
    \
    XX(ExecComplete, 48) \
    XX(ExecAlready, 49) \
    XX(ExecAborted, 50) \
    XX(ExecTimeout, 51) \
    XX(ExecError, 52) \
    XX(ExecInProgress, 53) \
    XX(ExecResponseData, 54) \
    XX(ExecResultUnavailable, 55) \
    XX(ExecCancelled, 56) \
    \
    XX(SynthBackendError, 64) \
    XX(SynthNoProxy, 65) \
    \
    XX(BackupTxIdNotExists, 80) \
    XX(TxIdIsNotABackup, 81) \

    struct TResultStatus {
        enum EStatus {
            TXUSERPROXY_RESULT_STATUS_MAP(ENUM_VALUE_GEN)
        };

        static void Out(IOutputStream& o, EStatus x) {
#define TEVPROPOSE_TRANSACTION_STATUS_TO_STRING_IMPL_ITEM(name, ...) \
    case EStatus::name: \
        o << #name; \
        return;
            switch (x) {
                TXUSERPROXY_RESULT_STATUS_MAP(TEVPROPOSE_TRANSACTION_STATUS_TO_STRING_IMPL_ITEM)
            default:
                o << static_cast<int>(x);
                return;
            }
#undef TEVPROPOSE_TRANSACTION_STATUS_TO_STRING_IMPL_ITEM
        }

        static const char* Str(EStatus x) {
#define TEVPROPOSE_TRANSACTION_STATUS_TO_STRING_IMPL_ITEM(name, ...) \
    case EStatus::name: \
        return #name;
            switch (x) {
                TXUSERPROXY_RESULT_STATUS_MAP(TEVPROPOSE_TRANSACTION_STATUS_TO_STRING_IMPL_ITEM)
            default:
                return "ProposeTransactionStatusOutOfRange";
            }
#undef TEVPROPOSE_TRANSACTION_STATUS_TO_STRING_IMPL_ITEM
        }

        static bool IsSoftErrorWithoutSideEffects(EStatus x) {
            switch (x) {
            case ProxyNotReady:
            case ProxyShardNotAvailable:
            case ProxyShardTryLater:
            case ProxyShardOverloaded:
            case CoordinatorDeclined:
            case CoordinatorOutdated:
            case CoordinatorAborted:
                return true;
            default:
                return false;
            }
        }
    };
}

}

template<>
inline void Out<NKikimr::NTxProxy::TResultStatus::EStatus>(IOutputStream& o, NKikimr::NTxProxy::TResultStatus::EStatus x) {
    return NKikimr::NTxProxy::TResultStatus::Out(o, x);
}
