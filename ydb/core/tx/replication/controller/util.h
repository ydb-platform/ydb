#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>

namespace NKikimr::NReplication::NController {

inline TString& TruncatedIssue(TString& issue) {
    static constexpr ui32 sizeLimit = 2_KB;
    static constexpr TStringBuf ellipsis = "...";

    if (issue.size() > sizeLimit) {
        issue.resize(sizeLimit - ellipsis.size());
        issue.append(ellipsis);
    }

    return issue;
}

inline auto DefaultRetryableErrors() {
    using EStatus = NYdb::EStatus;
    return TVector<EStatus>{
        EStatus::ABORTED,
        EStatus::UNAVAILABLE,
        EStatus::OVERLOADED,
        EStatus::GENERIC_ERROR,
        EStatus::TIMEOUT,
        EStatus::BAD_SESSION,
        EStatus::SESSION_EXPIRED,
        EStatus::CANCELLED,
        EStatus::UNDETERMINED,
        EStatus::SESSION_BUSY,
        EStatus::CLIENT_DISCOVERY_FAILED,
        EStatus::CLIENT_LIMITS_REACHED,
    };
}

inline bool IsRetryableError(const NYdb::TStatus status, const TVector<NYdb::EStatus>& retryable) {
    switch (status.GetStatus()) {
    case NYdb::EStatus::CLIENT_UNAUTHENTICATED:
    case NYdb::EStatus::CLIENT_CALL_UNIMPLEMENTED:
        return false;
    default:
        return status.IsTransportError() || Find(retryable, status.GetStatus()) != retryable.end();
    }
}

inline bool IsRetryableError(const NYdb::TStatus status) {
    static auto defaultRetryableErrors = DefaultRetryableErrors();
    return IsRetryableError(status, defaultRetryableErrors);
}

}
