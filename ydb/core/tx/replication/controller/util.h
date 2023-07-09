#pragma once

#include "replication.h"

#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>

#include <ydb/library/yverify_stream/yverify_stream.h>

#include <util/generic/algorithm.h>
#include <util/generic/maybe.h>
#include <util/generic/size_literals.h>

namespace NKikimr::NReplication::NController {

inline TMaybe<TReplication::ETargetKind> TryTargetKindFromEntryType(NYdb::NScheme::ESchemeEntryType type) {
    switch (type) {
    case NYdb::NScheme::ESchemeEntryType::Table:
        return TReplication::ETargetKind::Table;
    case NYdb::NScheme::ESchemeEntryType::Unknown:
    case NYdb::NScheme::ESchemeEntryType::Directory:
    case NYdb::NScheme::ESchemeEntryType::PqGroup:
    case NYdb::NScheme::ESchemeEntryType::SubDomain:
    case NYdb::NScheme::ESchemeEntryType::RtmrVolume:
    case NYdb::NScheme::ESchemeEntryType::BlockStoreVolume:
    case NYdb::NScheme::ESchemeEntryType::CoordinationNode:
    case NYdb::NScheme::ESchemeEntryType::Sequence:
    case NYdb::NScheme::ESchemeEntryType::Replication:
    case NYdb::NScheme::ESchemeEntryType::ColumnTable:
    case NYdb::NScheme::ESchemeEntryType::ColumnStore:
    case NYdb::NScheme::ESchemeEntryType::Topic:
        return Nothing();
    }
}

inline TReplication::ETargetKind TargetKindFromEntryType(NYdb::NScheme::ESchemeEntryType type) {
    auto res = TryTargetKindFromEntryType(type);
    Y_VERIFY_S(res, "Unexpected entry type: " << static_cast<i32>(type));
    return *res;
}

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
    return status.IsTransportError() || Find(retryable, status.GetStatus()) != retryable.end();
}

inline bool IsRetryableError(const NYdb::TStatus status) {
    static auto defaultRetryableErrors = DefaultRetryableErrors();
    return IsRetryableError(status, defaultRetryableErrors);
}

}
